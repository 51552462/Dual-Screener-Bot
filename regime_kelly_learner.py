"""
[P2-2] 국면별 kelly_cap 자가조정(학습화) + kelly_floor 도입.

문제: `ACTION_BY_REGIME.kelly_cap` 이 하드코딩 매직상수 + 모든 행 `kelly_floor=None` →
위기×클러치×둠스데이 곱셈으로 켈리가 0 근처로 락아웃될 수 있음.

해결: 국면별 **실현 Sharpe**(forward_trades.entry_regime 그룹)로 cap 을 부드럽게 자가조정
(하드코딩 기본의 0.5~1.5× 클램프) + cap×frac 의 floor 도입.

⚠️ 라이브 사이징 핵심 레버이므로 **기본 OFF**(`META_KELLY_LEARN_ENABLED!=1`) → 켜기 전까지
기존 동작과 **완전 동일**(cap=하드코딩, floor=None). 켜면 섀도우 관측 후 점진 적용 권장.
전부 읽기 전용·방어적(데이터 부족/오류 시 미적용).
"""
from __future__ import annotations

import math
import os
import sqlite3
from typing import Any, Dict, Optional

try:
    from market_db_paths import MARKET_DATA_DB_PATH as _DB_PATH
except Exception:  # pragma: no cover
    _DB_PATH = None

try:
    from low_ram_sqlite_pragmas import apply_busy_timeout
except Exception:  # pragma: no cover
    def apply_busy_timeout(conn) -> None:  # type: ignore
        try:
            conn.execute("PRAGMA busy_timeout=60000;")
        except Exception:
            pass

try:
    from meta_state_store import normalize_regime_key as _norm_rk
except Exception:  # pragma: no cover
    def _norm_rk(x: Any) -> str:  # type: ignore
        return str(x or "UNKNOWN").strip().upper()

_VALID_REGIMES = ("BULL", "BEAR", "SIDEWAYS", "HIGH_VOL", "UNKNOWN")


def is_enabled() -> bool:
    return str(os.environ.get("META_KELLY_LEARN_ENABLED", "0")).strip().lower() in (
        "1", "true", "yes", "on",
    )


def _floor_frac() -> float:
    try:
        return min(0.9, max(0.0, float(os.environ.get("META_KELLY_FLOOR_FRAC", "0.2"))))
    except (TypeError, ValueError):
        return 0.2


def _cap_mult_bounds() -> tuple:
    try:
        lo = float(os.environ.get("META_KELLY_CAP_MULT_FLOOR", "0.5"))
    except (TypeError, ValueError):
        lo = 0.5
    try:
        hi = float(os.environ.get("META_KELLY_CAP_MULT_CAP", "1.5"))
    except (TypeError, ValueError):
        hi = 1.5
    return lo, hi


def _min_trades() -> int:
    try:
        return max(5, int(os.environ.get("META_KELLY_LEARN_MIN_TRADES", "20")))
    except (TypeError, ValueError):
        return 20


def _connect(forward_db_path: Optional[str]) -> Optional[sqlite3.Connection]:
    path = forward_db_path or _DB_PATH
    if not path:
        return None
    try:
        conn = sqlite3.connect(path, timeout=60)
    except Exception:
        return None
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    apply_busy_timeout(conn)
    return conn


def _sharpe_by_regime(conn: sqlite3.Connection, lookback_days: int = 180) -> Dict[str, Dict[str, float]]:
    """국면별 (per-trade Sharpe, n) — 최근 청산 트레이드 기준."""
    out: Dict[str, Dict[str, float]] = {}
    try:
        rows = conn.execute(
            "SELECT entry_regime, final_ret FROM forward_trades "
            "WHERE status LIKE 'CLOSED%' AND final_ret IS NOT NULL "
            "AND IFNULL(entry_regime,'') <> ''"
        ).fetchall()
    except Exception:
        return out
    buckets: Dict[str, list] = {}
    for rk_raw, fr in rows or []:
        rk = _norm_rk(rk_raw)
        if rk not in _VALID_REGIMES:
            continue
        try:
            buckets.setdefault(rk, []).append(float(fr) / 100.0)
        except (TypeError, ValueError):
            continue
    import statistics as _st
    for rk, vals in buckets.items():
        if len(vals) < 2:
            continue
        try:
            mean = _st.fmean(vals)
            sd = _st.pstdev(vals) if len(vals) < 2 else _st.stdev(vals)
        except Exception:
            continue
        sr = (mean / sd) if sd and sd > 1e-12 else 0.0
        out[rk] = {"sharpe": float(sr), "n": float(len(vals))}
    return out


def _mult_from_sharpe(sr: float) -> float:
    """per-trade Sharpe → cap 배수. sr=0→1.0, 양수→상향, 음수→하향(로지스틱, 경계 클램프)."""
    lo, hi = _cap_mult_bounds()
    mid = (lo + hi) / 2.0
    span = (hi - lo) / 2.0
    try:
        scale = float(os.environ.get("META_KELLY_SHARPE_SCALE", "0.15"))
    except (TypeError, ValueError):
        scale = 0.15
    if scale <= 1e-9:
        scale = 0.15
    # tanh 로 [-1,1] → [lo,hi]
    return float(mid + span * math.tanh(sr / scale))


def compute_regime_kelly_overlay(
    forward_db_path: Optional[str] = None,
    base_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, float]]:
    """국면별 학습 cap/floor 오버레이. 비활성/데이터부족 시 {} (미적용)."""
    if not is_enabled():
        return {}
    conn = _connect(forward_db_path)
    if conn is None:
        return {}
    try:
        stats = _sharpe_by_regime(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if not stats:
        return {}

    # base cap 참조(없으면 meta_governor 의 ACTION_BY_REGIME 사용)
    bmap = base_map
    if bmap is None:
        try:
            from meta_governor import ACTION_BY_REGIME as bmap  # type: ignore
        except Exception:
            bmap = {}

    min_n = _min_trades()
    frac = _floor_frac()
    overlay: Dict[str, Dict[str, float]] = {}
    for rk, st in stats.items():
        if st.get("n", 0) < min_n:
            continue
        base = bmap.get(rk) if isinstance(bmap, dict) else None
        try:
            base_cap = float((base or {}).get("kelly_cap")) if base and base.get("kelly_cap") else None
        except (TypeError, ValueError):
            base_cap = None
        if not base_cap or base_cap <= 0:
            continue
        mult = _mult_from_sharpe(float(st.get("sharpe", 0.0)))
        learned_cap = round(base_cap * mult, 6)
        learned_floor = round(learned_cap * frac, 6)
        overlay[rk] = {
            "kelly_cap": learned_cap,
            "kelly_floor": learned_floor,
            "_base_cap": base_cap,
            "_mult": round(mult, 4),
            "_sharpe": round(float(st.get("sharpe", 0.0)), 4),
            "_n": int(st.get("n", 0)),
        }
    return overlay


def overlay_action_for_regime(
    regime_key: str,
    action: Dict[str, Any],
    forward_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """단일 국면 action 의 kelly_cap/floor 를 학습값으로 갱신(제자리). 비활성 시 무변경.

    meta_governor._step_regime 가 META_REGIME_ACTION 생성 직후 호출(방어적). 반환=action.
    """
    if not isinstance(action, dict) or not is_enabled():
        return action
    try:
        rk = _norm_rk(regime_key)
        ov = compute_regime_kelly_overlay(forward_db_path)
        sel = ov.get(rk)
        if sel and sel.get("kelly_cap"):
            action["kelly_cap"] = float(sel["kelly_cap"])
            action["kelly_floor"] = float(sel["kelly_floor"])
            note = str(action.get("notes") or "")
            action["notes"] = (
                note
                + f" | [학습cap×{sel['_mult']:g} SR{sel['_sharpe']:+.2f} "
                f"n{sel['_n']} floor{sel['kelly_floor']:.4f}]"
            )
    except Exception:
        return action
    return action
