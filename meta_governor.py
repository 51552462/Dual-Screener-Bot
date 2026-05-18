"""
MetaGovernor — 동적 메타 상태 전용 (system_config.json 과 물리 분리).

스캐너/시그널 엔진은 이 모듈을 읽지 않는다. OMS·포워드·자동파일럿 등 소비자만
meta_governor_consumer 를 통해 상태를 로드한다.

환경변수·`system_config.json`:
- `META_GOVERNOR_SKIP_VIX` = true/1 → VIX(yfinance) 스킵, Regime의 VIX>p90 승격 비활성.
- `META_GOVERNOR_WINDOWS` = { "calibrator_lookback_days", "treasury_lookback_days", "graveyard_rolling_days", ... } 롤링 덮어쓰기 (기본 90일 분기).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


def _truthy_skip_vix(val: Any) -> bool:
    if val is True or val == 1:
        return True
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "on")
    return False


_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_META_STATE_PATH = os.path.join(_BASE_DIR, "meta_governor_state.json")

# 거시 국면 → 메타 행동 맵 (소비자가 kelly_cap / weight bounds 해석)
ACTION_BY_REGIME: Dict[str, Dict[str, Any]] = {
    "HIGH_VOL": {
        "kelly_cap": 0.012,
        "kelly_floor": None,
        "weight_s1_bounds": [0.45, 1.05],
        "weight_s4_bounds": [0.9, 1.55],
        "notes": "고변동: 켈리 상한 축소, S1 상한 억제·S4 방어 가중 허용",
    },
    "BEAR": {
        "kelly_cap": 0.01,
        "kelly_floor": None,
        "weight_s1_bounds": [0.35, 0.95],
        "weight_s4_bounds": [1.05, 1.75],
        "notes": "하락/늪지: 공격 축소·역축/방어 축 확대",
    },
    "BULL": {
        "kelly_cap": 0.028,
        "kelly_floor": None,
        "weight_s1_bounds": [1.0, 1.85],
        "weight_s4_bounds": [0.55, 1.15],
        "notes": "강세: 모멘텀(S1) 상한 완화, 과도한 S4 가중 억제",
    },
    "SIDEWAYS": {
        "kelly_cap": 0.018,
        "kelly_floor": None,
        "weight_s1_bounds": [0.65, 1.25],
        "weight_s4_bounds": [0.85, 1.45],
        "notes": "횡보·혼조: 중립 켈리·양 축 밸런스",
    },
    "UNKNOWN": {
        "kelly_cap": 0.015,
        "kelly_floor": None,
        "weight_s1_bounds": [0.55, 1.35],
        "weight_s4_bounds": [0.75, 1.35],
        "notes": "데이터 불충분: 보수적 기본 행동 맵",
    },
}


def _load_json_file(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except Exception as e:
        logger.warning("MetaGovernor: config read failed %s: %s", path, e)
        return {}


def _kst_calendar_cutoff_iso(days_back: int) -> str:
    """최근 N일 롤링: KST 기준 달력일로 컷오프 (exit_date 문자열 비교용)."""
    try:
        import pytz

        d = datetime.now(pytz.timezone("Asia/Seoul")).date()
    except Exception:
        d = datetime.now(timezone.utc).date()
    return (d - timedelta(days=max(1, int(days_back)))).isoformat()


def _quantiles(vals: List[float]) -> Dict[str, float]:
    arr = np.asarray(vals, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 5:
        return {}
    return {
        "p10": float(np.quantile(arr, 0.10)),
        "p25": float(np.quantile(arr, 0.25)),
        "p50": float(np.quantile(arr, 0.50)),
        "p75": float(np.quantile(arr, 0.75)),
        "p90": float(np.quantile(arr, 0.90)),
    }


def _fetch_forward_scores(db_path: str, cutoff_iso: str) -> List[float]:
    uri = f"file:{db_path.replace(chr(92), '/')}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=30)
    except Exception as e:
        logger.warning("MetaGovernor calibrator: open forward db failed: %s", e)
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT total_score FROM forward_trades
            WHERE status LIKE 'CLOSED%' AND IFNULL(exit_date,'') >= ?
              AND total_score IS NOT NULL
            """,
            (cutoff_iso,),
        )
        out = [float(r[0]) for r in cur.fetchall() if r and r[0] is not None]
        return out
    except sqlite3.Error as e:
        logger.warning("MetaGovernor calibrator: forward_trades query failed: %s", e)
        return []
    finally:
        conn.close()


def _fetch_bitget_scores(db_path: str, cutoff_iso: str) -> List[float]:
    if not db_path or not os.path.isfile(db_path):
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT total_score FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%' AND IFNULL(exit_date,'') >= ?
              AND total_score IS NOT NULL
            """,
            (cutoff_iso,),
        )
        return [float(r[0]) for r in cur.fetchall() if r and r[0] is not None]
    except sqlite3.Error as e:
        logger.warning("MetaGovernor calibrator: bitget_forward_trades query failed: %s", e)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _fetch_forward_breadths(db_path: str, cutoff_iso: str) -> List[float]:
    uri = f"file:{db_path.replace(chr(92), '/')}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=30)
    except Exception:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT entry_breadth FROM forward_trades
            WHERE status LIKE 'CLOSED%' AND IFNULL(exit_date,'') >= ?
              AND entry_breadth IS NOT NULL
            """,
            (cutoff_iso,),
        )
        return [float(r[0]) for r in cur.fetchall() if r and r[0] is not None]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def _btc_atr_pct_quantiles(db_path: str, window_bars: int) -> Dict[str, Any]:
    if not db_path or not os.path.isfile(db_path):
        return {}
    tbl = "BITGET_FUT_BTC_USDT_1D"
    conn = None
    try:
        import pandas as pd

        conn = sqlite3.connect(db_path, timeout=30)
        df = pd.read_sql(
            f'SELECT Date, High, Low, Close FROM "{tbl}" ORDER BY Date ASC',
            conn,
        )
    except Exception as e:
        logger.info("MetaGovernor calibrator: BTC bench table skip (%s)", e)
        return {}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    if df is None or len(df) < max(30, window_bars):
        return {}
    h = df["High"].astype(float)
    l = df["Low"].astype(float)
    c = df["Close"].astype(float)
    prev_c = c.shift(1)
    tr = np.maximum(h - l, np.maximum((h - prev_c).abs(), (l - prev_c).abs()))
    atr = tr.ewm(span=14, adjust=False).mean()
    atr_pct = (atr / c.replace(0, np.nan)) * 100.0
    tail = atr_pct.dropna().iloc[-max(20, int(window_bars)) :]
    if len(tail) < 10:
        return {}
    last = float(tail.iloc[-1])
    qs = _quantiles(tail.tolist())
    return {
        "bench": "BITGET_FUT_BTC_USDT_1D",
        "window_bars": int(len(tail)),
        "atr_pct_last": round(last, 4),
        "atr_pct_quantiles": qs,
    }


def _vix_snapshot_quantiles(system_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ev = os.environ.get("META_GOVERNOR_SKIP_VIX", "").strip().lower()
    if ev in ("1", "true", "yes", "on"):
        return {"skipped": True, "reason": "META_GOVERNOR_SKIP_VIX(env)"}
    if isinstance(system_cfg, dict) and _truthy_skip_vix(system_cfg.get("META_GOVERNOR_SKIP_VIX")):
        return {"skipped": True, "reason": "META_GOVERNOR_SKIP_VIX(system_config)"}
    try:
        import yfinance as yf
    except ImportError:
        return {}
    try:
        df = yf.Ticker("^VIX").history(period="3mo", auto_adjust=True)
        if df is None or df.empty or "Close" not in df.columns:
            return {}
        closes = df["Close"].astype(float).dropna().tolist()
        if len(closes) < 15:
            return {}
        tail = closes[-60:]
        last = float(tail[-1])
        qs = _quantiles(tail)
        return {"vix_last": round(last, 4), "vix_window_days": len(tail), "vix_quantiles": qs}
    except Exception as e:
        logger.info("MetaGovernor calibrator: VIX fetch skip (%s)", e)
        return {}


def _merge_regime_keys(regime_main: str, regime_bitget: str) -> str:
    """스트레스 우선: HIGH_VOL > BEAR > SIDEWAYS > BULL > UNKNOWN."""
    keys = {str(regime_main or "").upper(), str(regime_bitget or "").upper()}
    if "HIGH_VOL" in keys:
        return "HIGH_VOL"
    if "BEAR" in keys:
        return "BEAR"
    if "SIDEWAYS" in keys or "CHOP" in keys or "WHIPSAW" in keys:
        return "SIDEWAYS"
    if "BULL" in keys:
        return "BULL"
    if regime_main and regime_main.upper() != "UNKNOWN":
        return regime_main.upper()
    if regime_bitget and regime_bitget.upper() not in ("UNKNOWN", "CHOP", ""):
        return regime_bitget.upper()
    return "UNKNOWN"


def _map_bitget_regime(bg: str) -> str:
    u = str(bg or "").upper()
    if u in ("BULL",):
        return "BULL"
    if u in ("BEAR",):
        return "BEAR"
    if u in ("CHOP", "WHIPSAW"):
        return "SIDEWAYS"
    return u or "UNKNOWN"


def _resolve_regime_from_configs(cfg_main: Dict[str, Any], cfg_bitget: Dict[str, Any]) -> Tuple[str, float, str]:
    ra = cfg_main.get("REGIME_ANALYSIS") if isinstance(cfg_main.get("REGIME_ANALYSIS"), dict) else {}
    rk_main = str(ra.get("regime_key") or "").upper()
    indices = ra.get("indices") if isinstance(ra.get("indices"), dict) else {}
    gsp = indices.get("GSPC") if isinstance(indices.get("GSPC"), dict) else {}
    ksp = indices.get("KOSPI") if isinstance(indices.get("KOSPI"), dict) else {}
    ok_ct = int(bool(gsp.get("ok"))) + int(bool(ksp.get("ok")))

    rk_bg = _map_bitget_regime(str(cfg_bitget.get("CURRENT_REGIME_KEY") or ""))
    merged = _merge_regime_keys(rk_main if rk_main else "UNKNOWN", rk_bg if rk_bg else "UNKNOWN")

    note_parts = []
    if rk_main:
        note_parts.append(f"REGIME_ANALYSIS={rk_main}")
    if rk_bg:
        note_parts.append(f"BITGET_CURRENT={rk_bg}")
    note = " / ".join(note_parts) if note_parts else "출처 불명 — UNKNOWN 기본"

    if merged == "HIGH_VOL":
        conf = 0.88 if ok_ct >= 1 else 0.55
    elif ok_ct >= 2:
        conf = 0.82
    elif ok_ct == 1:
        conf = 0.62
    else:
        conf = 0.42 if (rk_main or rk_bg) else 0.25
    return merged, float(conf), note


def _ledger_group_key(sig: str) -> str:
    raw = str(sig or "")
    if "[INCUBATOR_" in raw.upper():
        m = re.search(r"\[INCUBATOR_([^\]]+)\]", raw, flags=re.I)
        if m:
            return f"INCUBATOR_{m.group(1).strip()}"
    s = raw.replace("💀[기각/관찰용] ", "").replace("💀[기각] ", "")
    s = re.sub(r"^\[.*?\]\s*", "", s)
    return (s.split(" [")[0].strip() or "UNKNOWN")


def _mdd_pct_from_returns(rets: List[float]) -> float:
    if len(rets) < 3:
        return 0.0
    eq = 1.0
    peak = 1.0
    worst_dd = 0.0
    for r in rets:
        eq *= 1.0 + float(r) / 100.0
        peak = max(peak, eq)
        if peak > 0:
            dd = (eq - peak) / peak * 100.0
            worst_dd = min(worst_dd, dd)
    return float(worst_dd)


def _tail_loss_streak_returns(rets: List[float]) -> int:
    k = 0
    for r in reversed(rets):
        if float(r) <= 0.0:
            k += 1
        else:
            break
    return int(k)


def _fetch_kr_ledger_rows(db_path: str, cutoff_iso: str) -> List[Tuple[str, str, float, str]]:
    uri = f"file:{db_path.replace(chr(92), '/')}?mode=ro"
    out: List[Tuple[str, str, float, str]] = []
    conn = None
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT IFNULL(market,''), sig_type, final_ret, IFNULL(exit_date,'')
            FROM forward_trades
            WHERE status LIKE 'CLOSED%' AND IFNULL(exit_date,'') >= ?
              AND final_ret IS NOT NULL
            ORDER BY exit_date ASC
            """,
            (cutoff_iso,),
        )
        for mkt, sig, ret, exd in cur.fetchall():
            mp = str(mkt or "KR").upper().strip()
            if mp not in ("KR", "US"):
                mp = "KR"
            gk = _ledger_group_key(str(sig or ""))
            try:
                rv = float(ret)
            except (TypeError, ValueError):
                continue
            out.append((mp, gk, rv, str(exd or "")))
    except sqlite3.Error as e:
        logger.warning("MetaGovernor treasury: KR ledger query failed: %s", e)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return out


def _fetch_bitget_ledger_rows(db_path: str, cutoff_iso: str) -> List[Tuple[str, str, float, str]]:
    out: List[Tuple[str, str, float, str]] = []
    if not db_path or not os.path.isfile(db_path):
        return out
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sig_type, final_ret, IFNULL(exit_date,'')
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%' AND IFNULL(exit_date,'') >= ?
              AND final_ret IS NOT NULL
            ORDER BY exit_date ASC
            """,
            (cutoff_iso,),
        )
        for sig, ret, exd in cur.fetchall():
            gk = _ledger_group_key(str(sig or ""))
            try:
                rv = float(ret)
            except (TypeError, ValueError):
                continue
            out.append(("BG", gk, rv, str(exd or "")))
    except sqlite3.Error as e:
        logger.warning("MetaGovernor treasury: Bitget ledger query failed: %s", e)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return out


def _build_treasury_health_and_mult(
    rows: List[Tuple[str, str, float, str]],
    *,
    min_trades: int,
    max_streak: int,
    wr_hard: float,
    wr_soft: float,
    mdd_floor_pct: float,
) -> Tuple[Dict[str, Any], Dict[str, float]]:
    from collections import defaultdict

    by: Dict[str, List[float]] = defaultdict(list)
    for mp, gk, ret, _ex in rows:
        if not gk:
            continue
        key = f"{mp}|{gk}"
        by[key].append(float(ret))

    health: Dict[str, Any] = {}
    for key, rets in by.items():
        n = len(rets)
        wins = sum(1 for x in rets if x > 0)
        wr = wins / n if n else 0.0
        streak = _tail_loss_streak_returns(rets)
        mdd = _mdd_pct_from_returns(rets)
        mult_val = 1.0
        reason = "ok"
        if n < min_trades:
            mult_val = 1.0
            reason = "insufficient_sample"
        elif streak >= max_streak or wr < wr_hard or mdd <= mdd_floor_pct:
            mult_val = 0.0
            reason = "hard_cut"
        elif streak >= 3 or wr < wr_soft:
            mult_val = 0.35
            reason = "soft_cut"
        health[key] = {
            "n": n,
            "rolling_wr": round(wr, 4),
            "tail_loss_streak": streak,
            "mdd_pct": round(mdd, 2),
            "mult": mult_val,
            "reason": reason,
        }
    mult_out: Dict[str, float] = {}
    for key, hv in health.items():
        if not isinstance(hv, dict):
            continue
        mv = float(hv.get("mult", 1.0) or 1.0)
        if mv >= 1.0 - 1e-9:
            continue
        _, _, gk_only = key.rpartition("|")
        if gk_only:
            mult_out[gk_only] = min(mult_out.get(gk_only, mv), mv)
    return health, mult_out


def _health_worst_mult(health: Dict[str, Any], group_key: str) -> float:
    worst = 1.0
    gk = str(group_key or "").strip()
    if not gk:
        return worst
    for hk, hv in health.items():
        if not isinstance(hv, dict):
            continue
        if hk == gk or hk.endswith("|" + gk):
            try:
                worst = min(worst, float(hv.get("mult", 1.0) or 1.0))
            except (TypeError, ValueError):
                continue
    return float(worst)


def _stable_mutant_id(name: str) -> str:
    h = hashlib.sha256(str(name).encode("utf-8")).hexdigest()[:14]
    return f"mutant:{h}"


def _load_validated_promoted(path: Optional[str]) -> List[Dict[str, Any]]:
    p = path or os.path.join(_BASE_DIR, "validated_live_mutants.json")
    if not os.path.isfile(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        prom = raw.get("promoted") if isinstance(raw, dict) else None
        if not isinstance(prom, list):
            return []
        return [x for x in prom if isinstance(x, dict)]
    except Exception as e:
        logger.warning("MetaGovernor lifecycle: read validated json failed %s: %s", p, e)
        return []


def default_meta_state() -> Dict[str, Any]:
    """스키마 v1.0.0 최소 골격 (Governor 첫 기동 시 시드)."""
    return {
        "META_SCHEMA_VERSION": "1.0.0",
        "META_GOVERNOR_LAST_RUN_AT": None,
        "META_GOVERNOR_LAST_RUN_STATUS": "NEVER",
        "META_GOVERNOR_INPUT_HASH": None,
        "META_VOL_ATR_PCT_Q": {},
        "META_VIX_LEVEL_Q": {},
        "META_BREADTH_THRESHOLDS": {},
        "META_TIER_CUTS": {},
        "META_SCORE_DIST_SNAPSHOT": {},
        "META_GLOBAL_KELLY_MULT": 1.0,
        "META_NS_KELLY_MULT": {},
        "META_GROUP_KELLY_MULT": {},
        "META_MAX_POSITION_PCT": None,
        "META_STRATEGY_HEALTH": {},
        "META_TREASURY_MODE": "NORMAL",
        "META_REGIME_KEY": "UNKNOWN",
        "META_REGIME_CONFIDENCE": 0.0,
        "META_REGIME_ACTION": {
            "kelly_cap": None,
            "kelly_floor": None,
            "weight_s1_bounds": None,
            "weight_s4_bounds": None,
            "allow_trade_sources": [],
            "block_trade_sources": [],
            "notes": "",
        },
        "META_STRATEGY_REGISTRY": [],
        "META_LIVE_STRATEGY_IDS": [],
        "META_RETIRED_STRATEGY_IDS": [],
        "META_CHANGELOG": [],
        "META_OPERATOR_FLAGS": {},
        "META_SATELLITE_INTEL": {},
    }


def meta_state_path() -> str:
    return os.environ.get("META_GOVERNOR_STATE_PATH", DEFAULT_META_STATE_PATH)


def load_meta_governor_state(path: Optional[str] = None) -> Dict[str, Any]:
    """SQLite SSOT 우선, 없으면 JSON (meta_state_store)."""
    try:
        from meta_state_store import load_meta_governor_state_unified

        return load_meta_governor_state_unified(path)
    except Exception as e:
        logger.warning("meta_state_store load fallback to JSON only: %s", e)
    p = path or meta_state_path()
    if not os.path.isfile(p):
        return default_meta_state()
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            logger.warning("meta_governor_state: root is not dict, resetting to default")
            return default_meta_state()
        out = default_meta_state()
        out.update(raw)
        return out
    except Exception as e:
        logger.exception("meta_governor_state: load failed (%s): %s", p, e)
        return default_meta_state()


def save_meta_governor_state_atomic(state: Dict[str, Any], path: Optional[str] = None) -> None:
    """SQLite config_kv + JSON 미러 원자 저장."""
    try:
        from meta_state_store import save_meta_governor_state_unified

        save_meta_governor_state_unified(state, path)
        return
    except Exception as e:
        logger.warning("meta_state_store save fallback to JSON only: %s", e)
    p = path or meta_state_path()
    os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2)
    d = os.path.dirname(os.path.abspath(p)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".meta_governor_", suffix=".json.tmp", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
    except Exception:
        try:
            if os.path.isfile(tmp):
                os.remove(tmp)
        except OSError:
            pass
        raise


def _load_system_config_for_governor(ctx: "GovernorRunContext") -> Dict[str, Any]:
    """SQLite system_config.sqlite 우선, 레거시 JSON 경로는 보조 병합."""
    merged: Dict[str, Any] = {}
    try:
        from config_manager import load_system_config

        merged = dict(load_system_config() or {})
    except Exception as e:
        logger.warning("MetaGovernor: load_system_config failed: %s", e)
    if ctx.system_config_path:
        fc = _load_json_file(ctx.system_config_path)
        if fc:
            for k, v in fc.items():
                if k not in merged or merged.get(k) in (None, "", {}):
                    merged[k] = v
    return merged


def _load_bitget_config_for_governor(ctx: "GovernorRunContext") -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    try:
        from config_manager import get_config_value

        for key in ("CURRENT_REGIME_KEY", "REGIME_ANALYSIS"):
            v = get_config_value(key)
            if v is not None:
                merged[key] = v
    except Exception:
        pass
    if ctx.bitget_system_config_path:
        fc = _load_json_file(ctx.bitget_system_config_path)
        if fc:
            merged = {**fc, **merged} if merged else fc
    return merged


@dataclass
class GovernorRunContext:
    """I/O 스키마: 입력 핸들 (경로만 넘기고 Governor 가 직접 읽는 형태)."""

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    as_of: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    forward_db_path: Optional[str] = None
    system_config_path: Optional[str] = None
    bitget_db_path: Optional[str] = None
    bitget_system_config_path: Optional[str] = None
    validated_mutants_path: Optional[str] = None
    windows: Dict[str, Any] = field(
        default_factory=lambda: {
            "health_rolling_n": 30,
            "dist_lookback_days": 90,
            "calibrator_lookback_days": 90,
            "atr_quantile_window_bars": 60,
            "treasury_lookback_days": 90,
            "treasury_min_trades": 10,
            "treasury_max_consecutive_losses": 5,
            "treasury_wr_hard": 0.34,
            "treasury_wr_soft": 0.42,
            "treasury_mdd_pct_floor": -30.0,
        }
    )
    snapshots: Dict[str, Any] = field(default_factory=dict)


class MetaGovernor:
    """
    단일 컨트롤 타워: run_governor_cycle() = 7단계 파이프라인.
    Calibrator / Regime 단계는 실제 DB·설정 입력을 사용한다.
    """

    def __init__(self, state_path: Optional[str] = None):
        self.state_path = state_path or meta_state_path()
        self._working: Dict[str, Any] = {}
        self._ctx: Optional[GovernorRunContext] = None
        self._prior: Dict[str, Any] = {}
        self._system_cfg_snapshot: Dict[str, Any] = {}

    def _apply_system_config_window_overrides(self) -> None:
        """주식·Bitget `system_config` 병합 후 `META_GOVERNOR_WINDOWS` 로 롤링·트레저리 임계 덮어쓰기."""
        ctx = self._ctx
        if ctx is None:
            return
        merged: Dict[str, Any] = {}
        for path in (ctx.system_config_path, ctx.bitget_system_config_path):
            if path and os.path.isfile(path):
                part = _load_json_file(path)
                if isinstance(part, dict):
                    merged.update(part)
        self._system_cfg_snapshot = merged
        if not merged:
            return
        mw = merged.get("META_GOVERNOR_WINDOWS")
        if not isinstance(mw, dict):
            return
        win = dict(ctx.windows or {})
        int_keys = {
            "health_rolling_n",
            "dist_lookback_days",
            "calibrator_lookback_days",
            "atr_quantile_window_bars",
            "treasury_lookback_days",
            "treasury_min_trades",
            "treasury_max_consecutive_losses",
        }
        float_keys = {"treasury_wr_hard", "treasury_wr_soft", "treasury_mdd_pct_floor"}
        for k, v in mw.items():
            if k in int_keys and v is not None:
                try:
                    win[k] = int(v)
                except (TypeError, ValueError):
                    pass
            elif k in float_keys and v is not None:
                try:
                    win[k] = float(v)
                except (TypeError, ValueError):
                    pass
        ctx.windows = win

    def run_governor_cycle(self, ctx: Optional[GovernorRunContext] = None) -> Dict[str, Any]:
        self._ctx = ctx or GovernorRunContext()
        self._apply_system_config_window_overrides()
        self._prior = load_meta_governor_state(self.state_path)
        self._working = default_meta_state()
        # 이전 스냅샷에서 운영자 플래그 등 유지해야 할 키만 선별 복사 (더미 단계)
        op = self._prior.get("META_OPERATOR_FLAGS")
        if isinstance(op, dict):
            self._working["META_OPERATOR_FLAGS"] = dict(op)

        try:
            self._step_collect_validate()
            self._step_calibrator()
            self._step_treasury()
            self._step_regime()
            self._step_lifecycle()
            self._step_changelog()
            self._finalize_meta_headers("OK")
            save_meta_governor_state_atomic(self._working, self.state_path)
        except Exception as e:
            logger.exception("MetaGovernor cycle failed: %s", e)
            # 실패 시에는 기존 meta_governor_state.json 을 덮어쓰지 않음 (마지막 정상 스냅샷 유지).
            raise
        return self._working

    # --- 1 수집·검증 ---
    def _step_collect_validate(self) -> None:
        ctx = self._ctx
        assert ctx is not None
        parts: list[str] = []
        parts.append(ctx.as_of)
        if ctx.forward_db_path and os.path.isfile(ctx.forward_db_path):
            parts.append(str(os.path.getmtime(ctx.forward_db_path)))
        else:
            parts.append("no_forward_db")
        if ctx.system_config_path and os.path.isfile(ctx.system_config_path):
            parts.append(str(os.path.getmtime(ctx.system_config_path)))
        else:
            parts.append("no_system_config")
        if ctx.bitget_db_path and os.path.isfile(ctx.bitget_db_path):
            parts.append(str(os.path.getmtime(ctx.bitget_db_path)))
        else:
            parts.append("no_bitget_db")
        if ctx.bitget_system_config_path and os.path.isfile(ctx.bitget_system_config_path):
            parts.append(str(os.path.getmtime(ctx.bitget_system_config_path)))
        else:
            parts.append("no_bitget_cfg")
        h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
        self._working["META_GOVERNOR_INPUT_HASH"] = f"sha256:{h}"
        self._working["_collect_debug"] = {
            "forward_db": ctx.forward_db_path,
            "system_config": ctx.system_config_path,
            "bitget_db": ctx.bitget_db_path,
            "bitget_system_config": ctx.bitget_system_config_path,
            "windows": dict(ctx.windows or {}),
        }

    # --- 2 Calibrator ---
    def _step_calibrator(self) -> None:
        ctx = self._ctx
        assert ctx is not None
        win = dict(ctx.windows or {})
        cal_days = int(win.get("calibrator_lookback_days", 90))
        atr_w = int(win.get("atr_quantile_window_bars", 60))
        cutoff = _kst_calendar_cutoff_iso(cal_days)

        dist_block: Dict[str, Any] = {"window_days_kst": cal_days, "cutoff_exit_date_gte": cutoff}

        # KR/US forward_trades: 롤링 청산 구간 total_score 분포
        scores_kr: List[float] = []
        if ctx.forward_db_path:
            scores_kr = _fetch_forward_scores(ctx.forward_db_path, cutoff)
        snap_kr: Dict[str, Any] = {"n": len(scores_kr)}
        if len(scores_kr) >= 5:
            arr = np.asarray(scores_kr, dtype=float)
            snap_kr["mean"] = float(np.mean(arr))
            snap_kr["std"] = float(np.std(arr))
            snap_kr["quantiles"] = _quantiles(scores_kr)
        dist_block["KR_forward_trades"] = snap_kr

        scores_bg: List[float] = []
        if ctx.bitget_db_path:
            scores_bg = _fetch_bitget_scores(ctx.bitget_db_path, cutoff)
        snap_bg: Dict[str, Any] = {"n": len(scores_bg)}
        if len(scores_bg) >= 5:
            arr_b = np.asarray(scores_bg, dtype=float)
            snap_bg["mean"] = float(np.mean(arr_b))
            snap_bg["std"] = float(np.std(arr_b))
            snap_bg["quantiles"] = _quantiles(scores_bg)
        dist_block["bitget_forward_trades"] = snap_bg

        self._working["META_SCORE_DIST_SNAPSHOT"] = dist_block

        tier: Dict[str, Any] = {}
        qkr = snap_kr.get("quantiles") if isinstance(snap_kr.get("quantiles"), dict) else {}
        if qkr:
            tier["KR_total_score"] = {
                "report_mid_p50": qkr.get("p50"),
                "report_high_p75": qkr.get("p75"),
                "n": snap_kr.get("n"),
            }
        qbg = snap_bg.get("quantiles") if isinstance(snap_bg.get("quantiles"), dict) else {}
        if qbg:
            tier["bitget_total_score"] = {
                "report_mid_p50": qbg.get("p50"),
                "report_high_p75": qbg.get("p75"),
                "n": snap_bg.get("n"),
            }
        self._working["META_TIER_CUTS"] = tier

        breadth_vals: List[float] = []
        if ctx.forward_db_path:
            breadth_vals = _fetch_forward_breadths(ctx.forward_db_path, cutoff)
        br_out: Dict[str, Any] = {}
        if len(breadth_vals) >= 5:
            bqs = _quantiles(breadth_vals)
            br_out["forward_entry_breadth"] = {
                "n": len(breadth_vals),
                "quantiles": bqs,
                "recommended_low": bqs.get("p25"),
                "recommended_high": bqs.get("p75"),
            }
        self._working["META_BREADTH_THRESHOLDS"] = br_out

        self._working["META_VOL_ATR_PCT_Q"] = (
            _btc_atr_pct_quantiles(ctx.bitget_db_path or "", atr_w) if ctx.bitget_db_path else {}
        )
        sys_for_vix = self._system_cfg_snapshot if isinstance(self._system_cfg_snapshot, dict) else {}
        self._working["META_VIX_LEVEL_Q"] = _vix_snapshot_quantiles(sys_for_vix if sys_for_vix else None)
        self._ingest_satellite_intel_snapshot()

    def _ingest_satellite_intel_snapshot(self) -> None:
        """텔레그램 위성망(스마트머니·오답노트·센티) 스냅샷 → META_SATELLITE_INTEL."""
        ctx = self._ctx
        cfg_path = ctx.system_config_path if ctx else None
        cfg = _load_json_file(cfg_path)
        if not cfg:
            self._working["META_SATELLITE_INTEL"] = {}
            return
        try:
            from toxic_antipattern_core import collect_merged_antipattern_rules

            n_rules = len(collect_merged_antipattern_rules(cfg))
        except Exception:
            n_rules = 0
        sm = cfg.get("SMART_MONEY_RADAR") or {}
        picks = sm.get("picks") if isinstance(sm, dict) else {}
        if not isinstance(picks, dict):
            picks = {}
        codes = list(picks.keys())[:120]
        sent: Dict[str, Any] = {}
        from news_data_paths import news_db_path

        news_p = news_db_path()
        try:
            if os.path.isfile(news_p):
                uri = f"file:{news_p.replace(chr(92), '/')}?mode=ro"
                con = sqlite3.connect(uri, uri=True, timeout=15)
                try:
                    row = con.execute(
                        "SELECT date, top_keyword_1, top_keyword_2, top_keyword_3, sentiment_score "
                        "FROM daily_sentiment ORDER BY date DESC LIMIT 1"
                    ).fetchone()
                    if row:
                        sent = {
                            "date": row[0],
                            "top_keyword_1": row[1],
                            "top_keyword_2": row[2],
                            "top_keyword_3": row[3],
                            "sentiment_score": row[4],
                        }
                finally:
                    con.close()
        except Exception:
            pass
        self._working["META_SATELLITE_INTEL"] = {
            "smart_money_codes": codes,
            "smart_money_n": len(picks),
            "antipattern_rule_count": n_rules,
            "sentiment": sent,
            "system_config_path": cfg_path,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    # --- 3 Treasury ---
    def _step_treasury(self) -> None:
        ctx = self._ctx
        assert ctx is not None
        win = dict(ctx.windows or {})
        t_days = int(win.get("treasury_lookback_days", win.get("calibrator_lookback_days", 90)))
        min_trades = int(win.get("treasury_min_trades", 10))
        max_streak = int(win.get("treasury_max_consecutive_losses", 5))
        wr_hard = float(win.get("treasury_wr_hard", 0.34))
        wr_soft = float(win.get("treasury_wr_soft", 0.42))
        mdd_floor = float(win.get("treasury_mdd_pct_floor", -30.0))
        cutoff = _kst_calendar_cutoff_iso(t_days)

        rows: List[Tuple[str, str, float, str]] = []
        if ctx.forward_db_path:
            rows.extend(_fetch_kr_ledger_rows(ctx.forward_db_path, cutoff))
        if ctx.bitget_db_path:
            rows.extend(_fetch_bitget_ledger_rows(ctx.bitget_db_path, cutoff))

        health, _mult_unused = _build_treasury_health_and_mult(
            rows,
            min_trades=min_trades,
            max_streak=max_streak,
            wr_hard=wr_hard,
            wr_soft=wr_soft,
            mdd_floor_pct=mdd_floor,
        )
        meta_h = dict(health)
        meta_h["__meta__"] = {
            "window_days_kst": t_days,
            "cutoff_exit_date_gte": cutoff,
            "n_rows": len(rows),
        }
        self._working["META_STRATEGY_HEALTH"] = meta_h
        # 그룹별 승수: 이번 헬스 스냅샷 전체 반영 (회복 시 mult=1.0 으로 자연 복귀, 누락 그룹은 소비자 측 1.0 취급)
        full_gk_mult: Dict[str, float] = {}
        for key, hv in health.items():
            if key == "__meta__" or not isinstance(hv, dict):
                continue
            _, _, gk_only = str(key).rpartition("|")
            if not gk_only:
                gk_only = str(key)
            try:
                m = float(hv.get("mult", 1.0) or 1.0)
            except (TypeError, ValueError):
                m = 1.0
            full_gk_mult[gk_only] = min(full_gk_mult.get(gk_only, 1.0), m)
        self._working["META_GROUP_KELLY_MULT"] = full_gk_mult

        prior_g = float(self._prior.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
        actionable = [
            v for v in health.values() if isinstance(v, dict) and int(v.get("n", 0) or 0) >= min_trades
        ]
        zeroed = sum(1 for v in actionable if float(v.get("mult", 1.0) or 1.0) <= 0.0)
        if actionable and (zeroed / len(actionable)) >= 0.45:
            self._working["META_GLOBAL_KELLY_MULT"] = round(max(0.5, prior_g * 0.88), 4)
        else:
            self._working["META_GLOBAL_KELLY_MULT"] = prior_g
        self._working["META_TREASURY_MODE"] = "DEFENSE" if zeroed > 0 else "NORMAL"

    # --- 4 Regime ---
    def _step_regime(self) -> None:
        ctx = self._ctx
        assert ctx is not None
        cfg_m = _load_system_config_for_governor(ctx)
        cfg_b = _load_bitget_config_for_governor(ctx)
        rk, conf, note = _resolve_regime_from_configs(cfg_m, cfg_b)

        vix_block = self._working.get("META_VIX_LEVEL_Q") or {}
        if isinstance(vix_block, dict) and vix_block.get("skipped"):
            note = (note or "") + " | VIX 스킵(오프라인/설정) — VIX> p90 HIGH_VOL 규칙 미적용"
        if isinstance(vix_block, dict) and not vix_block.get("skipped"):
            vlast = vix_block.get("vix_last")
            vqs = vix_block.get("vix_quantiles") or {}
            p90 = vqs.get("p90")
            try:
                if (
                    vlast is not None
                    and p90 is not None
                    and float(vlast) > float(p90)
                ):
                    rk = "HIGH_VOL"
                    conf = max(float(conf), 0.86)
                    note = (note or "") + " | VIX>롤링p90 → HIGH_VOL 승격"
            except (TypeError, ValueError):
                pass

        self._working["META_REGIME_KEY"] = rk
        self._working["META_REGIME_CONFIDENCE"] = float(conf)
        action_template = dict(ACTION_BY_REGIME.get(rk, ACTION_BY_REGIME["UNKNOWN"]))
        base_ra = {**default_meta_state()["META_REGIME_ACTION"], **action_template}
        base_ra["notes"] = (note or "").strip()
        self._working["META_REGIME_ACTION"] = base_ra

    # --- 5 Lifecycle ---
    def _step_lifecycle(self) -> None:
        ctx = self._ctx
        assert ctx is not None
        health = self._working.get("META_STRATEGY_HEALTH")
        if not isinstance(health, dict):
            health = {}

        prior_reg = self._prior.get("META_STRATEGY_REGISTRY")
        reg: List[Dict[str, Any]] = []
        if isinstance(prior_reg, list):
            for item in prior_reg:
                if isinstance(item, dict):
                    reg.append(dict(item))

        by_sid: Dict[str, Dict[str, Any]] = {
            str(r.get("strategy_id")): r for r in reg if r.get("strategy_id")
        }
        now_iso = datetime.now(timezone.utc).isoformat()

        for prom in _load_validated_promoted(ctx.validated_mutants_path):
            name = str(prom.get("name") or "").strip()
            if not name:
                continue
            sid = _stable_mutant_id(name)
            if sid in by_sid:
                continue
            gk = f"INCUBATOR_{name}"
            row: Dict[str, Any] = {
                "strategy_id": sid,
                "display_name": name,
                "state": "CANDIDATE",
                "group_key": gk,
                "capital_mult": 0.0,
                "source": "validated_live_mutants",
                "updated_at": now_iso,
                "oos_win_rate": prom.get("oos_win_rate"),
                "oos_avg_return": prom.get("oos_avg_return"),
            }
            reg.append(row)
            by_sid[sid] = row

        win = dict(ctx.windows or {})
        min_tr = int(win.get("treasury_min_trades", 10))

        live_before = {str(r.get("strategy_id")) for r in reg if str(r.get("state") or "").upper() == "LIVE"}

        for row in reg:
            if str(row.get("state") or "").upper() != "LIVE":
                continue
            gk = str(row.get("group_key") or row.get("display_name") or "").strip()
            wm = _health_worst_mult(health, gk)
            wr_candidates = [
                float(v.get("rolling_wr", 1.0))
                for k, v in health.items()
                if isinstance(v, dict) and (k.endswith("|" + gk) or k == gk)
            ]
            n_candidates = [
                int(v.get("n", 0))
                for k, v in health.items()
                if isinstance(v, dict) and (k.endswith("|" + gk) or k == gk)
            ]
            n_match = max(n_candidates) if n_candidates else 0
            wr_worst = min(wr_candidates) if wr_candidates else 1.0
            if n_match >= min_tr and (wm <= 0.0 or wr_worst < 0.36):
                row["state"] = "COOLED"
                row["capital_mult"] = 0.0
                row["updated_at"] = now_iso
                row["demote_reason"] = "treasury_health_fail"

        # CANDIDATE → LIVE: OOS 입성 후 롤링 헬스가 통과하면 실전 승격
        for row in reg:
            if str(row.get("state") or "").upper() != "CANDIDATE":
                continue
            gk = str(row.get("group_key") or row.get("display_name") or "").strip()
            wm = _health_worst_mult(health, gk)
            wr_candidates = [
                float(v.get("rolling_wr", 0.0))
                for k, v in health.items()
                if isinstance(v, dict) and (k.endswith("|" + gk) or k == gk)
            ]
            n_candidates = [
                int(v.get("n", 0))
                for k, v in health.items()
                if isinstance(v, dict) and (k.endswith("|" + gk) or k == gk)
            ]
            n_match = max(n_candidates) if n_candidates else 0
            wr_best = max(wr_candidates) if wr_candidates else 0.0
            if n_match >= min_tr and wm >= 1.0 - 1e-9 and wr_best >= 0.42:
                row["state"] = "LIVE"
                row["capital_mult"] = 1.0
                row["updated_at"] = now_iso
                row["promote_reason"] = "treasury_health_recovered"

        live_after = {str(r.get("strategy_id")) for r in reg if str(r.get("state") or "").upper() == "LIVE"}
        demoted_ids = list(live_before - live_after)
        prev_retired = self._prior.get("META_RETIRED_STRATEGY_IDS")
        retired_list = list(prev_retired) if isinstance(prev_retired, list) else []
        for did in demoted_ids:
            if did and did not in retired_list:
                retired_list.append(did)
        self._working["META_STRATEGY_REGISTRY"] = reg
        self._working["META_LIVE_STRATEGY_IDS"] = [str(r["strategy_id"]) for r in reg if str(r.get("state") or "").upper() == "LIVE"]
        self._working["META_RETIRED_STRATEGY_IDS"] = retired_list[-200:]

    # --- 6 Changelog ---
    def _step_changelog(self) -> None:
        log = list(self._prior.get("META_CHANGELOG") or [])
        now = datetime.now(timezone.utc).isoformat()

        def _push(ckey: str, old: Any, new: Any, reason: str) -> None:
            if old != new:
                log.append({"at": now, "key": ckey, "old": old, "new": new, "reason": reason})

        _push(
            "META_REGIME_KEY",
            self._prior.get("META_REGIME_KEY"),
            self._working.get("META_REGIME_KEY"),
            "regime_resolve",
        )
        _push(
            "META_GLOBAL_KELLY_MULT",
            self._prior.get("META_GLOBAL_KELLY_MULT"),
            self._working.get("META_GLOBAL_KELLY_MULT"),
            "treasury_stub",
        )
        _push(
            "META_GROUP_KELLY_MULT",
            json.dumps(self._prior.get("META_GROUP_KELLY_MULT") or {}, sort_keys=True),
            json.dumps(self._working.get("META_GROUP_KELLY_MULT") or {}, sort_keys=True),
            "treasury_groups",
        )
        _push(
            "META_STRATEGY_REGISTRY",
            len(self._prior.get("META_STRATEGY_REGISTRY") or []),
            len(self._working.get("META_STRATEGY_REGISTRY") or []),
            "lifecycle_registry",
        )
        self._working["META_CHANGELOG"] = log[-200:]

    # --- 7 헤더 확정 (저장 직전) ---
    def _finalize_meta_headers(self, status: str) -> None:
        self._working["META_GOVERNOR_LAST_RUN_AT"] = datetime.now(timezone.utc).isoformat()
        self._working["META_GOVERNOR_LAST_RUN_STATUS"] = status


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    from market_db_paths import market_db_read_path

    home_cfg = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "system_config.json")
    bg_db = os.path.join(_BASE_DIR, "bitget_market_data.sqlite")
    bg_cfg = os.path.join(_BASE_DIR, "bitget_system_config.json")
    val_json = os.path.join(_BASE_DIR, "validated_live_mutants.json")
    fwd = market_db_read_path()
    ctx = GovernorRunContext(
        forward_db_path=fwd if os.path.isfile(fwd) else None,
        system_config_path=home_cfg if os.path.isfile(home_cfg) else None,
        bitget_db_path=bg_db if os.path.isfile(bg_db) else None,
        bitget_system_config_path=bg_cfg if os.path.isfile(bg_cfg) else None,
        validated_mutants_path=val_json if os.path.isfile(val_json) else None,
    )
    gov = MetaGovernor()
    out = gov.run_governor_cycle(ctx)
    logger.info("MetaGovernor done: status=%s path=%s", out.get("META_GOVERNOR_LAST_RUN_STATUS"), meta_state_path())


if __name__ == "__main__":
    main()
