"""
Dual North Star Progress Ledger — 주식(Track A) vs Bitget(Track B) 목표 달성도 SSOT.

읽기 전용: treasury JSON·performance_budget 만 조회. 리스크 로직·config 쓰기 없음.
격리: 두 트랙의 MDD/CAGR 숫자를 섞지 않고, 비교 점수만 산출한다.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from factory_data_paths import factory_data_dir
except Exception:  # pragma: no cover

    def factory_data_dir() -> str:  # type: ignore[misc]
        d = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot")
        os.makedirs(d, exist_ok=True)
        return d


LEDGER_FILENAME = "dual_north_star_ledger.json"
SCHEMA = "dual_north_star_ledger.v1"

# --- North Star targets (SSOT: bitget/docs/work_phases/00_마스터_로드맵.md §0.4) ---
TRACK_A = {
    "track_id": "A",
    "label": "주식 KR+US",
    "mdd_cap_pct": 10.0,
    "cagr_target_lo": 40.0,
    "cagr_target_hi": 70.0,
    "phase": "A",
    "phase_label": "운영",
}

TRACK_B_DEFAULTS = {
    "track_id": "B",
    "label": "Bitget 코인",
    "mdd_cap_pct": 5.0,
    "cagr_target_lo": 12.0,
    "cagr_target_hi": 25.0,
    "phase": "B0",
    "phase_label": "검증·측정",
}

HISTORY_LIMITS = {"daily": 400, "weekly": 60, "monthly": 36, "yearly": 10}
G1_MIN_DAILY_SNAPSHOTS = 28
G2_MIN_DAILY_SNAPSHOTS = 56
G3_MIN_DAILY_SNAPSHOTS = 84
G2_MIN_FORWARD_TRADES = 30
MDD_CAP_CONTINUOUS_DAYS = 28
# OBS-HOLD 갈림길 재소집 (FWD-OBS-HOLD-01 SSOT) — Pass/Fail 아님, n만 트리거
OBS_HOLD_RECALL_N = 20

R1_BANNER_TEXT = "초기 관측 중 · 페이스 미확정 · 연 목표 대비 참고용 아님"
R3_BANNER_TEXT = "⚠️ Bitget paper 미검증(C-2 funding PnL 전) · 참고용 · 실전 아님"


def ledger_path() -> str:
    return os.path.join(factory_data_dir(), LEDGER_FILENAME)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today_kst() -> str:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _empty_ledger() -> Dict[str, Any]:
    return {
        "schema": SCHEMA,
        "updated_at": None,
        "tracks_meta": {"A": dict(TRACK_A), "B": dict(TRACK_B_DEFAULTS)},
        "latest": None,
        "history": {k: [] for k in HISTORY_LIMITS},
        "commercialization": {
            "A": {"gate": "G0", "gate_label": "측정·구조"},
            "B": {"gate": "G0", "gate_label": "측정·구조"},
        },
    }


def load_ledger() -> Dict[str, Any]:
    path = ledger_path()
    if not os.path.isfile(path):
        return _empty_ledger()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _empty_ledger()
        base = _empty_ledger()
        base.update({k: v for k, v in data.items() if k in base or k == "latest"})
        if not isinstance(base.get("history"), dict):
            base["history"] = {k: [] for k in HISTORY_LIMITS}
        for k in HISTORY_LIMITS:
            if not isinstance(base["history"].get(k), list):
                base["history"][k] = []
        return base
    except (OSError, json.JSONDecodeError, ValueError):
        return _empty_ledger()


def save_ledger(state: Dict[str, Any]) -> bool:
    path = ledger_path()
    state = dict(state)
    state["schema"] = SCHEMA
    state["updated_at"] = _now_iso()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".north_star_", suffix=".json", dir=os.path.dirname(path))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
        return True
    except OSError:
        return False


def _safe_pct(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return (num / den) * 100.0


def _return_since_base(nav: float, base: float) -> float:
    if base <= 0:
        return 0.0
    return ((nav / base) - 1.0) * 100.0


def _pace_score(current: float, target_lo: float, *, measure_only: bool = False) -> float:
    if measure_only or target_lo <= 0:
        return 0.0
    return max(0.0, min(150.0, (current / target_lo) * 100.0))


def _mdd_safety_score(current_mdd: float, cap: float) -> float:
    if cap <= 0:
        return 100.0
    if current_mdd >= cap:
        return 0.0
    return max(0.0, min(100.0, (1.0 - current_mdd / cap) * 100.0))


def _composite_score(return_pace: float, mdd_safety: float, *, measure_only: bool) -> float:
    if measure_only:
        return round(mdd_safety * 0.5, 2)
    return round(return_pace * 0.6 + mdd_safety * 0.4, 2)


def _goal_achievement_pct(track: Dict[str, Any]) -> float:
    agg = track.get("aggregate") or {}
    return float(agg.get("return_pace_score", 0) or 0)


def _config_flag_true(*keys: str) -> bool:
    for key in keys:
        try:
            from bitget.config_manager import get_config_value

            raw = str(get_config_value(key, "") or "").strip().lower()
            if raw in ("1", "true", "yes", "on"):
                return True
        except Exception:
            pass
        try:
            from factory_data_paths import factory_data_dir
            import json as _json

            cfg_path = os.path.join(factory_data_dir(), "system_config.json")
            if os.path.isfile(cfg_path):
                with open(cfg_path, encoding="utf-8") as f:
                    cfg = _json.load(f)
                raw = str((cfg or {}).get(key, "") or "").strip().lower()
                if raw in ("1", "true", "yes", "on"):
                    return True
        except Exception:
            pass
    return False


def _a06_first_pass() -> bool:
    return _config_flag_true("A06_CHECKLIST_FIRST_PASS", "NORTH_STAR_A06_FIRST_PASS")


def _c2_funding_pnl_complete() -> bool:
    return _config_flag_true(
        "C2_FUNDING_PNL_COMPLETE",
        "BITGET_FUNDING_PNL_IN_LEDGER",
        "NORTH_STAR_C2_FUNDING_COMPLETE",
    )


def _count_forward_trades(track_id: str) -> int:
    import sqlite3

    try:
        if track_id == "B":
            from bitget.infra.data_paths import market_data_db_path

            db = market_data_db_path()
            table = "bitget_forward_trades"
        else:
            from factory_data_paths import market_data_db_path

            db = market_data_db_path()
            table = "forward_trades"
        if not os.path.isfile(db):
            return 0
        conn = sqlite3.connect(db, timeout=5.0)
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return int(row[0] or 0) if row else 0
        finally:
            conn.close()
    except Exception:
        return 0


def _mdd_continuous_under_cap(
    daily_history: List[Dict[str, Any]],
    track_id: str,
    cap: float,
    *,
    days: int = MDD_CAP_CONTINUOUS_DAYS,
) -> bool:
    recent = daily_history[-days:]
    if len(recent) < days:
        return False
    for row in recent:
        t = (row.get("tracks") or {}).get(track_id) or {}
        mdd = float((t.get("aggregate") or {}).get("max_mdd_pct", 999.0) or 999.0)
        if mdd >= cap:
            return False
    return True


def _read_stock_track() -> Dict[str, Any]:
    meta = dict(TRACK_A)
    markets: Dict[str, Any] = {}
    try:
        from live_nav_manager import get_market_state, load_treasury_state

        state = load_treasury_state()
        for mkt in ("KR", "US"):
            st = get_market_state(mkt)
            nav = float(st.get("nav", 0) or 0)
            hwm = float(st.get("hwm", nav) or nav)
            base = float(st.get("base_capital", nav) or nav)
            mdd = float(st.get("mdd_pct", 0) or 0)
            if hwm > 0 and nav > 0:
                mdd = max(mdd, max(0.0, (hwm - nav) / hwm * 100.0))
            markets[mkt] = {
                "nav": nav,
                "hwm": hwm,
                "base_capital": base,
                "mdd_pct": round(mdd, 4),
                "return_pct": round(_return_since_base(nav, base), 4),
                "n_closed": int(st.get("n_closed", 0) or 0),
            }
    except Exception as exc:
        return {**meta, "error": str(exc)[:120], "markets": {}, "available": False}

    try:
        from performance_budget_governor import evaluate_performance_budget

        for mkt in markets:
            ev = evaluate_performance_budget(market=mkt)
            markets[mkt]["exhaustion_pct"] = float(ev.get("exhaustion_pct", 0) or 0)
            markets[mkt]["budget_band"] = str(ev.get("band", "UNKNOWN"))
    except Exception:
        pass

    max_mdd = max((markets[m]["mdd_pct"] for m in markets), default=0.0)
    avg_return = 0.0
    if markets:
        avg_return = sum(markets[m]["return_pct"] for m in markets) / len(markets)

    return_pace = _pace_score(avg_return, meta["cagr_target_lo"])
    mdd_safety = _mdd_safety_score(max_mdd, meta["mdd_cap_pct"])
    composite = _composite_score(return_pace, mdd_safety, measure_only=False)

    return {
        **meta,
        "available": bool(markets),
        "forward_trades_count": _count_forward_trades("A"),
        "a06_first_pass": _a06_first_pass(),
        "markets": markets,
        "aggregate": {
            "max_mdd_pct": round(max_mdd, 4),
            "avg_return_pct": round(avg_return, 4),
            "return_pace_score": round(return_pace, 2),
            "mdd_safety_score": round(mdd_safety, 2),
            "composite_score": composite,
        },
    }


def _read_bitget_track() -> Dict[str, Any]:
    meta = dict(TRACK_B_DEFAULTS)
    try:
        from bitget.live_nav_manager import load_treasury_state, portfolio_nav_snapshot

        snap = portfolio_nav_snapshot()
        state = load_treasury_state()
        spot = state.get("spot") if isinstance(state.get("spot"), dict) else {}
        fut = state.get("futures") if isinstance(state.get("futures"), dict) else {}
        spot_base = float(spot.get("base_capital", 0) or 0)
        fut_base = float(fut.get("base_capital", 0) or 0)
        total_base = spot_base + fut_base if (spot_base + fut_base) > 0 else 1.0
        nav = float(snap.get("nav", 0) or 0)
        mdd = float(snap.get("mdd_pct", 0) or 0)
        ret = _return_since_base(nav, total_base)

        try:
            from bitget.config_manager import get_config_value

            phase = str(get_config_value("BITGET_NORTH_STAR_PHASE", "B0") or "B0").upper()
        except Exception:
            phase = "B0"

        if phase.startswith("B1"):
            meta["cagr_target_lo"], meta["cagr_target_hi"] = 12.0, 18.0
            meta["phase_label"] = "보수"
        elif phase.startswith("B2"):
            meta["cagr_target_lo"], meta["cagr_target_hi"] = 18.0, 25.0
            meta["phase_label"] = "기본"
        elif phase.startswith("B3"):
            meta["cagr_target_lo"], meta["cagr_target_hi"] = 25.0, 35.0
            meta["phase_label"] = "스트레치"
        meta["phase"] = phase

        measure_only = phase == "B0"
        return_pace = _pace_score(ret, meta["cagr_target_lo"], measure_only=measure_only)
        mdd_safety = _mdd_safety_score(mdd, meta["mdd_cap_pct"])
        composite = _composite_score(return_pace, mdd_safety, measure_only=measure_only)

        tier = "NORMAL"
        try:
            from bitget.config_manager import get_config_value as gcv

            tier = str(gcv("PORTFOLIO_MDD_CURRENT_TIER", "NORMAL") or "NORMAL")
        except Exception:
            pass

        return {
            **meta,
            "available": nav > 0,
            "c2_funding_complete": _c2_funding_pnl_complete(),
            "forward_trades_count": _count_forward_trades("B"),
            "portfolio": {
                "nav": nav,
                "hwm": float(snap.get("hwm", 0) or 0),
                "base_capital": total_base,
                "mdd_pct": round(mdd, 4),
                "return_pct": round(ret, 4),
                "spot_nav": float(snap.get("spot_nav", 0) or 0),
                "futures_nav": float(snap.get("futures_nav", 0) or 0),
                "mdd_tier": tier,
            },
            "aggregate": {
                "max_mdd_pct": round(mdd, 4),
                "avg_return_pct": round(ret, 4),
                "return_pace_score": round(return_pace, 2),
                "mdd_safety_score": round(mdd_safety, 2),
                "composite_score": composite,
                "measure_only": measure_only,
            },
        }
    except Exception as exc:
        return {**meta, "error": str(exc)[:120], "available": False, "aggregate": {}}


def _period_return(current_return: float, prev_return: Optional[float]) -> Optional[float]:
    if prev_return is None:
        return None
    return round(current_return - prev_return, 4)


def _find_prev_snapshot(history: List[Dict[str, Any]], *, days: int) -> Optional[Dict[str, Any]]:
    if not history:
        return None
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    for row in reversed(history):
        if str(row.get("date_kst", "")) <= cutoff:
            return row
    return history[0] if history else None


def _compute_leader(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Q8: B0=리더 폐지(side-by-side). B1+=목표달성률% 비교."""
    phase = str(b.get("phase") or "B0").upper()
    ga = _goal_achievement_pct(a)
    gb = _goal_achievement_pct(b)
    if phase.startswith("B0"):
        return {
            "leader_track": None,
            "leader_mode": "side_by_side",
            "leader_reason": "B0 측정 — 리더 미표시 (나란히 비교만)",
            "scores": {"A": ga, "B": gb},
        }
    if abs(ga - gb) < 1.0:
        return {
            "leader_track": "TIE",
            "leader_mode": "goal_achievement",
            "leader_reason": f"목표달성률 비슷 (A {ga:.0f}% · B {gb:.0f}%)",
            "scores": {"A": ga, "B": gb},
        }
    if ga > gb:
        return {
            "leader_track": "A",
            "leader_mode": "goal_achievement",
            "leader_reason": f"주식 목표달성률 {ga:.0f}% > Bitget {gb:.0f}%",
            "scores": {"A": ga, "B": gb},
        }
    return {
        "leader_track": "B",
        "leader_mode": "goal_achievement",
        "leader_reason": f"Bitget 목표달성률 {gb:.0f}% > 주식 {ga:.0f}%",
        "scores": {"A": ga, "B": gb},
    }


def _gate_for_track(
    track_id: str,
    daily_history: List[Dict[str, Any]],
    track_snap: Dict[str, Any],
    *,
    forward_trade_count: int = 0,
) -> Dict[str, Any]:
    """상품화 G0~G3 (G4=수동). 종합점수=게이트만. R4 AND 조건."""
    scores = [
        float((row.get("tracks") or {}).get(track_id, {}).get("aggregate", {}).get("composite_score", 0) or 0)
        for row in daily_history[-G3_MIN_DAILY_SNAPSHOTS:]
        if isinstance(row.get("tracks"), dict)
    ]
    n = len(scores)
    mdd = float((track_snap.get("aggregate") or {}).get("max_mdd_pct", 0) or 0)
    cap = float(track_snap.get("mdd_cap_pct", 10) or 10)
    mdd_ok = mdd < cap
    avg = sum(scores) / n if scores else 0.0
    block_reasons: List[str] = []

    gate = "G0"
    label = "측정·구조"
    g3_blocked = False

    if n < 7:
        return {"gate": gate, "gate_label": label, "block_reasons": block_reasons}

    if n >= G1_MIN_DAILY_SNAPSHOTS and avg >= 40:
        gate, label = "G1", "페이스 형성"
    if n >= G2_MIN_DAILY_SNAPSHOTS and avg >= 60:
        if forward_trade_count > G2_MIN_FORWARD_TRADES:
            gate, label = "G2", "목표 근접"
        else:
            block_reasons.append(f"forward_trades≤{G2_MIN_FORWARD_TRADES} (현재 {forward_trade_count})")
    if n >= G3_MIN_DAILY_SNAPSHOTS and avg >= 75 and mdd_ok:
        g3_reasons: List[str] = []
        if track_id == "A" and not _a06_first_pass():
            g3_reasons.append("A `06` 1차 미통과")
        if track_id == "B" and not _c2_funding_pnl_complete():
            g3_reasons.append("C-2 funding PnL 미반영")
        if not _mdd_continuous_under_cap(daily_history, track_id, cap):
            g3_reasons.append("MDD 4주 연속 캡 이내 미달")
        if not g3_reasons:
            gate, label = "G3", "상품화 후보"
        else:
            g3_blocked = True
            block_reasons.extend(g3_reasons)

    out: Dict[str, Any] = {
        "gate": gate,
        "gate_label": label,
        "block_reasons": block_reasons,
        "g3_blocked": g3_blocked,
    }
    if g3_blocked:
        out["not_candidate_reason"] = " · ".join(block_reasons)
    return out


def build_snapshot(*, cadence: str = "daily") -> Dict[str, Any]:
    """현재 시점 듀얼 트랙 스냅샷 + 기간 수익(ledger 이력 대비)."""
    track_a = _read_stock_track()
    track_b = _read_bitget_track()
    leader = _compute_leader(track_a, track_b)

    date_kst = _today_kst()
    snap: Dict[str, Any] = {
        "cadence": cadence,
        "date_kst": date_kst,
        "ts_utc": _now_iso(),
        "tracks": {"A": track_a, "B": track_b},
        "comparison": leader,
    }

    ledger = load_ledger()
    hist = ledger.get("history") or {}
    daily = hist.get("daily") if isinstance(hist.get("daily"), list) else []
    daily_count = len(daily)

    prev_day = daily[-1] if daily else None
    prev_week = _find_prev_snapshot(daily, days=7)
    prev_month = _find_prev_snapshot(daily, days=30)
    prev_year = _find_prev_snapshot(daily, days=365)

    for tid in ("A", "B"):
        track = track_a if tid == "A" else track_b
        cur = float(track.get("aggregate", {}).get("avg_return_pct", 0) or 0)

        def _prev_ret(prev: Optional[Dict[str, Any]]) -> Optional[float]:
            if not prev:
                return None
            t = (prev.get("tracks") or {}).get(tid) or {}
            return float((t.get("aggregate") or {}).get("avg_return_pct", 0) or 0)

        periods = snap.setdefault("period_returns", {}).setdefault(tid, {})
        periods["day_pct"] = _period_return(cur, _prev_ret(prev_day))
        periods["week_pct"] = _period_return(cur, _prev_ret(prev_week))
        periods["month_pct"] = _period_return(cur, _prev_ret(prev_month))
        periods["year_pct"] = _period_return(cur, _prev_ret(prev_year))
        periods["total_pct"] = round(cur, 4)

    snap["meta"] = {
        "daily_snapshot_count": daily_count,
        "show_r1_caveat": daily_count < G1_MIN_DAILY_SNAPSHOTS,
        "r1_banner": R1_BANNER_TEXT,
        "r3_banner": R3_BANNER_TEXT,
        "show_r3_bitget_banner": not track_b.get("c2_funding_complete", False),
    }

    return snap


def append_snapshot_to_ledger(snap: Dict[str, Any]) -> Dict[str, Any]:
    """스냅샷을 ledger history에 적재하고 상품화 게이트 갱신."""
    ledger = load_ledger()
    cadence = str(snap.get("cadence") or "daily")
    date_kst = str(snap.get("date_kst") or _today_kst())

    hist = ledger.setdefault("history", {k: [] for k in HISTORY_LIMITS})
    bucket = hist.setdefault(cadence, [])
    if bucket and str(bucket[-1].get("date_kst")) == date_kst and cadence == "daily":
        bucket[-1] = snap
    else:
        bucket.append(snap)
        limit = HISTORY_LIMITS.get(cadence, 400)
        if len(bucket) > limit:
            hist[cadence] = bucket[-limit:]

    daily_hist = hist.get("daily") if isinstance(hist.get("daily"), list) else []
    ledger["latest"] = snap
    ledger["commercialization"] = {
        "A": _gate_for_track(
            "A",
            daily_hist,
            snap["tracks"]["A"],
            forward_trade_count=int(snap["tracks"]["A"].get("forward_trades_count", 0) or 0),
        ),
        "B": _gate_for_track(
            "B",
            daily_hist,
            snap["tracks"]["B"],
            forward_trade_count=int(snap["tracks"]["B"].get("forward_trades_count", 0) or 0),
        ),
    }
    save_ledger(ledger)
    return ledger


def resolve_obs_hold_action(*, cadence: str, daily_n: int) -> str:
    """OBS-HOLD cursor_action. weekly/monthly 등 daily가 아니면 NONE."""
    if str(cadence or "").lower() != "daily":
        return "NONE"
    if int(daily_n or 0) >= OBS_HOLD_RECALL_N:
        return "RECALL_FORK"
    return "OBSERVE_HOLD"


def enrich_obs_hold_meta(snap: Dict[str, Any], *, daily_n: Optional[int] = None) -> Dict[str, Any]:
    """snap['meta']에 OBS-HOLD 재소집 필드·cursor_action 주입."""
    meta = snap.setdefault("meta", {})
    cadence = str(snap.get("cadence") or "daily")
    if daily_n is None:
        daily_n = int(meta.get("daily_snapshot_count") or 0)
    daily_n = int(daily_n or 0)
    remaining = max(0, OBS_HOLD_RECALL_N - daily_n)
    active = cadence.lower() == "daily" and daily_n < OBS_HOLD_RECALL_N
    action = resolve_obs_hold_action(cadence=cadence, daily_n=daily_n)
    meta.update(
        {
            "daily_snapshot_count": daily_n,
            "daily_n": daily_n,
            "obs_hold_recall_n": OBS_HOLD_RECALL_N,
            "obs_hold_remaining": remaining,
            "obs_hold_active": active,
            "cursor_action": action,
        }
    )
    return snap


def run_north_star_digest(*, cadence: str = "daily", persist: bool = True) -> Dict[str, Any]:
    snap = build_snapshot(cadence=cadence)
    if persist:
        append_snapshot_to_ledger(snap)
        ledger = load_ledger()
        snap["ledger"] = ledger.get("commercialization")
        hist = ledger.get("history") or {}
        daily = hist.get("daily") if isinstance(hist.get("daily"), list) else []
        enrich_obs_hold_meta(snap, daily_n=len(daily))
    else:
        enrich_obs_hold_meta(snap)
    return snap
