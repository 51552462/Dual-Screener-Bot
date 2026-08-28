"""
B1-LADDER-R1a-FASTCHECK — read-only weekly R1a verdict (CAT-F).

Reuses §6 SQL counts + short_funnel_report_bg output + §3 PASS/관측유지/FAIL(a|b).
Does NOT invent gates, auto-block, or R1b (CAT-C) transitions.
Writes one ``b1_ladder_fastcheck_weekly`` ops_events row per market_type (SPOT/FUT).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from bitget.infra.clock import utc_hours_ago_iso, utc_now_iso

logger = logging.getLogger(__name__)

_SUMMARY_EVENT = "b1_ladder_fastcheck_weekly"
_SUMMARY_COMPONENT = "observability.b1_ladder"
_FORWARD_TABLE = "bitget_forward_trades"
# 13_B1 §3 R0 확정일 앵커 (신규 상수 아님 — SSOT 재사용)
_R0_ANCHOR_DATE = "2026-08-23"
# 13_B1 §3 Kill R1 / FAIL(a) — 4주 (신규 상수 아님)
_R1_FAIL_A_DAYS = 28
# 13_B1 R6 — trades≥30 · ≥56일 (페이스 환산용 재사용)
_R6_MIN_TRADES = 30
_R6_DAYS = 56
_MARKET_KEYS = ("SPOT", "FUT")


def b1_ladder_fastcheck_enabled() -> bool:
    env = os.environ.get("B1_LADDER_FASTCHECK_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("B1_LADDER_FASTCHECK_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import B1_LADDER_FASTCHECK_ENABLED

    return bool(B1_LADDER_FASTCHECK_ENABLED)


def _resolve_window_days(window_days: Optional[int] = None) -> int:
    if window_days is not None:
        return max(1, int(window_days))
    env = os.environ.get("B1_LADDER_FASTCHECK_WINDOW_DAYS")
    if env is not None and str(env).strip():
        try:
            return max(1, int(float(env)))
        except (TypeError, ValueError):
            pass
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("B1_LADDER_FASTCHECK_WINDOW_DAYS", None)
        if raw is not None:
            return max(1, int(float(raw)))
    except Exception:
        pass
    from bitget.infra.memory_policy import B1_LADDER_FASTCHECK_WINDOW_DAYS

    return max(1, int(B1_LADDER_FASTCHECK_WINDOW_DAYS))


def _forward_db_path() -> str:
    from bitget.infra.data_paths import market_data_db_path

    return market_data_db_path()


def _ops_db_path() -> str:
    from bitget.infra.data_paths import ops_events_db_path

    return ops_events_db_path()


def _norm_mt(raw: Any) -> str:
    from bitget.evolution.market_key_normalize import normalize_market_key

    mk = normalize_market_key(str(raw or "spot"))
    return "FUT" if mk == "FUT" else "SPOT"


def _days_since_r0(*, now: Optional[datetime] = None) -> int:
    """Calendar days since R0 anchor (13_ §3) — FAIL(a) 4주 판정용."""
    anchor = datetime.strptime(_R0_ANCHOR_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    cur = now or datetime.now(timezone.utc)
    if cur.tzinfo is None:
        cur = cur.replace(tzinfo=timezone.utc)
    return max(0, int((cur.date() - anchor.date()).days))


def _open_and_closed_weekly(
    *,
    window_days: int,
    forward_db_path: Optional[str] = None,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """§6 SQL (+ market_type) · CLOSED 주간 Δ = 기존 exit/entry timestamp 창 필터만."""
    open_by = {k: 0 for k in _MARKET_KEYS}
    closed_weekly = {k: 0 for k in _MARKET_KEYS}
    path = forward_db_path or _forward_db_path()
    if not path or not os.path.isfile(path):
        return open_by, closed_weekly
    since = utc_hours_ago_iso(float(window_days) * 24.0)
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (_FORWARD_TABLE,),
            ).fetchone()
            if not table:
                return open_by, closed_weekly
            # §6: SELECT status, COUNT(*) … GROUP BY status — market_type 분리만 추가
            rows = conn.execute(
                f"""
                SELECT market_type, status, COUNT(*)
                FROM {_FORWARD_TABLE}
                GROUP BY market_type, status
                """
            ).fetchall()
            for market_type, status, cnt in rows:
                mk = _norm_mt(market_type)
                st = str(status or "").upper()
                n = int(cnt or 0)
                if st.startswith("OPEN"):
                    open_by[mk] += n
            # CLOSED 주간 Δ — COALESCE(exit_date, entry_date) 기존 컬럼 창 필터
            since_day = str(since or "")[:10]
            closed_rows = conn.execute(
                f"""
                SELECT market_type, COUNT(*)
                FROM {_FORWARD_TABLE}
                WHERE UPPER(COALESCE(status, '')) LIKE 'CLOSED%'
                  AND substr(COALESCE(exit_date, entry_date), 1, 10) >= ?
                GROUP BY market_type
                """,
                (since_day,),
            ).fetchall()
            for market_type, cnt in closed_rows:
                closed_weekly[_norm_mt(market_type)] += int(cnt or 0)
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("b1 ladder fastcheck trade counts failed: %s", ex)
    return open_by, closed_weekly


def _blocked_short_by_mt(
    *,
    forward_db_path: Optional[str] = None,
) -> Dict[str, int]:
    """Cite short_funnel_report_bg — no bucket recompute. SPOT SHORT structural 0."""
    out = {"SPOT": 0, "FUT": 0}
    try:
        from bitget.observability.short_funnel_report_bg import collect_short_funnel_report

        report = collect_short_funnel_report(forward_db_path=forward_db_path)
        # FUT only meaningful (SPOT SHORT hard-block footnote); cite total as FUT
        out["FUT"] = int(report.get("blocked_short_total") or 0)
        out["SPOT"] = 0
    except Exception as ex:
        logger.warning("b1 ladder fastcheck short_funnel cite failed: %s", ex)
    return out


def _r6_pace_flag(closed_weekly_delta: int, window_days: int) -> str:
    """(Δ ÷ window × 56) vs R6 trades≥30 — flag only, gate unchanged."""
    days = max(1, int(window_days))
    projected = (float(closed_weekly_delta) / float(days)) * float(_R6_DAYS)
    if projected < float(_R6_MIN_TRADES):
        return "페이스부족"
    return "ok"


def _prior_week_payload(
    market_type: str,
    *,
    ops_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    path = ops_db_path or _ops_db_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM ops_events
                WHERE event = ?
                ORDER BY id DESC
                LIMIT 40
                """,
                (_SUMMARY_EVENT,),
            ).fetchall()
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return None
    for (payload_json,) in rows:
        try:
            payload = json.loads(payload_json or "{}")
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("market_type") or "").upper() in (market_type,):
            return payload
        # nested by_mt form
        by_mt = payload.get("by_market_type")
        if isinstance(by_mt, dict) and market_type in by_mt:
            inner = by_mt[market_type]
            if isinstance(inner, dict):
                return inner
    return None


def _assign_verdict(
    *,
    open_count: int,
    blocked_short_total: int,
    days_since_r0: int,
    prior: Optional[Dict[str, Any]],
) -> str:
    """§3 R1a 판정표 대입만 — 신규 로직 없음."""
    if int(open_count) > 0:
        return "PASS"
    # FAIL(b): blocked 반복 (OPEN=0 ∧ blocked>0 가 이번+직전 주)
    prior_open = int((prior or {}).get("open_count") or 0) if prior else None
    prior_blocked = int((prior or {}).get("blocked_short_total") or 0) if prior else None
    if (
        prior is not None
        and prior_open == 0
        and prior_blocked is not None
        and prior_blocked > 0
        and int(blocked_short_total) > 0
    ):
        return "FAIL(b)"
    # FAIL(a): 4주 경과 + OPEN=0
    if int(days_since_r0) >= _R1_FAIL_A_DAYS:
        return "FAIL(a)"
    return "관측유지"


def compute_b1_ladder_fastcheck_bg(
    window_days: int = 7,
    *,
    forward_db_path: Optional[str] = None,
    ops_db_path: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    market_type(SPOT/FUT)별 개별 dict — 병합 없음.

    Keys per mt: open_count, closed_weekly_delta, blocked_short_total,
    r6_pace_flag, verdict, window_days, days_since_r0, r0_anchor.
    """
    days = _resolve_window_days(window_days)
    open_by, closed_weekly = _open_and_closed_weekly(
        window_days=days, forward_db_path=forward_db_path
    )
    blocked_by = _blocked_short_by_mt(forward_db_path=forward_db_path)
    days_r0 = _days_since_r0(now=now)
    out: Dict[str, Dict[str, Any]] = {}
    for mt in _MARKET_KEYS:
        open_n = int(open_by.get(mt) or 0)
        closed_d = int(closed_weekly.get(mt) or 0)
        blocked_n = int(blocked_by.get(mt) or 0)
        prior = _prior_week_payload(mt, ops_db_path=ops_db_path)
        verdict = _assign_verdict(
            open_count=open_n,
            blocked_short_total=blocked_n,
            days_since_r0=days_r0,
            prior=prior,
        )
        out[mt] = {
            "market_type": mt,
            "window_days": days,
            "open_count": open_n,
            "closed_weekly_delta": closed_d,
            "blocked_short_total": blocked_n,
            "r6_pace_flag": _r6_pace_flag(closed_d, days),
            "r6_pace_projected_trades": round(
                (float(closed_d) / float(max(1, days))) * float(_R6_DAYS), 2
            ),
            "verdict": verdict,
            "days_since_r0": days_r0,
            "r0_anchor": _R0_ANCHOR_DATE,
            "long_blocked_visibility": False,  # LS-GOAL-UX caveat
        }
    return out


def persist_b1_ladder_fastcheck_weekly(
    by_mt: Dict[str, Dict[str, Any]],
    *,
    ops_db_path: Optional[str] = None,
) -> Dict[str, bool]:
    """One ops_events row per market_type (bad_tick_skip_summary_weekly pattern)."""
    from bitget.infra.ops_logger import insert_ops_event

    _ = ops_db_path
    results: Dict[str, bool] = {}
    recorded = utc_now_iso()
    for mt in _MARKET_KEYS:
        payload = dict(by_mt.get(mt) or {})
        payload["market_type"] = mt
        payload["recorded_at"] = recorded
        results[mt] = bool(
            insert_ops_event(
                component=_SUMMARY_COMPONENT,
                severity="INFO",
                event=_SUMMARY_EVENT,
                payload=payload,
            )
        )
    return results


def run_b1_ladder_fastcheck_job(
    *,
    window_days: Optional[int] = None,
    forward_db_path: Optional[str] = None,
    ops_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """weekly_evolution pipeline hook — read-only aggregate."""
    if not b1_ladder_fastcheck_enabled():
        logger.info("b1 ladder fastcheck disabled — skip batch")
        return None

    by_mt = compute_b1_ladder_fastcheck_bg(
        window_days=_resolve_window_days(window_days),
        forward_db_path=forward_db_path,
        ops_db_path=ops_db_path,
    )
    inserted = persist_b1_ladder_fastcheck_weekly(by_mt, ops_db_path=ops_db_path)
    logger.info(
        "b1 ladder fastcheck window=%sd SPOT=%s FUT=%s inserted=%s",
        by_mt.get("SPOT", {}).get("window_days"),
        by_mt.get("SPOT", {}).get("verdict"),
        by_mt.get("FUT", {}).get("verdict"),
        inserted,
    )
    return {"inserted": inserted, "by_market_type": by_mt}
