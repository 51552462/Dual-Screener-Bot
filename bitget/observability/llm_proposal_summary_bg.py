"""
D-1b — weekly LLM proposal observability (read-only).

Reads ``bitget_llm_proposals`` + ``ops_events.llm_proposal_parse_error``; writes one
``llm_proposal_summary_weekly`` ops_events row — no new tables/DB/cron.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections import Counter
from typing import Any, Dict, List, Optional

from bitget.infra.clock import utc_hours_ago_iso, utc_now_iso

logger = logging.getLogger(__name__)

_PROPOSALS_TABLE = "bitget_llm_proposals"
_PARSE_ERROR_EVENT = "llm_proposal_parse_error"
_SUMMARY_EVENT = "llm_proposal_summary_weekly"
_SUMMARY_COMPONENT = "observability.llm_proposal"


def ai_proposal_summary_enabled() -> bool:
    env = os.environ.get("AI_PROPOSAL_SUMMARY_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("AI_PROPOSAL_SUMMARY_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import AI_PROPOSAL_SUMMARY_ENABLED

    return bool(AI_PROPOSAL_SUMMARY_ENABLED)


def _resolve_window_days(window_days: Optional[int] = None) -> int:
    if window_days is not None:
        return max(1, int(window_days))
    env = os.environ.get("AI_PROPOSAL_SUMMARY_WINDOW_DAYS")
    if env is not None and str(env).strip():
        try:
            return max(1, int(float(env)))
        except (TypeError, ValueError):
            pass
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("AI_PROPOSAL_SUMMARY_WINDOW_DAYS", None)
        if raw is not None:
            return max(1, int(float(raw)))
    except Exception:
        pass
    from bitget.infra.memory_policy import AI_PROPOSAL_SUMMARY_WINDOW_DAYS

    return max(1, int(AI_PROPOSAL_SUMMARY_WINDOW_DAYS))


def _market_db_path() -> str:
    from bitget.infra.data_paths import market_data_db_path

    return market_data_db_path()


def _ops_db_path() -> str:
    from bitget.infra.data_paths import ops_events_db_path

    return ops_events_db_path()


def _counter_groups(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    c: Counter[str] = Counter()
    for r in rows:
        c[str(r.get(key) or "unknown")] += 1
    return [{"key": k, "count": int(v)} for k, v in sorted(c.items(), key=lambda x: (-x[1], x[0]))]


def _load_proposal_rows(
    *,
    window_days: int,
    market_db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    path = market_db_path or _market_db_path()
    if not path or not os.path.isfile(path):
        return []
    since = utc_hours_ago_iso(float(window_days) * 24.0)
    rows: List[Dict[str, Any]] = []
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (_PROPOSALS_TABLE,),
            ).fetchone()
            if not table:
                return []
            cur = conn.execute(
                f"""
                SELECT recorded_at, category, risk_class
                FROM {_PROPOSALS_TABLE}
                WHERE recorded_at >= ?
                ORDER BY id ASC
                """,
                (since,),
            )
            for recorded_at, category, risk_class in cur.fetchall():
                rows.append(
                    {
                        "recorded_at": recorded_at,
                        "category": str(category or "unknown"),
                        "risk_class": str(risk_class or "unknown"),
                    }
                )
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("llm proposal summary load proposals failed: %s", ex)
        return []
    return rows


def _load_parse_error_count(
    *,
    window_days: int,
    ops_db_path: Optional[str] = None,
) -> int:
    path = ops_db_path or _ops_db_path()
    if not path or not os.path.isfile(path):
        return 0
    since = utc_hours_ago_iso(float(window_days) * 24.0)
    try:
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(path, read_only=True, check_same_thread=False)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM ops_events
                WHERE event = ? AND ts_utc >= ?
                """,
                (_PARSE_ERROR_EVENT, since),
            ).fetchone()
            return int(row[0] if row else 0)
        finally:
            conn.close()
    except Exception as ex:
        logger.warning("llm proposal summary load parse errors failed: %s", ex)
        return 0


def compute_llm_proposal_summary_bg(
    window_days: int = 7,
    *,
    market_db_path: Optional[str] = None,
    ops_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    days = _resolve_window_days(window_days)
    rows = _load_proposal_rows(window_days=days, market_db_path=market_db_path)
    total_count = len(rows)
    parse_error_count = _load_parse_error_count(window_days=days, ops_db_path=ops_db_path)
    denominator = total_count + parse_error_count
    parse_error_rate_pct: Optional[float] = None
    if denominator > 0:
        parse_error_rate_pct = round(
            100.0 * float(parse_error_count) / float(denominator),
            6,
        )
    return {
        "window_days": days,
        "total_count": total_count,
        "by_category": _counter_groups(rows, "category"),
        "by_risk_class": _counter_groups(rows, "risk_class"),
        "parse_error_count": parse_error_count,
        "parse_error_rate_pct": parse_error_rate_pct,
        "parse_attempt_denominator": denominator if denominator > 0 else None,
    }


def persist_llm_proposal_summary_weekly(
    summary: Dict[str, Any],
    *,
    ops_db_path: Optional[str] = None,
) -> bool:
    from bitget.infra.ops_logger import insert_ops_event

    payload = dict(summary)
    payload["recorded_at"] = utc_now_iso()
    _ = ops_db_path
    return bool(
        insert_ops_event(
            component=_SUMMARY_COMPONENT,
            severity="INFO",
            event=_SUMMARY_EVENT,
            payload=payload,
        )
    )


def run_llm_proposal_summary_job(
    *,
    window_days: Optional[int] = None,
    market_db_path: Optional[str] = None,
    ops_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """weekly_evolution pipeline hook — read-only aggregate."""
    if not ai_proposal_summary_enabled():
        logger.info("llm proposal summary disabled — skip batch")
        return None

    summary = compute_llm_proposal_summary_bg(
        window_days=_resolve_window_days(window_days),
        market_db_path=market_db_path,
        ops_db_path=ops_db_path,
    )
    ok = persist_llm_proposal_summary_weekly(summary, ops_db_path=ops_db_path)
    logger.info(
        "llm proposal summary window=%dd count=%d parse_errors=%d rate=%s inserted=%s",
        summary["window_days"],
        summary["total_count"],
        summary["parse_error_count"],
        summary["parse_error_rate_pct"],
        ok,
    )
    return {
        "inserted": ok,
        "summary": summary,
    }
