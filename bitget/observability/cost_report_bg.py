"""
D-3a — weekly cost observability (read-only).

Reads forward trades + Gemini call proxy; writes one ``cost_report_weekly`` ops_events row.
No config_kv writes. No arbitrary fee/API unit rates.
"""
from __future__ import annotations

import dataclasses
import logging
import os
import sqlite3
from typing import Any, Dict, Optional

from bitget.infra.clock import utc_hours_ago_iso, utc_now_iso

logger = logging.getLogger(__name__)

_SUMMARY_EVENT = "cost_report_weekly"
_SUMMARY_COMPONENT = "observability.cost"
_FORWARD_TABLE = "bitget_forward_trades"
_COST_BASIS_NO_RATE = "no_usd_unit_rate"
_FEE_BASIS_NO_RATE = "no_fee_rate_ssot"


def cost_report_enabled() -> bool:
    env = os.environ.get("COST_REPORT_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("COST_REPORT_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import COST_REPORT_ENABLED

    return bool(COST_REPORT_ENABLED)


def _resolve_window_days(window_days: Optional[int] = None) -> int:
    if window_days is not None:
        return max(1, int(window_days))
    env = os.environ.get("COST_REPORT_WINDOW_DAYS")
    if env is not None and str(env).strip():
        try:
            return max(1, int(float(env)))
        except (TypeError, ValueError):
            pass
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("COST_REPORT_WINDOW_DAYS", None)
        if raw is not None:
            return max(1, int(float(raw)))
    except Exception:
        pass
    from bitget.infra.memory_policy import COST_REPORT_WINDOW_DAYS

    return max(1, int(COST_REPORT_WINDOW_DAYS))


def _forward_db_path() -> str:
    from bitget.forward.shared import DB_PATH

    return str(DB_PATH or "")


def _llm_cache_db_path() -> str:
    try:
        import llm_gemini_core as core

        return str(getattr(core, "_LLM_CACHE_DB", "") or "").strip()
    except Exception:
        return ""


def _llm_result_has_token_metering() -> bool:
    try:
        from llm_gemini_core import LlmResult

        names = {f.name for f in dataclasses.fields(LlmResult)}
        return bool(
            names.intersection(
                {
                    "input_tokens",
                    "output_tokens",
                    "total_tokens",
                    "usage_metadata",
                    "prompt_token_count",
                    "candidates_token_count",
                }
            )
        )
    except Exception:
        return False


def _count_gemini_calls_ops_proxy(
    *,
    since_iso: str,
    ops_db_path: Optional[str] = None,
) -> int:
    from bitget.infra.data_paths import ops_events_db_path

    path = ops_db_path or ops_events_db_path()
    if not path or not os.path.isfile(path):
        return 0
    try:
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(path, read_only=True, check_same_thread=False)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM ops_events
                WHERE ts_utc >= ?
                  AND (
                    event IN ('gemini_api_call', 'llm_call_completed', 'gemini_call')
                    OR (event LIKE '%gemini%' AND event LIKE '%call%')
                  )
                """,
                (since_iso,),
            ).fetchone()
            return int(row[0] if row else 0)
        finally:
            conn.close()
    except Exception as ex:
        logger.warning("cost report gemini ops proxy failed: %s", ex)
        return 0


def _count_gemini_calls_cache_proxy(
    *,
    since_iso: str,
    cache_db_path: Optional[str] = None,
) -> int:
    path = cache_db_path or _llm_cache_db_path()
    if not path or not os.path.isfile(path):
        return 0
    since_cmp = since_iso[:19]
    try:
        conn = sqlite3.connect(path, timeout=15)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM llm_cache
                WHERE datetime(created_at) >= datetime(?)
                """,
                (since_cmp,),
            ).fetchone()
            return int(row[0] if row else 0)
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("cost report llm cache proxy failed: %s", ex)
        return 0


def _resolve_gemini_call_count(
    *,
    since_iso: str,
    ops_db_path: Optional[str] = None,
    cache_db_path: Optional[str] = None,
) -> tuple[int, str]:
    ops_n = _count_gemini_calls_ops_proxy(since_iso=since_iso, ops_db_path=ops_db_path)
    if ops_n > 0:
        return ops_n, "ops_events"
    cache_n = _count_gemini_calls_cache_proxy(since_iso=since_iso, cache_db_path=cache_db_path)
    if cache_n > 0:
        return cache_n, "llm_call_cache_proxy"
    return 0, "none"


def _sum_paper_notional_traded_usd(
    *,
    since_iso: str,
    forward_db_path: Optional[str] = None,
) -> float:
    path = forward_db_path or _forward_db_path()
    if not path or not os.path.isfile(path):
        return 0.0
    since_cmp = since_iso[:19]
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (_FORWARD_TABLE,),
            ).fetchone()
            if not table:
                return 0.0
            row = conn.execute(
                f"""
                SELECT COALESCE(SUM(
                    CASE
                        WHEN COALESCE(quantity, 0) > 0 AND COALESCE(entry_price, 0) > 0 THEN
                            ABS(quantity * entry_price * COALESCE(leverage, 1.0))
                        ELSE ABS(COALESCE(sim_kelly_invest, 0))
                    END
                ), 0.0)
                FROM {_FORWARD_TABLE}
                WHERE (
                    (entry_date IS NOT NULL AND datetime(entry_date) >= datetime(?))
                    OR (exit_date IS NOT NULL AND datetime(exit_date) >= datetime(?))
                )
                """,
                (since_cmp, since_cmp),
            ).fetchone()
            return round(float(row[0] if row else 0.0), 6)
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("cost report paper notional failed: %s", ex)
        return 0.0


def compute_weekly_cost_report_bg(
    window_days: int = 7,
    *,
    forward_db_path: Optional[str] = None,
    ops_db_path: Optional[str] = None,
    cache_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    days = _resolve_window_days(window_days)
    since_iso = utc_hours_ago_iso(float(days) * 24.0)
    call_count, call_source = _resolve_gemini_call_count(
        since_iso=since_iso,
        ops_db_path=ops_db_path,
        cache_db_path=cache_db_path,
    )
    has_token_metering = _llm_result_has_token_metering()
    cost_estimate: Optional[float] = None
    cost_basis: Optional[str] = None
    if not has_token_metering:
        cost_basis = _COST_BASIS_NO_RATE
    paper_notional = _sum_paper_notional_traded_usd(
        since_iso=since_iso,
        forward_db_path=forward_db_path,
    )
    fee_estimate: Optional[float] = None
    fee_basis = _FEE_BASIS_NO_RATE
    return {
        "window_days": days,
        "gemini_call_count": int(call_count),
        "gemini_call_count_source": call_source,
        "gemini_cost_estimate_usd": cost_estimate,
        "cost_basis": cost_basis,
        "llm_token_metering_available": bool(has_token_metering),
        "paper_notional_traded_usd": paper_notional,
        "exchange_fee_estimate_usd": fee_estimate,
        "fee_basis": fee_basis,
    }


def persist_cost_report_weekly(
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


def run_cost_report_job(
    *,
    window_days: Optional[int] = None,
    forward_db_path: Optional[str] = None,
    ops_db_path: Optional[str] = None,
    cache_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """weekly_evolution pipeline hook — read-only aggregate."""
    if not cost_report_enabled():
        logger.info("cost report disabled — skip batch")
        return None

    summary = compute_weekly_cost_report_bg(
        window_days=_resolve_window_days(window_days),
        forward_db_path=forward_db_path,
        ops_db_path=ops_db_path,
        cache_db_path=cache_db_path,
    )
    ok = persist_cost_report_weekly(summary, ops_db_path=ops_db_path)
    logger.info(
        "cost report window=%dd gemini_calls=%d notional=%.2f inserted=%s",
        summary["window_days"],
        summary["gemini_call_count"],
        summary["paper_notional_traded_usd"],
        ok,
    )
    return {
        "inserted": ok,
        "summary": summary,
    }
