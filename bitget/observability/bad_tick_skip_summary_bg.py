"""
C-1b — weekly bad_tick_filtered skip summary (read-only).

Reads ``bitget_ops_events.sqlite`` ``bad_tick_filtered`` rows; writes one
``bad_tick_skip_summary_weekly`` ops_events row — no new tables/DB/cron.
"""
from __future__ import annotations

import json
import logging
import os
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from bitget.infra.clock import utc_hours_ago_iso, utc_now_iso

logger = logging.getLogger(__name__)

_BAD_TICK_FILTERED_EVENT = "bad_tick_filtered"
_SUMMARY_EVENT = "bad_tick_skip_summary_weekly"
_SUMMARY_COMPONENT = "observability.bad_tick"

# ops_events scan-volume denominator candidates (v1 — none exist today).
_DENOMINATOR_EVENT_CANDIDATES = (
    "scan_funnel_summary",
    "scan.universe_count",
    "gauge.scan_universe",
)


@dataclass
class BadTickSkipSummary:
    window_days: int
    total_skips: int
    skip_rate_pct: Optional[float]
    denominator_count: Optional[int]
    denominator_source: Optional[str]
    by_symbol: List[Dict[str, Any]] = field(default_factory=list)
    by_market_type: List[Dict[str, Any]] = field(default_factory=list)
    by_reason: List[Dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> Dict[str, Any]:
        return asdict(self)


def bad_tick_skip_summary_enabled() -> bool:
    env = os.environ.get("BAD_TICK_SKIP_SUMMARY_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("BAD_TICK_SKIP_SUMMARY_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import BAD_TICK_SKIP_SUMMARY_ENABLED

    return bool(BAD_TICK_SKIP_SUMMARY_ENABLED)


def _resolve_window_days(window_days: Optional[int] = None) -> int:
    if window_days is not None:
        return max(1, int(window_days))
    env = os.environ.get("BAD_TICK_SKIP_SUMMARY_WINDOW_DAYS")
    if env is not None and str(env).strip():
        try:
            return max(1, int(float(env)))
        except (TypeError, ValueError):
            pass
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("BAD_TICK_SKIP_SUMMARY_WINDOW_DAYS", None)
        if raw is not None:
            return max(1, int(float(raw)))
    except Exception:
        pass
    from bitget.infra.memory_policy import BAD_TICK_SKIP_SUMMARY_WINDOW_DAYS

    return max(1, int(BAD_TICK_SKIP_SUMMARY_WINDOW_DAYS))


def _ops_db_path() -> str:
    from bitget.infra.data_paths import ops_events_db_path

    return ops_events_db_path()


def _load_bad_tick_filtered_rows(
    *,
    window_days: int,
    db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    path = db_path or _ops_db_path()
    if not path or not os.path.isfile(path):
        return []
    since = utc_hours_ago_iso(float(window_days) * 24.0)
    rows: List[Dict[str, Any]] = []
    try:
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(path, read_only=True, check_same_thread=False)
        try:
            cur = conn.execute(
                """
                SELECT ts_utc, component, payload_json
                FROM ops_events
                WHERE event = ? AND ts_utc >= ?
                ORDER BY id ASC
                """,
                (_BAD_TICK_FILTERED_EVENT, since),
            )
            for ts_utc, component, payload_json in cur.fetchall():
                try:
                    payload = json.loads(payload_json or "{}")
                except json.JSONDecodeError:
                    payload = {}
                if not isinstance(payload, dict):
                    payload = {}
                rows.append(
                    {
                        "ts_utc": ts_utc,
                        "component": component,
                        "symbol": str(payload.get("symbol") or "UNKNOWN"),
                        "market_type": str(payload.get("market_type") or "unknown"),
                        "reason": str(payload.get("reason") or "unknown"),
                        "scanner": str(payload.get("scanner") or ""),
                    }
                )
        finally:
            conn.close()
    except Exception as ex:
        logger.warning("bad_tick skip summary load failed: %s", ex)
        return []
    return rows


def _find_ops_events_scan_denominator(
    *,
    window_days: int,
    db_path: Optional[str] = None,
) -> tuple[Optional[int], Optional[str]]:
    """Return (denominator_count, source_event) if an ops_events scan counter exists."""
    path = db_path or _ops_db_path()
    if not path or not os.path.isfile(path):
        return None, None
    since = utc_hours_ago_iso(float(window_days) * 24.0)
    try:
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(path, read_only=True, check_same_thread=False)
        try:
            for ev in _DENOMINATOR_EVENT_CANDIDATES:
                row = conn.execute(
                    """
                    SELECT payload_json
                    FROM ops_events
                    WHERE event = ? AND ts_utc >= ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (ev, since),
                ).fetchone()
                if not row:
                    continue
                try:
                    payload = json.loads(row[0] or "{}")
                except json.JSONDecodeError:
                    payload = {}
                if not isinstance(payload, dict):
                    continue
                for key in ("universe_size", "scan_count", "total_scans", "count"):
                    if key in payload:
                        try:
                            val = int(float(payload[key]))
                        except (TypeError, ValueError):
                            continue
                        if val > 0:
                            return val, ev
        finally:
            conn.close()
    except Exception:
        return None, None
    return None, None


def _counter_groups(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    c: Counter[str] = Counter()
    for r in rows:
        c[str(r.get(key) or "unknown")] += 1
    return [{"key": k, "count": int(v)} for k, v in sorted(c.items(), key=lambda x: (-x[1], x[0]))]


def compute_bad_tick_skip_summary_bg(
    window_days: int = 7,
    *,
    ops_db_path: Optional[str] = None,
) -> BadTickSkipSummary:
    days = _resolve_window_days(window_days)
    rows = _load_bad_tick_filtered_rows(window_days=days, db_path=ops_db_path)
    total = len(rows)
    denom, denom_src = _find_ops_events_scan_denominator(window_days=days, db_path=ops_db_path)
    skip_rate: Optional[float] = None
    if denom is not None and denom > 0:
        skip_rate = round(100.0 * float(total) / float(denom), 6)
    return BadTickSkipSummary(
        window_days=days,
        total_skips=total,
        skip_rate_pct=skip_rate,
        denominator_count=denom,
        denominator_source=denom_src,
        by_symbol=_counter_groups(rows, "symbol"),
        by_market_type=_counter_groups(rows, "market_type"),
        by_reason=_counter_groups(rows, "reason"),
    )


def persist_bad_tick_skip_summary_weekly(
    summary: BadTickSkipSummary,
    *,
    ops_db_path: Optional[str] = None,
) -> bool:
    from bitget.infra.ops_logger import insert_ops_event

    payload = summary.to_payload()
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


def run_bad_tick_skip_summary_job(
    *,
    window_days: Optional[int] = None,
    ops_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """weekly_evolution pipeline hook — read-only aggregate."""
    if not bad_tick_skip_summary_enabled():
        logger.info("bad_tick skip summary disabled — skip batch")
        return None

    summary = compute_bad_tick_skip_summary_bg(
        window_days=_resolve_window_days(window_days),
        ops_db_path=ops_db_path,
    )
    ok = persist_bad_tick_skip_summary_weekly(summary, ops_db_path=ops_db_path)
    logger.info(
        "bad_tick skip summary window=%dd skips=%d rate=%s denom=%s inserted=%s",
        summary.window_days,
        summary.total_skips,
        summary.skip_rate_pct,
        summary.denominator_source,
        ok,
    )
    return {
        "inserted": ok,
        "summary": summary.to_payload(),
    }
