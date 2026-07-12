"""
Bitget 메모리·디스크 retention — memory_policy Tier-2 SSOT executor.

24/7 코인 데몬에서 append-only 테이블·ops_events·task_queue 가 무한 성장하지 않도록
일 1회(또는 throttled) purge 한다.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Optional

import memory_bounds

from bitget.infra.data_paths import market_data_db_path, ops_events_db_path
from bitget.infra.memory_policy import (
    OPS_EVENTS_KEEP_DAYS,
    REAL_EXECUTION_KEEP_DAYS,
    REAL_EXECUTION_KEEP_LAST,
    REGIME_FRICTION_KEEP_DAYS,
    RETENTION_SWEEP_MIN_INTERVAL_SEC,
    SCAN_FUNNEL_KEEP_DAYS,
    SCAN_FUNNEL_KEEP_LAST,
    SHADOW_HISTORY_KEEP_DAYS,
)
from bitget.infra.shared_db_connector import connect

logger = logging.getLogger(__name__)

_prune_gate = memory_bounds.ThrottledCallback(interval_sec=RETENTION_SWEEP_MIN_INTERVAL_SEC)


def _purge_market_data_tables(conn: sqlite3.Connection) -> dict[str, int]:
    out: dict[str, int] = {}
    for table, col, days in (
        ("bitget_blocked_trade_history", "blocked_at", SHADOW_HISTORY_KEEP_DAYS),
        ("bitget_virtual_trade_history", "logged_at", SHADOW_HISTORY_KEEP_DAYS),
        ("scan_funnel_snapshot", "ts", SCAN_FUNNEL_KEEP_DAYS),
        ("regime_friction_event", "date", REGIME_FRICTION_KEEP_DAYS),
    ):
        try:
            conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
        except sqlite3.OperationalError:
            continue
        try:
            out[table] = memory_bounds.prune_sqlite_by_date_prefix(conn, table, col, days)
        except sqlite3.Error as ex:
            logger.debug("bitget retention skip %s: %s", table, ex)
    try:
        conn.execute("SELECT 1 FROM scan_funnel_snapshot LIMIT 1")
        out["scan_funnel_snapshot_keep_last"] = memory_bounds.prune_sqlite_keep_last_ids(
            conn, "scan_funnel_snapshot", SCAN_FUNNEL_KEEP_LAST
        )
    except sqlite3.OperationalError:
        pass
    except sqlite3.Error as ex:
        logger.debug("bitget scan_funnel keep_last skip: %s", ex)

    try:
        conn.execute("SELECT 1 FROM bitget_real_execution LIMIT 1")
        cutoff = memory_bounds.prune_sqlite_by_date_prefix(
            conn, "bitget_real_execution", "created_at", REAL_EXECUTION_KEEP_DAYS
        )
        if cutoff:
            out["bitget_real_execution_by_age"] = cutoff
        before = conn.execute(
            "SELECT COUNT(*) FROM bitget_real_execution"
        ).fetchone()
        n_before = int(before[0] or 0) if before else 0
        conn.execute(
            """
            DELETE FROM bitget_real_execution
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id FROM bitget_real_execution
                    ORDER BY id DESC
                    LIMIT ?
                )
            )
            """,
            (int(REAL_EXECUTION_KEEP_LAST),),
        )
        after = conn.execute(
            "SELECT COUNT(*) FROM bitget_real_execution"
        ).fetchone()
        n_after = int(after[0] or 0) if after else 0
        removed = max(0, n_before - n_after)
        if removed:
            out["bitget_real_execution_keep_last"] = removed
    except sqlite3.OperationalError:
        pass
    except sqlite3.Error as ex:
        logger.debug("bitget real_execution retention skip: %s", ex)

    return out


def run_bitget_market_data_retention(*, db_path: Optional[str] = None) -> dict[str, int]:
    """bitget market_data.sqlite — shadow·friction 테이블 purge."""
    path = db_path or market_data_db_path()
    if not path:
        return {}
    try:
        with connect(path) as conn:
            stats = _purge_market_data_tables(conn)
        total = sum(v for v in stats.values() if v > 0)
        if total:
            logger.info("bitget market_data retention: %s", stats)
        return stats
    except Exception as ex:
        logger.warning("bitget market_data retention failed: %s", ex)
        return {}


def run_bitget_ops_events_retention(*, db_path: Optional[str] = None) -> int:
    """bitget_ops_events.sqlite — OPS_EVENTS_KEEP_DAYS 이전 행 DELETE."""
    path = db_path or ops_events_db_path()
    if not path:
        return 0
    try:
        with connect(path) as conn:
            n = memory_bounds.prune_ops_events_older_than_days(
                conn, keep_days=OPS_EVENTS_KEEP_DAYS
            )
        if n:
            logger.info("bitget ops_events retention removed %s rows", n)
        return n
    except Exception as ex:
        logger.warning("bitget ops_events retention failed: %s", ex)
        return 0


def run_bitget_task_queue_retention(*, db_path: Optional[str] = None) -> dict[str, int]:
    """bitget_task_queue.sqlite — DONE/FAILED purge + stuck RUNNING heal."""
    try:
        from bitget.infra.task_orchestrator import purge_terminal_tasks

        return purge_terminal_tasks(db_path=db_path)
    except Exception as ex:
        logger.warning("bitget task_queue retention failed: %s", ex)
        return {}


def run_bitget_retention_sweep(*, force: bool = False) -> dict[str, int]:
    """Tier-2 전체 sweep — force=True 면 throttle 무시."""
    if not force and not _prune_gate.due():
        return {}
    stats = run_bitget_market_data_retention()
    ops_n = run_bitget_ops_events_retention()
    if ops_n:
        stats["ops_events"] = ops_n
    tq = run_bitget_task_queue_retention()
    for k, v in tq.items():
        if v > 0:
            stats[k] = v
    return stats


def maybe_run_bitget_retention_after_write() -> None:
    """INSERT 직후 가벼운 hook — RETENTION_SWEEP_MIN_INTERVAL_SEC 당 최대 1회."""
    run_bitget_retention_sweep(force=False)
