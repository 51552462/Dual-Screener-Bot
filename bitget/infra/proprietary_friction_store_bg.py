"""Bitget proprietary friction store — scan_funnel_snapshot · regime_friction_event."""
from __future__ import annotations

import logging
import sqlite3
from typing import Optional

from bitget.forward.shared import DB_PATH
from bitget.infra.shared_db_connector import connect

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS scan_funnel_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    market TEXT NOT NULL,
    universe_size INTEGER NOT NULL DEFAULT 0,
    survivors INTEGER NOT NULL DEFAULT 0,
    pass_rate_pct REAL NOT NULL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS idx_scan_funnel_ts ON scan_funnel_snapshot(ts DESC);
CREATE INDEX IF NOT EXISTS idx_scan_funnel_mkt_ts ON scan_funnel_snapshot(market, ts DESC);

CREATE TABLE IF NOT EXISTS regime_friction_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    market TEXT NOT NULL,
    event_type TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_regime_friction_date ON regime_friction_event(date DESC, market);
"""


def friction_db_path() -> str:
    return DB_PATH


def normalize_friction_market(market_type: str) -> str:
    m = str(market_type or "spot").strip().lower()
    return "FUTURES" if m in ("futures", "fut", "linear") else "SPOT"


def ensure_proprietary_friction_schema(
    cursor: Optional[sqlite3.Cursor] = None,
    *,
    db_path: Optional[str] = None,
) -> None:
    if cursor is not None:
        cursor.executescript(_DDL)
        return
    path = db_path or friction_db_path()
    if not path:
        return
    try:
        with connect(path) as conn:
            conn.executescript(_DDL)
    except sqlite3.Error as ex:
        logger.warning("bitget proprietary_friction schema skip: %s", ex)


def insert_scan_funnel_snapshot(
    *,
    ts: str,
    market: str,
    universe_size: int,
    survivors: int,
    pass_rate_pct: float,
    db_path: Optional[str] = None,
) -> None:
    path = db_path or friction_db_path()
    if not path:
        return
    try:
        ensure_proprietary_friction_schema(db_path=path)
        with connect(path) as conn:
            conn.execute(
                """
                INSERT INTO scan_funnel_snapshot
                    (ts, market, universe_size, survivors, pass_rate_pct)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(ts)[:19],
                    normalize_friction_market(market),
                    int(universe_size),
                    int(survivors),
                    round(float(pass_rate_pct), 6),
                ),
            )
        try:
            from bitget.infra.memory_retention import maybe_run_bitget_retention_after_write

            maybe_run_bitget_retention_after_write()
        except Exception:
            pass
    except sqlite3.Error as ex:
        logger.debug("bitget scan_funnel_snapshot insert skip: %s", ex)


def insert_regime_friction_event(
    *,
    date: str,
    market: str,
    event_type: str,
    db_path: Optional[str] = None,
) -> None:
    path = db_path or friction_db_path()
    d = str(date or "")[:10]
    if len(d) != 10:
        return
    et = str(event_type or "").strip().upper()[:64]
    if not et:
        return
    try:
        ensure_proprietary_friction_schema(db_path=path)
        with connect(path) as conn:
            conn.execute(
                """
                INSERT INTO regime_friction_event (date, market, event_type)
                VALUES (?, ?, ?)
                """,
                (d, normalize_friction_market(market), et),
            )
        try:
            from bitget.infra.memory_retention import maybe_run_bitget_retention_after_write

            maybe_run_bitget_retention_after_write()
        except Exception:
            pass
    except sqlite3.Error as ex:
        logger.debug("bitget regime_friction_event insert skip: %s", ex)
