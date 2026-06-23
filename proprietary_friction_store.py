"""
Proprietary Regime — 일일 마찰 데이터 무음 적재 (Shadow · Meta 미연동).

scan_funnel_snapshot · regime_friction_event
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Optional

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


def _db_path() -> str:
    try:
        from market_db_paths import MARKET_DATA_DB_PATH

        return MARKET_DATA_DB_PATH
    except Exception:
        from forward.shared import DB_PATH

        return DB_PATH


def ensure_proprietary_friction_schema(
    cursor: Optional[sqlite3.Cursor] = None,
    *,
    db_path: Optional[str] = None,
) -> None:
    if cursor is not None:
        cursor.executescript(_DDL)
        return
    path = db_path or _db_path()
    if not path:
        return
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.executescript(_DDL)
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("proprietary_friction schema skip: %s", ex)


def insert_scan_funnel_snapshot(
    *,
    ts: str,
    market: str,
    universe_size: int,
    survivors: int,
    pass_rate_pct: float,
    db_path: Optional[str] = None,
) -> None:
    """평일 무음 적재 — PRI 계산 없음."""
    path = db_path or _db_path()
    if not path:
        return
    try:
        ensure_proprietary_friction_schema(db_path=path)
        conn = sqlite3.connect(path, timeout=15)
        try:
            conn.execute(
                """
                INSERT INTO scan_funnel_snapshot
                    (ts, market, universe_size, survivors, pass_rate_pct)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(ts)[:19],
                    str(market or "").upper()[:8],
                    int(universe_size),
                    int(survivors),
                    round(float(pass_rate_pct), 6),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.debug("scan_funnel_snapshot insert skip: %s", ex)


def insert_regime_friction_event(
    *,
    date: str,
    market: str,
    event_type: str,
    db_path: Optional[str] = None,
) -> None:
    """평일 무음 적재 — DM-A 등 이벤트."""
    path = db_path or _db_path()
    if not path:
        return
    d = str(date or "")[:10]
    if len(d) != 10:
        return
    et = str(event_type or "").strip().upper()[:64]
    if not et:
        return
    try:
        ensure_proprietary_friction_schema(db_path=path)
        conn = sqlite3.connect(path, timeout=15)
        try:
            conn.execute(
                """
                INSERT INTO regime_friction_event (date, market, event_type)
                VALUES (?, ?, ?)
                """,
                (d, str(market or "").upper()[:8], et),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.debug("regime_friction_event insert skip: %s", ex)
