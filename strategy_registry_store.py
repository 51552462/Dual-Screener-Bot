"""
strategy_registry · strategy_quality_daily — MetaGovernor 무기고 DB SSOT.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1

_DDL = """
CREATE TABLE IF NOT EXISTS strategy_registry (
    strategy_id TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    group_key TEXT NOT NULL,
    state TEXT NOT NULL,
    display_name TEXT,
    capital_mult REAL DEFAULT 0,
    source TEXT,
    rolling_wr REAL,
    rolling_pf REAL,
    n_closed INTEGER DEFAULT 0,
    promoted_at TEXT,
    last_promoted_at TEXT,
    last_demoted_at TEXT,
    promote_reason TEXT,
    demote_reason TEXT,
    health_miss_streak INTEGER DEFAULT 0,
    updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_strategy_registry_state ON strategy_registry(state);
CREATE INDEX IF NOT EXISTS idx_strategy_registry_market ON strategy_registry(market, state);

CREATE TABLE IF NOT EXISTS strategy_quality_daily (
    strategy_id TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    market TEXT NOT NULL,
    rolling_wr REAL,
    rolling_pf REAL,
    below_live_threshold INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT,
    PRIMARY KEY (strategy_id, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_strategy_quality_daily_sid
    ON strategy_quality_daily(strategy_id, trade_date DESC);
"""


def _db_path() -> str:
    from market_db_paths import market_db_read_path

    return market_db_read_path()


def _kst_today() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")


def _now_iso() -> str:
    return datetime.now(pytz.timezone("UTC")).isoformat()


def ensure_strategy_registry_schema(db_path: Optional[str] = None) -> None:
    path = db_path or _db_path()
    if not path or not os.path.isfile(path):
        return
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            conn.executescript(_DDL)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("strategy_registry schema DDL skip: %s", ex)


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def load_registry_rows(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    path = db_path or _db_path()
    if not path or not os.path.isfile(path):
        return []
    ensure_strategy_registry_schema(path)
    try:
        conn = sqlite3.connect(path, timeout=60)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                "SELECT * FROM strategy_registry ORDER BY market, group_key"
            )
            return [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("load_registry_rows failed: %s", ex)
        return []


def upsert_registry_rows(rows: List[Dict[str, Any]], db_path: Optional[str] = None) -> None:
    path = db_path or _db_path()
    if not path:
        return
    ensure_strategy_registry_schema(path)
    if not rows:
        return
    now = _now_iso()
    sql = """
    INSERT INTO strategy_registry (
        strategy_id, market, group_key, state, display_name, capital_mult, source,
        rolling_wr, rolling_pf, n_closed, promoted_at, last_promoted_at, last_demoted_at,
        promote_reason, demote_reason, health_miss_streak, updated_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(strategy_id) DO UPDATE SET
        market=excluded.market,
        group_key=excluded.group_key,
        state=excluded.state,
        display_name=excluded.display_name,
        capital_mult=excluded.capital_mult,
        source=excluded.source,
        rolling_wr=excluded.rolling_wr,
        rolling_pf=excluded.rolling_pf,
        n_closed=excluded.n_closed,
        promoted_at=excluded.promoted_at,
        last_promoted_at=excluded.last_promoted_at,
        last_demoted_at=excluded.last_demoted_at,
        promote_reason=excluded.promote_reason,
        demote_reason=excluded.demote_reason,
        health_miss_streak=excluded.health_miss_streak,
        updated_at=excluded.updated_at
    """
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            for r in rows:
                sid = str(r.get("strategy_id") or "").strip()
                if not sid:
                    continue
                conn.execute(
                    sql,
                    (
                        sid,
                        str(r.get("market") or "KR").upper(),
                        str(r.get("group_key") or ""),
                        str(r.get("state") or "OBSERVING").upper(),
                        r.get("display_name"),
                        float(r.get("capital_mult", 0) or 0),
                        r.get("source"),
                        r.get("rolling_wr"),
                        r.get("rolling_pf"),
                        int(r.get("n_closed", 0) or 0),
                        r.get("promoted_at"),
                        r.get("last_promoted_at"),
                        r.get("last_demoted_at"),
                        r.get("promote_reason"),
                        r.get("demote_reason"),
                        int(r.get("health_miss_streak", 0) or 0),
                        r.get("updated_at") or now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("upsert_registry_rows failed: %s", ex)


def record_quality_daily(
    strategy_id: str,
    market: str,
    *,
    rolling_wr: Optional[float],
    rolling_pf: Optional[float],
    below_live_threshold: bool,
    trade_date: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    path = db_path or _db_path()
    if not path or not strategy_id:
        return
    ensure_strategy_registry_schema(path)
    td = trade_date or _kst_today()
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            conn.execute(
                """
                INSERT INTO strategy_quality_daily
                    (strategy_id, trade_date, market, rolling_wr, rolling_pf,
                     below_live_threshold, recorded_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(strategy_id, trade_date) DO UPDATE SET
                    rolling_wr=excluded.rolling_wr,
                    rolling_pf=excluded.rolling_pf,
                    below_live_threshold=excluded.below_live_threshold,
                    recorded_at=excluded.recorded_at
                """,
                (
                    strategy_id,
                    td,
                    str(market or "KR").upper(),
                    rolling_wr,
                    rolling_pf,
                    1 if below_live_threshold else 0,
                    _now_iso(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("record_quality_daily %s: %s", strategy_id, ex)


def consecutive_below_live_days(
    strategy_id: str,
    *,
    max_lookback: int = 14,
    db_path: Optional[str] = None,
) -> int:
    """최근 연속 below_live_threshold=1 일수 (오늘 포함, KST trade_date 내림차순)."""
    path = db_path or _db_path()
    if not path or not strategy_id:
        return 0
    ensure_strategy_registry_schema(path)
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            cur = conn.execute(
                """
                SELECT below_live_threshold FROM strategy_quality_daily
                WHERE strategy_id = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (strategy_id, max(1, int(max_lookback))),
            )
            streak = 0
            for (flag,) in cur.fetchall():
                if int(flag or 0) == 1:
                    streak += 1
                else:
                    break
            return streak
        finally:
            conn.close()
    except sqlite3.Error:
        return 0


def merge_registry_sources(
    db_rows: List[Dict[str, Any]],
    meta_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """DB 우선, meta 리스트로 보강."""
    by_sid: Dict[str, Dict[str, Any]] = {}
    for r in meta_rows:
        if isinstance(r, dict) and r.get("strategy_id"):
            by_sid[str(r["strategy_id"])] = dict(r)
    for r in db_rows:
        if isinstance(r, dict) and r.get("strategy_id"):
            sid = str(r["strategy_id"])
            if sid in by_sid:
                prev = by_sid[sid]
                merged = dict(prev)
                merged.update({k: v for k, v in r.items() if v is not None})
                by_sid[sid] = merged
            else:
                by_sid[sid] = dict(r)
    return list(by_sid.values())
