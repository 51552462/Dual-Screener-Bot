"""
deathmatch_arm_snapshot · deathmatch_champion · deathmatch_elimination_event — Battle Royal SSOT.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional

import pytz
from datetime import datetime

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS deathmatch_arm_snapshot (
    trade_date TEXT NOT NULL,
    market TEXT NOT NULL,
    arm_id TEXT NOT NULL,
    arm_kind TEXT NOT NULL,
    registry_state TEXT,
    label TEXT NOT NULL,
    n_closed INTEGER NOT NULL DEFAULT 0,
    n_valid INTEGER NOT NULL DEFAULT 0,
    mean_ret REAL,
    win_rate REAL,
    profit_factor REAL,
    mdd_pct REAL,
    vol_pct REAL,
    composite_score REAL,
    rank INTEGER,
    below_floor INTEGER NOT NULL DEFAULT 0,
    relative_exempt INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT,
    PRIMARY KEY (trade_date, market, arm_id)
);
CREATE INDEX IF NOT EXISTS idx_dm_arm_snap_mkt ON deathmatch_arm_snapshot(market, trade_date DESC);

CREATE TABLE IF NOT EXISTS deathmatch_champion (
    market TEXT PRIMARY KEY,
    champion_arm_id TEXT,
    champion_label TEXT,
    champion_registry_state TEXT,
    mean_ret REAL,
    win_rate REAL,
    composite_score REAL,
    n_valid INTEGER,
    as_of_date TEXT,
    run_id TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS deathmatch_elimination_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date TEXT NOT NULL,
    market TEXT NOT NULL,
    arm_id TEXT NOT NULL,
    prior_rank INTEGER,
    reason TEXT,
    proposed_action TEXT,
    relative_exempt INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_dm_elim_mkt ON deathmatch_elimination_event(market, event_date DESC);
"""

_SCORECARD_EXTRA_COLS = [
    ("expectancy", "REAL"),
    ("sum_ret", "REAL"),
    ("meta_mult", "REAL"),
    ("tail_loss_streak", "INTEGER"),
    ("kelly_path_ret", "REAL"),
    ("outperform_pp", "REAL"),
    ("hurdle_passed", "INTEGER"),
    ("champion_eligible", "INTEGER"),
    ("score_breakdown", "TEXT"),
]


def _migrate_snapshot_columns(conn: sqlite3.Connection) -> None:
    try:
        cur = conn.execute("PRAGMA table_info(deathmatch_arm_snapshot)")
        existing = {str(r[1]) for r in cur.fetchall()}
        for col, typ in _SCORECARD_EXTRA_COLS:
            if col not in existing:
                conn.execute(f"ALTER TABLE deathmatch_arm_snapshot ADD COLUMN {col} {typ}")
    except sqlite3.Error:
        pass


def _db_path() -> str:
    from market_db_paths import market_db_read_path

    return market_db_read_path()


def _kst_today() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")


def _now_str() -> str:
    return datetime.now(pytz.timezone("UTC")).isoformat()


def ensure_deathmatch_schema(db_path: Optional[str] = None) -> None:
    path = db_path or _db_path()
    if not path or not os.path.isfile(path):
        return
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            conn.executescript(_DDL)
            _migrate_snapshot_columns(conn)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("deathmatch schema DDL skip: %s", ex)


def save_battle_royal_result(
    market: str,
    arms: List[Dict[str, Any]],
    champion: Optional[Dict[str, Any]],
    *,
    trade_date: Optional[str] = None,
    run_id: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    path = db_path or _db_path()
    if not path:
        return
    ensure_deathmatch_schema(path)
    td = trade_date or _kst_today()
    now = _now_str()
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            _migrate_snapshot_columns(conn)
            for a in arms:
                conn.execute(
                    """
                    INSERT INTO deathmatch_arm_snapshot (
                        trade_date, market, arm_id, arm_kind, registry_state, label,
                        n_closed, n_valid, mean_ret, win_rate, profit_factor,
                        mdd_pct, vol_pct, composite_score, rank, below_floor,
                        relative_exempt, recorded_at,
                        expectancy, sum_ret, meta_mult, tail_loss_streak,
                        kelly_path_ret, outperform_pp, hurdle_passed, champion_eligible,
                        score_breakdown
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(trade_date, market, arm_id) DO UPDATE SET
                        arm_kind=excluded.arm_kind,
                        registry_state=excluded.registry_state,
                        label=excluded.label,
                        n_closed=excluded.n_closed,
                        n_valid=excluded.n_valid,
                        mean_ret=excluded.mean_ret,
                        win_rate=excluded.win_rate,
                        profit_factor=excluded.profit_factor,
                        mdd_pct=excluded.mdd_pct,
                        vol_pct=excluded.vol_pct,
                        composite_score=excluded.composite_score,
                        rank=excluded.rank,
                        below_floor=excluded.below_floor,
                        relative_exempt=excluded.relative_exempt,
                        recorded_at=excluded.recorded_at,
                        expectancy=excluded.expectancy,
                        sum_ret=excluded.sum_ret,
                        meta_mult=excluded.meta_mult,
                        tail_loss_streak=excluded.tail_loss_streak,
                        kelly_path_ret=excluded.kelly_path_ret,
                        outperform_pp=excluded.outperform_pp,
                        hurdle_passed=excluded.hurdle_passed,
                        champion_eligible=excluded.champion_eligible,
                        score_breakdown=excluded.score_breakdown
                    """,
                    (
                        td,
                        str(market).upper(),
                        str(a.get("arm_id") or ""),
                        str(a.get("arm_kind") or "REGISTRY"),
                        a.get("registry_state"),
                        str(a.get("label") or ""),
                        int(a.get("n_closed", 0) or 0),
                        int(a.get("n_valid", 0) or 0),
                        a.get("mean_ret"),
                        a.get("win_rate_pct"),
                        a.get("profit_factor"),
                        a.get("mdd_pct"),
                        a.get("vol_pct"),
                        a.get("composite_score"),
                        int(a.get("rank", 0) or 0),
                        1 if a.get("below_floor") else 0,
                        1 if a.get("relative_exempt") else 0,
                        now,
                        a.get("expectancy"),
                        a.get("sum_ret"),
                        a.get("meta_mult"),
                        int(a.get("tail_loss_streak", 0) or 0),
                        a.get("kelly_path_ret"),
                        a.get("outperform_pp"),
                        1 if a.get("hurdle_passed") else 0,
                        1 if a.get("champion_eligible") else 0,
                        a.get("score_breakdown"),
                    ),
                )
            if champion:
                conn.execute(
                    """
                    INSERT INTO deathmatch_champion (
                        market, champion_arm_id, champion_label, champion_registry_state,
                        mean_ret, win_rate, composite_score, n_valid, as_of_date, run_id, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(market) DO UPDATE SET
                        champion_arm_id=excluded.champion_arm_id,
                        champion_label=excluded.champion_label,
                        champion_registry_state=excluded.champion_registry_state,
                        mean_ret=excluded.mean_ret,
                        win_rate=excluded.win_rate,
                        composite_score=excluded.composite_score,
                        n_valid=excluded.n_valid,
                        as_of_date=excluded.as_of_date,
                        run_id=excluded.run_id,
                        updated_at=excluded.updated_at
                    """,
                    (
                        str(market).upper(),
                        champion.get("arm_id"),
                        champion.get("label"),
                        champion.get("registry_state"),
                        champion.get("mean_ret"),
                        champion.get("win_rate_pct"),
                        champion.get("composite_score"),
                        int(champion.get("n_valid", 0) or 0),
                        td,
                        run_id,
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("save_battle_royal_result failed: %s", ex)


def log_elimination_events(
    market: str,
    events: List[Dict[str, Any]],
    *,
    event_date: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    if not events:
        return
    path = db_path or _db_path()
    if not path:
        return
    ensure_deathmatch_schema(path)
    td = event_date or _kst_today()
    now = _now_str()
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            for ev in events:
                conn.execute(
                    """
                    INSERT INTO deathmatch_elimination_event (
                        event_date, market, arm_id, prior_rank, reason,
                        proposed_action, relative_exempt, recorded_at
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        td,
                        str(market).upper(),
                        str(ev.get("arm_id") or ""),
                        ev.get("prior_rank"),
                        str(ev.get("reason") or ""),
                        str(ev.get("proposed_action") or "STANDBY"),
                        1 if ev.get("relative_exempt") else 0,
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("log_elimination_events failed: %s", ex)


def load_champion(market: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    path = db_path or _db_path()
    if not path or not os.path.isfile(path):
        return None
    ensure_deathmatch_schema(path)
    try:
        conn = sqlite3.connect(path, timeout=60)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                "SELECT * FROM deathmatch_champion WHERE market = ?",
                (str(market).upper(),),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except sqlite3.Error:
        return None
