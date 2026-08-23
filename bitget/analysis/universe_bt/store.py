"""Write UNIVERSE-BT results only to bitget_universe_bt.sqlite."""
from __future__ import annotations

import sqlite3
from typing import Any, Iterable, Optional

from bitget.infra.clock import utc_datetime_str
from bitget.analysis.universe_bt.paths import universe_bt_db_path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bitget_universe_bt_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bar_ts INTEGER NOT NULL,
    regime_label TEXT,
    candidate_generated INTEGER NOT NULL DEFAULT 0,
    gate_passed INTEGER NOT NULL DEFAULT 0,
    virtual_entry INTEGER NOT NULL DEFAULT 0,
    side TEXT,
    exit_trigger TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ubt_run ON bitget_universe_bt_results(run_id);
CREATE INDEX IF NOT EXISTS idx_ubt_sym ON bitget_universe_bt_results(market_type, symbol, bar_ts);
"""


def ensure_results_schema(db_path: Optional[str] = None) -> str:
    path = db_path or universe_bt_db_path()
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
    finally:
        conn.close()
    return path


def write_bt_results(rows: list[dict], *, db_path: Optional[str] = None) -> int:
    """Persist rows to isolated results DB only. No paper / config contact."""
    if not rows:
        return 0
    path = ensure_results_schema(db_path)
    now = utc_datetime_str()
    conn = sqlite3.connect(path)
    try:
        conn.executemany(
            """
            INSERT INTO bitget_universe_bt_results (
                run_id, market_type, symbol, bar_ts, regime_label,
                candidate_generated, gate_passed, virtual_entry, side,
                exit_trigger, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    str(r.get("run_id") or ""),
                    str(r.get("market_type") or "").lower(),
                    str(r.get("symbol") or ""),
                    int(r.get("bar_ts") or 0),
                    r.get("regime_label"),
                    int(bool(r.get("candidate_generated"))),
                    int(bool(r.get("gate_passed"))),
                    int(bool(r.get("virtual_entry"))),
                    r.get("side"),
                    r.get("exit_trigger"),  # C3: always None
                    str(r.get("created_at") or now),
                )
                for r in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def count_results(run_id: str, *, db_path: Optional[str] = None) -> int:
    path = ensure_results_schema(db_path)
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM bitget_universe_bt_results WHERE run_id=?",
            (run_id,),
        ).fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()
