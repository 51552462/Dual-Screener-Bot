"""FULL-BT-2 checkpoint — bitget_full_bt.sqlite only (paper/config_kv untouched)."""
from __future__ import annotations

import sqlite3
from typing import Optional

from bitget.full_bt.paths import full_bt_db_path
from bitget.infra.clock import utc_datetime_str

_CHECKPOINT_SCHEMA = """
CREATE TABLE IF NOT EXISTS bitget_full_bt_checkpoint (
    run_id TEXT NOT NULL,
    market_type TEXT NOT NULL,
    shard_index INTEGER NOT NULL DEFAULT 0,
    completed_symbol TEXT NOT NULL,
    completed_batch_idx INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, market_type, completed_symbol, completed_batch_idx)
);
"""


def ensure_checkpoint_schema(db_path: Optional[str] = None) -> str:
    path = db_path or full_bt_db_path()
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_CHECKPOINT_SCHEMA)
        conn.commit()
    finally:
        conn.close()
    return path


def load_full_bt_checkpoint(
    run_id: str, market_type: str, *, db_path: Optional[str] = None
) -> dict | None:
    path = ensure_checkpoint_schema(db_path)
    mt = str(market_type).lower()
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            """
            SELECT shard_index, completed_symbol, completed_batch_idx, updated_at
            FROM bitget_full_bt_checkpoint
            WHERE run_id=? AND market_type=?
            ORDER BY shard_index, completed_symbol, completed_batch_idx
            """,
            (run_id, mt),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    done = {(str(r[1]), int(r[2])) for r in rows}
    last = rows[-1]
    return {
        "run_id": run_id,
        "market_type": mt,
        "completed": done,
        "last_shard_index": int(last[0]),
        "last_symbol": str(last[1]),
        "last_batch_idx": int(last[2]),
        "updated_at": str(last[3]),
    }


def save_full_bt_checkpoint(
    run_id: str,
    market_type: str,
    symbol: str,
    batch_idx: int,
    *,
    shard_index: int = 0,
    db_path: Optional[str] = None,
) -> None:
    path = ensure_checkpoint_schema(db_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO bitget_full_bt_checkpoint (
                run_id, market_type, shard_index, completed_symbol,
                completed_batch_idx, updated_at
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                run_id,
                str(market_type).lower(),
                int(shard_index),
                str(symbol),
                int(batch_idx),
                utc_datetime_str(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
