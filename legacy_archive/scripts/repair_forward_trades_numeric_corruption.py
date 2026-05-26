#!/usr/bin/env python3
"""forward_trades REAL 컬럼 BLOB/bytes 오염 탐지·복구 (일회성 운영 스크립트)."""
from __future__ import annotations

import argparse
import sqlite3
import struct
import sys
from pathlib import Path
from typing import Optional

import numpy as np

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from market_db_paths import MARKET_DATA_DB_PATH

REAL_COLS = (
    "entry_price",
    "max_high",
    "min_low",
    "up_vol_sum",
    "down_vol_sum",
    "final_ret",
    "mfe",
    "total_score",
    "dyn_rs",
    "dyn_cpv",
    "dyn_tb",
    "v_cpv",
    "v_yang",
    "v_rs",
    "v_energy",
    "entry_atr",
    "invest_amount",
    "sim_kelly_invest",
    "sim_kelly_risk_pct",
    "market_breadth",
    "entry_breadth",
    "entry_cos_score",
    "entry_dtw_score",
)


def blob_to_float(b: bytes) -> Optional[float]:
    for fmt in ("<d", "<f", ">d", ">f"):
        if len(b) != struct.calcsize(fmt):
            continue
        try:
            x = float(struct.unpack(fmt, b)[0])
            if np.isfinite(x):
                return x
        except struct.error:
            continue
    return None


def scan(conn: sqlite3.Connection) -> list[tuple]:
    bad: list[tuple] = []
    for col in REAL_COLS:
        try:
            rows = conn.execute(
                f"SELECT id, market, code, status FROM forward_trades "
                f"WHERE typeof({col}) = 'blob'",
            ).fetchall()
        except sqlite3.OperationalError:
            continue
        for rid, mkt, code, st in rows:
            raw = conn.execute(
                f"SELECT {col} FROM forward_trades WHERE id = ?", (rid,)
            ).fetchone()
            if raw and isinstance(raw[0], (bytes, bytearray)):
                bad.append((rid, mkt, code, st, col, bytes(raw[0])))
    return bad


def repair_row(
    conn: sqlite3.Connection, rid: int, col: str, raw: bytes, apply: bool
) -> str:
    x = blob_to_float(raw)
    if x is None or not (0 < abs(x) < 1e10):
        sql = (
            "UPDATE forward_trades SET status='CLOSED_LOSS', final_ret=-15.0, "
            "exit_reason=? WHERE id=? AND status='OPEN'"
        )
        args = (f"DATA_CORRUPT_{col}", rid)
        action = "FORCE_CLOSE"
    else:
        sql = f"UPDATE forward_trades SET {col}=? WHERE id=?"
        args = (x, rid)
        action = f"SET_{col}={x}"
    if apply:
        conn.execute(sql, args)
    return action


def main() -> int:
    p = argparse.ArgumentParser(description="Repair BLOB corruption in forward_trades")
    p.add_argument("--db", default=MARKET_DATA_DB_PATH, help="market_data.sqlite path")
    p.add_argument(
        "--apply",
        action="store_true",
        help="write changes (default: dry-run only)",
    )
    args = p.parse_args()
    db_path = args.db
    if not Path(db_path).is_file():
        print(f"ERROR: DB not found: {db_path}")
        return 1

    conn = sqlite3.connect(db_path, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL;")
    bad = scan(conn)
    print(f"found {len(bad)} blob cell(s) in {db_path}")
    for rid, mkt, code, st, col, raw in bad:
        act = repair_row(conn, rid, col, raw, args.apply)
        print(
            f"id={rid} {mkt} {code} status={st} col={col} "
            f"hex={raw.hex()[:32]} -> {act}"
        )
    if args.apply:
        conn.commit()
        print("committed")
    else:
        print("dry-run only; re-run with --apply to write")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
