"""
PUMP_DNA 선취매 룰 스캐너 → bitget_virtual_trade_history 기록.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

import bitget_shadow_tracking
from bitget_pump_forensics import PATTERN_KEYS, load_config, _extract_flags

DB_PATH = bitget_shadow_tracking.DB_PATH
STRATEGY_NAME = "bitget_forensics_pioneer"


def _load_scan_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_%_1D'").fetchall()
    return [r[0] for r in rows if "BTC_USDT" not in r[0]]


def _required_rules(cfg: Dict[str, Any]) -> List[str]:
    dna = cfg.get("PUMP_DNA", {})
    if not isinstance(dna, dict):
        return []
    g = dna.get("GLOBAL", {})
    if not isinstance(g, dict):
        return []
    rr = g.get("pre_emptive_rule", {})
    if not isinstance(rr, dict):
        return []
    return [k for k in PATTERN_KEYS if rr.get(k)]


def _matched(flags: Dict[str, bool], required: List[str]) -> bool:
    return bool(required) and all(flags.get(k, False) for k in required)


def run_forensics_pioneer():
    print("🔭 [Bitget Forensics Pioneer] 선취매 룰 스캔 시작...")
    cfg = load_config()
    required = _required_rules(cfg)
    if not required:
        print("⚠️ PUMP_DNA 합의 룰 없음, 스캔 스킵.")
        return

    conn = sqlite3.connect(DB_PATH, timeout=30)
    tables = _load_scan_tables(conn)
    hits = 0
    for tbl in tables:
        try:
            symbol = "_".join(tbl.split("_")[2:-1])
            df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC', conn)
            if len(df) < 25:
                continue
            t_idx = len(df) - 1
            flags = _extract_flags(df, t_idx)
            if not flags or not _matched(flags, required):
                continue

            cur = conn.cursor()
            bitget_shadow_tracking.init_shadow_tables(cur)
            logged_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            tags = "PUMP_DNA_PREEMPTIVE"
            bitget_shadow_tracking.insert_virtual_trade_row(
                cur,
                "spot",
                symbol,
                symbol,
                float(df["Close"].iloc[-1]),
                STRATEGY_NAME,
                tags,
                logged_at,
                position_side="LONG",
                timeframe="1D",
            )
            conn.commit()
            hits += 1
        except Exception:
            continue
    conn.close()
    print(f"✅ Pioneer 기록 완료: {hits}건")


if __name__ == "__main__":
    run_forensics_pioneer()
