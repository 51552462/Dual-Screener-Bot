"""
PUMP_DNA 선취매 룰 스캐너 → bitget_virtual_trade_history 기록.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

import pandas as pd

import memory_bounds

import bitget.shadow_tracking as bitget_shadow_tracking
from bitget.infra.bounded_reads import sqlite_bitget_ohlcv_1d_tables_sql
from bitget.infra.clock import utc_datetime_str
from bitget.infra.gc_cycle import flush_gc
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import GC_AFTER_OHLCV_BATCH, OHLCV_FORENSICS_BAR_LIMIT
from bitget.infra.shared_db_connector import get_connection
from bitget.pump_forensics import PATTERN_KEYS, load_config, _extract_flags

DB_PATH = bitget_shadow_tracking.DB_PATH
STRATEGY_NAME = "bitget_forensics_pioneer"
logger = get_logger("bitget.forensics_pioneer")


def _load_scan_tables(conn: sqlite3.Connection) -> List[str]:
    sql, params = sqlite_bitget_ohlcv_1d_tables_sql(exclude_btc=True)
    rows = conn.execute(sql, params).fetchall()
    return [r[0] for r in rows]


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
    logger.info("[Forensics Pioneer] preemptive rule scan start")
    cfg = load_config()
    required = _required_rules(cfg)
    if not required:
        logger.warning("PUMP_DNA consensus rules missing — scan skip")
        return

    conn = get_connection(DB_PATH, read_only=True)
    tables = _load_scan_tables(conn)
    hits = 0
    for tbl in tables:
        try:
            symbol = "_".join(tbl.split("_")[2:-1])
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}"'
                f"{memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_FORENSICS_BAR_LIMIT)}",
                conn,
            )
            if not df.empty:
                df = df.sort_values("Date")
            if len(df) < 25:
                continue
            t_idx = len(df) - 1
            flags = _extract_flags(df, t_idx)
            if not flags or not _matched(flags, required):
                continue

            cur = conn.cursor()
            bitget_shadow_tracking.init_shadow_tables(cur)
            logged_at = utc_datetime_str()
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
            del df
            flush_gc(label=GC_AFTER_OHLCV_BATCH)
        except Exception as e:
            log_exception(logger, "forensics pioneer table skip %s: %s", tbl, e)
            continue
    conn.close()
    flush_gc(label="forensics_pioneer_complete")
    logger.info("Pioneer records complete: %s", hits)


if __name__ == "__main__":
    run_forensics_pioneer()
