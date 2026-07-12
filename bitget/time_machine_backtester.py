import json
import os
import sqlite3

import numpy as np
import pandas as pd

import memory_bounds

from bitget.config_hub import load_config
from bitget.infra.bounded_reads import sqlite_bitget_ohlcv_tables_sql
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.gc_cycle import flush_gc
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import (
    GC_AFTER_BACKTEST_TABLE,
    TIME_MACHINE_MAX_BARS_PER_TABLE,
    TIME_MACHINE_MAX_TABLES,
)
from bitget.infra.shared_db_connector import get_connection

DB_PATH = market_data_db_path()
logger = get_logger("bitget.time_machine_backtester")

CRASH_PERIODS = {
    "LUNA_CRASH_2022": {"start": "2022-05-01", "end": "2022-06-15"},
    "FTX_COLLAPSE_2022": {"start": "2022-11-01", "end": "2022-12-20"},
    "COVID_CRASH_2020": {"start": "2020-02-15", "end": "2020-04-30"},
}


def _load_tables(conn, *, max_tables: int | None = None):
    cap = int(max_tables if max_tables is not None else TIME_MACHINE_MAX_TABLES)
    sql, params = sqlite_bitget_ohlcv_tables_sql(limit=cap)
    rows = conn.execute(sql, params).fetchall()
    tfs = ("_1D", "_4H", "_2H", "_1H")
    return [r[0] for r in rows if any(tf in r[0] for tf in tfs)]


def _extract_symbol(table_name: str):
    p = table_name.split("_")
    if len(p) < 4:
        return table_name
    return "_".join(p[2:-1])


def _ret(entry, now, side):
    return ((now - entry) / entry) * 100.0 if side == "LONG" else ((entry - now) / entry) * 100.0


def run_time_machine_backtest(period_key="FTX_COLLAPSE_2022", leverage=3.0):
    if period_key not in CRASH_PERIODS:
        raise ValueError(f"Unknown period: {period_key}")
    period = CRASH_PERIODS[period_key]
    cfg = load_config()
    mae_sl = float(cfg.get("DYNAMIC_MAE_SL", -3.5))
    mfe_tp = float(cfg.get("DYNAMIC_MFE_TP", 10.0))

    conn = get_connection(DB_PATH, read_only=True)
    tables = _load_tables(conn)
    results = []
    for tbl in tables:
        try:
            range_sql, range_params = memory_bounds.ohlcv_date_range_sql(
                start=period["start"],
                end=period["end"],
                bar_limit=TIME_MACHINE_MAX_BARS_PER_TABLE,
            )
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close FROM "{tbl}"{range_sql}',
                conn,
                params=range_params,
            )
            if len(df) < 50:
                del df
                continue
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            test_df = df.dropna(subset=["Date"]).copy()
            if len(test_df) < 20:
                del df
                del test_df
                continue
            symbol = _extract_symbol(tbl)
            for side in ("LONG", "SHORT"):
                entry = float(test_df["Close"].iloc[0])
                liq = False
                final_ret = 0.0
                for _, row in test_df.iloc[1:].iterrows():
                    hi = float(row["High"])
                    lo = float(row["Low"])
                    close = float(row["Close"])
                    cur_ret = _ret(entry, close, side)
                    bar_mae = _ret(entry, lo if side == "LONG" else hi, side)
                    bar_mfe = _ret(entry, hi if side == "LONG" else lo, side)

                    if (bar_mae * leverage) <= -100.0:
                        liq = True
                        final_ret = -100.0 / leverage
                        break
                    if bar_mae <= mae_sl:
                        final_ret = mae_sl
                        break
                    if bar_mfe >= mfe_tp:
                        final_ret = mfe_tp
                        break
                    final_ret = cur_ret

                results.append(
                    {
                        "period": period_key,
                        "symbol": symbol,
                        "side": side,
                        "final_ret": float(final_ret),
                        "roe": float(final_ret * leverage),
                        "liquidated": int(liq),
                        "leverage": float(leverage),
                    }
                )
            del df
            del test_df
            flush_gc(label=GC_AFTER_BACKTEST_TABLE)
        except Exception:
            continue
    conn.close()
    flush_gc(label="backtest_complete")

    if not results:
        logger.warning("time machine: no testable data for period=%s", period_key)
        return pd.DataFrame()

    res = pd.DataFrame(results)
    for side in ("LONG", "SHORT"):
        sub = res[res["side"] == side]
        if sub.empty:
            continue
        wr = (sub["final_ret"] > 0).mean() * 100.0
        liq_rate = sub["liquidated"].mean() * 100.0
        avg_ret = sub["final_ret"].mean()
        logger.info(
            "[%s] %s | win=%.2f%% | avg_ret=%+.2f%% | liq=%.2f%%",
            period_key,
            side,
            wr,
            avg_ret,
            liq_rate,
        )
    del results
    flush_gc(label="backtest_result_frame")
    return res


if __name__ == "__main__":
    run_time_machine_backtest("LUNA_CRASH_2022", leverage=3.0)
    run_time_machine_backtest("FTX_COLLAPSE_2022", leverage=3.0)
