import json
import os
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")

CRASH_PERIODS = {
    "LUNA_CRASH_2022": {"start": "2022-05-01", "end": "2022-06-15"},
    "FTX_COLLAPSE_2022": {"start": "2022-11-01", "end": "2022-12-20"},
    "COVID_CRASH_2020": {"start": "2020-02-15", "end": "2020-04-30"},
}


def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _load_tables(conn):
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_%'").fetchall()
    return [r[0] for r in rows if any(tf in r[0] for tf in ("_1D", "_4H", "_2H", "_1H"))]


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

    conn = sqlite3.connect(DB_PATH, timeout=30)
    tables = _load_tables(conn)
    results = []
    for tbl in tables:
        try:
            df = pd.read_sql(f'SELECT Date, Open, High, Low, Close FROM "{tbl}" ORDER BY Date ASC', conn)
            if len(df) < 50:
                continue
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            test_df = df[(df["Date"] >= period["start"]) & (df["Date"] <= period["end"])].copy()
            if len(test_df) < 20:
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
        except Exception:
            continue
    conn.close()

    if not results:
        print("⚠️ 테스트 가능한 데이터가 없습니다.")
        return pd.DataFrame()

    res = pd.DataFrame(results)
    for side in ("LONG", "SHORT"):
        sub = res[res["side"] == side]
        if sub.empty:
            continue
        wr = (sub["final_ret"] > 0).mean() * 100.0
        liq_rate = sub["liquidated"].mean() * 100.0
        avg_ret = sub["final_ret"].mean()
        print(f"[{period_key}] {side} | 승률 {wr:.2f}% | 평균RET {avg_ret:+.2f}% | 강제청산율 {liq_rate:.2f}%")
    return res


if __name__ == "__main__":
    run_time_machine_backtest("LUNA_CRASH_2022", leverage=3.0)
    run_time_machine_backtest("FTX_COLLAPSE_2022", leverage=3.0)
