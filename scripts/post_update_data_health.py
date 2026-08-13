"""
update_factory.sh [5c] — 배포 직후 운영 DB 신선도 스모크.

캔들(SPY/KOSPI_IDX)이 anchor 대비 2영업일+ 지연이면 exit 1 (진짜 데이터 정체).
청산 워터마크만 지연(YELLOW)이면 exit 0 + 경고 — OHLCV·팩토리 경로는 정상.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3

from factory_data_paths import factory_data_dir, install_root
from forward_dual_track_queries import query_latest_closed_trade_date
from fluid_time_anchor import load_kr_kospi_session_from_db, load_spy_session_from_db
from market_db_paths import MARKET_DATA_DB_PATH
from reports.report_staleness_gate import evaluate_staleness
from reports.report_timekeeper import ReportTimekeeper, business_lag_days, resolve_data_candle_watermark


def _legacy_shadow_path() -> str:
    return os.path.join(install_root(), "market_data.sqlite")


def main() -> int:
    data_root = factory_data_dir()
    prod_db = MARKET_DATA_DB_PATH
    legacy = _legacy_shadow_path()
    print(f"data_root={data_root}")
    print(f"market_db={prod_db}")
    if os.path.isfile(legacy) and os.path.normpath(legacy) != os.path.normpath(prod_db):
        print(f"WARN legacy_shadow={legacy} still exists — run update_factory quarantine")

    if not os.path.isfile(prod_db):
        print(f"FAIL prod db missing: {prod_db}")
        return 1

    conn = sqlite3.connect(prod_db, timeout=30)
    try:
        worst = 0
        for mk in ("KR", "US"):
            wm = query_latest_closed_trade_date(conn, mk)
            candle = load_spy_session_from_db() if mk == "US" else load_kr_kospi_session_from_db()
            if not candle:
                candle = resolve_data_candle_watermark(mk)
            tk = ReportTimekeeper.for_market(mk, db_watermark_exit=wm)
            v = evaluate_staleness(tk, live_row_count=0, data_candle_watermark=candle)
            print(f"{mk}: grade={v.grade} exit_wm={wm} candle={candle} lag={v.lag_business_days} - {v.reason}")
            if v.grade == "RED":
                worst = 1
    finally:
        conn.close()

    if worst:
        print("FAIL candle staleness RED — check data_refresh cron / FDR·yfinance")
        return 1
    print("OK data path + candle freshness")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
