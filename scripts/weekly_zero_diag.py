#!/usr/bin/env python3
"""주간 Flow 표본 0건 — True Zero vs False Zero 교차 검증 (Ubuntu에서 실행)."""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timedelta

# 디렉토리 위치와 무관하게 시스템 루트(상위 폴더)의 모듈을 임포트할 수 있게 경로 주입.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz

WEEK_START = "2026-06-06"
WEEK_END = "2026-06-13"


def main() -> int:
    from market_db_paths import MARKET_DATA_DB_PATH, report_db_read_path
    from weekly_flow_report import _load_week_closed_df, build_weekly_flow_snapshot
    from report_date_utils import closed_event_dates, in_date_window, normalize_date_series

    db_main = MARKET_DATA_DB_PATH
    db_report = report_db_read_path()
    print(f"DB_MAIN={db_main}")
    print(f"DB_REPORT={db_report}")

    conn = sqlite3.connect(db_main, timeout=60)
    cur = conn.cursor()

    print(f"\n=== Window {WEEK_START} ~ {WEEK_END} ===")
    total_truth = 0
    total_legacy = 0
    total_loader = 0
    for mkt in ("KR", "US"):
        # A) Strict weekly SQL (legacy — exit_date >= start only)
        legacy = cur.execute(
            """
            SELECT COUNT(*) FROM forward_trades
            WHERE market=? AND status LIKE 'CLOSED%'
              AND exit_date >= ?
              AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
              AND final_ret IS NOT NULL
            """,
            (mkt, WEEK_START),
        ).fetchone()[0]

        # B) Window with normalized coalesce dates (ground truth)
        rows = cur.execute(
            """
            SELECT exit_date, entry_date, trade_date, final_ret, sig_type, status
            FROM forward_trades
            WHERE market=? AND status LIKE 'CLOSED%'
              AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
            """,
            (mkt,),
        ).fetchall()
        import pandas as pd

        df = pd.DataFrame(
            rows,
            columns=["exit_date", "entry_date", "trade_date", "final_ret", "sig_type", "status"],
        )
        if not df.empty:
            cd = closed_event_dates(df)
            if "trade_date" in df.columns:
                td = normalize_date_series(df["trade_date"])
                cd = cd.where(cd != "", td)
            win = in_date_window(cd, WEEK_START, WEEK_END)
            df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce")
            truth = int((win & df["final_ret"].notna()).sum())
        else:
            truth = 0

        # C) New _load_week_closed_df
        loaded = _load_week_closed_df(conn, mkt, WEEK_START, WEEK_END)
        loader_n = len(loaded)

        max_exit = cur.execute(
            """
            SELECT MAX(substr(COALESCE(NULLIF(trim(exit_date),''), entry_date),1,10))
            FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'
            """,
            (mkt,),
        ).fetchone()[0]

        print(
            f"{mkt}: legacy_sql={legacy} truth_window={truth} "
            f"loader={loader_n} max_exit={max_exit}"
        )
        total_truth += truth
        total_legacy += legacy
        total_loader += loader_n

    conn.close()

    try:
        from system_auto_pilot import load_or_create_config

        cfg = load_or_create_config()
    except Exception:
        cfg = {}
    snap = build_weekly_flow_snapshot(db_path=db_report, sys_config=cfg, week_days=7)
    kr_n = snap.kr.week_n_closed if snap.kr else 0
    us_n = snap.us.week_n_closed if snap.us else 0
    print(f"\n=== build_weekly_flow_snapshot ===")
    print(f"week={snap.week_start}~{snap.week_end} KR={kr_n} US={us_n}")
    print(f"DNA KR={snap.dna_kr.n_total if snap.dna_kr else 0} US={snap.dna_us.n_total if snap.dna_us else 0}")

    if total_truth == 0 and total_legacy == 0 and kr_n == 0 and us_n == 0:
        print("\nVERDICT: TRUE_ZERO — 해당 주간 CLOSED 실데이터 없음")
        return 0
    if total_truth > 0 and (total_legacy == 0 or kr_n + us_n == 0):
        print("\nVERDICT: FALSE_ZERO — 데이터 있으나 로더/SQL 불일치 (패치 후 재실행)")
        return 1
    print("\nVERDICT: DATA_PRESENT — 리포트에 숫자 반영 기대")
    return 0


if __name__ == "__main__":
    sys.exit(main())
