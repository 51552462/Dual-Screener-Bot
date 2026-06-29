"""
포워드 딥다이브 'fail-safe RED / 데이터 정체' 원인 진단기.

청산 워터마크(forward_trades 최근 CLOSED)와 시장 캔들 워터마크(SPY/KOSPI_IDX)를
분리 출력하여, RED가 '진짜 데이터 정체'인지 '청산 공백(보유 지속·무청산)'인지 판별한다.

    python scripts/diag_forward_staleness.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3

import pandas as pd

from market_db_paths import MARKET_DATA_DB_PATH
from forward_dual_track_queries import query_latest_closed_trade_date
from fluid_time_anchor import (
    load_kr_kospi_session_from_db,
    load_spy_session_from_db,
)
from forward.forward_book_integrity import (
    compute_open_book_stats,
    reporter_valid_holding_mask,
)
from reports.report_staleness_gate import evaluate_staleness
from reports.report_timekeeper import ReportTimekeeper, business_lag_days


def _diag_open_book(conn, mk: str, anchor: str) -> None:
    """[4/9] '현금 관망' 오인 진단 — 원시/유효/유령 OPEN 분해 + 샘플 명목 필드."""
    try:
        df = pd.read_sql(
            """
            SELECT id, market, code, name, status, entry_date, entry_price,
                   shares, sim_kelly_invest, invest_amount, sig_type
            FROM forward_trades
            WHERE UPPER(TRIM(COALESCE(market,''))) = ?
            """,
            conn,
            params=(mk,),
        )
    except Exception as e:  # noqa: BLE001
        print(f"  OPEN 장부 조회 실패: {e}")
        return

    stats = compute_open_book_stats(df, market=mk, session_anchor=anchor)
    print(
        f"  OPEN 장부       : 원시 {stats.open_raw} · 유효 {stats.open_valid} · "
        f"유령 {stats.open_ghost} · 당일진입(유효) {stats.open_today_valid}"
    )
    print(f"  장부정합 note    : {stats.integrity_note}")
    if stats.open_valid == 0 and stats.open_raw == 0:
        print("     진단: 유효/원시 OPEN 모두 0 → 실제 현금 관망(또는 스캔 진입 경로 미작동).")
    elif stats.open_raw > 0 and stats.open_valid == 0:
        print("     진단: 원시 OPEN 은 있으나 유효 명목 0 → '유령'. [4/9]가 현금 관망으로 오인.")

    # 원시 OPEN 샘플(명목 필드 노출) — 왜 유효/투입에서 빠지는지 눈으로 확인
    try:
        st = df["status"].astype(str).str.upper().str.strip()
        df_open_raw = df[st.isin(["OPEN", "ACTIVE"])].copy()
        if not df_open_raw.empty:
            valid = reporter_valid_holding_mask(df_open_raw)
            for _, r in df_open_raw.head(8).iterrows():
                ok = "✓유효" if bool(valid.get(r.name, False)) else "✗제외"
                print(
                    f"     - {ok} {str(r.get('code'))[:10]:<10} "
                    f"shares={r.get('shares')} invest={r.get('invest_amount')} "
                    f"kelly={r.get('sim_kelly_invest')} entry_px={r.get('entry_price')} "
                    f"sig={str(r.get('sig_type'))[:40]}"
                )
    except Exception as e:  # noqa: BLE001
        print(f"     샘플 출력 실패: {e}")


def main() -> None:
    print(f"MAIN DB: {MARKET_DATA_DB_PATH}  exists={os.path.isfile(MARKET_DATA_DB_PATH)}")
    if not os.path.isfile(MARKET_DATA_DB_PATH):
        print("  -> 메인 DB가 없습니다. 경로/배포 환경을 확인하세요.")
        return

    conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=30)
    try:
        for mk in ("KR", "US"):
            wm = query_latest_closed_trade_date(conn, mk)
            candle = load_spy_session_from_db() if mk == "US" else load_kr_kospi_session_from_db()
            tk = ReportTimekeeper.for_market(mk, db_watermark_exit=wm)
            close_lag = business_lag_days(wm, tk.session_anchor, market=mk)
            candle_lag = (
                business_lag_days(candle, tk.session_anchor, market=mk) if candle else None
            )
            try:
                open_n = conn.execute(
                    "SELECT COUNT(*) FROM forward_trades WHERE market=? AND UPPER(TRIM(status))='OPEN'",
                    (mk,),
                ).fetchone()[0]
            except sqlite3.OperationalError:
                open_n = "—"
            v = evaluate_staleness(tk, live_row_count=0, data_candle_watermark=candle)

            print(f"\n== {mk} ==")
            print(f"  세션앵커        : {tk.session_anchor}")
            print(f"  청산 워터마크    : {wm}  (lag {close_lag} 영업일)")
            print(f"  시장캔들 워터마크 : {candle}  (lag {candle_lag} 영업일)")
            print(f"  OPEN 포지션(원시): {open_n}")
            _diag_open_book(conn, mk, tk.session_anchor)
            print(f"  -> Staleness     : {v.grade}  ({v.reason})")
            if v.grade == "RED":
                print("     진단: 시장 캔들도 지연 → 진짜 데이터 정체. OHLCV 증분 업데이터/피드 점검.")
            elif v.grade == "YELLOW" and close_lag >= 2:
                print("     진단: 시장데이터는 신선, 단지 청산 공백. 정상(보유 지속/무청산/오픈북 공백).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
