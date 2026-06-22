"""daily_report_context.load_market_slice — market 컬럼 blank legacy 행."""
from __future__ import annotations

import sqlite3
from datetime import datetime

import pandas as pd
import pytz

from forward.shared import _daily_report_trades_for_market
from reports.daily_report_context import DailyReportContext
from reports.report_collectors import _df_long_only


def _valid_open_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    return pd.Series(True, index=df.index)


def test_load_market_slice_includes_blank_market_kr_codes():
    """SQL WHERE market='KR' 는 legacy 빈 market 행을 놓침 — code 정규화로 포함."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY,
            market TEXT,
            code TEXT,
            sig_type TEXT,
            status TEXT,
            final_ret REAL,
            exit_date TEXT,
            entry_date TEXT,
            sim_kelly_invest REAL,
            invest_amount REAL,
            shares REAL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO forward_trades
        (id, market, code, sig_type, status, final_ret, exit_date, entry_date,
         sim_kelly_invest, invest_amount, shares)
        VALUES (1, '', '005930', 'RANK_C', 'CLOSED', 8.5, '2026-06-20', '2026-06-10',
                400000, 0, 10)
        """
    )
    conn.commit()

    ref = datetime(2026, 6, 22, 18, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
    ctx = DailyReportContext.build(ref_kst=ref, rolling_days=90)
    sl = ctx.load_market_slice(
        conn,
        "KR",
        df_long_only_fn=_df_long_only,
        normalize_market_fn=_daily_report_trades_for_market,
        valid_open_mask_fn=_valid_open_mask,
    )
    conn.close()

    assert len(sl.df_real) >= 1
    assert sl.n_closed_window >= 1


def test_df_long_only_keeps_rank_sig_types():
    """'RANK_C' must not match INVERSE '[INVERSE_ETF]' regex character-class by accident."""
    import pandas as pd
    from reports.report_collectors import _df_long_only

    df = pd.DataFrame({"sig_type": ["RANK_C_단기테마", "Dante_INVERSE_ETF_Sniper[V1][INVERSE_ETF]"]})
    out = _df_long_only(df)
    assert len(out) == 1
    assert "RANK_C" in out.iloc[0]["sig_type"]


def test_colosseum_brief_no_inf_in_top3_line():
    from forward.shared import _strategy_colosseum_brief

    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            sig_type TEXT, final_ret REAL, code TEXT, name TEXT,
            market TEXT, strategy_name TEXT, exit_date TEXT, status TEXT
        )
        """
    )
    for i in range(5):
        conn.execute(
            "INSERT INTO forward_trades VALUES (?,?,?,?,?,?,?,?)",
            (
                "RANK_C_test",
                12.5 if i == 0 else float("inf"),
                f"00593{i}",
                "T",
                "KR",
                "",
                "2026-06-20",
                "CLOSED",
            ),
        )
    conn.commit()
    import tempfile
    import os

    path = tempfile.mktemp(suffix=".sqlite")
    try:
        disk = sqlite3.connect(path)
        conn.backup(disk)
        disk.close()
        conn.close()
        text = _strategy_colosseum_brief(db_path=path)
        assert "inf" not in text.lower()
        assert "RANK_C" in text
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
