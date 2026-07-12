"""Bitget forward_trade_identity — classify + diagnostic smoke tests."""
from __future__ import annotations

import sqlite3

from bitget.forward.forward_trade_identity import (
    classify_identity_row,
    diagnose_forward_trade_identity,
    is_blank_symbol,
)
from bitget.reports.bitget_report_context import BitgetReportContext, BitgetReportTimekeeper


def test_classify_identity_row():
    assert classify_identity_row("BTC_USDT") == "ok"
    assert classify_identity_row("") == "symbol_missing"
    assert classify_identity_row("spot:1H") == "synthetic_label"
    assert is_blank_symbol("unknown") is True


def test_diagnose_sets_generated_at_utc(tmp_path, monkeypatch):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            market_type TEXT,
            symbol TEXT,
            status TEXT,
            entry_date TEXT,
            exit_date TEXT,
            final_ret REAL,
            flow_tags TEXT,
            sig_type TEXT,
            timeframe TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO bitget_forward_trades
        (market_type, symbol, status, entry_date, exit_date, final_ret, flow_tags, sig_type, timeframe)
        VALUES ('spot', 'BTC_USDT', 'CLOSED_WIN', '2026-07-10', '2026-07-11', 1.5, '', 'S1', '1H')
        """
    )
    conn.commit()

    tk = BitgetReportTimekeeper.for_market(
        "spot",
        rolling_days=30,
        db_watermark_exit="2026-07-11",
    )
    fake_ctx = BitgetReportContext(
        tk_spot=tk,
        tk_futures=BitgetReportTimekeeper.for_market("futures", rolling_days=30),
        db_read_path=str(db),
        window_days=30,
        calendar_today_utc=tk.session_anchor,
    )
    monkeypatch.setattr(
        "bitget.forward.forward_trade_identity.BitgetReportContext.build",
        lambda **kw: fake_ctx,
    )

    rep = diagnose_forward_trade_identity(conn, "spot", db_path=str(db), rolling_days=30)
    assert rep.generated_at_utc.endswith("UTC")
    assert rep.market_type == "spot"
    assert rep.n_gap_all == 0
    conn.close()


def test_fetch_trades_df_excludes_old_closed_outside_window(tmp_path, monkeypatch):
    import sqlite3

    from bitget.forward.forward_trade_identity import _fetch_trades_df
    from bitget.reports.bitget_report_context import BitgetReportTimekeeper

    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            market_type TEXT,
            symbol TEXT,
            status TEXT,
            entry_date TEXT,
            exit_date TEXT,
            final_ret REAL,
            flow_tags TEXT,
            sig_type TEXT,
            timeframe TEXT
        )
        """
    )
    rows = [
        ("spot", "BTC_USDT", "OPEN", "2026-07-10", None, None, "", "S1", "1H"),
        ("spot", "ETH_USDT", "CLOSED_WIN", "2026-07-09", "2026-07-10", 1.0, "", "S1", "1H"),
        ("spot", "", "CLOSED_LOSS", "2020-01-01", "2020-01-02", -1.0, "", "S1", "1D"),
    ]
    for r in rows:
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            (market_type, symbol, status, entry_date, exit_date, final_ret, flow_tags, sig_type, timeframe)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            r,
        )
    conn.commit()

    tk = BitgetReportTimekeeper.for_market("spot", rolling_days=30, db_watermark_exit="2026-07-10")
    df = _fetch_trades_df(
        conn,
        "spot",
        rolling_cutoff=tk.rolling_cutoff,
        session_anchor=tk.session_anchor,
    )
    conn.close()
    assert len(df) == 2
    assert set(df["status"].astype(str).str.upper()) == {"OPEN", "CLOSED_WIN"}


def test_forward_trade_identity_module_no_print():
    import inspect

    from bitget.forward import forward_trade_identity as fti

    src = inspect.getsource(fti)
    assert "print(" not in src
    assert "get_logger" in src
