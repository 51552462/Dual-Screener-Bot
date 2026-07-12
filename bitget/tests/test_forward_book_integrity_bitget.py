"""bitget.forward.forward_book_integrity — Tier-2 bounded OPEN reads."""
from __future__ import annotations

import sqlite3
from unittest import mock

import pandas as pd


def test_forward_book_integrity_module_uses_bounded_reads():
    import inspect

    from bitget.forward import forward_book_integrity as fbi

    src = inspect.getsource(fbi)
    assert "forward_open_integrity_open_sql" in src
    assert "forward_integrity_closed_window_count_sql" in src
    assert "warn_if_open_exceeds_safety" in src
    assert "utc_date_str" in src


def test_diagnose_open_book_from_db_open_only_and_closed_count(tmp_path):
    from bitget.forward import forward_book_integrity as fbi

    db = tmp_path / "m.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            market_type TEXT,
            symbol TEXT,
            status TEXT,
            entry_date TEXT,
            exit_date TEXT,
            quantity REAL,
            sim_kelly_invest REAL,
            margin_used REAL,
            entry_price REAL,
            sig_type TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO bitget_forward_trades
            (id, market_type, symbol, status, entry_date, exit_date, quantity,
             sim_kelly_invest, margin_used, entry_price, sig_type)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (1, "spot", "BTC_USDT", "OPEN", "2026-07-11", None, 1.0, 100.0, 0.0, 50000.0, "S1"),
            (2, "spot", "ETH_USDT", "CLOSED_WIN", "2026-07-01", "2026-07-10", 1.0, 0.0, 0.0, 3000.0, "S2"),
            (3, "futures", "SOL_USDT", "OPEN", "2026-07-11", None, 1.0, 50.0, 0.0, 150.0, "S3"),
        ],
    )
    conn.commit()
    conn.close()

    with mock.patch("bitget.forward.forward_book_integrity.utc_date_str", return_value="2026-07-11"), mock.patch(
        "bitget.forward.forward_book_integrity.utc_date_days_ago_str", return_value="2026-07-04"
    ):
        stats = fbi.diagnose_open_book_from_db("spot", db_path=str(db), session_anchor="2026-07-11")

    assert stats.open_raw == 1
    assert stats.open_valid == 1
    assert stats.closed_window == 1


def test_forward_open_integrity_open_sql_projection():
    from bitget.infra.bounded_reads import (
        FORWARD_OPEN_INTEGRITY_COLUMNS,
        forward_open_integrity_open_sql,
    )

    sql, params = forward_open_integrity_open_sql(market_type="futures")
    assert "SELECT *" not in sql
    assert "CLOSED" in sql
    assert "LIMIT ?" in sql
    for col in FORWARD_OPEN_INTEGRITY_COLUMNS.split(","):
        assert col.strip() in sql
    assert params[0] == "futures"
    assert isinstance(params[1], int)


def test_forward_integrity_closed_window_count_sql_scalar():
    from bitget.infra.bounded_reads import forward_integrity_closed_window_count_sql

    sql, params = forward_integrity_closed_window_count_sql(
        market_type="spot",
        since_date="2026-07-04",
    )
    assert "COUNT(*)" in sql
    assert "LIMIT" not in sql
    assert params == ("spot", "2026-07-04")
