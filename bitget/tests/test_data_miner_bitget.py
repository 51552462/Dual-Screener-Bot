"""bitget data_miner — Tier-2 bounded reads + UTC lookback SSOT."""
from __future__ import annotations

from unittest import mock


def test_data_miner_module_uses_bounded_reads():
    import inspect

    from bitget import data_miner as dm

    src = inspect.getsource(dm)
    assert "DATE('now'" not in src
    assert "forward_data_miner_mfe_winners_sql" in src
    assert "forward_data_miner_mfe_training_sql" in src
    assert "utc_date_days_ago_str" in src
    assert "print(" not in src
    assert "log_exception" in src
    assert "get_logger" in src


def test_load_mfe_winners_uses_bounded_sql():
    from bitget import data_miner as dm

    captured: dict = {}

    def _fake_read_sql(sql, conn, params=()):
        captured["sql"] = sql
        captured["params"] = params
        import pandas as pd

        return pd.DataFrame()

    with mock.patch("bitget.data_miner.get_connection"), mock.patch(
        "bitget.data_miner.pd.read_sql", side_effect=_fake_read_sql
    ):
        dm._load_mfe_winners("1D", mfe_min=8.0)

    assert "LIMIT ?" in captured["sql"]
    assert "SELECT *" not in captured["sql"]
    assert captured["params"] == ("1D", 8.0, 3000)


def test_load_recent_mfe_training_samples_uses_utc_since():
    from bitget import data_miner as dm

    captured: dict = {}

    def _fake_read_sql(sql, conn, params=()):
        captured["sql"] = sql
        captured["params"] = params
        import pandas as pd

        return pd.DataFrame()

    with mock.patch("bitget.data_miner.get_connection"), mock.patch(
        "bitget.data_miner.utc_date_days_ago_str", return_value="2026-06-11"
    ), mock.patch("bitget.data_miner.pd.read_sql", side_effect=_fake_read_sql):
        out = dm._load_recent_mfe_training_samples("4H", days=30)

    assert out == []
    assert "DATE('now'" not in captured["sql"]
    assert captured["params"] == ("4H", "2026-06-11", 500)


def test_parse_ohlcv_table_name():
    from bitget.data_miner import _parse_ohlcv_table_name

    assert _parse_ohlcv_table_name("BITGET_SPOT_BTC_USDT_1D") == ("SPOT", "BTC_USDT", "1D")
    assert _parse_ohlcv_table_name("BITGET_FUT_ETH_USDT_4H") == ("FUT", "ETH_USDT", "4H")
    assert _parse_ohlcv_table_name("BITGET_SPOT_X__tmp_1D") is None
    assert _parse_ohlcv_table_name("OTHER_TABLE") is None


def test_resolve_cluster_mining_tables_caps_and_prioritizes_forward():
    import sqlite3

    from bitget import data_miner as dm

    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            market_type TEXT,
            symbol TEXT,
            timeframe TEXT,
            entry_date TEXT,
            status TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO bitget_forward_trades
            (id, market_type, symbol, timeframe, entry_date, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (1, "spot", "BTC_USDT", "1D", "2026-07-01", "OPEN"),
            (2, "futures", "ETH_USDT", "4H", "2026-07-02", "CLOSED_WIN"),
        ],
    )
    conn.execute("CREATE TABLE BITGET_SPOT_BTC_USDT_1D (Date TEXT)")
    conn.execute("CREATE TABLE BITGET_FUT_ETH_USDT_4H (Date TEXT)")

    with mock.patch("bitget.data_miner.utc_date_days_ago_str", return_value="2026-06-01"):
        tables = dm._resolve_cluster_mining_tables(conn, max_tables=2)

    assert tables == ["BITGET_FUT_ETH_USDT_4H", "BITGET_SPOT_BTC_USDT_1D"]
    conn.close()


def test_build_supernova_csv_module_uses_bounded_table_resolution():
    import inspect

    from bitget import data_miner as dm

    src = inspect.getsource(dm.build_supernova_csv)
    assert "sqlite_master WHERE type='table' AND name NOT LIKE" not in src
    assert "_resolve_cluster_mining_tables" in src
    assert "SUPERNOVA_CLUSTER_MAX_TABLES" in src
    assert "SUPERNOVA_CLUSTER_OUT_MAX_ROWS" in src
