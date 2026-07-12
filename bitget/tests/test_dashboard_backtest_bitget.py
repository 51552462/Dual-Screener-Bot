"""bitget dashboard + heatmap + time_machine — Tier-2 bounded read audits."""
from __future__ import annotations


def test_dashboard_module_uses_bounded_closed_sql():
    import inspect

    from bitget import dashboard as dash

    src = inspect.getsource(dash)
    assert "forward_dashboard_closed_sql" in src
    assert "SELECT *" not in src.split("def load_factory_data")[1].split("def _pf")[0]


def test_heatmap_dashboard_module_uses_bounded_open_sql():
    import inspect

    from bitget import heatmap_dashboard as hm

    src = inspect.getsource(hm)
    assert "forward_heatmap_open_sql" in src
    assert "SELECT *" not in src.split("def load_open_positions")[1].split("df = load_open_positions")[0]


def test_time_machine_backtester_module_uses_bounded_table_scan():
    import inspect

    from bitget import time_machine_backtester as tmb

    src = inspect.getsource(tmb)
    assert "sqlite_bitget_ohlcv_tables_sql" in src
    assert "TIME_MACHINE_MAX_TABLES" in src
    assert "datetime.now(" not in src
    assert "ohlcv_date_range_sql" in src


def test_load_tables_caps_sqlite_master_scan():
    import sqlite3

    from bitget import time_machine_backtester as tmb

    conn = sqlite3.connect(":memory:")
    for i in range(5):
        conn.execute(f'CREATE TABLE "BITGET_SPOT_SYM{i}_USDT_1D" (Date TEXT)')

    with __import__("unittest").mock.patch(
        "bitget.time_machine_backtester.TIME_MACHINE_MAX_TABLES", 2
    ):
        tables = tmb._load_tables(conn, max_tables=2)

    assert len(tables) == 2
    conn.close()
