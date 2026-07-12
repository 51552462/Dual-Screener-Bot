"""memory_bounds — bounded cache / SQLite retention helpers."""
from __future__ import annotations

import sqlite3
import time

import memory_bounds


def test_evict_oldest_dict_keys():
    store = {f"k{i}": (float(i), None) for i in range(10)}
    memory_bounds.evict_oldest_dict_keys(
        store, 5, ts_getter=lambda k: store[k][0]
    )
    assert len(store) == 5
    assert "k5" in store
    assert "k0" not in store


def test_throttled_callback():
    gate = memory_bounds.ThrottledCallback(interval_sec=10.0)
    assert gate.due() is True
    assert gate.due() is False


def test_prune_sqlite_by_date_prefix(tmp_path):
    db = tmp_path / "t.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TEXT)")
    conn.executemany(
        "INSERT INTO t (ts) VALUES (?)",
        [("2020-01-01",), ("2099-01-01",)],
    )
    n = memory_bounds.prune_sqlite_by_date_prefix(conn, "t", "ts", keep_days=90)
    conn.commit()
    rows = conn.execute("SELECT ts FROM t").fetchall()
    conn.close()
    assert n == 1
    assert rows == [("2099-01-01",)]


def test_ohlcv_limit_sql():
    sql = memory_bounds.ohlcv_limit_sql(bar_limit=300)
    assert "LIMIT 300" in sql
    assert "DESC" in sql


def test_forward_trades_bounded_sql_open_preserved():
    sql, params = memory_bounds.forward_trades_bounded_sql(market_type="spot", closed_limit=100)
    assert "NOT LIKE 'CLOSED%'" in sql
    assert "LIMIT ?" in sql
    assert params == ("spot", "spot", 100)


def test_ohlcv_entry_window_sql():
    from bitget.infra.memory_policy import OHLCV_ENTRY_LOOKBACK_DAYS, OHLCV_SIGNAL_BAR_LIMIT

    sql = memory_bounds.ohlcv_entry_window_sql(
        bar_limit=OHLCV_SIGNAL_BAR_LIMIT,
        lookback_days=OHLCV_ENTRY_LOOKBACK_DAYS,
    )
    assert f"'-{OHLCV_ENTRY_LOOKBACK_DAYS} days'" in sql
    assert f"LIMIT {OHLCV_SIGNAL_BAR_LIMIT}" in sql


def test_forward_trades_bounded_sql_keeps_old_open(tmp_path):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            market_type TEXT,
            status TEXT
        );
        INSERT INTO bitget_forward_trades VALUES (1, 'spot', 'OPEN');
        INSERT INTO bitget_forward_trades VALUES (2, 'spot', 'CLOSED');
        INSERT INTO bitget_forward_trades VALUES (3, 'spot', 'CLOSED');
        """
    )
    sql, params = memory_bounds.forward_trades_bounded_sql(
        market_type="spot", closed_limit=1
    )
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    ids = sorted(r[0] for r in rows)
    assert ids == [1, 3]
