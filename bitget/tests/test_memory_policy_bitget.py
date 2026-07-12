"""Bitget institutional memory tier — unit tests."""
from __future__ import annotations

import sqlite3

import memory_bounds


def test_stream_buffer_truncates_orderbook_depth():
    from bitget.data.stream_buffer import StreamBuffer

    buf = StreamBuffer(max_symbols=10)
    deep_bids = [[str(100 - i), "1"] for i in range(100)]
    deep_asks = [[str(101 + i), "1"] for i in range(100)]
    buf.update_orderbook("BTCUSDT", bids=deep_bids, asks=deep_asks, inst_type="SPOT")
    row = buf.get_orderbook("BTCUSDT", "SPOT")
    assert row is not None
    assert len(row["bids"]) <= 25
    assert len(row["asks"]) <= 25
    spread = buf.orderbook_spread_bps("BTCUSDT", "SPOT")
    assert spread is not None


def test_stream_buffer_reuses_row_dict():
    from bitget.data.stream_buffer import StreamBuffer

    buf = StreamBuffer(max_symbols=10)
    buf.update_ticker("ETHUSDT", last=100.0, inst_type="SPOT", raw={"full": "payload"})
    first = buf._tickers[_cache_key := "SPOT:ETHUSDT"]
    buf.update_ticker("ETHUSDT", last=101.0, inst_type="SPOT", raw={"full": "payload"})
    second = buf._tickers[_cache_key]
    assert first is second
    assert second["last"] == 101.0
    assert "raw" not in second


def test_stream_buffer_store_raw_policy():
    from bitget.data.stream_buffer import StreamBuffer

    buf = StreamBuffer(max_symbols=5)
    buf._store_raw = True
    buf.update_ticker("XRPUSDT", last=1.0, inst_type="SPOT", raw={"a": 1})
    row = buf.get_ticker("XRPUSDT", "SPOT")
    assert row is not None
    assert row.get("raw") == {"a": 1}


def test_task_queue_purge_terminal(tmp_path):
    from bitget.infra.task_orchestrator import init_queue, purge_terminal_tasks

    db = str(tmp_path / "tq.sqlite")
    init_queue(db)
    conn = sqlite3.connect(db)
    conn.executemany(
        """
        INSERT INTO task_queue
        (engine, mode, payload, priority, status, attempts, max_attempts,
         enqueued_at, available_at, finished_at)
        VALUES ('BITGET', 'scan', '{}', 1, ?, 1, 3, '2020-01-01', '2020-01-01', ?)
        """,
        [
            ("DONE", "2020-06-01"),
            ("DONE", "2099-06-01"),
            ("FAILED", "2020-06-01"),
            ("FAILED", "2099-06-01"),
            ("PENDING", "2099-06-01"),
        ],
    )
    conn.commit()
    conn.close()

    stats = purge_terminal_tasks(db_path=db)
    assert stats.get("done_by_age", 0) >= 1
    assert stats.get("failed_by_age", 0) >= 1

    conn = sqlite3.connect(db)
    pending = conn.execute(
        "SELECT COUNT(*) FROM task_queue WHERE status='PENDING'"
    ).fetchone()[0]
    done_recent = conn.execute(
        "SELECT COUNT(*) FROM task_queue WHERE status='DONE'"
    ).fetchone()[0]
    conn.close()
    assert pending == 1
    assert done_recent >= 1


def test_ohlcv_date_range_sql():
    sql, params = memory_bounds.ohlcv_date_range_sql(start="2022-11-01", end="2022-12-20")
    assert "substr(Date" in sql
    assert "LIMIT" in sql
    assert params == ("2022-11-01", "2022-12-20")


def test_heavy_data_cycle_runs_gc():
    from bitget.infra.gc_cycle import flush_gc, heavy_data_cycle

    with heavy_data_cycle("test"):
        buf = [object()] * 1000
        del buf
    n = flush_gc(label="test_tail")
    assert isinstance(n, int)


def test_truncate_orderbook_levels():
    levels = [["1", "a"]] * 50
    out = memory_bounds.truncate_orderbook_levels(levels, 10)
    assert len(out) == 10
