"""FULL-BT-2 tests — checkpoint resume idempotency + paper DB invariant."""
from __future__ import annotations

import sqlite3
from datetime import date
from unittest import mock

import pandas as pd


def _seed_paper(path: str, n: int = 2) -> None:
    from bitget.forward.shared import _init_forward_db_schema
    from bitget.infra.shared_db_connector import get_connection

    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)
        for i in range(n):
            conn.execute(
                "INSERT INTO bitget_forward_trades (symbol, market_type, status, entry_date) "
                "VALUES (?,?,?,?)",
                (f"P{i}_USDT", "spot", "OPEN", "2026-01-01"),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_ohlcv(path: str, symbol: str = "BTC_USDT", n_bars: int = 12) -> None:
    conn = sqlite3.connect(path)
    try:
        tbl = f"BITGET_SPOT_{symbol}_1D"
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{tbl}" '
            "(Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)"
        )
        dates = pd.date_range("2025-01-01", periods=n_bars, freq="D")
        for d in dates:
            conn.execute(
                f'INSERT INTO "{tbl}" VALUES (?,?,?,?,?,?)',
                (d.strftime("%Y-%m-%d"), 100.0, 101.0, 99.0, 100.0, 1e6),
            )
        conn.commit()
    finally:
        conn.close()


def test_shards_and_time_machine_reuse():
    from bitget.full_bt.batch import REUSED_TIME_MACHINE, build_full_bt_shards
    from bitget.infra.memory_policy import (
        TIME_MACHINE_MAX_BARS_PER_TABLE,
        TIME_MACHINE_MAX_TABLES,
    )

    assert REUSED_TIME_MACHINE["TIME_MACHINE_MAX_TABLES"] == TIME_MACHINE_MAX_TABLES == 300
    assert (
        REUSED_TIME_MACHINE["TIME_MACHINE_MAX_BARS_PER_TABLE"]
        == TIME_MACHINE_MAX_BARS_PER_TABLE
        == 5000
    )
    shards = build_full_bt_shards([f"S{i}" for i in range(5)], 2)
    assert shards == [["S0", "S1"], ["S2", "S3"], ["S4"]]


def test_window_batches_from_ohlcv(tmp_path):
    from bitget.full_bt.batch import get_full_bt_window_batches

    market = str(tmp_path / "market.sqlite")
    _seed_ohlcv(market, n_bars=10)
    batches = get_full_bt_window_batches("BTC_USDT", "spot", batch_size=4, db_path=market)
    assert len(batches) == 3  # 4+4+2
    assert all(isinstance(b, tuple) and len(b) == 2 for b in batches)


def test_resume_idempotency_and_paper_invariant(tmp_path):
    from bitget.full_bt.batch import count_paper_forward_trades, run_full_bt_batch
    from bitget.full_bt.checkpoint import load_full_bt_checkpoint

    paper = str(tmp_path / "paper.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    market = str(tmp_path / "market.sqlite")
    _seed_paper(paper, 2)
    _seed_ohlcv(market, n_bars=6)

    calls: list[tuple] = []

    def _fake_replay(mt, symbol, engine, start, end, db_path):
        calls.append((mt, symbol, engine, start, end, db_path))

    with mock.patch("bitget.forward.shared.DB_PATH", paper), mock.patch(
        "bitget.forward.ledger.DB_PATH", paper
    ):
        out1 = run_full_bt_batch(
            "spot",
            "fb2-test-run",
            resume=True,
            results_db=full,
            market_db=market,
            paper_db=paper,
            symbols=["BTC_USDT"],
            replay_fn=_fake_replay,
        )
        n1 = len(calls)
        assert n1 >= 1
        assert out1["paper_before"] == out1["paper_after"] == 2
        assert all(e["paper_count"] == 2 for e in out1["paper_log"])

        out2 = run_full_bt_batch(
            "spot",
            "fb2-test-run",
            resume=True,
            results_db=full,
            market_db=market,
            paper_db=paper,
            symbols=["BTC_USDT"],
            replay_fn=_fake_replay,
        )
        assert len(calls) == n1  # no extra replay
        assert out2["batches_run"] == 0
        assert out2["batches_skipped"] == n1
        assert out2["paper_before"] == out2["paper_after"] == 2

    ckpt = load_full_bt_checkpoint("fb2-test-run", "spot", db_path=full)
    assert ckpt is not None
    assert len(ckpt["completed"]) == n1
    assert count_paper_forward_trades(paper) == 2


def test_engine_pool_five_base_names():
    """Optional FULL-BT-2 OUTBOX note — 5 base engines via CAT-C pool."""
    from bitget.master_scanner import _build_engine_pool

    names = [n for n, _ in _build_engine_pool(None)]
    base = {"EMA5", "MASTER", "NULRIM", "TV_SHORT_V1", "TV_SHORT_V2"}
    assert base.issubset(set(names))
