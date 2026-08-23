"""UNIVERSE-BT-U2 — checkpoint resume + paper invariant."""
from __future__ import annotations

import os
import sqlite3
from unittest import mock

import numpy as np
import pandas as pd


def _make_ohlcv_db(path: str, *, market: str, symbol: str, n: int = 260) -> None:
    prefix = "BITGET_FUT_" if market == "futures" else "BITGET_SPOT_"
    tbl = f"{prefix}{symbol}_1D"
    btc = f"{prefix}BTC_USDT_1D"
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    close = 100 + np.cumsum(np.random.RandomState(1).randn(n) * 0.4)
    df = pd.DataFrame(
        {
            "Date": dates.strftime("%Y-%m-%d"),
            "Open": close,
            "High": close + 1,
            "Low": close - 1,
            "Close": close,
            "Volume": np.full(n, 1e6),
        }
    )
    conn = sqlite3.connect(path)
    try:
        df.to_sql(tbl, conn, if_exists="replace", index=False)
        df.to_sql(btc, conn, if_exists="replace", index=False)
        conn.commit()
    finally:
        conn.close()


def _count_forward(path: str) -> int:
    if not os.path.isfile(path):
        return 0
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_forward_trades'"
        ).fetchone()
        if not row:
            return 0
        return int(conn.execute("SELECT COUNT(*) FROM bitget_forward_trades").fetchone()[0])
    finally:
        conn.close()


def _count_results(path: str, run_id: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM bitget_universe_bt_results WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
        )
    finally:
        conn.close()


def test_reused_time_machine_constants():
    from bitget.analysis.universe_bt.u2 import REUSED_TIME_MACHINE
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


def test_build_universe_shards_uses_size():
    from bitget.analysis.universe_bt.u2 import build_universe_shards

    shards = build_universe_shards([f"S{i}" for i in range(7)], 3)
    assert shards == [["S0", "S1", "S2"], ["S3", "S4", "S5"], ["S6"]]


def test_u2_resume_idempotent_and_paper_invariant(tmp_path, monkeypatch):
    from bitget.forward.shared import _init_forward_db_schema
    from bitget.infra.shared_db_connector import get_connection

    paper = str(tmp_path / "paper.sqlite")
    market = str(tmp_path / "ohlcv.sqlite")
    results = str(tmp_path / "ubt.sqlite")
    scratch = str(tmp_path / "scratch.sqlite")

    conn = get_connection(paper)
    try:
        _init_forward_db_schema(conn)
        conn.execute(
            """
            INSERT INTO bitget_forward_trades (symbol, market_type, status, entry_date)
            VALUES ('A','spot','OPEN','2024-01-01'),
                   ('B','spot','CLOSED','2024-01-02'),
                   ('C','spot','OPEN','2024-01-03')
            """
        )
        conn.commit()
    finally:
        conn.close()
    before = _count_forward(paper)
    assert before == 3

    _make_ohlcv_db(market, market="spot", symbol="ETH_USDT")
    monkeypatch.setenv("BITGET_UNIVERSE_BT_OHLCV_ONLY", "1")

    def _fake_pool():
        def _eng(df, idx, tf):
            return (
                True,
                "TEST",
                df,
                {
                    "score": 40.0,
                    "side": "LONG",
                    "last_close": float(df["Close"].iloc[-1]),
                },
            )

        return [("MASTER", _eng)]

    run_id = "u2-resume-test"
    with mock.patch(
        "bitget.analysis.universe_bt.replay._engine_pool_u1", _fake_pool
    ), mock.patch(
        "bitget.analysis.universe_bt.u2.resolve_universe_snapshot",
        return_value=["ETH_USDT"],
    ):
        from bitget.analysis.universe_bt.u2 import run_universe_bt_u2

        out1 = run_universe_bt_u2(
            "spot",
            run_id,
            resume=True,
            results_db=results,
            market_db=market,
            scratch_path=scratch,
            paper_db=paper,
            max_symbols=1,
        )
        n1 = _count_results(results, run_id)
        mid = _count_forward(paper)

        out2 = run_universe_bt_u2(
            "spot",
            run_id,
            resume=True,
            results_db=results,
            market_db=market,
            scratch_path=scratch,
            paper_db=paper,
            max_symbols=1,
        )
        n2 = _count_results(results, run_id)
        after = _count_forward(paper)

    assert mid == before == after == 3
    assert out1["paper_invariant_ok"] is True
    assert out2["paper_invariant_ok"] is True
    assert n1 >= 1
    assert n2 == n1  # resume skip — no duplicate rows
    assert out2["rows_written"] == 0
    assert out1["reused_time_machine"]["TIME_MACHINE_MAX_TABLES"] == 300
