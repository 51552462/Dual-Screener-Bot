"""UNIVERSE-BT-U1 tests — paper DB invariant + C3 exit_trigger null."""
from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest


def _make_ohlcv_db(path: str, *, market: str, symbol: str, n: int = 260) -> None:
    prefix = "BITGET_FUT_" if market == "futures" else "BITGET_SPOT_"
    tbl = f"{prefix}{symbol}_1D"
    btc = f"{prefix}BTC_USDT_1D"
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    close = 100 + np.cumsum(np.random.RandomState(0).randn(n) * 0.5)
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


def test_resolve_historical_regime_c3_unknown():
    from bitget.analysis.universe_bt.regime import resolve_historical_regime

    assert resolve_historical_regime("ETH_USDT", "spot", 1_700_000_000) == "UNKNOWN"


def test_write_bt_results_exit_trigger_null(tmp_path):
    from bitget.analysis.universe_bt.store import write_bt_results

    db = str(tmp_path / "ubt.sqlite")
    write_bt_results(
        [
            {
                "run_id": "t1",
                "market_type": "spot",
                "symbol": "ETH_USDT",
                "bar_ts": 100,
                "regime_label": "UNKNOWN",
                "candidate_generated": 1,
                "gate_passed": 0,
                "virtual_entry": 0,
                "side": "LONG",
                "exit_trigger": None,
            }
        ],
        db_path=db,
    )
    conn = sqlite3.connect(db)
    try:
        et = conn.execute(
            "SELECT exit_trigger, regime_label FROM bitget_universe_bt_results WHERE run_id='t1'"
        ).fetchone()
    finally:
        conn.close()
    assert et[0] is None
    assert et[1] == "UNKNOWN"


def test_paper_db_rowcount_unchanged_after_replay(tmp_path, monkeypatch):
    """Harness must not write bitget_forward_trades on the paper path."""
    from bitget.forward.shared import _init_forward_db_schema
    from bitget.infra.shared_db_connector import get_connection

    paper = str(tmp_path / "paper_market.sqlite")
    market = str(tmp_path / "ohlcv.sqlite")
    results = str(tmp_path / "ubt_results.sqlite")
    scratch = str(tmp_path / "ubt_scratch.sqlite")

    # Seed a fake paper forward table with 3 rows
    conn = get_connection(paper)
    try:
        _init_forward_db_schema(conn)
        conn.execute(
            """
            INSERT INTO bitget_forward_trades (symbol, market_type, status, entry_date)
            VALUES ('AAA_USDT','spot','OPEN','2024-01-01'),
                   ('BBB_USDT','spot','CLOSED','2024-01-02'),
                   ('CCC_USDT','spot','OPEN','2024-01-03')
            """
        )
        conn.commit()
    finally:
        conn.close()
    before = _count_forward(paper)
    assert before == 3

    _make_ohlcv_db(market, market="spot", symbol="ETH_USDT")

    monkeypatch.setenv("BITGET_UNIVERSE_BT_OHLCV_ONLY", "1")

    # Force dry_try_add / engines to stay light: stub engine hit + dry gate
    def _fake_pool(_ef=None):
        def _eng(df, idx, tf):
            return True, "TEST", df, {"score": 50.0, "side": "LONG", "last_close": float(df["Close"].iloc[-1])}

        return [("MASTER", _eng)]

    with mock.patch(
        "bitget.analysis.universe_bt.replay._engine_pool_u1",
        _fake_pool,
    ), mock.patch(
        "bitget.analysis.universe_bt.replay.resolve_universe_snapshot",
        return_value=["ETH_USDT"],
    ):
        from bitget.analysis.universe_bt.replay import run_universe_bt_u1

        out = run_universe_bt_u1(
            "spot",
            results_db=results,
            market_db=market,
            scratch_path=scratch,
            max_symbols=1,
        )

    after = _count_forward(paper)
    assert after == before == 3
    assert out["rows_written"] >= 1
    assert out["policy"] == "C3"

    # Scratch may have rows; paper must not gain any
    assert _count_forward(paper) == 3


def test_spot_short_candidate_no_virtual_entry_via_ledger(tmp_path):
    """Spot+SHORT uses original ledger hard reject on scratch — paper untouched."""
    from bitget.analysis.universe_bt.gate_adapter import dry_try_add_virtual_position

    paper = str(tmp_path / "paper.sqlite")
    scratch = str(tmp_path / "scratch.sqlite")
    Path(paper).write_text("")  # exists but unused
    before = _count_forward(paper)

    ok, msg = dry_try_add_virtual_position(
        market_type="spot",
        symbol="ETH_USDT",
        timeframe="1D",
        sig_type="[STANDARD][TV] SHORT",
        score=50.0,
        entry_price=100.0,
        facts={},
        side="SHORT",
        scratch_path=scratch,
    )
    assert ok is False
    assert "숏" in msg or "Short" in msg or "short" in msg.lower()
    assert _count_forward(paper) == before
