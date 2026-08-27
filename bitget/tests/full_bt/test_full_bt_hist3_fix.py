"""FULL-BT-HIST-3-FIX — fetch-range warmup · multi-bar walk · insufficient skip."""
from __future__ import annotations

import sqlite3
from datetime import date
from unittest import mock

import pandas as pd

from bitget.forward.shared import _init_forward_db_schema
from bitget.infra.shared_db_connector import get_connection


def _init_full(path: str) -> None:
    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _seed(path: str, *, n_bars: int, start: str = "2025-01-01") -> None:
    conn = sqlite3.connect(path)
    try:
        for tf in ("1D",):
            tbl = f"BITGET_SPOT_BTC_USDT_{tf}"
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{tbl}" '
                "(Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)"
            )
            dates = pd.date_range(start, periods=n_bars, freq="D")
            for i, d in enumerate(dates):
                px = 100.0 + i * 0.1
                conn.execute(
                    f'INSERT INTO "{tbl}" VALUES (?,?,?,?,?,?)',
                    (d.strftime("%Y-%m-%d"), px, px * 1.01, px * 0.99, px, 1e6),
                )
        conn.commit()
    finally:
        conn.close()


def test_load_ohlcv_no_start_offset_documented():
    """조사 보고: universe_bt._load_ohlcv is tail-N only (no start kw)."""
    import inspect

    from bitget.analysis.universe_bt.replay import _load_ohlcv

    sig = inspect.signature(_load_ohlcv)
    assert "start" not in sig.parameters
    assert "bar_limit" not in sig.parameters


def test_reused_min_bars_from_u1():
    from bitget.analysis.universe_bt.replay import _U1_MIN_BARS
    from bitget.full_bt import harness as H

    assert H.REUSED_MIN_BARS == int(_U1_MIN_BARS)


def test_multi_bar_walk_engine_call_gt_1(tmp_path):
    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    _seed(market, n_bars=100)

    def _never(window, bench, tf):
        return False, "", window, {}

    rid = "hist3fix-multi"
    with mock.patch.object(H, "REUSED_MIN_BARS", 10), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _never)
    ):
        H.run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            date(2025, 1, 1),
            date(2025, 4, 10),
            full,
            market_db=market,
            run_id=rid,
        )
    s = H.summarize_diag(full, rid, "spot")
    assert s["engine_call_total"] > 1
    assert s["fetch_loaded_total"] >= 10
    assert s["fetch_requested_total"] >= s["walk_bar_expected_total"]


def test_warmup_insufficient_skips_no_invent(tmp_path):
    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    _seed(market, n_bars=20)

    def _never(window, bench, tf):
        return False, "", window, {}

    rid = "hist3fix-skip"
    with mock.patch.object(H, "REUSED_MIN_BARS", 60), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _never)
    ):
        ev = H.run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            date(2025, 1, 1),
            date(2025, 1, 20),
            full,
            market_db=market,
            run_id=rid,
        )
    assert ev == []
    s = H.summarize_diag(full, rid, "spot")
    assert s["engine_call_total"] == 0
    assert s["fetch_loaded_total"] < 60
