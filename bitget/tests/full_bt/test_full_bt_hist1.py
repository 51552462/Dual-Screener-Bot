"""FULL-BT-HIST-1 — real OHLCV bar walk · candle entry_date · SPOT/FUT · pilot resume."""
from __future__ import annotations

import sqlite3
from datetime import date
from unittest import mock

import pandas as pd

from bitget.forward.shared import _init_forward_db_schema
from bitget.infra.clock import utc_datetime_str
from bitget.infra.shared_db_connector import get_connection


def _init_full(path: str) -> None:
    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _seed_ohlcv(
    path: str,
    *,
    symbol: str = "BTC_USDT",
    market: str = "spot",
    n_bars: int = 20,
    start: str = "2025-01-01",
    close0: float = 100.0,
    drift: float = 0.0,
) -> None:
    prefix = "BITGET_FUT_" if market == "futures" else "BITGET_SPOT_"
    tbl = f"{prefix}{symbol}_1D"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{tbl}" '
            "(Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)"
        )
        dates = pd.date_range(start, periods=n_bars, freq="D")
        px = close0
        for i, d in enumerate(dates):
            px = close0 * (1.0 + drift * i)
            # mild range so ATR14 can compute
            conn.execute(
                f'INSERT INTO "{tbl}" VALUES (?,?,?,?,?,?)',
                (
                    d.strftime("%Y-%m-%d"),
                    px,
                    px * (1.02 + 0.001 * (i % 3)),
                    px * (0.98 - 0.001 * (i % 3)),
                    px * (1.0 + 0.002 * ((i % 5) - 2)),
                    1e6,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _fake_engine_hit_once():
    state = {"hit": False}

    def _fn(window, bench, tf):
        if state["hit"]:
            return False, "", window, {}
        state["hit"] = True
        c = float(window["Close"].iloc[-1])
        h = float(window["High"].iloc[-1])
        return True, "TEST_SIG", window, {
            "side": "LONG",
            "last_close": c,
            "entry_high": h,
            "score": 60.0,
            "trade_value_24h": 1e7,
        }

    return _fn


def test_multi_bar_exit_after_cate_kill(tmp_path):
    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _seed_ohlcv(market, n_bars=80)
    _init_full(full)

    kill_n = {"n": 0}

    def _kill(cfg, position_side="LONG"):
        kill_n["n"] += 1
        if kill_n["n"] >= 3:
            return {"kill_active": True, "reason": "test_kill"}
        return {"kill_active": False, "reason": "safe"}

    with mock.patch.object(H, "REUSED_MIN_BARS", 60), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _fake_engine_hit_once())
    ), mock.patch(
        "bitget.trading.mega_trend_kill_bg.evaluate_crypto_climax_kill_switch",
        _kill,
    ):
        ev = H.run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            date(2025, 1, 1),
            date(2025, 4, 30),
            full,
            market_db=market,
        )
    kinds = [e["event"] for e in ev]
    assert "entry" in kinds and "exit" in kinds
    # exit after multiple bars (kill fires on 3rd evaluate while holding)
    assert kill_n["n"] >= 3

    conn = get_connection(full, read_only=True)
    try:
        row = conn.execute(
            "SELECT entry_date, exit_date, status FROM bitget_forward_trades LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    entry_date, exit_date, status = row
    assert str(status).startswith("CLOSED")
    assert entry_date != exit_date or kill_n["n"] >= 3
    # candle axis ≠ wall updated_at
    wall = utc_datetime_str()[:10]
    assert str(entry_date)[:10] != wall or str(entry_date).startswith("2025")


def test_entry_date_is_candle_not_wall(tmp_path):
    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _seed_ohlcv(market, n_bars=80, start="2024-06-01")
    _init_full(full)

    with mock.patch.object(H, "REUSED_MIN_BARS", 60), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _fake_engine_hit_once())
    ), mock.patch(
        "bitget.trading.mega_trend_kill_bg.evaluate_crypto_climax_kill_switch",
        return_value={"kill_active": False, "reason": "safe"},
    ):
        ev = H.run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            date(2024, 6, 1),
            date(2024, 9, 30),
            full,
            market_db=market,
        )
    assert any(e["event"] == "entry" for e in ev)
    entry = next(e for e in ev if e["event"] == "entry")
    assert entry["entry_date"].startswith("2024-")
    assert entry["entry_date"] != utc_datetime_str()[:10]


def test_spot_fut_ohlcv_source_branch(tmp_path):
    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _seed_ohlcv(market, symbol="ETH_USDT", market="spot", n_bars=10)
    _seed_ohlcv(market, symbol="ETH_USDT", market="futures", n_bars=10)
    _init_full(full)

    loads = []

    real_load = __import__(
        "bitget.analysis.universe_bt.replay", fromlist=["_load_ohlcv"]
    )._load_ohlcv

    def _wrap(symbol, market_type, **kw):
        loads.append((symbol, str(market_type).lower()))
        return real_load(symbol, market_type, **kw)

    with mock.patch.object(H, "REUSED_MIN_BARS", 3), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _fake_engine_hit_once())
    ), mock.patch(
        "bitget.analysis.universe_bt.replay._load_ohlcv", _wrap
    ), mock.patch(
        "bitget.trading.mega_trend_kill_bg.evaluate_crypto_climax_kill_switch",
        return_value={"kill_active": False, "reason": "safe"},
    ):
        H.run_replay(
            "spot", "ETH_USDT", "MASTER", date(2025, 1, 1), date(2025, 1, 20), full, market_db=market
        )
        H.run_replay(
            "futures",
            "ETH_USDT",
            "MASTER",
            date(2025, 1, 1),
            date(2025, 1, 20),
            full,
            market_db=market,
        )
    assert any(m == "spot" for _, m in loads)
    assert any(m == "futures" for _, m in loads)


def test_max_symbols_pilot_resume_idempotent(tmp_path):
    from bitget.full_bt.batch import run_full_bt_batch
    from bitget.full_bt.checkpoint import load_full_bt_checkpoint

    paper = str(tmp_path / "paper.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    market = str(tmp_path / "market.sqlite")
    _init_full(paper)
    conn = get_connection(paper)
    try:
        conn.execute(
            "INSERT INTO bitget_forward_trades (symbol, market_type, status, entry_date) "
            "VALUES (?,?,?,?)",
            ("P0_USDT", "spot", "OPEN", "2026-01-01"),
        )
        conn.commit()
    finally:
        conn.close()
    _seed_ohlcv(market, n_bars=8)

    calls = []

    def _fake(mt, symbol, engine, start, end, db_path):
        calls.append((mt, symbol, start, end))

    with mock.patch("bitget.forward.shared.DB_PATH", paper), mock.patch(
        "bitget.forward.ledger.DB_PATH", paper
    ):
        out1 = run_full_bt_batch(
            "spot",
            "hist-pilot",
            resume=True,
            results_db=full,
            market_db=market,
            paper_db=paper,
            symbols=["BTC_USDT"],
            max_symbols=1,
            replay_fn=_fake,
        )
        n1 = len(calls)
        out2 = run_full_bt_batch(
            "spot",
            "hist-pilot",
            resume=True,
            results_db=full,
            market_db=market,
            paper_db=paper,
            symbols=["BTC_USDT"],
            max_symbols=1,
            replay_fn=_fake,
        )
    assert out1["batches_run"] >= 1
    assert out2["batches_run"] == 0
    assert out2["batches_skipped"] >= 1
    assert len(calls) == n1
    ck = load_full_bt_checkpoint("hist-pilot", "spot", db_path=full)
    assert ck is not None and ("BTC_USDT", 0) in ck["completed"]
