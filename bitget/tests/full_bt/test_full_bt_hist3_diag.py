"""FULL-BT-HIST-3 — engine_call / outcome / tf_ohlcv_coverage diag."""
from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from bitget.forward.shared import _init_forward_db_schema
from bitget.infra.shared_db_connector import get_connection


def _init_full(path: str) -> None:
    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _seed_tf(path: str, *, symbol: str, market: str, tf: str, n_bars: int = 80) -> None:
    import sqlite3

    prefix = "BITGET_FUT_" if market == "futures" else "BITGET_SPOT_"
    tbl = f"{prefix}{symbol}_{tf}"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{tbl}" '
            "(Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)"
        )
        dates = pd.date_range("2025-01-01", periods=n_bars, freq="D")
        for i, d in enumerate(dates):
            px = 100.0 + i * 0.1
            conn.execute(
                f'INSERT INTO "{tbl}" VALUES (?,?,?,?,?,?)',
                (d.strftime("%Y-%m-%d"), px, px * 1.01, px * 0.99, px, 1e6),
            )
        conn.commit()
    finally:
        conn.close()


def test_diag_schema_extends_tf_column(tmp_path):
    from bitget.full_bt.harness import ensure_diag_schema

    path = str(tmp_path / "bitget_full_bt.sqlite")
    ensure_diag_schema(path)
    ensure_diag_schema(path)  # idempotent ALTER
    conn = get_connection(path, read_only=True)
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(full_bt_diag)").fetchall()}
    finally:
        conn.close()
    assert "tf" in cols


def test_hist3_call_none_and_tf_coverage(tmp_path):
    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    _seed_tf(market, symbol="BTC_USDT", market="spot", tf="1D", n_bars=80)
    # 4H/2H/1H absent → coverage False

    def _never_hit(window, bench, tf):
        return False, "", window, {}

    rid = "hist3-ut"

    with mock.patch.object(H, "REUSED_MIN_BARS", 60), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _never_hit)
    ):
        H.run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            "2025-01-01",
            "2025-12-31",
            full,
            market_db=market,
            run_id=rid,
        )

    s = H.summarize_diag(full, rid, "spot")
    assert s["engine_call_total"] >= 1
    assert s["engine_call_outcome_totals"]["none"] >= 1
    assert s["engine_call_outcome_totals"]["candidate"] == 0
    assert s["engine_call_outcome_totals"]["exception"] == 0
    assert s["tf_ohlcv_coverage"].get("1D") is True
    assert s["tf_ohlcv_coverage"].get("4H") is False
    assert s["tf_ohlcv_coverage"].get("2H") is False
    assert s["tf_ohlcv_coverage"].get("1H") is False


def test_hist3_exception_outcome(tmp_path):
    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    _seed_tf(market, symbol="BTC_USDT", market="spot", tf="1D", n_bars=80)

    def _boom(window, bench, tf):
        raise ValueError("boom_test")

    rid = "hist3-exc"

    with mock.patch.object(H, "REUSED_MIN_BARS", 60), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _boom)
    ):
        H.run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            "2025-01-01",
            "2025-12-31",
            full,
            market_db=market,
            run_id=rid,
        )

    s = H.summarize_diag(full, rid, "spot")
    assert s["engine_call_total"] >= 1
    assert s["engine_call_outcome_totals"]["exception"] >= 1
    assert "ValueError" in s["engine_exception_types"]
