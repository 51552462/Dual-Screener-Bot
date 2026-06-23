"""Institutional macro shadow — unit tests (no network)."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile

import numpy as np
import pandas as pd
import pytest

from macro_matrix_incremental import (
    _add_observation,
    _touch_cell,
    load_macro_matrix,
    save_macro_matrix,
    update_macro_matrix_incremental,
)
from macro_sentinel_quant import (
    _ili_from_composite_z,
    _regime_from_z,
    _rolling_z,
    compute_macro_sentinel_panel,
)
from shadow_macro_validator import (
    _assert_shadow_isolation,
    simulate_shadow_pnl_improvement,
)


def test_rolling_z_and_regime_thresholds():
    s = pd.Series(np.linspace(1.0, 2.0, 40))
    z = _rolling_z(s, window=20).dropna()
    assert len(z) > 10
    assert _regime_from_z(0.6) == "UP"
    assert _regime_from_z(-0.6) == "DOWN"
    assert _regime_from_z(0.1) == "SIDEWAYS"
    assert _ili_from_composite_z(0.0) == 50.0
    assert _ili_from_composite_z(2.0) == 80.0


def test_sentinel_panel_synthetic():
    idx = pd.date_range("2025-01-01", periods=60, freq="D")
    rng = np.random.default_rng(42)
    close = 100 + np.cumsum(rng.normal(0, 1, 60))
    ohlc = pd.DataFrame(
        {
            ("Close", "BTC-USD"): close,
            ("High", "BTC-USD"): close + 1,
            ("Low", "BTC-USD"): close - 1,
        },
        index=idx,
    )
    ohlc.columns = pd.MultiIndex.from_tuples(ohlc.columns)
    panel, composite = compute_macro_sentinel_panel(ohlc)
    assert not composite.dropna().empty


def test_incremental_cell_math():
    cells: dict = {}
    c = _touch_cell(cells, "UP|B (초신성)")
    _add_observation(c, 2.5)
    _add_observation(c, -1.0)
    assert c["n"] == 2
    assert abs(c["sum_ret"] - 1.5) < 1e-9


def test_shadow_isolation_blocks_meta_writes():
    with pytest.raises(RuntimeError):
        _assert_shadow_isolation("META_GROUP_KELLY_MULT")


def test_shadow_pnl_improvement_synthetic():
    matrix = {
        "cells": {
            "UP|B (초신성)": {"n": 10, "sum_ret": 50.0, "sum_ret_sq": 300.0},
            "UP|C (야수/BEAST)": {"n": 10, "sum_ret": -20.0, "sum_ret_sq": 80.0},
        }
    }
    df = pd.DataFrame(
        {
            "sig_type": ["SUPERNOVA_COSINE", "SUPERNOVA_BEAST"],
            "final_ret": [2.0, -1.0],
        }
    )
    out = simulate_shadow_pnl_improvement(df, matrix=matrix, liquidity_regime="UP")
    assert out["n"] == 2
    assert "improvement_pct" in out


def test_matrix_incremental_sqlite(tmp_path, monkeypatch):
    db = tmp_path / "market_data.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE forward_trades (
            exit_date TEXT, trade_date TEXT, sig_type TEXT,
            final_ret REAL, market TEXT, status TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO forward_trades VALUES (?,?,?,?,?,?)",
        ("2026-06-01", "2026-06-01", "SUPERNOVA_COSINE", 3.5, "KR", "CLOSED"),
    )
    conn.commit()
    conn.close()

    mpath = tmp_path / "MACRO_EVOLUTION_MATRIX.json"
    monkeypatch.setattr(
        "macro_matrix_incremental.matrix_path", lambda: str(mpath)
    )
    monkeypatch.setattr(
        "macro_matrix_incremental.market_data_db_path", lambda: str(db)
    )

    matrix, stats = update_macro_matrix_incremental(
        regime_by_date={"2026-06-01": "UP"},
        bootstrap_if_missing=True,
    )
    assert stats["new_trades"] == 1
    assert matrix["last_exit_date_watermark"] == "2026-06-01"
    loaded = load_macro_matrix(str(mpath))
    assert loaded["totals"]["n"] == 1
