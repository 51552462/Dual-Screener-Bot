"""FULL-BT-FUT-RUN-2 — isolated load_full_bt_ohlcv + start_date gap guard."""
from __future__ import annotations

import sqlite3
from datetime import date

import pandas as pd
import pytest

from bitget.full_bt.ohlcv_load import FullBtDataGapError, load_full_bt_ohlcv
from bitget.full_bt.batch import get_full_bt_window_batches


def _seed(path: str, *, n: int = 300, start: str = "2025-10-31") -> None:
    tbl = "BITGET_FUT_BTC_USDT_1D"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            f'CREATE TABLE "{tbl}" '
            "(Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)"
        )
        dates = pd.date_range(start, periods=n, freq="D")
        for i, d in enumerate(dates):
            px = 100.0 + i
            conn.execute(
                f'INSERT INTO "{tbl}" VALUES (?,?,?,?,?,?)',
                (d.strftime("%Y-%m-%d"), px, px + 1, px - 1, px, 1e6),
            )
        conn.commit()
    finally:
        conn.close()


def test_load_full_bt_ohlcv_no_250_tail(tmp_path):
    db = str(tmp_path / "m.sqlite")
    _seed(db, n=300, start="2025-10-31")
    df = load_full_bt_ohlcv("BTC_USDT", "futures", db_path=db)
    assert len(df) == 300
    assert df["Date"].iloc[0].date() == date(2025, 10, 31)


def test_gap_raises_when_first_after_start(tmp_path):
    db = str(tmp_path / "m.sqlite")
    _seed(db, n=50, start="2025-12-01")
    with pytest.raises(FullBtDataGapError, match="data gap"):
        load_full_bt_ohlcv(
            "BTC_USDT",
            "futures",
            start_date=date(2025, 10, 31),
            db_path=db,
        )


def test_window_batches_respect_start_date(tmp_path):
    db = str(tmp_path / "m.sqlite")
    _seed(db, n=300, start="2025-10-31")
    batches = get_full_bt_window_batches(
        "BTC_USDT",
        "futures",
        batch_size=10_000,
        db_path=db,
        start_date=date(2025, 10, 31),
    )
    assert len(batches) == 1
    # first endpoint tied to first kept bar (~2025-10-31)
    start_ts, end_ts = batches[0]
    assert start_ts > 0 and end_ts > start_ts
