"""FULL-BT-FUT-RUN-2 — isolated OHLCV read (no CAT-C ``_load_ohlcv`` / no 250-bar tail)."""
from __future__ import annotations

import os
from datetime import date, datetime
from typing import Optional

import pandas as pd

from bitget.infra.data_paths import market_data_db_path, market_db_read_path
from bitget.infra.shared_db_connector import get_connection


class FullBtDataGapError(RuntimeError):
    """Raised when loaded first bar is later than requested ``start_date`` (no silent truncation)."""


def _normalize_mt(market_type: str) -> str:
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        return "futures"
    if mt not in ("spot", "futures"):
        raise ValueError(f"market_type must be spot|futures, got {market_type!r}")
    return mt


def _ohlcv_table(symbol: str, market_type: str, timeframe: str = "1D") -> str:
    """Reuse CAT naming (SPOT/FUT prefix) — no BITGET_FUTURES_ invention."""
    from bitget.analysis.universe_bt.replay import _ohlcv_table as _ubt_table

    return _ubt_table(str(symbol), _normalize_mt(market_type), timeframe)


def _to_date(val) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    ts = pd.Timestamp(val)
    if pd.isna(ts):
        return None
    return ts.date()


def load_full_bt_ohlcv(
    symbol: str,
    market_type: str,
    start_date: Optional[date] = None,
    *,
    db_path: Optional[str] = None,
    timeframe: str = "1D",
) -> pd.DataFrame:
    """Load full staging/prod OHLCV for FULL-BT windows — **no** ``OHLCV_SIGNAL_BAR_LIMIT``.

    If ``start_date`` is set and the first loaded bar is **after** ``start_date``,
    raise ``FullBtDataGapError`` (honest stop; do not silently truncate).
    """
    mt = _normalize_mt(market_type)
    want = _to_date(start_date)
    path = db_path or market_db_read_path()
    if not os.path.isfile(path):
        path = db_path or market_data_db_path()
    tbl = _ohlcv_table(str(symbol), mt, timeframe)
    conn = get_connection(path, read_only=True)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (tbl,),
        ).fetchone()
        if not exists:
            if want is not None:
                raise FullBtDataGapError(
                    f"full_bt OHLCV missing table={tbl} db={path} "
                    f"(start_date={start_date})"
                )
            return pd.DataFrame(
                columns=["Date", "Open", "High", "Low", "Close", "Volume"]
            )
        # No bar_limit / no ORDER BY Date DESC LIMIT — full chronological series.
        df = pd.read_sql(
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC',
            conn,
        )
    finally:
        conn.close()

    if df is None or df.empty:
        if want is not None:
            raise FullBtDataGapError(
                f"full_bt OHLCV empty table={tbl} db={path} start_date={start_date}"
            )
        return pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"]
        )
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    if df.empty:
        if want is not None:
            raise FullBtDataGapError(
                f"full_bt OHLCV no valid dates table={tbl} start_date={start_date}"
            )
        return pd.DataFrame(
            columns=["Date", "Open", "High", "Low", "Close", "Volume"]
        )

    first = _to_date(df["Date"].iloc[0])
    if want is not None and first is not None and first > want:
        raise FullBtDataGapError(
            f"full_bt data gap: first_bar={first.isoformat()} > start_date={want.isoformat()} "
            f"symbol={symbol} mt={mt} table={tbl} (silent truncation forbidden)"
        )
    return df
