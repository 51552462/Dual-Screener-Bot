"""
실현 손익(PnL) SSOT — sim_kelly_invest · invest_amount · 시장별 폴백 coalesce.
일일·주간·국고(Treasury) 집계가 동일 함수를 사용한다.
"""
from __future__ import annotations

from typing import Any, Optional, Union

import numpy as np
import pandas as pd

KR_ZERO_INVEST_FALLBACK = 400_000.0
US_ZERO_INVEST_FALLBACK = 400_000.0


def zero_invest_fallback_for_market(market: str) -> Optional[float]:
    m = str(market or "").upper().strip()
    if m == "KR":
        return KR_ZERO_INVEST_FALLBACK
    if m == "US":
        return US_ZERO_INVEST_FALLBACK
    return None


def _coerce_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if np.isnan(v):
            return default
        return v
    except (TypeError, ValueError):
        return default


def row_notional(
    row: Union[pd.Series, dict],
    *,
    market: str = "KR",
    zero_fallback: Optional[float] = None,
) -> float:
    """투입 노출액: sim_kelly_invest → invest_amount → 시장 폴백."""
    if isinstance(row, dict):
        sk = row.get("sim_kelly_invest")
        inv = row.get("invest_amount")
    else:
        sk = row.get("sim_kelly_invest") if "sim_kelly_invest" in row.index else None
        inv = row.get("invest_amount") if "invest_amount" in row.index else None

    sk_v = _coerce_float(sk, 0.0)
    if sk_v > 0:
        return sk_v
    inv_v = _coerce_float(inv, 0.0)
    if inv_v > 0:
        return inv_v
    fb = zero_fallback
    if fb is None:
        fb = zero_invest_fallback_for_market(market)
    return _coerce_float(fb, 0.0) if fb is not None else 0.0


def row_realized_pnl(
    row: Union[pd.Series, dict],
    *,
    market: str = "KR",
    zero_fallback: Optional[float] = None,
) -> float:
    """청산 1건 실현 손익 = notional × final_ret%."""
    ret = row.get("final_ret") if isinstance(row, dict) else row.get("final_ret")
    try:
        if pd.isna(ret):
            return 0.0
    except (TypeError, ValueError):
        if ret is None:
            return 0.0
    ret_v = _coerce_float(ret, 0.0)
    notional = row_notional(row, market=market, zero_fallback=zero_fallback)
    return notional * ret_v / 100.0


def add_realized_pnl_column(
    df: pd.DataFrame,
    *,
    market: str = "KR",
    zero_fallback: Optional[float] = None,
    col: str = "_realized_pnl",
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    fb = zero_fallback if zero_fallback is not None else zero_invest_fallback_for_market(market)
    out[col] = out.apply(
        lambda r: row_realized_pnl(r, market=market, zero_fallback=fb),
        axis=1,
    )
    return out


def dataframe_realized_pnl_sum(
    df: pd.DataFrame,
    *,
    market: str = "KR",
    zero_fallback: Optional[float] = None,
) -> float:
    if df is None or df.empty:
        return 0.0
    fb = zero_fallback if zero_fallback is not None else zero_invest_fallback_for_market(market)
    return float(
        add_realized_pnl_column(df, market=market, zero_fallback=fb)["_realized_pnl"].sum()
    )
