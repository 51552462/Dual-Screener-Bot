"""
포워드 리포트·딥다이브용 스칼라 추출 — Series/NaN/중복 컬럼 방어.
"""
from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd


def col_series(df: Optional[pd.DataFrame], col: str) -> pd.Series:
    """중복 컬럼명이 있어도 단일 Series 반환."""
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    x = df[col]
    if isinstance(x, pd.DataFrame):
        x = x.iloc[:, 0]
    return x


def scalar_float(val: Any, default: float = 0.0) -> float:
    """Series/DataFrame/NaN → 안전한 float (비유한값은 default)."""
    if val is None:
        return float(default)
    if isinstance(val, pd.DataFrame):
        if val.empty:
            return float(default)
        return scalar_float(val.iloc[0, 0], default)
    if isinstance(val, pd.Series):
        v = pd.to_numeric(val, errors="coerce").dropna()
        if v.empty:
            return float(default)
        val = v.iloc[0] if len(v) == 1 else v.mean()
    try:
        f = float(val)
    except (TypeError, ValueError):
        return float(default)
    return float(default) if not np.isfinite(f) else f


def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = pd.Index(df.columns)
    if cols.is_unique:
        return df
    return df.loc[:, ~cols.duplicated(keep="first")]


def prepare_forward_trades_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """딥다이브 입력 — 컬럼 dedupe + final_ret 수치화·NaN→0."""
    if df is None:
        return pd.DataFrame()
    out = dedupe_columns(df.copy())
    if out.empty or "final_ret" not in out.columns:
        return out
    out["final_ret"] = pd.to_numeric(col_series(out, "final_ret"), errors="coerce").fillna(0.0)
    return out
