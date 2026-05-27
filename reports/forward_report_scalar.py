"""
포워드 리포트·딥다이브용 스칼라 추출 — Series/NaN/중복 컬럼/ BLOB 방어.
"""
from __future__ import annotations

import logging
import struct
from typing import Any, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


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


def safe_float_cast(val: Any, default: float = 0.0) -> float:
    """
    bytes(BLOB) · Series · NaN · 비정상 문자열 → float.
    4/8바이트 IEEE 리틀/빅엔디언 디코딩 1회 시도 후 scalar_float 폴백.
    """
    if val is None:
        return float(default)
    if isinstance(val, (bytes, bytearray)):
        b = bytes(val)
        for fmt in ("<d", "<f", ">d", ">f"):
            if len(b) != struct.calcsize(fmt):
                continue
            try:
                x = float(struct.unpack(fmt, b)[0])
                if np.isfinite(x):
                    return x
            except struct.error:
                continue
        try:
            return scalar_float(b.decode("utf-8", errors="ignore").strip(), default)
        except Exception:
            return float(default)
    if isinstance(val, (np.floating, np.integer)):
        val = val.item()
    out = scalar_float(val, default)
    return float(default) if not np.isfinite(out) else out


def ohlcv_last_floats(df: Optional[pd.DataFrame]) -> Tuple[float, float, float, float, float]:
    """OHLCV 마지막 봉 — BLOB/NaN 방어. 비정상 시 nan."""
    nan = float("nan")
    if df is None or df.empty:
        return nan, nan, nan, nan, nan
    out: list[float] = []
    for col in ("Close", "Open", "High", "Low", "Volume"):
        if col not in df.columns:
            out.append(nan)
            continue
        s = pd.to_numeric(col_series(df, col), errors="coerce")
        if s.empty:
            out.append(nan)
        else:
            out.append(safe_float_cast(s.iloc[-1], nan))
    return tuple(out)  # type: ignore[return-value]


def row_scalar(row: pd.Series, col: str, default: float = 0.0) -> float:
    """iterrows() 행에서 단일 컬럼 → float (Series/DataFrame/NaN 방어)."""
    if col not in row.index:
        return float(default)
    val = row[col]
    if isinstance(val, pd.Series):
        val = val.iloc[0] if len(val) else default
    elif isinstance(val, pd.DataFrame):
        val = val.iloc[0, 0] if not val.empty else default
    return safe_float_cast(val, default)


def series_mean(df: Optional[pd.DataFrame], col: str, default: float = 0.0) -> float:
    """df[col]이 DataFrame이어도 안전한 평균."""
    s = col_series(df, col)
    if s.empty:
        return float(default)
    return scalar_float(s.mean(), default)


def duplicated_column_names(df: pd.DataFrame) -> list[str]:
    """중복으로 제거될 컬럼명 목록(첫 occurrence 제외)."""
    if df is None or df.empty:
        return []
    cols = pd.Index(df.columns)
    if cols.is_unique:
        return []
    dup_mask = cols.duplicated(keep="first")
    return sorted(set(str(c) for c in cols[dup_mask]))


def dedupe_columns(
    df: pd.DataFrame,
    *,
    log_warnings: bool = True,
    context: str = "",
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = pd.Index(df.columns)
    if cols.is_unique:
        return df
    duplicated_cols = duplicated_column_names(df)
    if log_warnings and duplicated_cols:
        ctx = f" ({context})" if context else ""
        logger.warning(
            "Duplicate columns detected and dropped: %s%s",
            duplicated_cols,
            ctx,
        )
    return df.loc[:, ~cols.duplicated(keep="first")]


def prepare_forward_trades_df(
    df: Optional[pd.DataFrame],
    *,
    context: str = "",
) -> pd.DataFrame:
    """딥다이브 입력 — 컬럼 dedupe + final_ret 수치화·NaN→0."""
    if df is None:
        return pd.DataFrame()
    out = dedupe_columns(df.copy(), log_warnings=True, context=context or "prepare_forward_trades_df")
    if out.empty or "final_ret" not in out.columns:
        return out
    out["final_ret"] = pd.to_numeric(col_series(out, "final_ret"), errors="coerce").fillna(0.0)
    return out
