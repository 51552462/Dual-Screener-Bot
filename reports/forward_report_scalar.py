"""
포워드 리포트·딥다이브용 스칼라 추출 — Series/NaN/중복 컬럼/ BLOB 방어.
"""
from __future__ import annotations

import logging
import struct
from typing import Any, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# 리포트·PF 표시 상한 — Telegram "inf" 노출 방지
DEFAULT_PROFIT_FACTOR_CAP = 99.99
_MAX_ABS_MONEY_DISPLAY = 9.999e15


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


def _sanitize_numeric_series(s: pd.Series, *, default: float = 0.0) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
    return out


def profit_factor_from_returns(
    rets: Union[Sequence[float], pd.Series],
    *,
    cap: float = DEFAULT_PROFIT_FACTOR_CAP,
    epsilon: float = 1e-12,
) -> float:
    """
    Profit Factor = 총 이익(%) / 총 손실(%) 절댓값.
    손실 0건·전승 구간은 cap(기본 99.99) — float('inf') 리포트 노출 금지.
    """
    if rets is None:
        return 0.0
    if isinstance(rets, pd.Series):
        vals = pd.to_numeric(rets, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if vals.empty:
            return 0.0
        wins = float(vals[vals > 0].sum())
        losses = abs(float(vals[vals < 0].sum()))
    else:
        seq = list(rets)
        if not seq:
            return 0.0
        wins = sum(float(x) for x in seq if float(x) > 0)
        losses = abs(sum(float(x) for x in seq if float(x) < 0))
    if losses < epsilon:
        return float(cap) if wins > epsilon else 0.0
    pf = wins / losses
    if not np.isfinite(pf):
        return float(cap)
    return float(min(pf, cap))


def fmt_money(
    value: Any,
    *,
    market: str = "KR",
    signed: bool = False,
    default: float = 0.0,
) -> str:
    """금액 텔레그램 표시 — inf/nan → 0."""
    v = scalar_float(value, default)
    v = max(-_MAX_ABS_MONEY_DISPLAY, min(_MAX_ABS_MONEY_DISPLAY, v))
    if str(market or "").upper() == "US":
        return f"${v:+,.0f}" if signed else f"${v:,.0f}"
    return f"{v:+,.0f}원" if signed else f"{v:,.0f}원"


def fmt_amount(value: Any, *, decimals: int = 0, default: float = 0.0) -> str:
    """일반 수치(국고·비율 등) — inf/nan 방어."""
    v = scalar_float(value, default)
    v = max(-_MAX_ABS_MONEY_DISPLAY, min(_MAX_ABS_MONEY_DISPLAY, v))
    spec = f",.{int(decimals)}f"
    return format(v, spec)


def fmt_pct(
    value: Any,
    *,
    decimals: int = 2,
    signed: bool = True,
    default: float = 0.0,
) -> str:
    """수익률·MDD% 텔레그램 표시 — inf/nan → 0."""
    v = scalar_float(value, default)
    v = max(-9999.99, min(9999.99, v))
    if signed:
        return f"{v:+.{int(decimals)}f}%"
    return f"{v:.{int(decimals)}f}%"


def prepare_forward_trades_df(
    df: Optional[pd.DataFrame],
    *,
    context: str = "",
) -> pd.DataFrame:
    """딥다이브 입력 — 컬럼 dedupe + 수익·투입금 inf/NaN 정리."""
    if df is None:
        return pd.DataFrame()
    out = dedupe_columns(df.copy(), log_warnings=True, context=context or "prepare_forward_trades_df")
    if out.empty:
        return out
    for col in ("final_ret", "sim_kelly_invest", "invest_amount", "profit_amount"):
        if col in out.columns:
            out[col] = _sanitize_numeric_series(col_series(out, col), default=0.0)
    return out
