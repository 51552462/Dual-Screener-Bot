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


# =============================================================================
# [P0-3] 위험조정 성과지표 (Sharpe / Sortino / Calmar / MDD / 최대연속손실) + 어트리뷰션
#   - 전부 청산(CLOSED) 트레이드 원장(final_ret 등)에서 산출되는 **리포팅 전용** 순수 함수.
#   - 거래단위(per-trade) 지표를 거래빈도로 연율화(annualize)하며 가정은 라벨로 명시한다.
#   - 데이터 부족/오류 시 0·중립으로 수렴(예외 전파 금지) → 기존 리포트 무영향.
# =============================================================================

_TRADING_DAYS_PER_YEAR = 252.0


def _max_consecutive_losses(rets: Sequence[float]) -> int:
    """수익률 시퀀스(시간순)에서 최장 연속 손실(<0) 구간 길이."""
    longest = 0
    cur = 0
    for r in rets:
        try:
            v = float(r)
        except (TypeError, ValueError):
            continue
        if v < 0:
            cur += 1
            longest = max(longest, cur)
        else:
            cur = 0
    return int(longest)


def _equity_mdd(rets_frac_ordered: Sequence[float]) -> Tuple[float, float]:
    """시간순 수익률(소수)로 복리 자본곡선 → (MDD 소수(음수), 누적수익 소수)."""
    arr = np.asarray(list(rets_frac_ordered), dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0, 0.0
    eq = np.cumprod(1.0 + arr)
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / np.where(peak <= 0, np.nan, peak)
    mdd = float(np.nanmin(dd)) if np.isfinite(np.nanmin(dd)) else 0.0
    total = float(eq[-1] - 1.0)
    return mdd, total


def risk_adjusted_metrics(
    df_closed: Optional[pd.DataFrame],
    *,
    ret_col: str = "final_ret",
    exit_col: str = "exit_date",
    entry_col: str = "entry_date",
    invest_col: str = "sim_kelly_invest",
) -> dict:
    """청산 트레이드 → 위험조정 성과지표 묶음.

    반환 키: n, win_rate, pf, mean_ret_pct, std_ret_pct, downside_dev_pct,
    sharpe, sortino, calmar, mdd_pct, total_return_pct, cagr_pct,
    max_consec_losses, daily_vol_pct, span_days, trades_per_year.
    (sharpe/sortino 는 거래단위 → 거래빈도 sqrt 연율화. calmar = CAGR/|MDD|.)
    """
    out = {
        "n": 0, "win_rate": 0.0, "pf": 0.0, "mean_ret_pct": 0.0,
        "std_ret_pct": 0.0, "downside_dev_pct": 0.0, "sharpe": 0.0,
        "sortino": 0.0, "calmar": 0.0, "mdd_pct": 0.0,
        "total_return_pct": 0.0, "cagr_pct": 0.0, "max_consec_losses": 0,
        "daily_vol_pct": 0.0, "span_days": 0, "trades_per_year": 0.0,
    }
    if df_closed is None or getattr(df_closed, "empty", True) or ret_col not in df_closed.columns:
        return out

    df = df_closed.copy()
    df[ret_col] = pd.to_numeric(col_series(df, ret_col), errors="coerce")
    df = df.dropna(subset=[ret_col])
    if df.empty:
        return out

    # 시간순 정렬(청산일 우선, 없으면 원순서 유지)
    if exit_col in df.columns:
        df["_ord"] = df[exit_col].astype(str).str[:10]
        df = df.sort_values("_ord", kind="stable")

    rets_pct = df[ret_col].to_numpy(dtype=np.float64)
    rets_pct = rets_pct[np.isfinite(rets_pct)]
    n = int(rets_pct.size)
    if n == 0:
        return out
    rets = rets_pct / 100.0

    mean_r = float(np.mean(rets))
    std_r = float(np.std(rets, ddof=1)) if n >= 2 else 0.0
    downside = rets[rets < 0]
    downside_dev = float(np.sqrt(np.mean(np.square(downside)))) if downside.size else 0.0
    wins = int(np.sum(rets > 0))

    # 거래 기간(연 단위) 추정 → 거래빈도 연율화
    span_days = 0
    try:
        if exit_col in df.columns and entry_col in df.columns:
            d_start = pd.to_datetime(df[entry_col].astype(str).str[:10], errors="coerce").min()
            d_end = pd.to_datetime(df[exit_col].astype(str).str[:10], errors="coerce").max()
            if pd.notna(d_start) and pd.notna(d_end):
                span_days = max(1, int((d_end - d_start).days))
    except Exception:
        span_days = 0
    years = (span_days / 365.0) if span_days > 0 else 0.0
    trades_per_year = (n / years) if years > 0 else float(n)
    ann = float(np.sqrt(trades_per_year)) if trades_per_year > 0 else 1.0

    sharpe = (mean_r / std_r * ann) if std_r > 1e-12 else 0.0
    sortino = (mean_r / downside_dev * ann) if downside_dev > 1e-12 else 0.0

    mdd, total_return = _equity_mdd(rets)
    cagr = 0.0
    if years > 0 and total_return > -1.0:
        try:
            cagr = float((1.0 + total_return) ** (1.0 / years) - 1.0)
        except Exception:
            cagr = 0.0
    calmar = (cagr / abs(mdd)) if abs(mdd) > 1e-9 else 0.0

    # 일별 변동성(실현일 자본가중 수익률 기준) — 보조 지표
    daily_vol_pct = 0.0
    try:
        if exit_col in df.columns and invest_col in df.columns:
            tmp = df[[exit_col, ret_col, invest_col]].copy()
            tmp["_d"] = tmp[exit_col].astype(str).str[:10]
            tmp["_cap"] = pd.to_numeric(tmp[invest_col], errors="coerce").fillna(0.0).abs()
            tmp["_pnl"] = tmp["_cap"] * (tmp[ret_col] / 100.0)
            g = tmp.groupby("_d").agg(pnl=("_pnl", "sum"), cap=("_cap", "sum"))
            g = g[g["cap"] > 0]
            if len(g) >= 2:
                daily_ret = (g["pnl"] / g["cap"]).to_numpy(dtype=np.float64)
                daily_ret = daily_ret[np.isfinite(daily_ret)]
                if daily_ret.size >= 2:
                    daily_vol_pct = float(np.std(daily_ret, ddof=1) * 100.0)
    except Exception:
        daily_vol_pct = 0.0

    out.update({
        "n": n,
        "win_rate": float(wins / n * 100.0),
        "pf": profit_factor_from_returns(rets_pct),
        "mean_ret_pct": float(mean_r * 100.0),
        "std_ret_pct": float(std_r * 100.0),
        "downside_dev_pct": float(downside_dev * 100.0),
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "calmar": float(calmar),
        "mdd_pct": float(mdd * 100.0),
        "total_return_pct": float(total_return * 100.0),
        "cagr_pct": float(cagr * 100.0),
        "max_consec_losses": _max_consecutive_losses(rets_pct.tolist()),
        "daily_vol_pct": daily_vol_pct,
        "span_days": int(span_days),
        "trades_per_year": float(trades_per_year),
    })
    return out


def _strategy_label_from_sig(sig_type: Any) -> str:
    """sig_type → 엔진/소스 라벨. 선두 [TRADE_SOURCE] 브래킷 우선, 없으면 첫 토큰."""
    s = str(sig_type or "").strip()
    if not s:
        return "기타"
    import re as _re

    m = _re.match(r"^\s*\[([^\]]+)\]", s)
    if m:
        return m.group(1).strip()
    return s.split()[0][:24] if s.split() else "기타"


def attribution_table(
    df_closed: Optional[pd.DataFrame],
    group_col: str,
    *,
    ret_col: str = "final_ret",
    invest_col: str = "sim_kelly_invest",
    derive_strategy: bool = False,
    top: int = 8,
) -> List[dict]:
    """전략/섹터/국면별 수익귀속 표.

    각 행: {key, n, win_rate, pf, mean_ret_pct, pnl, contribution_pct}.
    pnl = Σ(sim_kelly_invest × final_ret/100). contribution_pct = pnl / Σ|pnl| × 100.
    데이터/컬럼 부재 시 빈 리스트.
    """
    if df_closed is None or getattr(df_closed, "empty", True):
        return []
    df = df_closed.copy()
    if ret_col not in df.columns:
        return []
    df[ret_col] = pd.to_numeric(col_series(df, ret_col), errors="coerce")
    df = df.dropna(subset=[ret_col])
    if df.empty:
        return []

    if derive_strategy:
        src_col = group_col if group_col in df.columns else "sig_type"
        if src_col not in df.columns:
            return []
        df["_grp"] = df[src_col].apply(_strategy_label_from_sig)
    else:
        if group_col not in df.columns:
            return []
        df["_grp"] = df[group_col].astype(str).replace({"": "기타", "nan": "기타"}).fillna("기타")

    if invest_col in df.columns:
        df["_cap"] = pd.to_numeric(col_series(df, invest_col), errors="coerce").fillna(0.0).abs()
    else:
        df["_cap"] = 0.0
    df["_pnl"] = df["_cap"] * (df[ret_col] / 100.0)

    total_abs_pnl = float(df["_pnl"].abs().sum())
    rows: List[dict] = []
    for key, g in df.groupby("_grp"):
        r = g[ret_col].to_numpy(dtype=np.float64)
        r = r[np.isfinite(r)]
        if r.size == 0:
            continue
        pnl = float(g["_pnl"].sum())
        rows.append({
            "key": str(key),
            "n": int(r.size),
            "win_rate": float(np.sum(r > 0) / r.size * 100.0),
            "pf": profit_factor_from_returns(r),
            "mean_ret_pct": float(np.mean(r)),
            "pnl": pnl,
            "contribution_pct": float(pnl / total_abs_pnl * 100.0) if total_abs_pnl > 1e-9 else 0.0,
        })
    rows.sort(key=lambda x: x["pnl"], reverse=True)
    return rows[: int(top)] if top and top > 0 else rows


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
