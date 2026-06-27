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

# 가상 자본 곡선(복리) 기준 자본 — 고정 노출 합산을 대체하는 기관식 Equity Curve.
VIRTUAL_BASE_CAPITAL = {"KR": 100_000_000.0, "US": 100_000.0}  # 1억 원 / 10만 달러
# effective_kelly 미해결 시 보수적 기본 베팅 비중(f*).
DEFAULT_EFFECTIVE_KELLY = 0.02


def zero_invest_fallback_for_market(market: str) -> Optional[float]:
    m = str(market or "").upper().strip()
    if m == "KR":
        return KR_ZERO_INVEST_FALLBACK
    if m == "US":
        return US_ZERO_INVEST_FALLBACK
    return None


def virtual_base_capital_for_market(market: str) -> float:
    m = str(market or "").upper().strip()
    return VIRTUAL_BASE_CAPITAL.get(m, VIRTUAL_BASE_CAPITAL["KR"])


def _coerce_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if np.isnan(v) or not np.isfinite(v):
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
    """
    투입 노출액: sim_kelly_invest → invest_amount → **Live NAV × 유효 켈리**.

    [기관급 개편] 40만 원 고정 폴백을 폐기했다. 투입 금액이 누락된 거래는
    당일 시장 Live NAV(treasury_state.json) × 현재 유효 켈리 비중으로 노출액을 산출한다.
    zero_fallback 인자는 하위호환을 위해 시그니처만 유지하며 더 이상 평면 폴백으로 쓰지 않는다.
    """
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
    # 평면 40만 폐기 → Live NAV × 유효 켈리.
    try:
        from live_nav_manager import live_notional

        return float(live_notional(market))
    except Exception:
        # NAV 엔진 부재 시에만 최후 안전망(기준 자본 × 기본 켈리).
        return virtual_base_capital_for_market(market) * DEFAULT_EFFECTIVE_KELLY


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


def compute_virtual_equity_curve(
    df: pd.DataFrame,
    *,
    market: str = "KR",
    effective_kelly_risk: Optional[float] = None,
    base_capital: Optional[float] = None,
    col: str = "_realized_pnl",
) -> pd.DataFrame:
    """
    가상 자본 곡선(Virtual Equity Curve) — 고정 40만 노출 합산을 폐기한 기관식 복리 모델.

    시간순(exit_date)으로 청산된 각 거래에 그 시점의 켈리 비중 f* 를 곱해 자본을 복리로 증감:

        E_t = E_{t-1} × (1 + f* · R_t),   R_t = final_ret / 100,   f* = effective_kelly_risk

    반환 DataFrame 의 `col`(기본 _realized_pnl) 에는 **각 거래의 자본 증감액 ΔE_t = E_t − E_{t-1}** 을 적재한다.
    → 일자별 groupby-sum 은 텔레스코핑되어 그 날의 복리 Net PnL(=하루 시작/끝 자본 차)과 같고,
      전체 sum 은 주간 복리 Net PnL(=E_end − E_start) 과 같다. (단순 합산이 아닌 경로의존 복리)
    """
    if df is None or df.empty:
        out = df.copy() if df is not None else pd.DataFrame()
        if isinstance(out, pd.DataFrame):
            out[col] = pd.Series(dtype="float64")
        return out

    out = df.copy()
    base = (
        float(base_capital)
        if base_capital is not None
        else virtual_base_capital_for_market(market)
    )
    f = _coerce_float(effective_kelly_risk, 0.0)
    if f <= 0.0:
        f = DEFAULT_EFFECTIVE_KELLY

    # 경로의존 복리 → 시간순 정렬 필수(안정 정렬). exit_date → entry_date → id 순.
    sort_cols = [c for c in ("exit_date", "entry_date", "id") if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="mergesort")

    r = pd.to_numeric(out["final_ret"], errors="coerce").fillna(0.0).to_numpy(dtype=float) / 100.0
    growth = 1.0 + f * r
    growth = np.clip(growth, 0.0, None)  # -100% 미만(자본 음수) 방어
    equity = base * np.cumprod(growth)
    prev = np.concatenate(([base], equity[:-1])) if equity.size else np.array([])
    out[col] = (equity - prev) if equity.size else []
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
    total = float(
        add_realized_pnl_column(df, market=market, zero_fallback=fb)["_realized_pnl"].sum()
    )
    if not np.isfinite(total):
        return 0.0
    return total
