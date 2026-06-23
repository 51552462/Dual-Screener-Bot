"""
Institutional Macro Sentinel — 8자산 롤링 Z-Score · ATR-adjusted Momentum · ILI(0~100).

Shadow 전용. MetaGovernor·실전 config 에 쓰지 않는다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ROLLING_WINDOW = 20
ATR_PERIOD = 14
MOMENTUM_LAG = 5
REGIME_Z_UP = 0.50
REGIME_Z_DOWN = -0.50

# yfinance tickers (8 heterogeneous macro sleeves)
INSTITUTIONAL_ASSETS: Dict[str, str] = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "GOLD": "GC=F",
    "SILVER": "SI=F",
    "COPPER": "HG=F",
    "OIL": "CL=F",
    "DXY": "DX-Y.NYB",
    "US10Y": "^TNX",
}


@dataclass(frozen=True)
class MacroSentinelSnapshot:
    as_of: str
    institutional_liquidity_index: float
    composite_z: float
    liquidity_regime: str  # UP | DOWN | SIDEWAYS
    per_asset_z: Dict[str, float]
    per_asset_mom_z: Dict[str, float]
    regime_by_date: Dict[str, str]


def _rolling_z(series: pd.Series, window: int = ROLLING_WINDOW) -> pd.Series:
    mu = series.rolling(window, min_periods=max(5, window // 2)).mean()
    sd = series.rolling(window, min_periods=max(5, window // 2)).std(ddof=0)
    return (series - mu) / sd.replace(0, np.nan)


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(), (high - prev).abs(), (low - prev).abs()],
        axis=1,
    ).max(axis=1)
    return tr


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = ATR_PERIOD) -> pd.Series:
    tr = _true_range(high, low, close)
    return tr.rolling(period, min_periods=max(3, period // 2)).mean()


def _atr_adjusted_momentum(close: pd.Series, atr: pd.Series, lag: int = MOMENTUM_LAG) -> pd.Series:
    raw_mom = close - close.shift(lag)
    denom = atr.replace(0, np.nan)
    return raw_mom / denom


def _ili_from_composite_z(z: float) -> float:
    """Composite Z → 0~100 (σ-scaled, clipped). z=0 → 50."""
    if not np.isfinite(z):
        return 50.0
    return float(np.clip(50.0 + 15.0 * z, 0.0, 100.0))


def _regime_from_z(z: float) -> str:
    if not np.isfinite(z):
        return "SIDEWAYS"
    if z >= REGIME_Z_UP:
        return "UP"
    if z <= REGIME_Z_DOWN:
        return "DOWN"
    return "SIDEWAYS"


def _extract_ohlcv(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        if ticker in df.columns.get_level_values(-1):
            sub = df.xs(ticker, axis=1, level=-1, drop_level=False)
            if isinstance(sub.columns, pd.MultiIndex):
                sub.columns = sub.columns.get_level_values(0)
            return sub.copy()
        return pd.DataFrame()
    return df.copy()


def _fetch_prices(period: str = "1y") -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError as ex:
        raise RuntimeError("yfinance required for macro_sentinel_quant") from ex

    tickers = list(INSTITUTIONAL_ASSETS.values())
    raw = yf.download(
        tickers=tickers,
        period=period,
        interval="1d",
        group_by="column",
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    if raw is None or raw.empty:
        raise RuntimeError("yfinance returned empty macro panel")
    return raw


def compute_macro_sentinel_panel(
    prices: Optional[pd.DataFrame] = None,
    *,
    period: str = "1y",
) -> tuple[pd.DataFrame, pd.Series]:
    """
    자산별 composite signal 시계열 + 일별 composite_z 반환.
    columns: asset keys (BTC, ETH, ...)
    """
    raw = prices if prices is not None else _fetch_prices(period=period)
    asset_frames: Dict[str, pd.Series] = {}

    for key, yf_ticker in INSTITUTIONAL_ASSETS.items():
        ohlc = _extract_ohlcv(raw, yf_ticker)
        if ohlc.empty or "Close" not in ohlc.columns:
            logger.warning("macro_sentinel skip %s (%s) — no Close", key, yf_ticker)
            continue
        close = pd.to_numeric(ohlc["Close"], errors="coerce")
        high = pd.to_numeric(ohlc.get("High", close), errors="coerce")
        low = pd.to_numeric(ohlc.get("Low", close), errors="coerce")
        log_ret = np.log(close / close.shift(1))
        z_ret = _rolling_z(log_ret, ROLLING_WINDOW)
        atr = _atr(high, low, close, ATR_PERIOD)
        mom = _atr_adjusted_momentum(close, atr, MOMENTUM_LAG)
        z_mom = _rolling_z(mom, ROLLING_WINDOW)
        composite = 0.5 * z_ret + 0.5 * z_mom
        asset_frames[key] = composite

    if not asset_frames:
        raise RuntimeError("no macro assets produced signals")

    panel = pd.DataFrame(asset_frames)
    composite_z = panel.mean(axis=1, skipna=True)
    return panel, composite_z


def compute_macro_sentinel_snapshot(
    prices: Optional[pd.DataFrame] = None,
    *,
    period: str = "1y",
) -> MacroSentinelSnapshot:
    panel, composite_z = compute_macro_sentinel_panel(prices=prices, period=period)
    if composite_z.dropna().empty:
        raise RuntimeError("composite_z empty after macro panel build")

    latest_z = float(composite_z.dropna().iloc[-1])
    as_of = str(composite_z.dropna().index[-1])[:10]

    per_asset_z: Dict[str, float] = {}
    per_asset_mom_z: Dict[str, float] = {}
    for col in panel.columns:
        s = panel[col].dropna()
        if not s.empty:
            per_asset_z[col] = float(s.iloc[-1])

    regime_by_date: Dict[str, str] = {}
    for idx, z in composite_z.dropna().items():
        d = str(idx)[:10]
        regime_by_date[d] = _regime_from_z(float(z))

    return MacroSentinelSnapshot(
        as_of=as_of,
        institutional_liquidity_index=_ili_from_composite_z(latest_z),
        composite_z=latest_z,
        liquidity_regime=_regime_from_z(latest_z),
        per_asset_z=per_asset_z,
        per_asset_mom_z=per_asset_mom_z,
        regime_by_date=regime_by_date,
    )


def snapshot_to_dict(snap: MacroSentinelSnapshot) -> Dict[str, Any]:
    return {
        "as_of": snap.as_of,
        "institutional_liquidity_index": round(snap.institutional_liquidity_index, 2),
        "composite_z": round(snap.composite_z, 4),
        "liquidity_regime": snap.liquidity_regime,
        "per_asset_z": {k: round(v, 4) for k, v in snap.per_asset_z.items()},
        "regime_by_date": snap.regime_by_date,
    }
