"""
Price sanity / bad-tick / flash-crash gate.

Invariants:
  - Block new entries when entry (or last bar) is an extreme outlier vs recent closes
  - Soft-pass if OHLCV too short / unreadable (never invent prices)
  - Never auto-flatten open inventory
  - ≤0 on a threshold disables that sub-check
"""
from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from bitget.infra.memory_policy import (
    BAD_TICK_LOOKBACK_BARS,
    BAD_TICK_MAX_BAR_RANGE_PCT,
    BAD_TICK_MAX_GAP_PCT,
    BAD_TICK_MAX_VS_MEDIAN_PCT,
    BAD_TICK_OHLCV_TF,
)


def _cfg_float(cfg: dict, key: str, default: float) -> float:
    try:
        raw = (cfg or {}).get(key, default)
        if raw is None or raw == "":
            return float(default)
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _cfg_int(cfg: dict, key: str, default: int) -> int:
    try:
        return max(1, int(float((cfg or {}).get(key, default) or default)))
    except (TypeError, ValueError):
        return int(default)


def _pct_gap(a: float, b: float) -> float:
    if b == 0 or b != b or a != a:
        return 0.0
    return abs(float(a) / float(b) - 1.0) * 100.0


def load_ohlcv_tail(
    *,
    symbol: str,
    market_type: str,
    timeframe: str,
    limit: int,
) -> Optional[pd.DataFrame]:
    try:
        from bitget.trading.concentration_gate import ohlcv_symbol_key
        from bitget.infra.data_paths import market_data_db_path
        from bitget.infra.shared_db_connector import get_connection
        import memory_bounds

        prefix = "SPOT" if str(market_type).lower() == "spot" else "FUT"
        key = ohlcv_symbol_key(symbol)
        tbl = f"BITGET_{prefix}_{key}_{timeframe}"
        tail = memory_bounds.ohlcv_limit_sql(bar_limit=int(limit))
        conn = get_connection(market_data_db_path())
        try:
            df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}"{tail}', conn)
        finally:
            conn.close()
    except Exception:
        return None
    if df is None or len(df) < 2:
        return None
    for col in ("Open", "High", "Low", "Close", "Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=("Open", "High", "Low", "Close"))
    return df if len(df) >= 2 else None


def analyze_price_sanity(
    cfg: dict,
    *,
    entry_price: Optional[float] = None,
    hist_df: Optional[pd.DataFrame] = None,
) -> tuple[bool, dict[str, Any]]:
    """
    True → block. Uses hist_df Close/OHLC only (caller supplies or loader).
    """
    lookback = _cfg_int(cfg, "BAD_TICK_LOOKBACK_BARS", BAD_TICK_LOOKBACK_BARS)
    max_gap = _cfg_float(cfg, "BAD_TICK_MAX_GAP_PCT", BAD_TICK_MAX_GAP_PCT)
    max_med = _cfg_float(cfg, "BAD_TICK_MAX_VS_MEDIAN_PCT", BAD_TICK_MAX_VS_MEDIAN_PCT)
    max_range = _cfg_float(cfg, "BAD_TICK_MAX_BAR_RANGE_PCT", BAD_TICK_MAX_BAR_RANGE_PCT)

    meta: dict[str, Any] = {
        "bad_tick_lookback": lookback,
        "bad_tick_max_gap_pct": max_gap,
        "bad_tick_max_vs_median_pct": max_med,
        "bad_tick_max_bar_range_pct": max_range,
    }

    if hist_df is None or len(hist_df) < 2:
        meta["price_sanity"] = "soft_pass_insufficient_ohlcv"
        return False, meta

    closes = pd.to_numeric(hist_df["Close"], errors="coerce").dropna()
    if len(closes) < 2:
        meta["price_sanity"] = "soft_pass_insufficient_ohlcv"
        return False, meta

    last_close = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2])
    meta["last_close"] = last_close
    meta["prev_close"] = prev_close

    # OHLC integrity on last bar
    try:
        last = hist_df.iloc[-1]
        o = float(last["Open"])
        h = float(last["High"])
        low = float(last["Low"])
        c = float(last["Close"])
    except Exception:
        o = h = low = c = last_close

    if min(o, h, low, c) <= 0:
        meta["price_sanity"] = "block_non_positive_ohlc"
        return True, meta
    if h + 1e-12 < low:
        meta["price_sanity"] = "block_high_lt_low"
        return True, meta
    if c > h * 1.002 or c < low * 0.998 or o > h * 1.002 or o < low * 0.998:
        meta["price_sanity"] = "block_ohlc_inconsistent"
        meta["ohlc"] = {"o": o, "h": h, "l": low, "c": c}
        return True, meta

    if max_range > 0 and c > 0:
        range_pct = (h - low) / c * 100.0
        meta["last_bar_range_pct"] = round(range_pct, 4)
        if range_pct >= max_range:
            meta["price_sanity"] = "block_bar_range"
            return True, meta

    gap_last = _pct_gap(last_close, prev_close)
    meta["last_vs_prev_gap_pct"] = round(gap_last, 4)
    if max_gap > 0 and gap_last >= max_gap:
        meta["price_sanity"] = "block_last_bar_gap"
        return True, meta

    need = lookback + 1
    if len(closes) < need:
        # still ran integrity + last gap; median soft if short
        if entry_price is None:
            meta["price_sanity"] = "ok_short_history"
            return False, meta
        # entry vs prev only
        try:
            ep = float(entry_price)
        except (TypeError, ValueError):
            meta["price_sanity"] = "soft_pass_bad_entry"
            return False, meta
        if ep <= 0:
            meta["price_sanity"] = "block_non_positive_entry"
            return True, meta
        eg = _pct_gap(ep, prev_close)
        meta["entry_vs_prev_gap_pct"] = round(eg, 4)
        meta["entry_price"] = ep
        if max_gap > 0 and eg >= max_gap:
            meta["price_sanity"] = "block_entry_gap"
            return True, meta
        meta["price_sanity"] = "ok"
        return False, meta

    ref = closes.iloc[-(lookback + 1) : -1]
    median = float(ref.median())
    meta["ref_median"] = median

    if entry_price is not None:
        try:
            ep = float(entry_price)
        except (TypeError, ValueError):
            meta["price_sanity"] = "soft_pass_bad_entry"
            return False, meta
        if ep <= 0:
            meta["price_sanity"] = "block_non_positive_entry"
            return True, meta
        meta["entry_price"] = ep
        eg = _pct_gap(ep, prev_close)
        em = _pct_gap(ep, median) if median > 0 else 0.0
        meta["entry_vs_prev_gap_pct"] = round(eg, 4)
        meta["entry_vs_median_gap_pct"] = round(em, 4)
        if max_gap > 0 and eg >= max_gap:
            meta["price_sanity"] = "block_entry_gap"
            return True, meta
        if max_med > 0 and em >= max_med:
            meta["price_sanity"] = "block_entry_vs_median"
            return True, meta
    else:
        # live path without explicit entry — median check on last close
        lm = _pct_gap(last_close, median) if median > 0 else 0.0
        meta["last_vs_median_gap_pct"] = round(lm, 4)
        if max_med > 0 and lm >= max_med:
            meta["price_sanity"] = "block_last_vs_median"
            return True, meta

    meta["price_sanity"] = "ok"
    return False, meta


def price_sanity_entry_blocked(
    cfg: dict,
    *,
    symbol: str,
    market_type: str = "futures",
    timeframe: Optional[str] = None,
    entry_price: Optional[float] = None,
    hist_df: Optional[pd.DataFrame] = None,
) -> tuple[bool, dict[str, Any]]:
    """Paper/live helper. Soft-pass when disabled thresholds all ≤0 and no OHLC fault…"""
    max_gap = _cfg_float(cfg, "BAD_TICK_MAX_GAP_PCT", BAD_TICK_MAX_GAP_PCT)
    max_med = _cfg_float(cfg, "BAD_TICK_MAX_VS_MEDIAN_PCT", BAD_TICK_MAX_VS_MEDIAN_PCT)
    max_range = _cfg_float(cfg, "BAD_TICK_MAX_BAR_RANGE_PCT", BAD_TICK_MAX_BAR_RANGE_PCT)
    # All disable → only OHLC integrity still runs if hist present
    df = hist_df
    if df is None:
        tf = str(timeframe or cfg.get("BAD_TICK_OHLCV_TF") or BAD_TICK_OHLCV_TF)
        lookback = _cfg_int(cfg, "BAD_TICK_LOOKBACK_BARS", BAD_TICK_LOOKBACK_BARS)
        df = load_ohlcv_tail(
            symbol=symbol,
            market_type=market_type,
            timeframe=tf,
            limit=max(lookback + 5, 20),
        )
    return analyze_price_sanity(cfg, entry_price=entry_price, hist_df=df)
