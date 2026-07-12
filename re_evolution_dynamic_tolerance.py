"""
Re-Evolution Dynamic EV Tolerance — ATR × 국면 가중 동적 슬리피지 SSOT.

고정 1~2% 폐기:
  tolerance_pct = (ATR14 / Price × 100) × regime_weight
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

ATR_LOOKBACK_DAYS = 14
DEFAULT_MIN_TOLERANCE_PCT = 0.8
DEFAULT_MAX_TOLERANCE_PCT = 8.0

# META_REGIME_KEY → EV 오차율 가중 (Architect)
REGIME_EV_TOLERANCE_WEIGHT: Dict[str, float] = {
    "HIGH_VOL": 1.5,
    "BEAR_PANIC": 1.5,
    "BEAR": 1.3,
    "BEAR_ACCEL": 1.35,
    "BEAR_GRIND": 1.2,
    "SIDEWAYS": 0.8,
    "CHOP": 0.8,
    "WHIPSAW": 0.8,
    "BULL": 1.0,
    "UNKNOWN": 1.0,
}


def _cfg_float(cfg: Optional[Dict[str, Any]], key: str, default: float) -> float:
    if not isinstance(cfg, dict):
        return default
    block = cfg.get("RE_EVOLUTION_DYNAMIC_TOLERANCE") or {}
    base = block if isinstance(block, dict) else cfg
    try:
        return float(base.get(key, cfg.get(key, default)))
    except (TypeError, ValueError):
        return default


def dynamic_tolerance_config(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "atr_lookback_days": int(
            _cfg_float(sys_config, "RE_EVOLUTION_ATR_LOOKBACK_DAYS", ATR_LOOKBACK_DAYS)
        ),
        "min_tolerance_pct": _cfg_float(
            sys_config, "RE_EVOLUTION_MIN_TOLERANCE_PCT", DEFAULT_MIN_TOLERANCE_PCT
        ),
        "max_tolerance_pct": _cfg_float(
            sys_config, "RE_EVOLUTION_MAX_TOLERANCE_PCT", DEFAULT_MAX_TOLERANCE_PCT
        ),
        "fallback_tolerance_pct": _cfg_float(
            sys_config, "RE_EVOLUTION_EV_SLIPPAGE_TOLERANCE_PCT", 2.5
        ),
    }


def resolve_regime_ev_tolerance_weight(
    regime_key: str,
    *,
    meta: Optional[Mapping[str, Any]] = None,
) -> float:
    """META_REGIME_KEY(+ bear sub-phase) → EV 오차율 가중."""
    raw = str(regime_key or "").strip().upper()
    if not raw and isinstance(meta, Mapping):
        try:
            from strategy_lifecycle_config import resolve_effective_regime_key

            raw, _ = resolve_effective_regime_key(meta)
        except Exception:
            raw = str(meta.get("META_REGIME_KEY") or "UNKNOWN").upper()

    if raw in REGIME_EV_TOLERANCE_WEIGHT:
        return float(REGIME_EV_TOLERANCE_WEIGHT[raw])
    if "PANIC" in raw or raw == "HIGH_VOL":
        return float(REGIME_EV_TOLERANCE_WEIGHT["HIGH_VOL"])

    try:
        from strategy_lifecycle_config import resolve_bear_stress_subphase

        sub = resolve_bear_stress_subphase(meta)
        if sub == "BEAR_PANIC":
            return float(REGIME_EV_TOLERANCE_WEIGHT["BEAR_PANIC"])
        if sub == "BEAR_ACCEL":
            return float(REGIME_EV_TOLERANCE_WEIGHT["BEAR_ACCEL"])
        if sub == "BEAR_GRIND":
            return float(REGIME_EV_TOLERANCE_WEIGHT["BEAR_GRIND"])
    except Exception:
        pass

    try:
        from meta_state_store import normalize_regime_key

        nk = normalize_regime_key(raw or "UNKNOWN")
    except Exception:
        nk = raw or "UNKNOWN"
        if nk in ("CHOP", "WHIPSAW"):
            nk = "SIDEWAYS"

    return float(REGIME_EV_TOLERANCE_WEIGHT.get(nk, REGIME_EV_TOLERANCE_WEIGHT["UNKNOWN"]))


def _atr_pct_from_ohlc(high, low, close, span: int = 14) -> Optional[float]:
    try:
        import numpy as np
    except ImportError:
        return None

    h = np.asarray(high, dtype=float)
    l = np.asarray(low, dtype=float)
    c = np.asarray(close, dtype=float)
    if len(c) < span + 2:
        return None

    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))

    # Wilder-style EMA (span=14)
    alpha = 1.0 / float(span)
    atr = np.empty_like(tr)
    atr[0] = tr[0]
    for i in range(1, len(tr)):
        atr[i] = alpha * tr[i] + (1.0 - alpha) * atr[i - 1]

    last_atr = float(atr[-1])
    last_close = float(c[-1])
    if last_close <= 0:
        return None
    return (last_atr / last_close) * 100.0


def fetch_market_atr14_volatility_pct(
    market: str,
    *,
    lookback_days: int = ATR_LOOKBACK_DAYS,
    meta: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    시장 벤치마크 14일 ATR / Price (%).
    KR: ^KS11, US: SPY. BG: META_VOL_ATR_PCT_Q 폴백.
    """
    mk = str(market or "KR").upper()
    out: Dict[str, Any] = {
        "market": mk,
        "atr_lookback_days": int(lookback_days),
        "atr_pct": None,
        "benchmark": None,
        "source": "unavailable",
    }

    if mk == "BG" and isinstance(meta, Mapping):
        vol = meta.get("META_VOL_ATR_PCT_Q")
        if isinstance(vol, dict):
            try:
                v = float(vol.get("atr_pct_last"))
                if v > 0:
                    out["atr_pct"] = round(v, 4)
                    out["benchmark"] = vol.get("bench")
                    out["source"] = "meta_vol_atr_pct_q"
                    return out
            except (TypeError, ValueError):
                pass

    sym = "^KS11" if mk == "KR" else "SPY"
    out["benchmark"] = sym
    try:
        import yfinance as yf

        hist = yf.Ticker(sym).history(period="4mo", auto_adjust=True)
        if hist is None or hist.empty:
            return out
        atr_pct = _atr_pct_from_ohlc(
            hist["High"].values,
            hist["Low"].values,
            hist["Close"].values,
            span=int(lookback_days),
        )
        if atr_pct is not None:
            out["atr_pct"] = round(float(atr_pct), 4)
            out["source"] = "yfinance_benchmark"
    except Exception as ex:
        logger.debug("fetch_market_atr14 failed %s: %s", mk, ex)
    return out


def compute_dynamic_ev_tolerance_pct(
    market: str,
    *,
    meta: Optional[Mapping[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    regime_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    동적 EV 오차율(%p):
      tolerance_pct = clamp(atr_pct × regime_weight, min, max)
    """
    cfg = dynamic_tolerance_config(sys_config)
    mk = str(market or "KR").upper()

    rk = str(regime_key or "").strip().upper()
    if not rk and isinstance(meta, Mapping):
        try:
            from strategy_lifecycle_config import resolve_effective_regime_key

            rk, _raw = resolve_effective_regime_key(meta, market=mk)
        except Exception:
            rk = str(meta.get("META_REGIME_KEY") or "UNKNOWN").upper()

    regime_w = resolve_regime_ev_tolerance_weight(rk, meta=meta)
    atr_block = fetch_market_atr14_volatility_pct(
        mk,
        lookback_days=int(cfg["atr_lookback_days"]),
        meta=meta,
    )
    atr_pct = atr_block.get("atr_pct")
    fallback = float(cfg["fallback_tolerance_pct"])

    if atr_pct is None:
        tolerance = fallback
        source = "fallback_fixed"
    else:
        tolerance = float(atr_pct) * float(regime_w)
        source = "atr_x_regime"

    tol_min = float(cfg["min_tolerance_pct"])
    tol_max = float(cfg["max_tolerance_pct"])
    tolerance = max(tol_min, min(tol_max, tolerance))

    return {
        "market": mk,
        "regime_key": rk or "UNKNOWN",
        "regime_weight": round(regime_w, 4),
        "atr_pct": atr_pct,
        "atr_benchmark": atr_block.get("benchmark"),
        "atr_source": atr_block.get("source"),
        "tolerance_pct": round(tolerance, 4),
        "tolerance_min_pct": tol_min,
        "tolerance_max_pct": tol_max,
        "formula": "atr14_pct * regime_weight",
        "source": source,
    }


def enrich_ev_ramp_config_with_dynamic_tolerance(
    base_cfg: Mapping[str, Any],
    market: str,
    *,
    meta: Optional[Mapping[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """ev_rampup cfg에 동적 slippage_tolerance_pct 주입."""
    out = dict(base_cfg)
    dyn = compute_dynamic_ev_tolerance_pct(market, meta=meta, sys_config=sys_config)
    out["slippage_tolerance_pct"] = float(dyn["tolerance_pct"])
    out["dynamic_tolerance"] = dyn
    return out
