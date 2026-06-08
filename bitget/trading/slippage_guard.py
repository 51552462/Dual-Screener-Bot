"""
3-stage slippage pipeline: pre-scan (liquidity) / pre-trade (WS orderbook spread) / post-trade (fill audit).
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.data.stream_buffer import get_stream_buffer


def _inst_type_for_market(market_type: str) -> str:
    return "SPOT" if str(market_type).lower() == "spot" else "USDT-FUTURES"


def _normalize_inst_id(symbol: str) -> str:
    return str(symbol).replace("_", "").upper()


def _resolve_tv24_usdt(facts: dict | None, symbol: str, market_type: str) -> Optional[float]:
    fv = facts or {}
    if fv.get("trade_value_24h") is not None and str(fv.get("trade_value_24h")).strip() != "":
        try:
            return float(fv["trade_value_24h"])
        except (TypeError, ValueError):
            pass
    marcap = fv.get("marcap_eok")
    if marcap is not None and str(marcap).strip() != "":
        try:
            return float(marcap) * 100_000_000.0
        except (TypeError, ValueError):
            pass
    buf = get_stream_buffer()
    inst_type = _inst_type_for_market(market_type)
    inst_id = _normalize_inst_id(symbol)
    row = buf.get_ticker(inst_id, inst_type)
    if row and row.get("quote_volume_24h") is not None:
        try:
            return float(row["quote_volume_24h"])
        except (TypeError, ValueError):
            pass
    return None


def check_pre_scan_liquidity(
    group_seed_usdt: float,
    facts: dict | None,
    cfg: dict,
    *,
    symbol: str = "",
    market_type: str = "futures",
    is_incubator_shadow: bool = False,
) -> tuple[bool, str]:
    """
    Stage 1 — virtual entry: block large seed vs thin 24h volume.
    Returns (ok, reason).
    """
    if is_incubator_shadow:
        return True, "incubator_skip"
    seed_slip_thr = float(cfg.get("SEED_SLIPPAGE_GUARD_USDT", 50000.0))
    min_tv24_usdt = float(cfg.get("MIN_TRADE_VALUE_24H_SLIP_USDT", 5_000_000.0))
    fv = facts or {}
    has_liq = "trade_value_24h" in fv or "marcap_eok" in fv or symbol
    if group_seed_usdt <= seed_slip_thr or not has_liq:
        return True, "below_seed_threshold_or_no_liq_fields"
    tv24 = _resolve_tv24_usdt(fv, symbol, market_type)
    if tv24 is None:
        return True, "no_tv24_data_skip"
    if tv24 < min_tv24_usdt:
        return False, (
            f"pre_scan_liquidity: seed {group_seed_usdt:,.0f} USDT > {seed_slip_thr:,.0f} "
            f"but 24h vol {tv24:,.0f} < {min_tv24_usdt:,.0f} USDT"
        )
    return True, "ok"


def estimate_slippage_bps(
    symbol: str,
    *,
    market_type: str = "spot",
    max_spread_bps: float = 30.0,
    max_stale_sec: float = 30.0,
    require_orderbook: bool = False,
) -> tuple[bool, Optional[float], str]:
    """
    Stage 2 — pre-trade: WS orderbook spread (fallback ticker) + stale gate.
    Returns (ok, spread_bps, reason).
    """
    buf = get_stream_buffer()
    inst_type = _inst_type_for_market(market_type)
    inst_id = _normalize_inst_id(symbol)

    age = buf.age_sec(inst_id, inst_type)
    if age is None:
        return True, None, "no_ws_data_skip"
    if age > max_stale_sec:
        return False, None, f"stale_ws_{age:.1f}s"

    ob_spread = buf.orderbook_spread_bps(inst_id, inst_type)
    tk_spread = buf.ticker_spread_bps(inst_id, inst_type)

    if require_orderbook and ob_spread is None:
        return False, None, "no_orderbook_data"

    spread = ob_spread if ob_spread is not None else tk_spread
    source = "orderbook" if ob_spread is not None else "ticker"

    if spread is None:
        return True, None, "no_spread_data"

    if spread > max_spread_bps:
        return False, spread, f"{source}_spread_{spread:.1f}bps>{max_spread_bps}"

    return True, spread, f"ok_{source}"


def run_pre_trade_gate(symbol: str, market_type: str, cfg: dict) -> tuple[bool, dict[str, Any]]:
    """Stage 2 wrapper for executor — blocks live orders when spread/stale exceeds limits."""
    if not bool(cfg.get("ENABLE_SLIPPAGE_GUARD", True)):
        return True, {"slippage_reason": "guard_disabled"}

    max_spread = float(cfg.get("SLIPPAGE_MAX_SPREAD_BPS", 30.0))
    max_stale = float(cfg.get("SLIPPAGE_MAX_STALE_SEC", 30.0))
    require_ob = bool(cfg.get("SLIPPAGE_REQUIRE_ORDERBOOK", False))

    ok, spread_bps, reason = estimate_slippage_bps(
        symbol,
        market_type=market_type,
        max_spread_bps=max_spread,
        max_stale_sec=max_stale,
        require_orderbook=require_ob,
    )
    meta = {
        "slippage_spread_bps": spread_bps,
        "slippage_reason": reason,
        "slippage_max_spread_bps": max_spread,
    }
    return ok, meta


def audit_post_trade_slippage(
    expected_price: float,
    fill_price: float,
    *,
    max_bps: float = 50.0,
) -> dict[str, Any]:
    """
    Stage 3 — post-trade fill vs expected entry/reference price.
    """
    exp = float(expected_price or 0.0)
    fill = float(fill_price or 0.0)
    if exp <= 0 or fill <= 0:
        return {"ok": True, "skipped": True, "reason": "missing_prices"}
    bps = abs(fill - exp) / exp * 10000.0
    exceeded = bps > float(max_bps)
    return {
        "ok": not exceeded,
        "exceeded": exceeded,
        "slippage_bps": round(bps, 2),
        "max_bps": float(max_bps),
        "expected": exp,
        "fill": fill,
    }
