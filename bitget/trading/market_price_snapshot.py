"""
Reference price SSOT — public StreamBuffer ticker with REST fallback.

Institutional rules:
  - Prefer fresh public WS ``last`` (opt-in daemon fills StreamBuffer)
  - Stale / missing / non-positive → None → REST ``fetch_ticker``
  - Never invent a price from an empty buffer
  - Symbol key via ``ws_stream_producer.normalize_inst_id`` (BTC/USDT:USDT → BTCUSDT)
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.data.ws_stream_producer import normalize_inst_id, normalize_inst_type
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import PUBLIC_REF_PRICE_MAX_AGE_SEC
from bitget.infra.network_retry import call_with_retry
from bitget.trading.oms_source_stats import record_oms_source

logger = get_logger("bitget.trading.market_price_snapshot")


def _inst_type_for_market(market_type: str) -> str:
    return normalize_inst_type(
        "SPOT" if str(market_type or "").strip().lower() == "spot" else "USDT-FUTURES"
    )


def try_public_ws_ref_price(
    symbol: str,
    *,
    market_type: str = "futures",
    max_age_sec: float | None = None,
) -> Optional[float]:
    """Fresh public ticker last (or bid/ask mid), else None."""
    try:
        from bitget.data.stream_buffer import get_stream_buffer

        buf = get_stream_buffer()
        inst_id = normalize_inst_id(symbol)
        inst_type = _inst_type_for_market(market_type)
        if not inst_id:
            return None
        max_age = float(
            max_age_sec if max_age_sec is not None else PUBLIC_REF_PRICE_MAX_AGE_SEC
        )
        age = buf.age_sec(inst_id, inst_type, source="ticker")
        if age is None or age > max_age:
            return None
        last = buf.get_last_price(inst_id, inst_type)
        try:
            if last is not None and float(last) > 0:
                return float(last)
        except (TypeError, ValueError):
            pass
        row = buf.get_ticker(inst_id, inst_type)
        if not isinstance(row, dict):
            return None
        bid = row.get("bid")
        ask = row.get("ask")
        try:
            b = float(bid) if bid is not None else None
            a = float(ask) if ask is not None else None
        except (TypeError, ValueError):
            return None
        if b is not None and a is not None and b > 0 and a > 0:
            return (b + a) / 2.0
        if b is not None and b > 0:
            return b
        if a is not None and a > 0:
            return a
        return None
    except Exception as e:
        logger.warning("public WS ref price unavailable: %s", e)
        return None


def _rest_ref_price(ex: Any, market_symbol: str) -> float:
    t = call_with_retry(
        lambda: ex.fetch_ticker(market_symbol),
        op="oms.fetch_ticker",
        throttle_key="bitget.fetch_ticker",
        throttle_interval_sec=0.2,
        default=None,
        swallow=True,
    )
    if not isinstance(t, dict):
        return 0.0
    try:
        return float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def fetch_ref_price(
    ex: Any,
    market_symbol: str,
    *,
    market_type: str = "futures",
    prefer_ws: bool = True,
    explicit: Optional[float] = None,
) -> float:
    """Resolve reference price for qty/notional normalize.

    ``explicit`` > 0 wins (caller-provided). Else public WS when fresh, else REST.
    """
    try:
        if explicit is not None and float(explicit) > 0:
            return float(explicit)
    except (TypeError, ValueError):
        pass

    if prefer_ws:
        ws_px = try_public_ws_ref_price(market_symbol, market_type=market_type)
        if ws_px is not None and ws_px > 0:
            record_oms_source("fetch_ticker", "public_ws")
            return float(ws_px)

    record_oms_source("fetch_ticker", "rest")
    return _rest_ref_price(ex, market_symbol)
