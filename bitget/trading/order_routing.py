"""
Dynamic order routing — bridges ``DynamicOrderRouter`` to live OMS execution.

Resolves signal / ATR / orderbook inputs with safe HYBRID_TWAP 50/50 fallbacks,
then dispatches market, limit, or split hybrid orders.
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.logging_setup import get_logger
from bitget.trading.oms_core import (
    generate_client_oid,
    oms_place_limit_order,
    oms_place_market_order,
)
from dynamic_order_router import DynamicOrderRouter, RoutingDecision

logger = get_logger("bitget.trading.order_routing")

_ROUTER = DynamicOrderRouter()
_HYBRID_FALLBACK = RoutingDecision(
    route="HYBRID_TWAP",
    urgency_score=0.5,
    impact_cost=0.5,
    maker_ratio=0.5,
    taker_ratio=0.5,
    reason="routing_pipeline_exception_fallback",
)


def _normalize_signal_score(score: object) -> float:
    try:
        s = float(score)
    except (TypeError, ValueError):
        return 0.0
    if s > 10.0:
        s = s / 10.0
    return max(0.0, min(10.0, s))


def _resolve_atr_pct(
    *,
    atr_pct: Optional[float],
    atr_value: Optional[float],
    current_price: Optional[float],
) -> float:
    if atr_pct is not None:
        try:
            v = float(atr_pct)
            if v > 0.0:
                return v
        except (TypeError, ValueError):
            pass
    try:
        atr = float(atr_value or 0.0)
        px = float(current_price or 0.0)
        if atr > 0.0 and px > 0.0:
            return (atr / px) * 100.0
    except (TypeError, ValueError):
        pass
    return 0.0


def _sum_book_side(levels: Any) -> float:
    total = 0.0
    for lvl in levels or []:
        try:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                total += float(lvl[1])
            elif isinstance(lvl, dict):
                total += float(
                    lvl.get("size") or lvl.get("sz") or lvl.get("amount") or 0.0
                )
        except (TypeError, ValueError):
            continue
    return total


def compute_orderbook_imbalance(
    symbol: str,
    *,
    market_type: str = "futures",
    ex: Any = None,
    market_symbol: Optional[str] = None,
) -> float:
    """
    Bid/ask depth imbalance in [-1, 1].

    Prefers WS ``StreamBuffer``; optional REST ``fetch_order_book`` fallback.
    Returns 0.0 when data is unavailable.
    """
    try:
        from bitget.data.stream_buffer import get_stream_buffer
        from bitget.data.ws_stream_producer import normalize_inst_id, normalize_inst_type

        buf = get_stream_buffer()
        inst_id = normalize_inst_id(symbol)
        inst_type = normalize_inst_type(
            "SPOT" if str(market_type or "").strip().lower() == "spot" else "USDT-FUTURES"
        )
        ob = buf.get_orderbook(inst_id, inst_type)
        if ob:
            bid_vol = _sum_book_side(ob.get("bids"))
            ask_vol = _sum_book_side(ob.get("asks"))
            denom = bid_vol + ask_vol
            if denom > 0.0:
                return max(-1.0, min(1.0, (bid_vol - ask_vol) / denom))
    except Exception as e:
        logger.debug("WS orderbook imbalance unavailable: %s", e)

    if ex is not None and market_symbol:
        try:
            from bitget.infra.network_retry import call_with_retry

            book = call_with_retry(
                lambda: ex.fetch_order_book(market_symbol, limit=5),
                op="routing.fetch_order_book",
                throttle_key="bitget.routing.fetch_order_book",
                throttle_interval_sec=0.25,
                default=None,
                swallow=True,
            )
            if isinstance(book, dict):
                bid_vol = _sum_book_side(book.get("bids"))
                ask_vol = _sum_book_side(book.get("asks"))
                denom = bid_vol + ask_vol
                if denom > 0.0:
                    return max(-1.0, min(1.0, (bid_vol - ask_vol) / denom))
        except Exception as e:
            logger.debug("REST orderbook imbalance unavailable: %s", e)
    return 0.0


def resolve_top_of_book_price(
    ex: Any,
    market_symbol: str,
    order_side: str,
    *,
    market_type: str = "futures",
    px_ref: Optional[float] = None,
) -> Optional[float]:
    """Best bid for buy, best ask for sell — WS first, REST fallback."""
    side = str(order_side or "").strip().lower()
    is_buy = side == "buy"

    try:
        from bitget.data.stream_buffer import get_stream_buffer
        from bitget.data.ws_stream_producer import normalize_inst_id, normalize_inst_type

        buf = get_stream_buffer()
        inst_id = normalize_inst_id(market_symbol)
        inst_type = normalize_inst_type(
            "SPOT" if str(market_type or "").strip().lower() == "spot" else "USDT-FUTURES"
        )
        ob = buf.get_orderbook(inst_id, inst_type)
        if ob:
            px = ob.get("best_bid") if is_buy else ob.get("best_ask")
            if px is not None and float(px) > 0.0:
                return float(px)
        tk = buf.get_ticker(inst_id, inst_type)
        if tk:
            px = tk.get("bid") if is_buy else tk.get("ask")
            if px is not None and float(px) > 0.0:
                return float(px)
    except Exception:
        pass

    try:
        from bitget.infra.network_retry import call_with_retry

        book = call_with_retry(
            lambda: ex.fetch_order_book(market_symbol, limit=5),
            op="routing.fetch_order_book_price",
            throttle_key="bitget.routing.fetch_order_book",
            throttle_interval_sec=0.25,
            default=None,
            swallow=True,
        )
        if isinstance(book, dict):
            levels = book.get("bids") if is_buy else book.get("asks")
            if levels:
                try:
                    px = float(levels[0][0])
                    if px > 0.0:
                        return px
                except (TypeError, ValueError, IndexError):
                    pass
        ticker = call_with_retry(
            lambda: ex.fetch_ticker(market_symbol),
            op="routing.fetch_ticker_price",
            throttle_key="bitget.fetch_ticker",
            throttle_interval_sec=0.2,
            default=None,
            swallow=True,
        )
        if isinstance(ticker, dict):
            key = "bid" if is_buy else "ask"
            px = ticker.get(key)
            if px is not None and float(px) > 0.0:
                return float(px)
    except Exception as e:
        logger.debug("top-of-book price resolve failed: %s", e)

    try:
        if px_ref is not None and float(px_ref) > 0.0:
            return float(px_ref)
    except (TypeError, ValueError):
        pass
    return None


def safe_evaluate_routing(
    *,
    signal_score: object,
    atr_pct: Optional[float] = None,
    atr_value: Optional[float] = None,
    current_price: Optional[float] = None,
    orderbook_imbalance: Optional[float] = None,
    order_size_usd: object,
    symbol: str = "",
    market_type: str = "futures",
    ex: Any = None,
    market_symbol: Optional[str] = None,
) -> RoutingDecision:
    """Evaluate routing; any input/resolution failure → HYBRID_TWAP 50/50."""
    try:
        sig = _normalize_signal_score(signal_score)
        atr = _resolve_atr_pct(
            atr_pct=atr_pct,
            atr_value=atr_value,
            current_price=current_price,
        )
        if orderbook_imbalance is None:
            imbalance = compute_orderbook_imbalance(
                symbol or market_symbol or "",
                market_type=market_type,
                ex=ex,
                market_symbol=market_symbol,
            )
        else:
            imbalance = float(orderbook_imbalance)
        size_usd = float(order_size_usd)
        return _ROUTER.evaluate_routing(sig, atr, imbalance, size_usd)
    except Exception as e:
        logger.warning("order routing evaluation failed — HYBRID 50/50 fallback: %s", e)
        return _HYBRID_FALLBACK


def _split_qty_for_hybrid(
    ex: Any,
    market_symbol: str,
    total_qty: float,
    maker_ratio: float,
    taker_ratio: float,
) -> tuple[float, float]:
    total = float(total_qty or 0.0)
    if total <= 0.0:
        return 0.0, 0.0
    taker = total * float(taker_ratio)
    maker = total - taker
    try:
        taker = float(ex.amount_to_precision(market_symbol, taker))
        maker = float(ex.amount_to_precision(market_symbol, maker))
    except Exception:
        pass
    if maker + taker > total + 1e-12:
        maker = max(0.0, total - taker)
    if maker <= 0.0 and taker > 0.0:
        return 0.0, taker
    if taker <= 0.0 and maker > 0.0:
        return maker, 0.0
    return maker, taker


def routing_decision_to_meta(decision: RoutingDecision) -> dict[str, Any]:
    return {
        "routing_route": decision.route,
        "routing_urgency": decision.urgency_score,
        "routing_impact_cost": decision.impact_cost,
        "routing_maker_ratio": decision.maker_ratio,
        "routing_taker_ratio": decision.taker_ratio,
        "routing_reason": decision.reason,
    }


def execute_routed_orders(
    ex: Any,
    *,
    market_symbol: str,
    order_side: str,
    position_side: str,
    qty: float,
    px_ref: float,
    routing: RoutingDecision,
    params_base: dict,
    client_oid_prefix: str,
    max_attempts: int,
    market_type: str,
) -> dict[str, Any]:
    """
    Dispatch OMS orders according to ``routing.route``.

    Returns merged OMS outcome dict (``ok``, ``status``, ``filled``, …).
    """
    route = str(routing.route or "HYBRID_TWAP").upper()
    prefix = str(client_oid_prefix or "bg")[:12]

    if route == "TAKER_IMMEDIATE":
        coid = generate_client_oid(prefix)
        om = oms_place_market_order(
            ex,
            market_symbol,
            order_side,
            qty,
            params_base=params_base,
            client_oid=coid,
            max_attempts=max_attempts,
            market_type=market_type,
            position_side=position_side,
        )
        om["routing_route"] = route
        return om

    if route == "MAKER_PASSIVE":
        limit_px = resolve_top_of_book_price(
            ex,
            market_symbol,
            order_side,
            market_type=market_type,
            px_ref=px_ref,
        )
        if limit_px is None or limit_px <= 0.0:
            logger.warning("MAKER_PASSIVE: no limit price — falling back to market")
            coid = generate_client_oid(prefix)
            om = oms_place_market_order(
                ex,
                market_symbol,
                order_side,
                qty,
                params_base=params_base,
                client_oid=coid,
                max_attempts=max_attempts,
                market_type=market_type,
                position_side=position_side,
            )
            om["routing_route"] = "TAKER_IMMEDIATE"
            om["routing_fallback"] = "maker_price_unavailable"
            return om
        coid = generate_client_oid(prefix)
        om = oms_place_limit_order(
            ex,
            market_symbol,
            order_side,
            qty,
            limit_px,
            params_base=params_base,
            client_oid=coid,
            max_attempts=max_attempts,
            market_type=market_type,
            position_side=position_side,
        )
        om["routing_route"] = route
        om["limit_price"] = limit_px
        return om

    # HYBRID_TWAP — maker limit first, then taker market for residual urgency.
    maker_qty, taker_qty = _split_qty_for_hybrid(
        ex, market_symbol, qty, routing.maker_ratio, routing.taker_ratio
    )
    legs: list[dict[str, Any]] = []
    total_filled = 0.0
    total_remaining = 0.0
    ok = True
    last_status = "hybrid_submitted"

    if maker_qty > 0.0:
        limit_px = resolve_top_of_book_price(
            ex,
            market_symbol,
            order_side,
            market_type=market_type,
            px_ref=px_ref,
        )
        if limit_px is None or limit_px <= 0.0:
            taker_qty = float(qty)
            maker_qty = 0.0
        else:
            coid_m = generate_client_oid(prefix)
            om_m = oms_place_limit_order(
                ex,
                market_symbol,
                order_side,
                maker_qty,
                limit_px,
                params_base=params_base,
                client_oid=coid_m,
                max_attempts=max_attempts,
                market_type=market_type,
                position_side=position_side,
            )
            om_m["leg"] = "maker"
            om_m["limit_price"] = limit_px
            legs.append(om_m)
            total_filled += float(om_m.get("filled") or 0.0)
            total_remaining += float(om_m.get("remaining") or 0.0)
            if not om_m.get("ok", False):
                ok = False
                last_status = str(om_m.get("status") or "maker_reject")

    if taker_qty > 0.0:
        coid_t = generate_client_oid(prefix)
        om_t = oms_place_market_order(
            ex,
            market_symbol,
            order_side,
            taker_qty,
            params_base=params_base,
            client_oid=coid_t,
            max_attempts=max_attempts,
            market_type=market_type,
            position_side=position_side,
        )
        om_t["leg"] = "taker"
        legs.append(om_t)
        total_filled += float(om_t.get("filled") or 0.0)
        total_remaining += float(om_t.get("remaining") or 0.0)
        if not om_t.get("ok", False):
            ok = False
            last_status = str(om_t.get("status") or "taker_reject")
        elif ok:
            last_status = str(om_t.get("status") or "hybrid_submitted")

    primary = legs[-1] if legs else {}
    return {
        "ok": ok and bool(legs),
        "order_id": str(primary.get("order_id") or ""),
        "client_order_id": str(primary.get("client_order_id") or ""),
        "raw": primary.get("raw"),
        "filled": total_filled,
        "remaining": total_remaining,
        "status": last_status if legs else "hybrid_empty",
        "message": primary.get("message") or "",
        "routing_route": route,
        "hybrid_legs": legs,
        "maker_qty": maker_qty,
        "taker_qty": taker_qty,
    }
