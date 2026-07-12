"""
Open-order snapshot SSOT — private WS orders channel with REST fallback.

Same institutional rule as position index:
  Trust private WS only when the *orders* channel is fresh.
  Account/position ticks must not make open-order book look current.
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import PRIVATE_POS_INDEX_MAX_AGE_SEC
from bitget.infra.network_retry import call_with_retry
from bitget.trading.oms_source_stats import record_oms_source
from bitget.trading.position_manager import private_inst_id_to_ccxt_futures

logger = get_logger("bitget.trading.order_snapshot")

# Bitget / ccxt open-ish statuses (terminal states excluded)
_OPEN_ORDER_STATUS = frozenset(
    {
        "live",
        "new",
        "open",
        "init",
        "pending",
        "partially_filled",
        "partial-filled",
        "partially-filled",
        "partial",
    }
)
_CLOSED_ORDER_STATUS = frozenset(
    {
        "filled",
        "canceled",
        "cancelled",
        "rejected",
        "expired",
        "closed",
    }
)


def is_open_order_status(status: Any) -> bool:
    st = str(status or "").strip().lower().replace(" ", "_")
    if not st:
        return False
    if st in _CLOSED_ORDER_STATUS:
        return False
    if st in _OPEN_ORDER_STATUS:
        return True
    # Unknown non-empty: treat as open only if not clearly terminal
    return "fill" not in st and "cancel" not in st and "reject" not in st


def _private_row_to_ccxt_like(row: dict[str, Any], *, for_hydrate: bool = False) -> dict[str, Any]:
    inst_id = str(row.get("instId") or "")
    sym = private_inst_id_to_ccxt_futures(inst_id) if inst_id else ""
    oid = str(row.get("orderId") or "")
    coid = str(row.get("clientOid") or "")
    size = row.get("size")
    filled = row.get("filledQty") or row.get("accBaseVolume") or row.get("baseVolume")
    try:
        sz_f = float(size) if size is not None else None
    except (TypeError, ValueError):
        sz_f = None
    try:
        filled_f = float(filled) if filled is not None else 0.0
    except (TypeError, ValueError):
        filled_f = 0.0
    remaining = None
    if sz_f is not None:
        remaining = max(0.0, sz_f - filled_f)
    avg = row.get("priceAvg") or row.get("fillPrice") or row.get("price")
    try:
        avg_f = float(avg) if avg is not None else None
    except (TypeError, ValueError):
        avg_f = None
    out: dict[str, Any] = {
        "id": oid,
        "clientOrderId": coid,
        "symbol": sym,
        "side": str(row.get("side") or ""),
        "status": str(row.get("status") or ""),
        "remaining": remaining if remaining is not None else "",
        "info": {"clientOid": coid},
        "_source": "private_ws",
    }
    if for_hydrate:
        out["filled"] = filled_f
        out["average"] = avg_f
        out["price"] = avg_f if avg_f is not None else row.get("price")
        if remaining is not None:
            out["remaining"] = remaining
    return out


def try_private_ws_open_orders(
    *,
    max_age_sec: float = PRIVATE_POS_INDEX_MAX_AGE_SEC,
    inst_type: str = "USDT-FUTURES",
) -> Optional[list[dict[str, Any]]]:
    """
    Return open-order list from PrivateStreamBuffer when orders channel is fresh.
    Fresh empty list is valid. Stale/never-updated orders channel → None.
    """
    try:
        from bitget.data.stream_buffer import get_private_stream_buffer
    except Exception:
        return None

    try:
        buf = get_private_stream_buffer()
        age = float(buf.channel_age_sec("orders"))
        if age > float(max_age_sec):
            return None
        out: list[dict[str, Any]] = []
        for row in buf.list_orders(inst_type=inst_type):
            if not isinstance(row, dict):
                continue
            if not is_open_order_status(row.get("status")):
                continue
            out.append(_private_row_to_ccxt_like(row))
        return out
    except Exception as e:
        logger.warning("private WS open orders unavailable: %s", e)
        return None


def try_private_ws_order(
    order_id: str,
    *,
    max_age_sec: float = PRIVATE_POS_INDEX_MAX_AGE_SEC,
    inst_type: str = "USDT-FUTURES",
) -> Optional[dict[str, Any]]:
    """
    Single-order lookup for hydrate (includes filled/canceled rows).

    Trust when:
      - order row exists AND (row age ≤ max OR orders channel age ≤ max)
    Miss / stale → None (caller REST). Never invent an order.
    """
    import time

    oid = str(order_id or "").strip()
    if not oid:
        return None
    try:
        from bitget.data.stream_buffer import get_private_stream_buffer
    except Exception:
        return None

    try:
        buf = get_private_stream_buffer()
        row = buf.get_order(oid, inst_type=inst_type)
        if row is None:
            return None
        try:
            row_age = max(0.0, time.monotonic() - float(row.get("ts_mono") or 0.0))
        except (TypeError, ValueError):
            row_age = 1e9
        chan_age = float(buf.channel_age_sec("orders"))
        if row_age > float(max_age_sec) and chan_age > float(max_age_sec):
            return None
        return _private_row_to_ccxt_like(row, for_hydrate=True)
    except Exception as e:
        logger.warning("private WS order lookup unavailable: %s", e)
        return None


def fetch_order_snapshot(
    ex,
    order_id: str,
    symbol: str,
    *,
    market_type: str = "futures",
    prefer_private_ws: bool = True,
    max_private_age_sec: float = PRIVATE_POS_INDEX_MAX_AGE_SEC,
) -> Optional[dict[str, Any]]:
    """
    Hydrate-grade order snapshot. None = failed / unavailable.
    Spot always REST (private daemon currently USDT-FUTURES).
    """
    mt = str(market_type or "futures").lower()
    if prefer_private_ws and mt != "spot":
        cached = try_private_ws_order(
            order_id, max_age_sec=max_private_age_sec, inst_type="USDT-FUTURES"
        )
        if cached is not None:
            record_oms_source("fetch_order", "private_ws")
            logger.info("fetch_order source=private_ws oid=%s", order_id)
            return cached

    od = call_with_retry(
        lambda: ex.fetch_order(str(order_id), symbol),
        op="oms.fetch_order",
        throttle_key="bitget.oms.fetch_order",
        throttle_interval_sec=0.35,
        default=None,
        swallow=True,
    )
    if od is None:
        return None
    if isinstance(od, dict):
        od = dict(od)
        od["_source"] = "rest"
    record_oms_source("fetch_order", "rest")
    logger.info("fetch_order source=rest oid=%s", order_id)
    return od if isinstance(od, dict) else None


def list_open_orders(
    ex,
    *,
    prefer_private_ws: bool = True,
    max_private_age_sec: float = PRIVATE_POS_INDEX_MAX_AGE_SEC,
) -> Optional[list[dict[str, Any]]]:
    """
    Open orders snapshot. None = transport failure (caller should not treat as flat).
    Empty list = confirmed no open orders.
    """
    if prefer_private_ws:
        cached = try_private_ws_open_orders(max_age_sec=max_private_age_sec)
        if cached is not None:
            record_oms_source("open_orders", "private_ws")
            logger.info("open orders source=private_ws n=%s", len(cached))
            return cached

    rows = call_with_retry(
        lambda: ex.fetch_open_orders(),
        op="oms.fetch_open_orders",
        throttle_key="bitget.oms.fetch_open_orders",
        throttle_interval_sec=0.45,
        default=None,
        swallow=True,
    )
    if rows is None:
        return None
    out = [r for r in (rows or []) if isinstance(r, dict)]
    record_oms_source("open_orders", "rest")
    logger.info("open orders source=rest n=%s", len(out))
    return out
