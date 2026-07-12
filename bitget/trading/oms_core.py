"""
Bitget OMS core — exchange factory, clientOid, market order placement.

Network resilience: ``bitget.infra.network_retry`` SSOT (Ch2).
"""
from __future__ import annotations

import re
import uuid
from typing import Optional

from bitget.env import bitget_access_key, bitget_passphrase, bitget_secret_key
from bitget.infra.logging_setup import get_logger, setup_logging
from bitget.infra.network_retry import NetworkRetryExhausted, call_with_retry
from bitget.trading.execution_safety import meta_kill_switch_active, oms_defense_block_reason

try:
    import ccxt
except Exception:
    ccxt = None

setup_logging()
logger = get_logger("bitget.trading.oms_core")

# Backward-compatible alias for reconciliation imports
_meta_kill_switch_active = meta_kill_switch_active


def create_trade_exchange(market_type="futures"):
    if ccxt is None:
        raise RuntimeError("ccxt not available")
    api_key = bitget_access_key()
    api_secret = bitget_secret_key()
    passphrase = bitget_passphrase()
    if not api_key or not api_secret or not passphrase:
        raise RuntimeError(
            "missing Bitget API credentials: set BITGET_ACCESS_KEY, BITGET_SECRET_KEY, BITGET_PASSPHRASE "
            "(or legacy BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE)"
        )
    ex = ccxt.bitget(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "password": passphrase,
            "enableRateLimit": True,
            "options": {"defaultType": "spot" if market_type == "spot" else "swap"},
        }
    )

    def _load():
        ex.load_markets()
        return True

    call_with_retry(
        _load,
        op="oms.load_markets",
        throttle_key="bitget.oms.load_markets",
        throttle_interval_sec=0.5,
        swallow=False,
    )
    return ex


def generate_client_oid(prefix="bg"):
    """Bitget clientOid: alphanumeric, max 40 chars."""
    core = uuid.uuid4().hex + uuid.uuid4().hex
    s = f"{prefix}{core}"[:40]
    return re.sub(r"[^a-zA-Z0-9]", "x", s)


def oms_place_market_order(
    ex,
    market_symbol: str,
    order_side: str,
    amount: float,
    params_base: Optional[dict] = None,
    client_oid: Optional[str] = None,
    max_attempts: int = 3,
    *,
    market_type: str = "futures",
    position_side: Optional[str] = None,
):
    """
    Market order with clientOid idempotency on transient network errors.
    Returns ok, order_id, client_order_id, raw, filled, remaining, status, message.
    """
    if meta_kill_switch_active():
        return {
            "ok": False,
            "order_id": "",
            "client_order_id": "",
            "raw": None,
            "filled": 0.0,
            "remaining": float(amount),
            "status": "meta_blocked",
            "message": "MetaGovernor KILL_SWITCH: new orders blocked",
        }
    defense = oms_defense_block_reason(
        market_symbol=market_symbol,
        market_type=market_type,
        position_side=position_side,
    )
    if defense and defense != "meta_kill_switch":
        status = (
            "circuit_blocked"
            if defense == "global_circuit_breaker"
            else "orphan_blocked"
            if defense == "oms_orphan_active"
            else "nav_blocked"
            if defense == "nav_dd_block"
            else "gross_blocked"
            if defense == "gross_notional_cap"
            else "tail_risk_blocked"
            if defense == "tail_risk_reserve"
            else "doomsday_blocked"
            if defense == "doomsday_defcon"
            else "concentration_blocked"
            if defense == "concentration_cap"
            else "price_sanity_blocked"
            if defense == "price_sanity"
            else "risk_blocked"
        )
        return {
            "ok": False,
            "order_id": "",
            "client_order_id": "",
            "raw": None,
            "filled": 0.0,
            "remaining": float(amount),
            "status": status,
            "message": f"OMS defense blocked: {defense}",
        }
    params_base = dict(params_base or {})
    oid_in = client_oid or generate_client_oid()
    merged = dict(params_base)
    merged["clientOid"] = oid_in
    used_oid = oid_in

    def _create():
        return ex.create_order(
            market_symbol, "market", order_side, float(amount), None, merged
        )

    try:
        order = call_with_retry(
            _create,
            op="oms.create_order",
            max_attempts=max(1, int(max_attempts)),
            throttle_key="bitget.oms.create_order",
            throttle_interval_sec=0.38,
            swallow=False,
        )
    except NetworkRetryExhausted as e:
        detail = e.last_error
        msg = str(detail.exc if detail else e)
        kind = detail.kind.value if detail else "exhausted"
        logger.warning("OMS create_order exhausted kind=%s: %s", kind, msg)
        return {
            "ok": False,
            "order_id": "",
            "client_order_id": used_oid,
            "raw": None,
            "filled": 0.0,
            "remaining": float(amount),
            "status": "error",
            "message": msg,
            "network_kind": kind,
        }
    except Exception as e:
        logger.warning("OMS create_order fatal: %s", e)
        return {
            "ok": False,
            "order_id": "",
            "client_order_id": used_oid,
            "raw": None,
            "filled": 0.0,
            "remaining": float(amount),
            "status": "error",
            "message": str(e),
        }

    filled = float(order.get("filled") or 0.0)
    remaining = float(order.get("remaining") or 0.0)
    amt = float(amount)
    st = str(order.get("status") or "")
    oid_out = str(order.get("id") or "")
    ok = True
    stat = "filled_submitted"
    if remaining > 0 and filled > 0 and filled + 1e-12 < amt:
        stat = "partial_fill"
    if st in ("rejected", "canceled", "cancelled"):
        ok = False
        stat = st
    return {
        "ok": ok,
        "order_id": oid_out,
        "client_order_id": used_oid,
        "raw": order,
        "filled": filled,
        "remaining": remaining,
        "status": stat,
        "message": "",
    }
