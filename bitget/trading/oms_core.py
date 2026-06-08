"""
Bitget OMS core — exchange factory, clientOid, market order placement.
"""
from __future__ import annotations

import re
import uuid
from typing import Optional

from bitget.env import bitget_access_key, bitget_passphrase, bitget_secret_key
from bitget.infra.logging_setup import get_logger, setup_logging
from bitget.rate_limit_guard import backoff_sleep, throttle
from meta_governor_consumer import load_meta_state_resolved

try:
    import ccxt
except Exception:
    ccxt = None

setup_logging()
logger = get_logger("bitget.trading.oms_core")


def _meta_kill_switch_active() -> bool:
    try:
        st = load_meta_state_resolved()
        fl = st.get("META_OPERATOR_FLAGS") or {}
        return bool(fl.get("KILL_SWITCH"))
    except Exception:
        return False


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
    ex.load_markets()
    return ex


def generate_client_oid(prefix="bg"):
    """Bitget clientOid: alphanumeric, max 40 chars."""
    core = uuid.uuid4().hex + uuid.uuid4().hex
    s = f"{prefix}{core}"[:40]
    return re.sub(r"[^a-zA-Z0-9]", "x", s)


def _transient_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    keys = (
        "timeout",
        "timed out",
        "network",
        "gateway",
        "502",
        "503",
        "504",
        "429",
        "ratelimit",
        "unavailable",
        "econnreset",
        "temporar",
    )
    return any(k in msg for k in keys)


def oms_place_market_order(
    ex,
    market_symbol: str,
    order_side: str,
    amount: float,
    params_base: Optional[dict] = None,
    client_oid: Optional[str] = None,
    max_attempts: int = 3,
):
    """
    Market order with clientOid idempotency on transient errors.
    Returns ok, order_id, client_order_id, raw, filled, remaining, status, message.
    """
    if _meta_kill_switch_active():
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
    params_base = dict(params_base or {})
    oid_in = client_oid or generate_client_oid()
    merged = dict(params_base)
    merged["clientOid"] = oid_in

    last_err = None
    used_oid = oid_in

    for attempt in range(max(1, int(max_attempts))):
        try:
            throttle("bitget.oms.create_order", 0.38)
            order = ex.create_order(market_symbol, "market", order_side, float(amount), None, merged)
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
        except Exception as e:
            last_err = e
            if _transient_error(e) and attempt < max_attempts - 1:
                backoff_sleep(attempt + 1)
                continue
            logger.warning("OMS create_order fail attempt %s: %s", attempt + 1, e)
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
    return {
        "ok": False,
        "order_id": "",
        "client_order_id": used_oid,
        "raw": None,
        "filled": 0.0,
        "remaining": float(amount),
        "status": "error",
        "message": str(last_err or "unknown"),
    }
