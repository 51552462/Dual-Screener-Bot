"""
USDT equity snapshot SSOT — private WS account channel with REST fallback.

Institutional rules:
  - Trust account channel only when ``channel_age_sec("account")`` is fresh
  - Never-initialized / stale / empty-touch-without-row → None (REST), never invent 0
  - True zero equity in a fresh row is valid
  - Spot wallet is not on the futures private account feed → always REST for spot
  - Post-trade balance reads should force REST (account push may lag the fill)
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import PRIVATE_POS_INDEX_MAX_AGE_SEC
from bitget.infra.network_retry import call_with_retry
from bitget.trading.oms_source_stats import record_oms_source

logger = get_logger("bitget.trading.account_snapshot")


def _parse_nonneg_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def usdt_total_from_account_row(row: Optional[dict[str, Any]]) -> Optional[float]:
    """Map projected private account row → total USDT-like equity.

    Preference: usdtEquity → equity → available+frozen.
    Returns None when unparseable (caller must not treat as 0).
    """
    if not isinstance(row, dict):
        return None
    for key in ("usdtEquity", "equity"):
        parsed = _parse_nonneg_float(row.get(key))
        if parsed is not None:
            return parsed
    if "available" in row or "frozen" in row:
        avail = _parse_nonneg_float(row.get("available"))
        frozen = _parse_nonneg_float(row.get("frozen"))
        if avail is None and frozen is None:
            return None
        return float(avail or 0.0) + float(frozen or 0.0)
    return None


def try_private_ws_usdt_total(
    *,
    inst_type: str = "USDT-FUTURES",
    max_age_sec: float | None = None,
) -> Optional[float]:
    """Fresh account-channel equity, or None (stale / missing / unparseable)."""
    try:
        from bitget.data.stream_buffer import get_private_stream_buffer

        buf = get_private_stream_buffer()
        max_age = float(
            max_age_sec if max_age_sec is not None else PRIVATE_POS_INDEX_MAX_AGE_SEC
        )
        age = float(buf.channel_age_sec("account"))
        if age > max_age:
            return None
        row = buf.get_account(inst_type)
        # Fresh empty touch without a projected row → do not invent flat balance
        if row is None:
            return None
        return usdt_total_from_account_row(row)
    except Exception as e:
        logger.warning("private WS account equity unavailable: %s", e)
        return None


def _rest_usdt_total(ex: Any) -> float:
    bal = call_with_retry(
        lambda: ex.fetch_balance(),
        op="oms.fetch_balance",
        throttle_key="bitget.fetch_balance",
        throttle_interval_sec=0.22,
        default=None,
        swallow=True,
    )
    if not isinstance(bal, dict):
        return 0.0
    total = bal.get("total", {}) if isinstance(bal.get("total"), dict) else {}
    try:
        return float(total.get("USDT", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def fetch_usdt_balance(
    ex: Any,
    *,
    market_type: str = "futures",
    prefer_ws: bool = True,
    inst_type: str = "USDT-FUTURES",
) -> float:
    """USDT total for OMS sizing / PnL delta.

    - spot: always REST (private account feed is futures wallet)
    - futures + prefer_ws: private account channel when fresh
    - prefer_ws=False: force REST (post-fill truth)
    """
    mkt = str(market_type or "futures").strip().lower()
    if mkt == "spot" or not prefer_ws:
        record_oms_source("fetch_balance", "rest")
        return _rest_usdt_total(ex)

    ws_val = try_private_ws_usdt_total(inst_type=inst_type)
    if ws_val is not None:
        record_oms_source("fetch_balance", "private_ws")
        return float(ws_val)

    record_oms_source("fetch_balance", "rest")
    return _rest_usdt_total(ex)
