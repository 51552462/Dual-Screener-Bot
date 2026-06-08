"""
Bitget position side abstraction + exchange position index.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from bitget.rate_limit_guard import throttle
from bitget.symbol_utils import normalize_market_symbol


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


def normalize_position_side(side: Any) -> str:
    s = str(side or "LONG").upper()
    return s if s in ("LONG", "SHORT") else "LONG"


def ccxt_order_side(position_side: str, *, opening: bool = True) -> str:
    """Map LONG/SHORT + open/close intent to ccxt buy/sell."""
    ps = normalize_position_side(position_side)
    if opening:
        return "buy" if ps == "LONG" else "sell"
    return "sell" if ps == "LONG" else "buy"


def row_ccxt_future_symbol(internal_sym: str) -> str:
    return normalize_market_symbol(str(internal_sym).replace("_", "/"), "futures")


def build_open_position_index(ex) -> dict[tuple[str, str], float]:
    """Return {(ccxt_symbol, 'LONG'|'SHORT'): contracts}."""
    throttle("bitget.oms.fetch_positions", 0.4)
    rows = ex.fetch_positions()
    out: dict[tuple[str, str], float] = {}
    for p in rows or []:
        try:
            c = float(p.get("contracts") or p.get("contractSize") or 0.0)
            if c is None or abs(c) < 1e-12:
                c = float(p.get("size") or 0.0)
        except (TypeError, ValueError):
            c = 0.0
        if abs(c) < 1e-12:
            continue
        sym = p.get("symbol")
        if not sym:
            continue
        sd = str(p.get("side") or "").lower()
        if sd in ("long", "short"):
            side = "LONG" if sd == "long" else "SHORT"
        else:
            side = "LONG" if c > 0 else "SHORT"
        out[(sym, side)] = abs(c)
    return out


def open_position(
    symbol: str,
    side: str | PositionSide,
    amount: float,
    *,
    market_type: str = "futures",
    leverage: float = 3.0,
    strategy_key: Optional[str] = None,
    margin_mode: Optional[str] = None,
) -> dict[str, Any]:
    """High-level entry — delegates to executor (dry-run gate preserved)."""
    from bitget.executor import execute_real_order

    ps = normalize_position_side(side)
    return execute_real_order(
        symbol,
        ps,
        amount,
        leverage=leverage,
        market_type=market_type,
        strategy_key=strategy_key,
        margin_mode=margin_mode,
    )
