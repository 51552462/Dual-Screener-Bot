"""
Bitget position side abstraction + exchange position index.

Institutional rule for phantom/orphan recon:
  Private WS cache may be used ONLY when fresh. Stale/empty-uninitialized
  cache must NEVER imply \"flat book\" — that would mass-close virtual OPENs.
  Fallback is always REST ``fetch_positions`` via network_retry SSOT.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import PRIVATE_POS_INDEX_MAX_AGE_SEC
from bitget.infra.network_retry import call_with_retry
from bitget.symbol_utils import normalize_market_symbol
from bitget.trading.oms_source_stats import record_oms_source

logger = get_logger("bitget.trading.position_manager")


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


def private_inst_id_to_ccxt_futures(inst_id: str) -> str:
    """Map Bitget private WS instId (BTCUSDT) → ccxt futures (BTC/USDT:USDT)."""
    s = str(inst_id or "").strip().upper()
    if not s:
        return ""
    if "/" in s:
        return normalize_market_symbol(s, "futures")
    if s.endswith("USDT") and len(s) > 4:
        return normalize_market_symbol(f"{s[:-4]}/USDT", "futures")
    return normalize_market_symbol(s.replace("_", "/"), "futures")


def _contracts_from_ccxt_row(p: dict[str, Any]) -> float:
    try:
        c = float(p.get("contracts") or p.get("contractSize") or 0.0)
        if c is None or abs(c) < 1e-12:
            c = float(p.get("size") or 0.0)
    except (TypeError, ValueError):
        c = 0.0
    return float(c or 0.0)


def _side_from_ccxt_row(p: dict[str, Any], contracts: float) -> str:
    sd = str(p.get("side") or "").lower()
    if sd in ("long", "short"):
        return "LONG" if sd == "long" else "SHORT"
    return "LONG" if contracts > 0 else "SHORT"


def position_index_from_rest_rows(rows: Any) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = {}
    for p in rows or []:
        if not isinstance(p, dict):
            continue
        c = _contracts_from_ccxt_row(p)
        if abs(c) < 1e-12:
            continue
        sym = p.get("symbol")
        if not sym:
            continue
        side = _side_from_ccxt_row(p, c)
        out[(str(sym), side)] = abs(c)
    return out


def try_private_ws_position_index(
    *,
    max_age_sec: float = PRIVATE_POS_INDEX_MAX_AGE_SEC,
    inst_type: str = "USDT-FUTURES",
) -> Optional[dict[tuple[str, str], float]]:
    """
    Return position index from PrivateStreamBuffer when fresh; else None.

    Fresh empty dict is valid (true flat book after positions snapshot).
    Never-updated / other-channel-only buffer → None.
    """
    try:
        from bitget.data.stream_buffer import get_private_stream_buffer
    except Exception:
        return None

    try:
        buf = get_private_stream_buffer()
        age = float(buf.channel_age_sec("positions"))
        if age > float(max_age_sec):
            return None
        out: dict[tuple[str, str], float] = {}
        for row in buf.list_positions(inst_type=inst_type):
            if not isinstance(row, dict):
                continue
            inst_id = str(row.get("instId") or "").strip()
            if not inst_id:
                continue
            try:
                total = float(row.get("total") or row.get("available") or 0.0)
            except (TypeError, ValueError):
                total = 0.0
            if abs(total) < 1e-12:
                continue
            hold = str(row.get("holdSide") or row.get("posSide") or "").lower()
            if hold in ("long", "short"):
                side = "LONG" if hold == "long" else "SHORT"
            else:
                side = "LONG" if total > 0 else "SHORT"
            ccxt_sym = private_inst_id_to_ccxt_futures(inst_id)
            if not ccxt_sym:
                continue
            out[(ccxt_sym, side)] = abs(total)
        return out
    except Exception as e:
        logger.warning("private WS position index unavailable: %s", e)
        return None


def build_open_position_index(
    ex,
    *,
    prefer_private_ws: bool = True,
    max_private_age_sec: float = PRIVATE_POS_INDEX_MAX_AGE_SEC,
) -> dict[tuple[str, str], float]:
    """
    Return {(ccxt_symbol, 'LONG'|'SHORT'): contracts}.

    Prefer fresh private WS; REST fallback is authoritative when cache is cold/stale.
    """
    if prefer_private_ws:
        cached = try_private_ws_position_index(max_age_sec=max_private_age_sec)
        if cached is not None:
            record_oms_source("position_index", "private_ws")
            logger.info(
                "position index source=private_ws n=%s max_age=%.1fs",
                len(cached),
                max_private_age_sec,
            )
            return cached

    rows = call_with_retry(
        lambda: ex.fetch_positions(),
        op="oms.fetch_positions",
        throttle_key="bitget.oms.fetch_positions",
        throttle_interval_sec=0.4,
        default=None,
        swallow=True,
    )
    if rows is None:
        return {}
    out = position_index_from_rest_rows(rows)
    record_oms_source("position_index", "rest")
    logger.info("position index source=rest n=%s", len(out))
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
