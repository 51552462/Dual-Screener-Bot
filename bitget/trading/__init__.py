"""Bitget trading layer exports."""
from bitget.trading.leverage_manager import (
    enforce_margin_mode,
    prepare_futures_order_params,
    resolve_leverage,
    resolve_margin_mode,
)
from bitget.trading.oms_core import create_trade_exchange, generate_client_oid, oms_place_market_order
from bitget.trading.position_manager import PositionSide, open_position
from bitget.trading.reconciliation import run_scheduled_reconciliation
from bitget.trading.slippage_guard import (
    audit_post_trade_slippage,
    check_pre_scan_liquidity,
    estimate_slippage_bps,
    run_pre_trade_gate,
)

__all__ = [
    "PositionSide",
    "audit_post_trade_slippage",
    "check_pre_scan_liquidity",
    "create_trade_exchange",
    "enforce_margin_mode",
    "estimate_slippage_bps",
    "generate_client_oid",
    "oms_place_market_order",
    "open_position",
    "prepare_futures_order_params",
    "resolve_leverage",
    "resolve_margin_mode",
    "run_pre_trade_gate",
    "run_scheduled_reconciliation",
]
