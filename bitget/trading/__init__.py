"""Bitget trading layer exports."""
from bitget.trading.execution_safety import (
    ExecutionGateOutcome,
    GateResult,
    evaluate_config_gates,
    evaluate_slippage_gate,
    meta_kill_switch_active,
    run_pre_execution_gates,
)
from bitget.trading.leverage_manager import (
    enforce_margin_mode,
    prepare_futures_order_params,
    resolve_leverage,
    resolve_margin_mode,
)
from bitget.trading.oms_core import create_trade_exchange, generate_client_oid, oms_place_market_order
from bitget.trading.position_manager import PositionSide, open_position
from bitget.trading.reconciliation import (
    detect_orphan_positions,
    reconcile_phantom_opens,
    run_scheduled_reconciliation,
)
from bitget.trading.slippage_guard import (
    audit_post_trade_slippage,
    check_pre_scan_liquidity,
    estimate_slippage_bps,
    run_pre_trade_gate,
)

__all__ = [
    "ExecutionGateOutcome",
    "GateResult",
    "PositionSide",
    "audit_post_trade_slippage",
    "check_pre_scan_liquidity",
    "create_trade_exchange",
    "detect_orphan_positions",
    "enforce_margin_mode",
    "estimate_slippage_bps",
    "evaluate_config_gates",
    "evaluate_slippage_gate",
    "generate_client_oid",
    "meta_kill_switch_active",
    "oms_place_market_order",
    "open_position",
    "prepare_futures_order_params",
    "reconcile_phantom_opens",
    "resolve_leverage",
    "resolve_margin_mode",
    "run_pre_execution_gates",
    "run_pre_trade_gate",
    "run_scheduled_reconciliation",
]
