"""
Backward-compatible OMS facade (Phase 5 — logic lives in bitget/trading/*).

Import paths preserved for legacy callers:
  bitget.oms.create_trade_exchange
  bitget.oms.oms_place_market_order
  bitget.oms.run_scheduled_reconciliation
"""
from bitget.trading.execution_safety import meta_kill_switch_active
from bitget.trading.oms_core import create_trade_exchange, generate_client_oid, oms_place_market_order
from bitget.trading.reconciliation import (
    detect_orphan_positions,
    reconcile_phantom_opens,
    run_scheduled_reconciliation,
)

__all__ = [
    "create_trade_exchange",
    "detect_orphan_positions",
    "generate_client_oid",
    "meta_kill_switch_active",
    "oms_place_market_order",
    "reconcile_phantom_opens",
    "run_scheduled_reconciliation",
]
