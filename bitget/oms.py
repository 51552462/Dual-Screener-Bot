"""
Backward-compatible OMS facade (Phase 5 — logic lives in bitget/trading/*).
"""
from bitget.trading.oms_core import create_trade_exchange, generate_client_oid, oms_place_market_order
from bitget.trading.reconciliation import run_scheduled_reconciliation

__all__ = [
    "create_trade_exchange",
    "generate_client_oid",
    "oms_place_market_order",
    "run_scheduled_reconciliation",
]
