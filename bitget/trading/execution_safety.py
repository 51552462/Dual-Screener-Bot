"""
Execution safety gate chain — every live order must pass in order:

  1. ENABLE_REAL_EXECUTION (default false)
  2. REAL_EXECUTION_DRY_RUN (default true)
  3. MetaGovernor KILL_SWITCH
  4. Pre-trade slippage gate (WS orderbook spread)
  5. Leverage / margin manager (futures — applied in executor before OMS)
  6. OMS market order (oms_core — final defense-in-depth KILL_SWITCH)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from bitget.governance.meta_consumer import load_meta_state_resolved

from bitget.trading.slippage_guard import run_pre_trade_gate


class ExecutionGateOutcome(str, Enum):
    EXECUTION_DISABLED = "execution_disabled"
    DRY_RUN = "dry_run"
    META_BLOCKED = "meta_blocked"
    SLIPPAGE_BLOCKED = "slippage_blocked"
    APPROVED = "approved"


@dataclass
class GateResult:
    outcome: ExecutionGateOutcome
    message: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def proceed_to_exchange(self) -> bool:
        return self.outcome == ExecutionGateOutcome.APPROVED

    @property
    def is_dry_run(self) -> bool:
        return self.outcome == ExecutionGateOutcome.DRY_RUN

    @property
    def is_blocked(self) -> bool:
        return self.outcome in (
            ExecutionGateOutcome.EXECUTION_DISABLED,
            ExecutionGateOutcome.META_BLOCKED,
            ExecutionGateOutcome.SLIPPAGE_BLOCKED,
        )


def meta_kill_switch_active() -> bool:
    try:
        st = load_meta_state_resolved()
        fl = st.get("META_OPERATOR_FLAGS") or {}
        return bool(fl.get("KILL_SWITCH"))
    except Exception:
        return False


def evaluate_config_gates(cfg: dict) -> GateResult:
    """
    Gates 1–3: master switch, dry-run, MetaGovernor KILL_SWITCH.
    """
    if not bool(cfg.get("ENABLE_REAL_EXECUTION", False)):
        return GateResult(
            ExecutionGateOutcome.EXECUTION_DISABLED,
            message="ENABLE_REAL_EXECUTION is false",
        )
    if bool(cfg.get("REAL_EXECUTION_DRY_RUN", True)):
        return GateResult(
            ExecutionGateOutcome.DRY_RUN,
            message="REAL_EXECUTION_DRY_RUN is true",
        )
    if meta_kill_switch_active():
        return GateResult(
            ExecutionGateOutcome.META_BLOCKED,
            message="MetaGovernor KILL_SWITCH: new orders blocked",
        )
    return GateResult(ExecutionGateOutcome.APPROVED)


def evaluate_slippage_gate(
    market_symbol: str,
    market_type: str,
    cfg: dict,
) -> GateResult:
    """Gate 4: pre-trade slippage / spread check."""
    slip_ok, slip_meta = run_pre_trade_gate(market_symbol, market_type, cfg)
    if slip_ok:
        return GateResult(ExecutionGateOutcome.APPROVED, meta=dict(slip_meta))
    return GateResult(
        ExecutionGateOutcome.SLIPPAGE_BLOCKED,
        message=str(slip_meta.get("slippage_reason") or "slippage_blocked"),
        meta=dict(slip_meta),
    )


def run_pre_execution_gates(
    cfg: dict,
    *,
    market_symbol: str,
    market_type: str,
) -> GateResult:
    """
    Run gates 1–4 in order. Stops at first non-APPROVED outcome
    (except DRY_RUN which halts before exchange).
    """
    config_result = evaluate_config_gates(cfg)
    if config_result.outcome != ExecutionGateOutcome.APPROVED:
        return config_result
    return evaluate_slippage_gate(market_symbol, market_type, cfg)
