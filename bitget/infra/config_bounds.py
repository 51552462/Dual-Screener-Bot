"""
Config write-time hard bounds — capital-parameter SSOT.

Evolution / auto_pilot / meta sync may propose extreme Kelly, cutoffs, leverage.
Clamping at save prevents those writes from bypassing live risk gates.

Invariants:
  - Clamp (never reject the whole save) — unknown keys pass through
  - Non-numeric for a bounded key → leave unchanged (soft-pass)
  - After clamp, enforce NAV_DD_REDUCE ≤ BLOCK ≤ HALT monotonicity
  - Never invent keys; never flatten positions
"""
from __future__ import annotations

from typing import Any, Optional

# (lo, hi) inclusive. 0 is intentional for gates that use ≤0 as disable.
CONFIG_NUMERIC_BOUNDS: dict[str, tuple[float, float]] = {
    # Sizing / Kelly
    "DYNAMIC_KELLY_RISK": (0.001, 0.05),
    "MAX_POSITION_PCT": (0.01, 1.0),
    "FUTURES_LEVERAGE": (1.0, 25.0),
    "MAX_LEVERAGE": (1.0, 25.0),
    # Entry cutoffs (cosine / ML)
    "DYNAMIC_SUPERNOVA_CUTOFF": (0.05, 0.99),
    "DYNAMIC_ML_BOX_CUTOFF": (0.05, 0.99),
    "DYNAMIC_ALPHA_LIMIT": (0.05, 0.99),
    # Book / risk gates
    "BITGET_MAX_OPEN_POSITIONS": (1.0, 200.0),
    "GROSS_NOTIONAL_MAX_PCT": (0.0, 1000.0),
    "CORR_BTC_MIN": (-1.0, 1.0),
    "CORR_CLUSTER_MAX_PCT": (0.0, 500.0),
    "CORR_BTC_WINDOW": (10.0, 500.0),
    "CORR_BTC_MIN_OVERLAP": (5.0, 200.0),
    "NAV_DD_REDUCE_PCT": (1.0, 50.0),
    "NAV_DD_BLOCK_PCT": (5.0, 80.0),
    "NAV_DD_HALT_PCT": (5.0, 95.0),
    "NAV_DD_REDUCE_SIZE_MULT": (0.05, 1.0),
    "DOOMSDAY_BLOCK_LEVEL": (1.0, 5.0),
    "TAIL_RISK_ACCRUAL_PCT": (0.0, 10.0),
    "TAIL_RISK_MIN_COVERAGE_PCT": (0.0, 20.0),
    "TAIL_RISK_UNDERFUND_SIZE_MULT": (0.05, 1.0),
    "TAIL_RISK_CRISIS_ATR_PCT": (1.0, 50.0),
    "OMS_ORPHAN_STREAK_PROPOSE_KILL": (1.0, 20.0),
    "BAD_TICK_LOOKBACK_BARS": (2.0, 50.0),
    "BAD_TICK_MAX_GAP_PCT": (0.0, 100.0),
    "BAD_TICK_MAX_VS_MEDIAN_PCT": (0.0, 100.0),
    "BAD_TICK_MAX_BAR_RANGE_PCT": (0.0, 200.0),
}

_INT_KEYS = frozenset(
    {
        "BITGET_MAX_OPEN_POSITIONS",
        "CORR_BTC_WINDOW",
        "CORR_BTC_MIN_OVERLAP",
        "DOOMSDAY_BLOCK_LEVEL",
        "OMS_ORPHAN_STREAK_PROPOSE_KILL",
        "BAD_TICK_LOOKBACK_BARS",
    }
)


def clamp_config_value(key: str, value: Any) -> Any:
    """Clamp a single top-level capital key; pass through if unbounded / non-numeric."""
    bounds = CONFIG_NUMERIC_BOUNDS.get(str(key))
    if bounds is None:
        return value
    lo, hi = bounds
    try:
        num = float(value)
    except (TypeError, ValueError):
        return value
    if num != num:  # NaN
        return value
    clamped = min(hi, max(lo, num))
    if str(key) in _INT_KEYS:
        return int(round(clamped))
    # Preserve int-looking floats as float for Kelly-style keys
    return float(clamped)


def _enforce_nav_stage_order(cfg: dict[str, Any], changes: list[str]) -> None:
    """Ensure reduce ≤ block ≤ halt after individual clamps."""
    keys = ("NAV_DD_REDUCE_PCT", "NAV_DD_BLOCK_PCT", "NAV_DD_HALT_PCT")
    vals: list[Optional[float]] = []
    for k in keys:
        if k not in cfg:
            vals.append(None)
            continue
        try:
            vals.append(float(cfg[k]))
        except (TypeError, ValueError):
            vals.append(None)
    reduce_v, block_v, halt_v = vals
    if reduce_v is not None and block_v is not None and reduce_v > block_v:
        cfg["NAV_DD_BLOCK_PCT"] = float(reduce_v)
        changes.append(f"NAV_DD_BLOCK_PCT:order→{reduce_v}")
        block_v = reduce_v
    if block_v is not None and halt_v is not None and block_v > halt_v:
        cfg["NAV_DD_HALT_PCT"] = float(block_v)
        changes.append(f"NAV_DD_HALT_PCT:order→{block_v}")


def apply_config_hard_bounds(
    config_data: dict[str, Any],
    *,
    record_changes: bool = True,
) -> tuple[dict[str, Any], list[str]]:
    """
    Return (clamped_copy, change_notes).
    Does not mutate the caller's dict.
    """
    out = dict(config_data or {})
    changes: list[str] = []
    for key, bounds in CONFIG_NUMERIC_BOUNDS.items():
        if key not in out:
            continue
        before = out[key]
        after = clamp_config_value(key, before)
        if after != before:
            out[key] = after
            if record_changes:
                changes.append(f"{key}:{before!r}→{after!r}")
        else:
            out[key] = after
    _enforce_nav_stage_order(out, changes)
    return out, changes
