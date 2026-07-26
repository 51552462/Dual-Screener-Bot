"""Explicit in-memory Fast Safety snapshot builder (Chapter 3-B0C).

Builds PolicySnapshot, LiveRiskSnapshot, and ExposureCaps from caller-supplied
values only.  No config loading, file I/O, network, clocks, or implicit defaults
for Kelly or cap values beyond neutral alpha overlay generation.
"""

from __future__ import annotations

import math
from collections.abc import Mapping

from fast_safety_kernel import ExposureCaps, LiveRiskSnapshot, PolicySnapshot

_NEUTRAL_ALPHA = 1.0
_ALLOWED_MARKETS = frozenset({"KR", "US"})


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(converted):
        return None
    return converted


def _validate_market_version_generated_at(
    market: object,
    version: object,
    generated_at: object,
) -> tuple[str, str, float] | None:
    market_text = str(market).strip().upper()
    if market_text not in _ALLOWED_MARKETS:
        return None

    if not isinstance(version, str) or not version.strip():
        return None
    version_text = version

    generated = _finite_number(generated_at)
    if generated is None or generated < 0.0:
        return None

    return market_text, version_text, generated


def _validate_base_kelly_map(
    base_kelly_by_strategy: object,
    absolute_kelly_cap: object,
) -> tuple[dict[str, float], float] | None:
    if not isinstance(base_kelly_by_strategy, Mapping) or not base_kelly_by_strategy:
        return None

    cap = _finite_number(absolute_kelly_cap)
    if cap is None or cap <= 0.0 or cap > 1.0:
        return None

    base_copy: dict[str, float] = {}
    for raw_key, raw_value in base_kelly_by_strategy.items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            return None
        kelly = _finite_number(raw_value)
        if kelly is None or kelly <= 0.0 or kelly > 1.0:
            return None
        if kelly > cap:
            return None
        base_copy[raw_key] = kelly

    if not base_copy:
        return None

    return base_copy, cap


def _resolve_alpha_overlay(
    base_kelly_by_strategy: dict[str, float],
    alpha_overlay_by_strategy: Mapping[str, float] | None,
    max_alpha_overlay: object,
) -> dict[str, float] | None:
    max_overlay = _finite_number(max_alpha_overlay)
    if max_overlay != _NEUTRAL_ALPHA:
        return None

    if alpha_overlay_by_strategy is None:
        return {strategy_id: _NEUTRAL_ALPHA for strategy_id in base_kelly_by_strategy}

    if not isinstance(alpha_overlay_by_strategy, Mapping):
        return None
    if set(alpha_overlay_by_strategy.keys()) != set(base_kelly_by_strategy.keys()):
        return None

    alpha_copy: dict[str, float] = {}
    for strategy_id in base_kelly_by_strategy:
        overlay = _finite_number(alpha_overlay_by_strategy[strategy_id])
        if overlay != _NEUTRAL_ALPHA:
            return None
        alpha_copy[strategy_id] = _NEUTRAL_ALPHA

    return alpha_copy


def build_explicit_policy_snapshot(
    market,
    version,
    generated_at,
    base_kelly_by_strategy,
    absolute_kelly_cap,
    alpha_overlay_by_strategy=None,
    max_alpha_overlay=1.0,
):
    """Build a PolicySnapshot from explicitly supplied policy values.

    Returns a valid PolicySnapshot, or None when validation fails.  Never
    raises to the caller.
    """
    try:
        header = _validate_market_version_generated_at(market, version, generated_at)
        if header is None:
            return None
        market_text, version_text, generated = header

        base_result = _validate_base_kelly_map(
            base_kelly_by_strategy,
            absolute_kelly_cap,
        )
        if base_result is None:
            return None
        base_copy, cap = base_result

        alpha_copy = _resolve_alpha_overlay(
            base_copy,
            alpha_overlay_by_strategy,
            max_alpha_overlay,
        )
        if alpha_copy is None:
            return None

        return PolicySnapshot(
            market=market_text,
            version=version_text,
            generated_at=generated,
            base_kelly_by_strategy=dict(base_copy),
            alpha_overlay_by_strategy=dict(alpha_copy),
            max_alpha_overlay=_NEUTRAL_ALPHA,
            absolute_kelly_cap=cap,
        )
    except Exception:
        return None


def build_neutral_live_risk_snapshot(
    market,
    version,
    generated_at,
):
    """Build a neutral LiveRiskSnapshot with no gates or risk signals.

    Returns a valid LiveRiskSnapshot, or None when validation fails.  Never
    raises to the caller.
    """
    try:
        header = _validate_market_version_generated_at(market, version, generated_at)
        if header is None:
            return None
        market_text, version_text, generated = header

        return LiveRiskSnapshot(
            market=market_text,
            version=version_text,
            generated_at=generated,
            hard_gates=(),
            risk_signals=(),
        )
    except Exception:
        return None


def build_unbounded_exposure_caps() -> ExposureCaps:
    """Return ExposureCaps with every remaining-cap field unset (unbounded)."""

    return ExposureCaps(
        position_cap=None,
        sector_remaining_cap=None,
        portfolio_remaining_cap=None,
    )


__all__ = [
    "build_explicit_policy_snapshot",
    "build_neutral_live_risk_snapshot",
    "build_unbounded_exposure_caps",
]
