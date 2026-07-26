"""DANTE intraday Fast Safety Kernel.

The kernel is intentionally small and standard-library only.  It performs no
file, database, JSON, network, LLM, or pandas I/O.  Slow-plane code must build
immutable snapshots before the intraday path calls ``compute_kelly_decision``.

Decision chain:
    Hard Gate -> Base Kelly -> Bounded Alpha Overlay -> family min
    -> global Top-1 risk multiplier -> Exposure Caps -> Final Clamp
    -> optional non-blocking audit enqueue (separate helper)

Python: 3.10+
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, Protocol


_NEUTRAL_MULTIPLIER = 1.0
_ZERO = 0.0


@dataclass(frozen=True)
class RiskSignal:
    """One already-resolved in-memory risk signal.

    ``observed_at`` and ``ttl_seconds`` are metadata for audit and snapshot
    construction.  The slow/live-snapshot producer is responsible for applying
    Last-Known-Good and TTL policy before publishing the snapshot.  The kernel
    never reads a clock, file, database, or network resource.
    """

    name: str
    family: str
    multiplier: float
    blocked: bool = False
    reason: str = ""
    observed_at: float = 0.0
    ttl_seconds: float = 0.0


@dataclass(frozen=True)
class PolicySnapshot:
    """Slow-plane policy values read-only to the intraday path."""

    market: str
    version: str
    generated_at: float
    base_kelly_by_strategy: Mapping[str, float]
    alpha_overlay_by_strategy: Mapping[str, float]
    max_alpha_overlay: float
    absolute_kelly_cap: float

    def __post_init__(self) -> None:
        # Copy once in the slow plane so callers cannot mutate mappings later.
        object.__setattr__(
            self,
            "base_kelly_by_strategy",
            MappingProxyType(dict(self.base_kelly_by_strategy)),
        )
        object.__setattr__(
            self,
            "alpha_overlay_by_strategy",
            MappingProxyType(dict(self.alpha_overlay_by_strategy)),
        )


@dataclass(frozen=True)
class LiveRiskSnapshot:
    """Event-driven, in-memory risk state for exactly one market."""

    market: str
    version: str
    generated_at: float
    hard_gates: tuple[RiskSignal, ...]
    risk_signals: tuple[RiskSignal, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "hard_gates", tuple(self.hard_gates))
        object.__setattr__(self, "risk_signals", tuple(self.risk_signals))


@dataclass(frozen=True)
class ExposureCaps:
    """Optional remaining Kelly-fraction caps supplied by the caller.

    ``None`` means that cap is not present.  A supplied NaN, infinity, or
    negative cap is treated as a fail-closed integrity error.
    """

    position_cap: float | None = None
    sector_remaining_cap: float | None = None
    portfolio_remaining_cap: float | None = None


@dataclass(frozen=True)
class KellyDecision:
    market: str
    strategy_id: str
    base_kelly: float
    alpha_overlay: float
    selected_family: str
    selected_risk: str
    risk_multiplier: float
    uncapped_kelly: float
    final_kelly: float
    blocked: bool
    reason: str
    policy_version: str
    live_risk_version: str
    effective_cap: float
    cap_source: str


class AuditEmitter(Protocol):
    """Bounded queue adapter contract.

    Implementations must return immediately and must not perform synchronous
    SQLite, JSON, network, or other blocking I/O inside ``try_emit``.
    """

    def try_emit(self, event: Mapping[str, object]) -> bool:
        ...


def _finite_number(value: object) -> float | None:
    """Return a finite float, or ``None`` without raising."""

    if isinstance(value, bool):
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(converted):
        return None
    return converted


def _normalise_key(value: object) -> str:
    return str(value).strip().upper()


def _blocked_decision(
    *,
    market: str,
    strategy_id: str,
    base_kelly: float,
    alpha_overlay: float,
    selected_family: str,
    selected_risk: str,
    risk_multiplier: float,
    reason: str,
    policy_version: str,
    live_risk_version: str,
    effective_cap: float = 0.0,
    cap_source: str = "fail_closed",
) -> KellyDecision:
    return KellyDecision(
        market=market,
        strategy_id=strategy_id,
        base_kelly=max(_ZERO, base_kelly),
        alpha_overlay=max(_ZERO, alpha_overlay),
        selected_family=selected_family,
        selected_risk=selected_risk,
        risk_multiplier=max(_ZERO, min(_NEUTRAL_MULTIPLIER, risk_multiplier)),
        uncapped_kelly=_ZERO,
        final_kelly=_ZERO,
        blocked=True,
        reason=reason,
        policy_version=policy_version,
        live_risk_version=live_risk_version,
        effective_cap=max(_ZERO, effective_cap),
        cap_source=cap_source,
    )


def _resolve_exposure_cap(
    absolute_cap: float,
    caps: ExposureCaps,
) -> tuple[float | None, str, str]:
    """Return (effective_cap, source, error_reason)."""

    candidates: list[tuple[str, float]] = [("absolute", absolute_cap)]
    for name, raw_value in (
        ("position", caps.position_cap),
        ("sector_remaining", caps.sector_remaining_cap),
        ("portfolio_remaining", caps.portfolio_remaining_cap),
    ):
        if raw_value is None:
            continue
        value = _finite_number(raw_value)
        if value is None or value < _ZERO:
            return None, name, f"INVALID_EXPOSURE_CAP:{name}"
        candidates.append((name, value))

    source, effective = min(candidates, key=lambda item: (item[1], item[0]))
    return effective, source, ""


def _family_top1(
    signals: tuple[RiskSignal, ...],
) -> tuple[str, str, float, str]:
    """Apply family min, then select the global lowest multiplier.

    Returns ``(family, risk_name, multiplier, error_reason)``.  Invalid risk
    signal fields fail closed with multiplier 0.0.
    """

    family_winners: dict[str, tuple[float, str]] = {}

    for signal in signals:
        family = str(signal.family).strip()
        name = str(signal.name).strip()
        multiplier = _finite_number(signal.multiplier)

        if signal.blocked:
            return family or "invalid", name or "blocked_signal", _ZERO, (
                f"RISK_BLOCKED:{family or 'missing_family'}:{name or 'missing_name'}"
                + (f":{signal.reason}" if signal.reason else "")
            )
        if not family or not name or multiplier is None or multiplier < _ZERO:
            return family or "invalid", name or "invalid_signal", _ZERO, (
                f"INVALID_RISK_SIGNAL:{family or 'missing_family'}:{name or 'missing_name'}"
            )

        # Risk signals are dampeners only; they may never boost above 1.0.
        bounded = min(multiplier, _NEUTRAL_MULTIPLIER)
        current = family_winners.get(family)
        candidate = (bounded, name)
        if current is None or candidate < current:
            family_winners[family] = candidate

    if not family_winners:
        return "none", "none", _NEUTRAL_MULTIPLIER, ""

    family, (multiplier, name) = min(
        family_winners.items(),
        key=lambda item: (item[1][0], item[0], item[1][1]),
    )
    return family, name, multiplier, ""


def compute_kelly_decision(
    policy: PolicySnapshot,
    live_risk: LiveRiskSnapshot,
    strategy_id: str,
    exposure_caps: ExposureCaps | None = None,
) -> KellyDecision:
    """Compute one deterministic Fast Safety Kelly decision.

    This function performs no external I/O and does not mutate either snapshot.
    Missing or malformed authoritative policy values fail closed.  Missing
    alpha overlay is fail-neutral at 1.0.
    """

    caps = exposure_caps or ExposureCaps()
    market = _normalise_key(policy.market)
    live_market = _normalise_key(live_risk.market)
    strategy = str(strategy_id).strip()
    policy_version = str(policy.version)
    live_version = str(live_risk.version)

    if not market or market != live_market:
        return _blocked_decision(
            market=market or live_market,
            strategy_id=strategy,
            base_kelly=_ZERO,
            alpha_overlay=_NEUTRAL_MULTIPLIER,
            selected_family="operational_safety",
            selected_risk="market_snapshot_mismatch",
            risk_multiplier=_ZERO,
            reason=f"MARKET_MISMATCH:policy={market or '<empty>'},live={live_market or '<empty>'}",
            policy_version=policy_version,
            live_risk_version=live_version,
        )

    if not strategy:
        return _blocked_decision(
            market=market,
            strategy_id=strategy,
            base_kelly=_ZERO,
            alpha_overlay=_NEUTRAL_MULTIPLIER,
            selected_family="data_integrity",
            selected_risk="missing_strategy_id",
            risk_multiplier=_ZERO,
            reason="MISSING_STRATEGY_ID",
            policy_version=policy_version,
            live_risk_version=live_version,
        )

    # 1. Hard Gate.  The snapshot producer decides TTL/LKG policy beforehand.
    for gate in live_risk.hard_gates:
        if gate.blocked:
            gate_family = str(gate.family).strip() or "hard_gate"
            gate_name = str(gate.name).strip() or "unnamed_gate"
            return _blocked_decision(
                market=market,
                strategy_id=strategy,
                base_kelly=_ZERO,
                alpha_overlay=_NEUTRAL_MULTIPLIER,
                selected_family=gate_family,
                selected_risk=gate_name,
                risk_multiplier=_ZERO,
                reason=(
                    f"HARD_GATE:{gate_family}:{gate_name}"
                    + (f":{gate.reason}" if gate.reason else "")
                ),
                policy_version=policy_version,
                live_risk_version=live_version,
            )

    # 2. Base Kelly: missing/invalid values are authoritative-policy failures.
    raw_base = policy.base_kelly_by_strategy.get(strategy)
    base_kelly = _finite_number(raw_base)
    if base_kelly is None or base_kelly < _ZERO:
        return _blocked_decision(
            market=market,
            strategy_id=strategy,
            base_kelly=_ZERO,
            alpha_overlay=_NEUTRAL_MULTIPLIER,
            selected_family="data_integrity",
            selected_risk="invalid_base_kelly",
            risk_multiplier=_ZERO,
            reason=f"INVALID_BASE_KELLY:{strategy}",
            policy_version=policy_version,
            live_risk_version=live_version,
        )

    # 3. Bounded Alpha Overlay: missing overlay is neutral; malformed policy cap
    # fails closed because policy integrity is authoritative.
    max_overlay = _finite_number(policy.max_alpha_overlay)
    if max_overlay is None or max_overlay < _ZERO:
        return _blocked_decision(
            market=market,
            strategy_id=strategy,
            base_kelly=base_kelly,
            alpha_overlay=_NEUTRAL_MULTIPLIER,
            selected_family="data_integrity",
            selected_risk="invalid_alpha_overlay_cap",
            risk_multiplier=_ZERO,
            reason="INVALID_MAX_ALPHA_OVERLAY",
            policy_version=policy_version,
            live_risk_version=live_version,
        )

    raw_overlay = policy.alpha_overlay_by_strategy.get(strategy, _NEUTRAL_MULTIPLIER)
    overlay = _finite_number(raw_overlay)
    if overlay is None:
        overlay = _NEUTRAL_MULTIPLIER
    overlay = max(_ZERO, min(overlay, max_overlay))

    # 4-5. Family min, then global Top-1 strongest penalty.
    family, risk_name, risk_multiplier, risk_error = _family_top1(live_risk.risk_signals)
    if risk_error:
        return _blocked_decision(
            market=market,
            strategy_id=strategy,
            base_kelly=base_kelly,
            alpha_overlay=overlay,
            selected_family=family,
            selected_risk=risk_name,
            risk_multiplier=risk_multiplier,
            reason=risk_error,
            policy_version=policy_version,
            live_risk_version=live_version,
        )

    # 6. Exposure caps; absolute cap is always mandatory.
    absolute_cap = _finite_number(policy.absolute_kelly_cap)
    if absolute_cap is None or absolute_cap < _ZERO:
        return _blocked_decision(
            market=market,
            strategy_id=strategy,
            base_kelly=base_kelly,
            alpha_overlay=overlay,
            selected_family="data_integrity",
            selected_risk="invalid_absolute_kelly_cap",
            risk_multiplier=_ZERO,
            reason="INVALID_ABSOLUTE_KELLY_CAP",
            policy_version=policy_version,
            live_risk_version=live_version,
        )

    effective_cap, cap_source, cap_error = _resolve_exposure_cap(absolute_cap, caps)
    if effective_cap is None:
        return _blocked_decision(
            market=market,
            strategy_id=strategy,
            base_kelly=base_kelly,
            alpha_overlay=overlay,
            selected_family="data_integrity",
            selected_risk="invalid_exposure_cap",
            risk_multiplier=_ZERO,
            reason=cap_error,
            policy_version=policy_version,
            live_risk_version=live_version,
            cap_source=cap_source,
        )

    uncapped = base_kelly * overlay * risk_multiplier
    if not math.isfinite(uncapped) or uncapped < _ZERO:
        return _blocked_decision(
            market=market,
            strategy_id=strategy,
            base_kelly=base_kelly,
            alpha_overlay=overlay,
            selected_family="data_integrity",
            selected_risk="invalid_kelly_result",
            risk_multiplier=_ZERO,
            reason="INVALID_UNCAPPED_KELLY",
            policy_version=policy_version,
            live_risk_version=live_version,
            effective_cap=effective_cap,
            cap_source=cap_source,
        )

    # 7. Final clamp.  No Kelly floor is permitted after risk reduction.
    final_kelly = max(_ZERO, min(uncapped, effective_cap))
    blocked = final_kelly <= _ZERO

    reason_parts = [f"TOP1:{family}:{risk_name}:{risk_multiplier:.6g}"]
    if final_kelly < uncapped:
        reason_parts.append(f"CAP:{cap_source}:{effective_cap:.6g}")
    if blocked:
        reason_parts.append("FINAL_ZERO")

    return KellyDecision(
        market=market,
        strategy_id=strategy,
        base_kelly=base_kelly,
        alpha_overlay=overlay,
        selected_family=family,
        selected_risk=risk_name,
        risk_multiplier=risk_multiplier,
        uncapped_kelly=uncapped,
        final_kelly=final_kelly,
        blocked=blocked,
        reason=";".join(reason_parts),
        policy_version=policy_version,
        live_risk_version=live_version,
        effective_cap=effective_cap,
        cap_source=cap_source,
    )


def build_audit_event(decision: KellyDecision) -> Mapping[str, object]:
    """Build a small immutable audit payload without serialization or I/O."""

    severity = "CRITICAL" if decision.blocked else (
        "NORMAL"
        if (
            decision.risk_multiplier < _NEUTRAL_MULTIPLIER
            or decision.final_kelly < decision.uncapped_kelly
            or decision.alpha_overlay < _NEUTRAL_MULTIPLIER
        )
        else "DEBUG"
    )
    return MappingProxyType(
        {
            "event_type": "fast_safety_kelly_decision",
            "severity": severity,
            "market": decision.market,
            "strategy_id": decision.strategy_id,
            "base_kelly": decision.base_kelly,
            "alpha_overlay": decision.alpha_overlay,
            "selected_family": decision.selected_family,
            "selected_risk": decision.selected_risk,
            "risk_multiplier": decision.risk_multiplier,
            "uncapped_kelly": decision.uncapped_kelly,
            "effective_cap": decision.effective_cap,
            "cap_source": decision.cap_source,
            "final_kelly": decision.final_kelly,
            "blocked": decision.blocked,
            "reason": decision.reason,
            "policy_version": decision.policy_version,
            "live_risk_version": decision.live_risk_version,
        }
    )


def try_emit_audit(decision: KellyDecision, emitter: AuditEmitter | None) -> bool:
    """Best-effort audit enqueue; never raises into the trading path.

    The emitter implementation must itself be bounded and non-blocking.  This
    helper deliberately does not retry or wait for persistence.
    """

    if emitter is None:
        return False
    try:
        return bool(emitter.try_emit(build_audit_event(decision)))
    except Exception:
        return False


__all__ = [
    "AuditEmitter",
    "ExposureCaps",
    "KellyDecision",
    "LiveRiskSnapshot",
    "PolicySnapshot",
    "RiskSignal",
    "build_audit_event",
    "compute_kelly_decision",
    "try_emit_audit",
]
