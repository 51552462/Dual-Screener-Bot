"""Fast Safety Runtime Shadow Adapter — audit-only evaluation without trading side effects."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fast_safety_kernel import (
    ExposureCaps,
    KellyDecision,
    LiveRiskSnapshot,
    PolicySnapshot,
    compute_kelly_decision,
    try_emit_audit,
)
from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    load_fast_safety_policy_snapshot,
    policy_key_for_market,
)
from fast_safety_snapshot_builder import (
    build_neutral_live_risk_snapshot,
    build_unbounded_exposure_caps,
)
from fast_safety_strategy_identity import (
    StrategyIdentity,
    resolve_supernova_strategy_identity,
)

GetValueFunc = Callable[[str, Any], Any]


@dataclass(frozen=True)
class FastSafetyShadowContext:
    market: str
    shadow_enabled: bool
    ready: bool
    reason: str
    policy: PolicySnapshot | None
    live_risk: LiveRiskSnapshot | None
    exposure_caps: ExposureCaps | None
    emitter: object | None


@dataclass(frozen=True)
class FastSafetyShadowEvaluation:
    attempted: bool
    evaluated: bool
    audit_emitted: bool
    reason: str
    identity: StrategyIdentity | None
    decision: KellyDecision | None


def _display_market(market: object) -> str:
    if isinstance(market, str):
        return market.strip().upper()
    return ""


def _normalize_market(market: object) -> str | None:
    try:
        if not isinstance(market, str):
            return None
        text = market.strip().upper()
        if text not in {"KR", "US"}:
            return None
        key = policy_key_for_market(text)
        if key is None:
            return None
        if key not in set(FAST_SAFETY_POLICY_KEYS.values()):
            return None
        return text
    except Exception:
        return None


def _inactive_context(
    *,
    market: str,
    shadow_enabled: bool,
    reason: str,
    emitter: object | None,
) -> FastSafetyShadowContext:
    return FastSafetyShadowContext(
        market=market,
        shadow_enabled=shadow_enabled,
        ready=False,
        reason=reason,
        policy=None,
        live_risk=None,
        exposure_caps=None,
        emitter=emitter,
    )


def prepare_fast_safety_shadow_context(
    market: object,
    *,
    shadow_enabled: bool,
    emitter: object | None = None,
    get_value: GetValueFunc | None = None,
) -> FastSafetyShadowContext:
    market_display = _display_market(market)

    if not isinstance(shadow_enabled, bool):
        return _inactive_context(
            market=market_display if market_display in {"KR", "US"} else "",
            shadow_enabled=False,
            reason="context-error",
            emitter=emitter,
        )

    if not shadow_enabled:
        normalized = _normalize_market(market)
        return _inactive_context(
            market=normalized if normalized is not None else market_display,
            shadow_enabled=False,
            reason="shadow-disabled",
            emitter=emitter,
        )

    normalized_market = _normalize_market(market)
    if normalized_market is None:
        return _inactive_context(
            market=market_display,
            shadow_enabled=True,
            reason="invalid-market",
            emitter=emitter,
        )

    try:
        policy = load_fast_safety_policy_snapshot(
            normalized_market,
            get_value=get_value,
        )
        if policy is None:
            return _inactive_context(
                market=normalized_market,
                shadow_enabled=True,
                reason="policy-unavailable",
                emitter=emitter,
            )

        live_risk = build_neutral_live_risk_snapshot(
            normalized_market,
            policy.version,
            policy.generated_at,
        )
        if live_risk is None:
            return _inactive_context(
                market=normalized_market,
                shadow_enabled=True,
                reason="context-error",
                emitter=emitter,
            )

        exposure_caps = build_unbounded_exposure_caps()

        return FastSafetyShadowContext(
            market=normalized_market,
            shadow_enabled=True,
            ready=True,
            reason="context-ready",
            policy=policy,
            live_risk=live_risk,
            exposure_caps=exposure_caps,
            emitter=emitter,
        )
    except Exception:
        return _inactive_context(
            market=normalized_market,
            shadow_enabled=True,
            reason="context-error",
            emitter=emitter,
        )


def _context_not_ready_evaluation() -> FastSafetyShadowEvaluation:
    return FastSafetyShadowEvaluation(
        attempted=False,
        evaluated=False,
        audit_emitted=False,
        reason="context-not-ready",
        identity=None,
        decision=None,
    )


def evaluate_supernova_fast_safety_shadow(
    context: FastSafetyShadowContext,
    *,
    route: object,
    best_pass_name: object = None,
    best_pattern_name: object = None,
    ml_pattern_name: object = None,
) -> FastSafetyShadowEvaluation:
    if not isinstance(context, FastSafetyShadowContext):
        return _context_not_ready_evaluation()

    if not context.ready:
        return _context_not_ready_evaluation()

    if (
        context.policy is None
        or context.live_risk is None
        or context.exposure_caps is None
    ):
        return _context_not_ready_evaluation()

    identity: StrategyIdentity | None = None

    try:
        identity = resolve_supernova_strategy_identity(
            context.market,
            route,
            best_pass_name=best_pass_name,
            best_pattern_name=best_pattern_name,
            ml_pattern_name=ml_pattern_name,
        )
        if identity is None:
            return FastSafetyShadowEvaluation(
                attempted=True,
                evaluated=False,
                audit_emitted=False,
                reason="identity-unavailable",
                identity=None,
                decision=None,
            )

        if identity.market != context.market:
            return FastSafetyShadowEvaluation(
                attempted=True,
                evaluated=False,
                audit_emitted=False,
                reason="evaluation-error",
                identity=identity,
                decision=None,
            )

        decision = compute_kelly_decision(
            context.policy,
            context.live_risk,
            identity.strategy_id,
            exposure_caps=context.exposure_caps,
        )

        audit_emitted = False
        if context.emitter is not None:
            audit_emitted = bool(try_emit_audit(decision, context.emitter))

        reason = (
            "evaluated-and-emitted"
            if audit_emitted
            else "evaluated-not-emitted"
        )

        return FastSafetyShadowEvaluation(
            attempted=True,
            evaluated=True,
            audit_emitted=audit_emitted,
            reason=reason,
            identity=identity,
            decision=decision,
        )
    except Exception:
        return FastSafetyShadowEvaluation(
            attempted=True,
            evaluated=False,
            audit_emitted=False,
            reason="evaluation-error",
            identity=identity,
            decision=None,
        )


__all__ = [
    "FastSafetyShadowContext",
    "FastSafetyShadowEvaluation",
    "evaluate_supernova_fast_safety_shadow",
    "prepare_fast_safety_shadow_context",
]
