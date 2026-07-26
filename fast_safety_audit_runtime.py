"""Fast Safety Audit Runtime — scan-local emitter ownership and bounded drain."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from fast_safety_audit_queue import BoundedAuditEmitter

AuditSink = Callable[[Mapping[str, object]], bool]


@dataclass(frozen=True)
class FastSafetyAuditRuntime:
    shadow_enabled: bool
    ready: bool
    reason: str
    emitter: BoundedAuditEmitter | None


@dataclass(frozen=True)
class FastSafetyAuditDrainResult:
    attempted: bool
    drained_count: int
    delivered_count: int
    failed_count: int
    remaining_count: int
    reason: str


def create_fast_safety_audit_runtime(
    *,
    shadow_enabled: bool,
    maxsize: int = 1024,
) -> FastSafetyAuditRuntime:
    if not isinstance(shadow_enabled, bool):
        return FastSafetyAuditRuntime(
            shadow_enabled=False,
            ready=False,
            reason="invalid-shadow-enabled",
            emitter=None,
        )

    if not shadow_enabled:
        return FastSafetyAuditRuntime(
            shadow_enabled=False,
            ready=False,
            reason="shadow-disabled",
            emitter=None,
        )

    try:
        emitter = BoundedAuditEmitter(maxsize=maxsize)
    except Exception:
        return FastSafetyAuditRuntime(
            shadow_enabled=True,
            ready=False,
            reason="runtime-error",
            emitter=None,
        )

    return FastSafetyAuditRuntime(
        shadow_enabled=True,
        ready=True,
        reason="runtime-ready",
        emitter=emitter,
    )


def _drain_not_attempted(*, reason: str) -> FastSafetyAuditDrainResult:
    return FastSafetyAuditDrainResult(
        attempted=False,
        drained_count=0,
        delivered_count=0,
        failed_count=0,
        remaining_count=0,
        reason=reason,
    )


def _is_valid_drain_limit(limit: object) -> bool:
    if limit is None:
        return True
    if isinstance(limit, bool) or not isinstance(limit, int):
        return False
    return limit >= 0


def drain_fast_safety_audit_runtime(
    runtime: FastSafetyAuditRuntime,
    sink: AuditSink | None,
    *,
    limit: int | None = None,
) -> FastSafetyAuditDrainResult:
    if not isinstance(runtime, FastSafetyAuditRuntime):
        return _drain_not_attempted(reason="invalid-runtime")

    if not runtime.ready or runtime.emitter is None:
        return _drain_not_attempted(reason="runtime-not-ready")

    if not callable(sink):
        return _drain_not_attempted(reason="invalid-sink")

    if not _is_valid_drain_limit(limit):
        return _drain_not_attempted(reason="invalid-limit")

    try:
        events = runtime.emitter.drain(limit=limit)
    except Exception:
        return FastSafetyAuditDrainResult(
            attempted=True,
            drained_count=0,
            delivered_count=0,
            failed_count=0,
            remaining_count=0,
            reason="drain-error",
        )

    drained_count = len(events)
    if drained_count == 0:
        return FastSafetyAuditDrainResult(
            attempted=True,
            drained_count=0,
            delivered_count=0,
            failed_count=0,
            remaining_count=0,
            reason="drain-empty",
        )

    delivered_count = 0
    failed_count = 0

    for event in events:
        try:
            if sink(event) is True:
                delivered_count += 1
            else:
                failed_count += 1
        except Exception:
            failed_count += 1

    try:
        remaining_count = runtime.emitter.qsize()
    except Exception:
        return FastSafetyAuditDrainResult(
            attempted=True,
            drained_count=drained_count,
            delivered_count=delivered_count,
            failed_count=failed_count,
            remaining_count=0,
            reason="drain-error",
        )

    if failed_count == 0:
        reason = "drain-complete"
    else:
        reason = "drain-partial"

    return FastSafetyAuditDrainResult(
        attempted=True,
        drained_count=drained_count,
        delivered_count=delivered_count,
        failed_count=failed_count,
        remaining_count=remaining_count,
        reason=reason,
    )


__all__ = [
    "AuditSink",
    "FastSafetyAuditDrainResult",
    "FastSafetyAuditRuntime",
    "create_fast_safety_audit_runtime",
    "drain_fast_safety_audit_runtime",
]
