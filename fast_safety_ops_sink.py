"""Fast Safety audit event → ops telemetry envelope adapter (no I/O)."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

_PAYLOAD_ALLOWLIST: tuple[str, ...] = (
    "market",
    "strategy_id",
    "base_kelly",
    "alpha_overlay",
    "selected_family",
    "selected_risk",
    "risk_multiplier",
    "uncapped_kelly",
    "effective_cap",
    "cap_source",
    "final_kelly",
    "blocked",
    "reason",
    "policy_version",
    "live_risk_version",
)

_COMPONENT = "fast_safety"


class OpsEventWriter(Protocol):
    def __call__(
        self,
        *,
        component: str,
        severity: str,
        event: str,
        payload: Mapping[str, object],
    ) -> bool:
        ...


@dataclass(frozen=True)
class FastSafetyOpsEnvelope:
    component: str
    severity: str
    event: str
    payload: Mapping[str, object]


def _is_safe_scalar(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, str):
        return True
    return False


def _required_non_empty_str(mapping: Mapping[str, object], key: str) -> str | None:
    try:
        raw = mapping[key]
    except Exception:
        return None
    if not isinstance(raw, str):
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    return stripped


def _build_safe_payload(audit_event: Mapping[str, object]) -> Mapping[str, object] | None:
    payload: dict[str, object] = {}
    for key in _PAYLOAD_ALLOWLIST:
        try:
            if key not in audit_event:
                continue
            value = audit_event[key]
        except Exception:
            return None
        if not _is_safe_scalar(value):
            return None
        payload[key] = value
    return MappingProxyType(payload)


def build_fast_safety_ops_envelope(
    audit_event: object,
) -> FastSafetyOpsEnvelope | None:
    if not isinstance(audit_event, Mapping):
        return None
    try:
        event_type = _required_non_empty_str(audit_event, "event_type")
        severity = _required_non_empty_str(audit_event, "severity")
        if event_type is None or severity is None:
            return None
        payload = _build_safe_payload(audit_event)
        if payload is None:
            return None
        return FastSafetyOpsEnvelope(
            component=_COMPONENT,
            severity=severity,
            event=event_type,
            payload=payload,
        )
    except Exception:
        return None


def create_fast_safety_ops_sink(
    writer: object,
) -> Callable[[Mapping[str, object]], bool] | None:
    if not callable(writer):
        return None

    def sink(audit_event: Mapping[str, object]) -> bool:
        envelope = build_fast_safety_ops_envelope(audit_event)
        if envelope is None:
            return False
        try:
            result = writer(
                component=envelope.component,
                severity=envelope.severity,
                event=envelope.event,
                payload=dict(envelope.payload),
            )
        except Exception:
            return False
        return result is True

    return sink


__all__ = [
    "FastSafetyOpsEnvelope",
    "OpsEventWriter",
    "build_fast_safety_ops_envelope",
    "create_fast_safety_ops_sink",
]
