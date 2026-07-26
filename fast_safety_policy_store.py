"""Fast Safety Policy Store — slow-plane config_kv document I/O (Chapter 3-B0D1).

Reads and writes market-scoped policy documents via injectable config_kv accessors.
Never performs file I/O, SQLite direct access, or intraday snapshot caching.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, Callable

from fast_safety_kernel import PolicySnapshot
from fast_safety_snapshot_builder import build_explicit_policy_snapshot

FAST_SAFETY_POLICY_VERSION = "fast-safety-policy-v1"

FAST_SAFETY_POLICY_KEYS = {
    "KR": "FAST_SAFETY_POLICY_KR",
    "US": "FAST_SAFETY_POLICY_US",
}

_ALLOWED_MARKETS = frozenset({"KR", "US"})

_DISABLED_DOCUMENT_KEYS = frozenset(
    {"enabled", "market", "version", "generated_at"}
)
_ENABLED_DOCUMENT_KEYS = _DISABLED_DOCUMENT_KEYS | frozenset(
    {
        "base_kelly_by_strategy",
        "absolute_kelly_cap",
        "alpha_overlay_by_strategy",
        "max_alpha_overlay",
        "metadata",
    }
)

GetValueFunc = Callable[[str, Any], Any]
SetValueFunc = Callable[[str, Any], None]


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


def _normalize_market(market: object) -> str | None:
    try:
        text = str(market).strip().upper()
    except Exception:
        return None
    if text not in _ALLOWED_MARKETS:
        return None
    return text


def _validate_generated_at(value: object) -> float | None:
    generated = _finite_number(value)
    if generated is None or generated < 0.0:
        return None
    return generated


def _validate_version(value: object) -> bool:
    return isinstance(value, str) and value == FAST_SAFETY_POLICY_VERSION


def _resolve_get_value(get_value: GetValueFunc | None) -> GetValueFunc:
    if get_value is not None:
        return get_value
    from config_manager import get_config_value

    return get_config_value


def _resolve_set_value(set_value: SetValueFunc | None) -> SetValueFunc:
    if set_value is not None:
        return set_value
    from config_manager import set_config_value

    return set_config_value


def _document_keys_allowed(document: Mapping[str, Any], enabled: bool) -> bool:
    allowed = _ENABLED_DOCUMENT_KEYS if enabled else _DISABLED_DOCUMENT_KEYS
    for key in document.keys():
        if key not in allowed:
            return False
    return True


def _copy_mapping(value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    return dict(value)


def policy_key_for_market(market: object) -> str | None:
    """Return the config_kv key for *market*, or ``None`` when unsupported."""
    try:
        normalized = _normalize_market(market)
        if normalized is None:
            return None
        return FAST_SAFETY_POLICY_KEYS[normalized]
    except Exception:
        return None


def build_fast_safety_policy_payload(
    document: object,
) -> dict[str, Any] | None:
    """Validate and return one normalized storage payload without I/O."""
    try:
        if not isinstance(document, Mapping):
            return None

        enabled = document.get("enabled")
        if not isinstance(enabled, bool):
            return None

        if not _document_keys_allowed(document, enabled):
            return None

        if not _validate_version(document.get("version")):
            return None

        normalized_market = _normalize_market(document.get("market"))
        if normalized_market is None:
            return None

        generated_at = _validate_generated_at(document.get("generated_at"))
        if generated_at is None:
            return None

        if not enabled:
            return {
                "enabled": False,
                "market": normalized_market,
                "version": FAST_SAFETY_POLICY_VERSION,
                "generated_at": generated_at,
            }

        if "base_kelly_by_strategy" not in document:
            return None
        if "absolute_kelly_cap" not in document:
            return None

        base_copy = _copy_mapping(document.get("base_kelly_by_strategy"))
        if base_copy is None:
            return None

        alpha_raw = document.get("alpha_overlay_by_strategy")
        alpha_copy: dict[str, Any] | None
        if alpha_raw is None:
            alpha_copy = None
        else:
            alpha_copy = _copy_mapping(alpha_raw)
            if alpha_copy is None:
                return None

        metadata_raw = document.get("metadata")
        metadata_copy: dict[str, Any] | None = None
        if metadata_raw is not None:
            metadata_copy = _copy_mapping(metadata_raw)
            if metadata_copy is None:
                return None

        max_alpha = document.get("max_alpha_overlay", 1.0)

        snapshot = build_explicit_policy_snapshot(
            normalized_market,
            FAST_SAFETY_POLICY_VERSION,
            generated_at,
            base_copy,
            document.get("absolute_kelly_cap"),
            alpha_overlay_by_strategy=alpha_copy,
            max_alpha_overlay=max_alpha,
        )
        if snapshot is None:
            return None

        payload: dict[str, Any] = {
            "enabled": True,
            "market": normalized_market,
            "version": FAST_SAFETY_POLICY_VERSION,
            "generated_at": generated_at,
            "base_kelly_by_strategy": dict(snapshot.base_kelly_by_strategy),
            "absolute_kelly_cap": snapshot.absolute_kelly_cap,
            "alpha_overlay_by_strategy": dict(snapshot.alpha_overlay_by_strategy),
            "max_alpha_overlay": snapshot.max_alpha_overlay,
        }
        if metadata_copy is not None:
            payload["metadata"] = dict(metadata_copy)

        return payload
    except Exception:
        return None


def load_fast_safety_policy_snapshot(
    market: object,
    *,
    get_value: GetValueFunc | None = None,
) -> PolicySnapshot | None:
    """Load an enabled, valid policy document and return an immutable snapshot."""
    try:
        normalized_market = _normalize_market(market)
        if normalized_market is None:
            return None

        key = policy_key_for_market(normalized_market)
        if key is None:
            return None

        reader = _resolve_get_value(get_value)
        raw = reader(key, None)
        if not isinstance(raw, Mapping):
            return None

        enabled = raw.get("enabled")
        if enabled is not True:
            return None

        if not _document_keys_allowed(raw, True):
            return None

        metadata = raw.get("metadata")
        if metadata is not None and not isinstance(metadata, Mapping):
            return None

        if not _validate_version(raw.get("version")):
            return None

        doc_market = _normalize_market(raw.get("market"))
        if doc_market is None or doc_market != normalized_market:
            return None

        generated_at = _validate_generated_at(raw.get("generated_at"))
        if generated_at is None:
            return None

        if "base_kelly_by_strategy" not in raw or "absolute_kelly_cap" not in raw:
            return None

        alpha_overlay = raw.get("alpha_overlay_by_strategy")
        if alpha_overlay is not None and not isinstance(alpha_overlay, Mapping):
            return None

        max_alpha = raw.get("max_alpha_overlay", 1.0)

        return build_explicit_policy_snapshot(
            doc_market,
            FAST_SAFETY_POLICY_VERSION,
            generated_at,
            raw.get("base_kelly_by_strategy"),
            raw.get("absolute_kelly_cap"),
            alpha_overlay_by_strategy=alpha_overlay,
            max_alpha_overlay=max_alpha,
        )
    except Exception:
        return None


def write_fast_safety_policy_document(
    document: object,
    *,
    set_value: SetValueFunc | None = None,
) -> bool:
    """Validate, normalize, and persist a policy document to config_kv."""
    try:
        payload = build_fast_safety_policy_payload(document)
        if payload is None:
            return False

        key = policy_key_for_market(payload["market"])
        if key is None:
            return False

        writer = _resolve_set_value(set_value)
        writer(key, payload)
        return True
    except Exception:
        return False


def disable_fast_safety_policy(
    market: object,
    generated_at: object,
    *,
    set_value: SetValueFunc | None = None,
) -> bool:
    """Persist a minimal disabled policy document for *market*."""
    try:
        normalized_market = _normalize_market(market)
        if normalized_market is None:
            return False

        validated_generated_at = _validate_generated_at(generated_at)
        if validated_generated_at is None:
            return False

        return write_fast_safety_policy_document(
            {
                "enabled": False,
                "market": normalized_market,
                "version": FAST_SAFETY_POLICY_VERSION,
                "generated_at": validated_generated_at,
            },
            set_value=set_value,
        )
    except Exception:
        return False


__all__ = [
    "FAST_SAFETY_POLICY_KEYS",
    "FAST_SAFETY_POLICY_VERSION",
    "build_fast_safety_policy_payload",
    "disable_fast_safety_policy",
    "load_fast_safety_policy_snapshot",
    "policy_key_for_market",
    "write_fast_safety_policy_document",
]
