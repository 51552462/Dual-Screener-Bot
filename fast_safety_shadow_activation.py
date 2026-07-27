"""Fast Safety Shadow activation reader — config_kv SSOT (Chapter B0D3A4H)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

FAST_SAFETY_SHADOW_ACTIVATION_KEYS = {
    "KR": "FAST_SAFETY_SHADOW_KR",
    "US": "FAST_SAFETY_SHADOW_US",
}

_ALLOWED_MARKETS = frozenset(FAST_SAFETY_SHADOW_ACTIVATION_KEYS.keys())


def _normalize_market(market: object) -> str | None:
    try:
        text = str(market).strip().upper()
    except Exception:
        return None
    if text not in _ALLOWED_MARKETS:
        return None
    return text


def resolve_fast_safety_shadow_enabled(
    market: object,
    *,
    get_value: Callable[[str, Any], Any] | None = None,
) -> bool:
    normalized = _normalize_market(market)
    if normalized is None:
        return False

    key = FAST_SAFETY_SHADOW_ACTIVATION_KEYS[normalized]

    try:
        if get_value is not None:
            reader = get_value
        else:
            from config_manager import get_config_value

            reader = get_config_value

        raw_value = reader(key, False)
    except Exception:
        return False

    return raw_value is True


__all__ = [
    "FAST_SAFETY_SHADOW_ACTIVATION_KEYS",
    "resolve_fast_safety_shadow_enabled",
]
