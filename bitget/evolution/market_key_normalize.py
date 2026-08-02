"""
B-1 — deathmatch / registry market key SSOT (BG → SPOT | FUT).

Single entry: ``normalize_market_key``. Kill-switch: ``DEATHMATCH_KEY_NORMALIZE_ENABLED``.
"""
from __future__ import annotations

import os
from typing import Any, Literal, Optional

MarketKey = Literal["SPOT", "FUT"]

_LEGACY_UNIFIED_BG = frozenset({"BG", "BG_CRYPTO", "CRYPTO", ""})


def deathmatch_key_normalize_enabled() -> bool:
    env = os.environ.get("DEATHMATCH_KEY_NORMALIZE_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("DEATHMATCH_KEY_NORMALIZE_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import DEATHMATCH_KEY_NORMALIZE_ENABLED

    return bool(DEATHMATCH_KEY_NORMALIZE_ENABLED)


def _legacy_deathmatch_key(raw: str) -> MarketKey:
    from bitget.infra.market_keys import normalize_market_type

    return "FUT" if normalize_market_type(raw) == "futures" else "SPOT"


def normalize_market_key(
    raw: str,
    *,
    position_market_hint: Optional[str] = None,
) -> MarketKey:
    """
    BG→SPOT/FUT SSOT. When kill-switch off, preserves legacy ``to_deathmatch_key`` mapping.
    """
    if not deathmatch_key_normalize_enabled():
        return _legacy_deathmatch_key(raw)

    key = str(raw or "").strip().upper()
    if key in _LEGACY_UNIFIED_BG:
        if position_market_hint:
            return normalize_market_key(position_market_hint)
        return "SPOT"

    if key in ("FUT", "FUTURES", "BG_FUT", "BG_FUTURES"):
        return "FUT"
    if key in ("SPOT", "BG_SPOT"):
        return "SPOT"

    return _legacy_deathmatch_key(raw)


def registry_row_needs_bg_resolve(row: dict[str, Any]) -> bool:
    mk = str(row.get("market") or "").strip().upper()
    return mk in _LEGACY_UNIFIED_BG


def normalize_registry_row_market(
    row: dict[str, Any],
    *,
    position_market_hint: Optional[str] = None,
) -> tuple[dict[str, Any], bool]:
    """Return (row_copy, changed). Write-through caller persists when changed."""
    if not deathmatch_key_normalize_enabled():
        return row, False
    out = dict(row)
    before = str(out.get("market") or "").upper()
    if not registry_row_needs_bg_resolve(out) and before in ("SPOT", "FUT"):
        return out, False
    after = normalize_market_key(before or "BG", position_market_hint=position_market_hint)
    out["market"] = after
    return out, after != before


def normalize_registry_rows(
    rows: list[dict[str, Any]],
    *,
    hints: Optional[dict[str, str]] = None,
) -> tuple[list[dict[str, Any]], int]:
    """Normalize registry rows; return (rows, n_changed)."""
    if not deathmatch_key_normalize_enabled():
        return rows, 0
    hint_map = hints or {}
    out: list[dict[str, Any]] = []
    changed = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        gk = str(row.get("group_key") or row.get("display_name") or "").strip()
        norm, did = normalize_registry_row_market(
            row, position_market_hint=hint_map.get(gk)
        )
        out.append(norm)
        if did:
            changed += 1
    return out, changed
