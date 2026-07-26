"""Fast Safety Strategy Identity — pure in-memory contract for Supernova routes.

Resolves deterministic ``strategy_id`` values from explicit route metadata without
parsing dynamic ``final_sig`` or ``sig_type`` strings.
"""

from __future__ import annotations

from dataclasses import dataclass

from strategy_promotion_engine import stable_strategy_id

_INVALID_GROUP_KEYS = frozenset({"UNKNOWN", "NONE", "NULL"})
_VALID_MARKETS = frozenset({"KR", "US"})


def _normalize_market(market: str) -> str:
    return str(market or "").strip().upper()


def _normalize_group_key(group_key: str) -> str:
    return str(group_key or "").strip()


def _is_valid_group_key(group_key: str) -> bool:
    normalized = _normalize_group_key(group_key)
    if not normalized:
        return False
    return normalized.upper() not in _INVALID_GROUP_KEYS


@dataclass(frozen=True)
class StrategyIdentity:
    market: str
    group_key: str
    strategy_id: str


def build_strategy_identity(market: str, group_key: str) -> StrategyIdentity | None:
    """Build a frozen identity from market and Supernova group_key."""
    try:
        normalized_market = _normalize_market(market)
        if normalized_market not in _VALID_MARKETS:
            return None

        normalized_group_key = _normalize_group_key(group_key)
        if not _is_valid_group_key(normalized_group_key):
            return None

        strategy_id = stable_strategy_id(normalized_market, normalized_group_key)
        return StrategyIdentity(
            market=normalized_market,
            group_key=normalized_group_key,
            strategy_id=strategy_id,
        )
    except Exception:
        return None


def select_supernova_group_key(
    route: str,
    *,
    best_pass_name: str | None = None,
    best_pattern_name: str | None = None,
    ml_pattern_name: str | None = None,
) -> str | None:
    """Select the Supernova source group_key for a normalized route."""
    normalized_route = str(route or "").strip().upper()

    if normalized_route == "COSINE":
        if _is_valid_group_key(best_pass_name or ""):
            return _normalize_group_key(best_pass_name or "")
        if _is_valid_group_key(best_pattern_name or ""):
            return _normalize_group_key(best_pattern_name or "")
        return None

    if normalized_route in {"MLBOX", "UNDERDOG_MLBOX"}:
        if _is_valid_group_key(ml_pattern_name or ""):
            return _normalize_group_key(ml_pattern_name or "")
        return None

    if normalized_route == "SCOUT":
        return None

    return None


def resolve_supernova_strategy_identity(
    market: str,
    route: str,
    *,
    best_pass_name: str | None = None,
    best_pattern_name: str | None = None,
    ml_pattern_name: str | None = None,
) -> StrategyIdentity | None:
    """Resolve Supernova strategy identity from explicit route metadata."""
    group_key = select_supernova_group_key(
        route,
        best_pass_name=best_pass_name,
        best_pattern_name=best_pattern_name,
        ml_pattern_name=ml_pattern_name,
    )
    if group_key is None:
        return None
    return build_strategy_identity(market, group_key)
