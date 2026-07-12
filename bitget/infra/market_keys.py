"""
Bitget market-type key SSOT.

DB·리포트·데스매치·PIL·strategy_registry 등 모듈마다 spot/futures 표기가 달라
혼선이 생기므로 한 곳에서 정규화한다.
"""
from __future__ import annotations

_FUT_ALIASES = frozenset(
    {
        "futures",
        "fut",
        "future",
        "futures_usdt",
        "bg_futures",
        "fut_usdt",
        "bg_fut",
    }
)
_SPOT_ALIASES = frozenset({"spot", "bg_spot", "spot_usdt"})

# strategy_registry.market labels Bitget may persist (uppercase compare)
_BITGET_REGISTRY_MARKETS = frozenset(
    {
        "SPOT",
        "FUT",
        "FUTURES",
        "BG",  # legacy unified crypto book
        "BG_SPOT",
        "BG_FUT",
        "BG_FUTURES",
    }
)


def normalize_market_type(market_type: str) -> str:
    """DB·ledger·pipeline 공통 키: ``spot`` | ``futures``."""
    raw = str(market_type or "spot").strip().lower()
    if raw in _FUT_ALIASES or raw == "fut":
        return "futures"
    if raw in _SPOT_ALIASES:
        return "spot"
    if raw.startswith("fut") or raw.startswith("bg_fut"):
        return "futures"
    if raw.startswith("bg_") and "spot" in raw:
        return "spot"
    return "spot"


def is_bitget_registry_market(market: str) -> bool:
    """
    True if strategy_registry.market belongs to Bitget crypto book.
    Rejects stock KR/US labels so exploration MAB does not soft-miss Bitget rows.
    """
    m = str(market or "").strip().upper()
    if not m:
        return False
    if m in _BITGET_REGISTRY_MARKETS:
        return True
    raw = str(market or "").strip().lower()
    if raw in _FUT_ALIASES or raw in _SPOT_ALIASES:
        return True
    if raw.startswith("fut") or raw.startswith("bg_fut") or raw.startswith("bg_spot"):
        return True
    return False


def to_deathmatch_key(market_type: str) -> str:
    """데스매치·ACE·리포트 섹션 라벨: ``SPOT`` | ``FUT``."""
    return "FUT" if normalize_market_type(market_type) == "futures" else "SPOT"


def to_db_key(market_type: str) -> str:
    return normalize_market_type(market_type)


def to_report_label(market_type: str) -> str:
    return to_deathmatch_key(market_type)


def to_pil_key(market_type: str) -> str:
    """PIL·watchdog 등 레거시 BG_* 라벨."""
    return "BG_FUTURES" if normalize_market_type(market_type) == "futures" else "BG_SPOT"
