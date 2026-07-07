"""
Bitget market-type key SSOT.

DB·리포트·데스매치·PIL 등 모듈마다 spot/futures 표기가 달라 혼선이 생기므로
한 곳에서 정규화한다.
"""
from __future__ import annotations

_FUT_ALIASES = frozenset(
    {"futures", "fut", "future", "futures_usdt", "bg_futures", "fut_usdt"}
)
_SPOT_ALIASES = frozenset({"spot", "bg_spot", "spot_usdt"})


def normalize_market_type(market_type: str) -> str:
    """DB·ledger·pipeline 공통 키: ``spot`` | ``futures``."""
    raw = str(market_type or "spot").strip().lower()
    if raw in _FUT_ALIASES or raw == "fut":
        return "futures"
    if raw in _SPOT_ALIASES:
        return "spot"
    if raw.startswith("fut"):
        return "futures"
    return "spot"


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
