"""
AceEvolution Playbook JSON 스키마 v1 — KR/US 독립, 스캔 시 LLM 미사용.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict

SCHEMA_VERSION = 1
MarketCode = Literal["KR", "US"]

RULE_TYPES = frozenset(
    {
        "feature_band",
        "theme_token",
        "logic_match",
    }
)


class AceEvolutionRule(TypedDict, total=False):
    id: str
    type: str
    column: str
    op: str
    value: float
    tokens: List[str]
    match: str
    pattern: str
    bonus: float
    priority_rank: int


class AceEvolutionPlaybook(TypedDict, total=False):
    schema_version: int
    market: str
    logic_core: str
    as_of_kst: str
    window_days: int
    n_ace: int
    n_baseline: int
    confidence: float
    min_p_value: float
    ttl_days: int
    ttl_mode: str
    observe_only: bool
    human_insight_ko: str
    rules: List[AceEvolutionRule]
    max_stack_bonus: float
    mult_min: float
    mult_max: float
    provenance: Dict[str, Any]
    deathmatch_arm_label: str


def default_playbook(market: str) -> Dict[str, Any]:
    m = str(market).upper()
    return {
        "schema_version": SCHEMA_VERSION,
        "market": m,
        "logic_core": "",
        "as_of_kst": "",
        "window_days": 14,
        "n_ace": 0,
        "n_baseline": 0,
        "confidence": 0.0,
        "min_p_value": 1.0,
        "ttl_days": 1 if m == "KR" else 5,
        "ttl_mode": "fast_decay_kr" if m == "KR" else "slow_decay_us",
        "observe_only": True,
        "human_insight_ko": "",
        "rules": [],
        "max_stack_bonus": 0.08,
        "mult_min": 0.85,
        "mult_max": 1.08,
        "provenance": {},
        "deathmatch_arm_label": "M (Ace DNA)",
    }


def config_key_for_market(market: str) -> str:
    m = str(market).upper()
    return f"ACE_EVOLUTION_{m}"


def flow_tag_prefix(market: str) -> str:
    return f"ACE_EVOL_{str(market).upper()}"
