"""
AceEvolution 스캐너 소비자 (P1) — observe_only 시 태그만, 활성 시 clamped 배수.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

from ace_evolution_clamp import compute_ace_evolution_multiplier
from ace_evolution_schema import flow_tag_prefix
from ace_evolution_store import load_playbook
from ace_evolution_ttl import is_playbook_expired


def _match_feature_band(facts: Dict[str, float], rule: Dict[str, Any]) -> bool:
    col = str(rule.get("column") or "")
    if not col:
        return False
    try:
        val = float(facts.get(col))
        thr = float(rule.get("value"))
    except (TypeError, ValueError):
        return False
    op = str(rule.get("op") or "gte")
    if op == "lte":
        return val <= thr
    return val >= thr


def _match_theme(sector: str, rule: Dict[str, Any]) -> bool:
    tokens = rule.get("tokens") or []
    if not isinstance(tokens, list):
        return False
    s = str(sector or "").lower()
    for t in tokens:
        if str(t).lower() in s:
            return True
    return False


def _match_logic(sig_type: str, rule: Dict[str, Any]) -> bool:
    pat = str(rule.get("pattern") or "")
    if not pat:
        return False
    core = re.sub(r"\[.*?\]", "", str(sig_type or "")).strip()
    return pat in core or pat in str(sig_type or "")


def evaluate_playbook_rules(
    playbook: Dict[str, Any],
    *,
    facts: Dict[str, float],
    sector: str,
    sig_type: str,
) -> Tuple[float, Dict[str, Any]]:
    """규칙 매칭 보너스 합산 (stack cap은 playbook.max_stack_bonus)."""
    bonus = 0.0
    matched: list = []
    for rule in playbook.get("rules") or []:
        if not isinstance(rule, dict):
            continue
        rt = str(rule.get("type") or "")
        ok = False
        if rt == "feature_band":
            ok = _match_feature_band(facts, rule)
        elif rt == "theme_token":
            ok = _match_theme(sector, rule)
        elif rt == "logic_match":
            ok = _match_logic(sig_type, rule)
        if ok:
            try:
                b = float(rule.get("bonus", 0.0))
            except (TypeError, ValueError):
                b = 0.0
            bonus += b
            matched.append(rule.get("id") or rt)
    return bonus, {"matched_rules": matched, "stack_bonus": bonus}


def apply_ace_evolution_to_score(
    total_score: float,
    *,
    market: str,
    sig_type: str,
    sector: str,
    entry_facts: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    P1: observe_only → score 유지 + meta만 반환.
    활성 시 score * multiplier.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    pb = load_playbook(market, cfg)
    meta: Dict[str, Any] = {"market": str(market).upper(), "applied": False}
    if is_playbook_expired(pb, cfg) or pb.get("_expired"):
        meta["reason"] = "expired"
        return float(total_score), meta

    facts: Dict[str, float] = {}
    if isinstance(entry_facts, dict):
        for k, v in entry_facts.items():
            try:
                facts[str(k)] = float(v)
            except (TypeError, ValueError):
                pass

    stack_bonus, rule_meta = evaluate_playbook_rules(
        pb, facts=facts, sector=sector, sig_type=sig_type
    )
    meta.update(rule_meta)
    mult, clamp_meta = compute_ace_evolution_multiplier(pb, rule_match_bonus=stack_bonus, sys_config=cfg)
    meta.update(clamp_meta)
    meta["flow_tag"] = flow_tag_prefix(market)

    if bool(clamp_meta.get("observe_only")):
        meta["applied"] = False
        return float(total_score), meta

    meta["applied"] = True
    return float(total_score) * float(mult), meta
