"""
동적 클램프 배수 — confidence · p-value 기반 상한 가변 (슈퍼 돌연변이 1.15~1.20).
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

from ace_evolution_schema import AceEvolutionPlaybook


def _f(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        return float(cfg.get(key, default))
    except (TypeError, ValueError):
        return float(default)


def compute_dynamic_multiplier_bounds(
    playbook: Dict[str, Any],
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[float, float]:
    """
    Returns (mult_min, mult_max) for total_score scaling.
    슈퍼 돌연변이: confidence>=0.9 & p<=0.05 → 상한 1.20
    강신호: confidence>=0.8 & p<=0.08 → 상한 1.15
    기본: confidence 비례 1.08까지
  하한은 설정·playbook 또는 0.85.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    pb: AceEvolutionPlaybook = playbook if isinstance(playbook, dict) else {}

    mult_min = _f(pb, "mult_min", _f(cfg, "ACE_EVOLUTION_MULT_MIN", 0.85))
    base_max = _f(pb, "mult_max", _f(cfg, "ACE_EVOLUTION_MULT_MAX_DEFAULT", 1.08))
    super_max = _f(cfg, "ACE_EVOLUTION_MULT_MAX_SUPER", 1.20)
    strong_max = _f(cfg, "ACE_EVOLUTION_MULT_MAX_STRONG", 1.15)

    conf = _f(pb, "confidence", 0.0)
    conf = min(1.0, max(0.0, conf))
    try:
        p_val = float(pb.get("min_p_value", 1.0))
    except (TypeError, ValueError):
        p_val = 1.0

    p_super = _f(cfg, "ACE_EVOLUTION_P_SUPER", 0.05)
    p_strong = _f(cfg, "ACE_EVOLUTION_P_STRONG", 0.08)
    c_super = _f(cfg, "ACE_EVOLUTION_CONF_SUPER", 0.90)
    c_strong = _f(cfg, "ACE_EVOLUTION_CONF_STRONG", 0.80)

    if conf >= c_super and p_val <= p_super:
        mult_max = super_max
    elif conf >= c_strong and p_val <= p_strong:
        mult_max = strong_max
    else:
        # confidence 0.5→base_max, 0.9→strong_max 사이 선형 (p-value 패널티)
        t = (conf - 0.5) / 0.4 if conf > 0.5 else 0.0
        t = min(1.0, max(0.0, t))
        mult_max = base_max + t * (strong_max - base_max)
        if p_val > 0.10:
            mult_max = min(mult_max, base_max)
        elif p_val > p_strong:
            mult_max = min(mult_max, base_max + (strong_max - base_max) * 0.5)

    mult_min = min(mult_min, mult_max)
    mult_max = max(mult_min, mult_max)
    return mult_min, mult_max


def compute_ace_evolution_multiplier(
    playbook: Dict[str, Any],
    *,
    rule_match_bonus: float = 0.0,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    최종 배수 = 1.0 + clamped(stack_bonus), 전체 [mult_min, mult_max] 재클램프.
    observe_only 이면 항상 1.0.
    """
    meta: Dict[str, Any] = {"observe_only": True, "components": {}}
    if not isinstance(playbook, dict) or not playbook.get("logic_core"):
        return 1.0, meta

    if bool(playbook.get("observe_only", True)):
        meta["observe_only"] = True
        meta["reason"] = "observe_only"
        return 1.0, meta

    mult_min, mult_max = compute_dynamic_multiplier_bounds(playbook, sys_config=sys_config)
    meta["mult_bounds"] = {"min": mult_min, "max": mult_max}

    try:
        stack_cap = float(playbook.get("max_stack_bonus", 0.08))
    except (TypeError, ValueError):
        stack_cap = 0.08
    bonus = min(max(float(rule_match_bonus), 0.0), stack_cap)
    raw = 1.0 + bonus
    clamped = max(mult_min, min(mult_max, raw))
    meta["observe_only"] = False
    meta["rule_match_bonus"] = bonus
    meta["clamped_multiplier"] = clamped
    return clamped, meta
