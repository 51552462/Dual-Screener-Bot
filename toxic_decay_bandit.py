"""
Toxic Decay & Forgiveness Bandit — US 독성 ML 규칙 반감기 + 정찰 사면.

등록 후 시간이 지날수록 차단 강도 ↓. MAB 로 decayed 규칙에 소액 탐험 허용.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Literal, Optional, Tuple

import numpy as np

ToxicGate = Literal["allow", "block", "forgiveness_scout"]


@dataclass(frozen=True)
class ToxicGateDecision:
    action: ToxicGate
    rule_name: str = ""
    decay_strength: float = 1.0
    reason: str = ""


def _parse_recorded_at(bounds: Dict[str, Any]) -> Optional[datetime]:
    for k in ("recorded_at", "created_at", "as_of", "trained_at"):
        raw = bounds.get(k)
        if not raw:
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(raw)[:19], fmt)
            except ValueError:
                continue
    return None


def decay_strength(
    bounds: Dict[str, Any],
    *,
    half_life_days: float = 45.0,
    now: Optional[datetime] = None,
) -> float:
    """
    1.0 = 신규(전력 차단) → 0.0 = 완전 사면.
    """
    hl = float(bounds.get("half_life_days", half_life_days) or half_life_days)
    hl = max(7.0, hl)
    t0 = _parse_recorded_at(bounds) or datetime.now() - timedelta(days=hl * 0.5)
    ref = now or datetime.now()
    age_days = max(0.0, (ref - t0).total_seconds() / 86400.0)
    return float(0.5 ** (age_days / hl))


def enrich_rules_with_decay(
    rules: Dict[str, Any],
    *,
    default_half_life: float = 45.0,
) -> Dict[str, Any]:
    """JSON sync 직후 — recorded_at·half_life 메타 보강."""
    if not isinstance(rules, dict):
        return {}
    out: Dict[str, Any] = {}
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for name, bounds in rules.items():
        if not isinstance(bounds, dict):
            continue
        b = dict(bounds)
        if not b.get("recorded_at"):
            b["recorded_at"] = now_s
        if "half_life_days" not in b:
            b["half_life_days"] = default_half_life
        b["_decay_strength"] = round(decay_strength(b), 4)
        out[str(name)] = b
    return out


def _inside_bounds(bounds: Dict[str, Any], feats: Dict[str, float]) -> bool:
    inside = True
    for fname, val in feats.items():
        if val is None or (isinstance(val, float) and np.isnan(val)):
            continue
        kmin = f"{fname}_min"
        kmax = f"{fname}_max"
        if kmin in bounds and val < float(bounds[kmin]):
            inside = False
            break
        if kmax in bounds and val > float(bounds[kmax]):
            inside = False
            break
    return inside


def forgiveness_scout_roll(
    sys_config: Dict[str, Any],
    *,
    decay_str: float,
) -> bool:
    """
    반감기 경과 규칙 — 탐험 예산 내에서만 사면 스카우트.
    """
    base_p = float(sys_config.get("TOXIC_FORGIVENESS_SCOUT_PCT", 0.015) or 0.015)
    # decay 낮을수록(오래됨) 탐험 확률 ↑, 상한 8%
    p = min(0.08, base_p + (1.0 - decay_str) * 0.04)
    return random.random() < p


def _merged_toxic_rules(config: Dict[str, Any], market: str) -> Dict[str, Any]:
    def _map(ml_obj: Any) -> Dict[str, Any]:
        if not isinstance(ml_obj, dict):
            return {}
        inner = ml_obj.get("rules")
        if isinstance(inner, dict):
            return inner
        return {k: v for k, v in ml_obj.items() if k != "_metadata"}

    rules: Dict[str, Any] = {}
    rules.update(_map(config.get("TOXIC_ML_ANTIPATTERNS")))
    if str(market or "").upper() == "US":
        rules.update(_map(config.get("US_TOXIC_ML_ANTIPATTERNS")))
        try:
            from blackhole_hunter import _load_us_toxic_ml_patterns

            rules.update(_load_us_toxic_ml_patterns() or {})
        except Exception:
            pass
    return rules


def evaluate_toxic_ml_gate(
    config: Dict[str, Any],
    cpv: float,
    tb: float,
    bbe: float,
    dyn_rs_val: Any,
    *,
    market: str = "US",
) -> ToxicGateDecision:
    """
    supernova _blocked_by_toxic_ml_tree 대체·강화.
    """
    rules = _merged_toxic_rules(config, market)
    if not isinstance(rules, dict) or not rules:
        return ToxicGateDecision("allow", reason="no_rules")

    feats: Dict[str, float] = {
        "dyn_cpv": float(cpv),
        "dyn_tb": float(tb),
        "v_energy": float(bbe),
    }
    try:
        feats["dyn_rs"] = float(dyn_rs_val)
    except (TypeError, ValueError):
        feats["dyn_rs"] = float("nan")

    block_floor = float(config.get("TOXIC_DECAY_BLOCK_FLOOR", 0.55) or 0.55)
    forgive_ceil = float(config.get("TOXIC_DECAY_FORGIVE_CEIL", 0.35) or 0.35)

    worst: Optional[ToxicGateDecision] = None
    for rname, bounds in rules.items():
        if not str(rname).startswith("TOXIC_PATTERN_") or not isinstance(bounds, dict):
            continue
        if not _inside_bounds(bounds, feats):
            continue
        ds = decay_strength(bounds)
        if ds >= block_floor:
            return ToxicGateDecision("block", rule_name=str(rname), decay_strength=ds, reason="strong_toxic")
        if ds <= forgive_ceil and forgiveness_scout_roll(config, decay_str=ds):
            return ToxicGateDecision(
                "forgiveness_scout",
                rule_name=str(rname),
                decay_strength=ds,
                reason="mab_forgiveness",
            )
        cand = ToxicGateDecision("block", rule_name=str(rname), decay_strength=ds, reason="mid_decay_block")
        if worst is None or ds > worst.decay_strength:
            worst = cand

    if worst and worst.decay_strength > forgive_ceil:
        return worst
    return ToxicGateDecision("allow", reason="decayed_or_miss")


def sync_decayed_toxic_to_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """factory sync_us_toxic_ml_ssot 훅."""
    payload = cfg.get("US_TOXIC_ML_ANTIPATTERNS")
    if isinstance(payload, dict) and "rules" in payload:
        inner = payload.get("rules")
        if isinstance(inner, dict):
            payload["rules"] = enrich_rules_with_decay(inner)
            cfg["US_TOXIC_ML_ANTIPATTERNS"] = payload
            return {"n": len(inner), "kind": "nested"}
    if isinstance(payload, dict):
        cfg["US_TOXIC_ML_ANTIPATTERNS"] = enrich_rules_with_decay(payload)
        return {"n": len(cfg["US_TOXIC_ML_ANTIPATTERNS"]), "kind": "flat"}
    return {"n": 0, "kind": "empty"}
