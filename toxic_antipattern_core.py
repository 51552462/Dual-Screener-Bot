"""
오답노트(ANTI_PATTERNS / TOXIC_ML_ANTIPATTERNS) bbox 매칭 — 스캐너·메타 소비자 공용.
auto_forward_tester 와 meta_governor_consumer 간 순환 import 방지.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import numpy as np

_NUMERIC_BBOX_BASES = frozenset({"dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"})


def toxic_ml_antipatterns_rule_map(ml_obj: Any) -> Dict[str, Any]:
    """TOXIC_ML_ANTIPATTERNS: {_metadata, rules} 래퍼 또는 구형 평면 dict → 규칙 dict만."""
    if not isinstance(ml_obj, dict):
        return {}
    inner = ml_obj.get("rules")
    if isinstance(inner, dict):
        return dict(inner)
    return {k: v for k, v in ml_obj.items() if k != "_metadata"}


def collect_merged_antipattern_rules(config: Dict[str, Any]) -> Dict[str, Any]:
    """system_config 와 동일 병합: ANTI_PATTERNS(dict|list) + TOXIC_ML rules."""
    _ap = config.get("ANTI_PATTERNS") if isinstance(config, dict) else None
    _ml = toxic_ml_antipatterns_rule_map(config.get("TOXIC_ML_ANTIPATTERNS") if isinstance(config, dict) else None)
    merged: Dict[str, Any] = {}
    if isinstance(_ap, dict):
        merged.update(_ap)
    elif isinstance(_ap, list):
        for _i, _bounds in enumerate(_ap):
            if isinstance(_bounds, dict):
                merged[f"PATTERN_{_i}"] = _bounds
    if isinstance(_ml, dict) and _ml:
        merged = {**merged, **_ml}
    return merged


def fact_value_for_toxic_base(
    base: str, cpv: float, tb: float, bbe: float, dyn_rs_live: float
) -> float:
    if base == "dyn_cpv":
        return float(cpv)
    if base == "dyn_tb":
        return float(tb)
    if base == "v_energy":
        return float(bbe)
    if base == "dyn_rs":
        return float(dyn_rs_live)
    raise ValueError(base)


def evaluate_toxic_bbox_match(
    bounds: dict,
    cpv: float,
    tb: float,
    bbe: float,
    dyn_rs_live: float,
    sector_mapped: str,
    now_dt=None,
) -> bool:
    """
    ANTI_PATTERNS / ML 트리 bounding box 일치 여부.
    """
    if not isinstance(bounds, dict):
        return False
    now = now_dt or datetime.now()
    tw = int(now.weekday())
    match_flags: list = []
    for key, raw in bounds.items():
        if key in ("created_at",):
            continue
        if key == "sector_match":
            match_flags.append(str(sector_mapped) == str(raw))
            continue
        if key == "weekday_match":
            try:
                wm = int(raw)
            except (TypeError, ValueError):
                match_flags.append(False)
                continue
            match_flags.append(tw == wm)
            continue
        ks = str(key)
        if ks.endswith("_max"):
            base = ks[:-4]
            if base not in _NUMERIC_BBOX_BASES:
                continue
            try:
                val = fact_value_for_toxic_base(base, cpv, tb, bbe, dyn_rs_live)
            except ValueError:
                continue
            if base == "dyn_rs" and isinstance(val, float) and np.isnan(val):
                continue
            try:
                match_flags.append(float(val) <= float(raw))
            except (TypeError, ValueError):
                continue
            continue
        if ks.endswith("_min"):
            base = ks[:-4]
            if base not in _NUMERIC_BBOX_BASES:
                continue
            try:
                val = fact_value_for_toxic_base(base, cpv, tb, bbe, dyn_rs_live)
            except ValueError:
                continue
            if base == "dyn_rs" and isinstance(val, float) and np.isnan(val):
                continue
            try:
                match_flags.append(float(val) > float(raw))
            except (TypeError, ValueError):
                continue
            continue
    return bool(match_flags) and all(match_flags)


def entry_facts_to_toxic_inputs(facts: Dict[str, Any]) -> tuple[float, float, float, float]:
    """facts dict → (cpv, tb, v_energy, dyn_rs) for bbox match."""
    fd = facts if isinstance(facts, dict) else {}
    try:
        cpv = float(fd.get("dyn_cpv", 0) or 0)
    except (TypeError, ValueError):
        cpv = 0.0
    try:
        tb = float(fd.get("dyn_tb", 0) or 0)
    except (TypeError, ValueError):
        tb = 0.0
    try:
        bbe = float(fd.get("v_energy", 0) or 0)
    except (TypeError, ValueError):
        bbe = 0.0
    _dr = fd.get("dyn_rs", None)
    try:
        dyn_rs_live = float(_dr) if _dr is not None and str(_dr).strip() != "" else float("nan")
    except (TypeError, ValueError):
        dyn_rs_live = float("nan")
    return cpv, tb, bbe, dyn_rs_live


def any_toxic_rule_matches(
    sys_config: Dict[str, Any],
    facts: Dict[str, Any],
    sector_mapped: str,
    now_dt=None,
) -> bool:
    merged = collect_merged_antipattern_rules(sys_config)
    cpv, tb, bbe, drs = entry_facts_to_toxic_inputs(facts)
    for _, bounds in merged.items():
        if not isinstance(bounds, dict):
            continue
        if evaluate_toxic_bbox_match(bounds, cpv, tb, bbe, drs, sector_mapped, now_dt=now_dt):
            return True
    return False
