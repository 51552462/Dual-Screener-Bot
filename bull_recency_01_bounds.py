"""BULL-RECENCY-01 — targeted CLUSTER_1 폭발형 bounds tightening (S1 only).

Read-only w.r.t. config_kv / Phase A. Patches brain dict in-memory for RP-1 re-sim.
"""
from __future__ import annotations

import copy
import os
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Axis pairs: time_machine (dyn_*) and data_miner (cpv/tb/bbe_*) aliases.
_BOUNDS_AXES: Tuple[Tuple[Tuple[str, ...], Tuple[str, ...], str], ...] = (
    (("dyn_cpv_min", "cpv_min"), ("dyn_cpv_max", "cpv_max"), "cpv"),
    (("dyn_tb_min", "tb_min"), ("dyn_tb_max", "tb_max"), "tb"),
    (("v_energy_min", "bbe_min"), ("v_energy_max", "bbe_max"), "bbe"),
)

_CLUSTER_1_EXPLOSIVE_RE = re.compile(r"CLUSTER_1.*폭발", re.IGNORECASE)
_BR01_BINDING_TEMPLATE = "CLUSTER_1_강응축_폭발형_260628"

# 8/13 01:26 full RP-1 — validated bounds_before (repo/KV drift guard).
BR01_SSOT_BOUNDS_BEFORE: Dict[str, Dict[str, Any]] = {
    "CLUSTER_1_강응축_폭발형_260628": {
        "cpv_min": -0.51,
        "cpv_max": 1.0,
        "tb_min": -40.0,
        "tb_max": 102.8,
        "bbe_min": 98.1,
        "bbe_max": 101.9,
        "tml_min": -0.1,
        "tml_max": 0.1,
        "rs_min": -20.6,
        "rs_max": 26.4,
        "dyn_cpv_min": -0.51,
        "dyn_cpv_max": 1.0,
        "dyn_tb_min": -40.0,
        "dyn_tb_max": 102.8,
        "v_energy_min": 98.1,
        "v_energy_max": 101.9,
    },
    "CLUSTER_1_강응축_폭발형_260802": {
        "cpv_min": -0.84,
        "cpv_max": 0.33,
        "tb_min": -167.4,
        "tb_max": 658.4,
        "bbe_min": -193.1,
        "bbe_max": 466.6,
        "tml_min": -2.5,
        "tml_max": 1.8,
        "rs_min": -17.3,
        "rs_max": 26.4,
        "dyn_cpv_min": -0.84,
        "dyn_cpv_max": 0.33,
        "dyn_tb_min": -167.4,
        "dyn_tb_max": 658.4,
        "v_energy_min": -193.1,
        "v_energy_max": 466.6,
    },
}

# data_miner (cpv/tb/bbe) vs time_machine RP-1 (dyn_cpv/dyn_tb/v_energy) — both must stay in sync.
_TM_BOUNDS_MIRROR: Tuple[Tuple[Tuple[str, str], Tuple[str, str]], ...] = (
    (("cpv_min", "cpv_max"), ("dyn_cpv_min", "dyn_cpv_max")),
    (("tb_min", "tb_max"), ("dyn_tb_min", "dyn_tb_max")),
    (("bbe_min", "bbe_max"), ("v_energy_min", "v_energy_max")),
)

_DEFAULT_SHRINK = 0.45  # iter-2 frozen — BULL_03 NEAR_MISS; do not lower without Handoff
_DEFAULT_TB_FLOOR_LIFT = 0.15
_DEFAULT_BBE_FLOOR_LIFT = 0.15
_KR_BINDING_TEMPLATE_MARK = "260628"
_DEFAULT_KR_DYN_RS_MIN = 5.0


def resolve_bull_recency_01_patch() -> bool:
    return os.environ.get("BULL_RECENCY_01_PATCH", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _env_float(name: str, default: float, *, lo: float, hi: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(lo, min(hi, float(raw)))
    except ValueError:
        return default


def is_cluster_1_explosive_template(name: str) -> bool:
    return bool(_CLUSTER_1_EXPLOSIVE_RE.search(str(name or "")))


def scope_live_templates_for_br01(
    ml_templates: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Legacy scope helper (tests only). 8/13 SSOT used all LIVE templates with
    binding template first — see reorder_live_templates_for_br01().
    """
    if not isinstance(ml_templates, dict):
        return {}, {"scoped": False, "reason": "no_dict", "live_in": 0, "live_out": 0}
    filtered: Dict[str, Any] = {
        str(name): dict(bounds)
        for name, bounds in ml_templates.items()
        if isinstance(bounds, dict) and is_cluster_1_explosive_template(name)
    }
    audit = {
        "scoped": True,
        "live_in": len(ml_templates),
        "live_out": len(filtered),
        "excluded": sorted(
            str(name) for name in ml_templates if name not in filtered
        ),
    }
    return filtered, audit


def reorder_live_templates_for_br01(
    ml_templates: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    8/13 SSOT: keep all LIVE templates; put binding 260628 first for first-match.
    """
    if not isinstance(ml_templates, dict):
        return {}, {"reordered": False, "live_in": 0, "first": None}

    def _rank(name: str) -> Tuple[int, str]:
        if name == _BR01_BINDING_TEMPLATE:
            return (0, name)
        if is_cluster_1_explosive_template(name):
            return (1, name)
        return (2, name)

    ordered_names = sorted((str(n) for n in ml_templates), key=_rank)
    out: Dict[str, Any] = {}
    for name in ordered_names:
        bounds = ml_templates.get(name)
        if isinstance(bounds, dict):
            out[name] = dict(bounds)
    audit = {
        "reordered": True,
        "live_in": len(ml_templates),
        "live_out": len(out),
        "first": ordered_names[0] if ordered_names else None,
    }
    return out, audit


def _bounds_need_ssot_overlay(bounds: Mapping[str, Any]) -> bool:
    if not isinstance(bounds, dict) or not bounds:
        return True
    dyn_lo = bounds.get("dyn_cpv_min", bounds.get("cpv_min"))
    ve_lo = bounds.get("v_energy_min", bounds.get("bbe_min"))
    if dyn_lo is None or ve_lo is None:
        return True
    try:
        if float(ve_lo) < -50.0:
            return True
    except (TypeError, ValueError):
        return True
    return False


def apply_br01_ssot_bounds_to_brain(
    brain: Mapping[str, Any],
    *,
    force: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Overlay 8/13 bounds_before on CLUSTER_1 폭발형 before shrink patch."""
    out = copy.deepcopy(dict(brain))
    ml = out.get("LIVE_CLUSTER_TEMPLATES")
    if not isinstance(ml, dict):
        return out, {"applied": False, "reason": "no_live_templates"}

    touched: List[str] = []
    for name, ssot_bounds in BR01_SSOT_BOUNDS_BEFORE.items():
        if name not in ml:
            continue
        current = ml.get(name)
        if not isinstance(current, dict):
            continue
        if not force and not _bounds_need_ssot_overlay(current):
            continue
        merged = mirror_bounds_for_time_machine(dict(ssot_bounds))
        ml[name] = merged
        touched.append(name)

    return out, {
        "applied": bool(touched),
        "templates": touched,
        "source": "br01_ssot_20260813",
    }


def resolve_bull_recency_01_kr_lever() -> bool:
    if not resolve_bull_recency_01_patch():
        return False
    return os.environ.get("BULL_RECENCY_01_KR_LEVER", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def is_kr_ticker(code: str) -> bool:
    base = str(code or "").strip().upper().split(".")[0]
    return bool(base) and base.isdigit()


def apply_kr_rs_lever_to_brain(
    brain: Mapping[str, Any],
    *,
    template_mark: str = _KR_BINDING_TEMPLATE_MARK,
    kr_dyn_rs_min: Optional[float] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
  BULL_05 KR-only gate on binding CLUSTER_1 template (260628).
  US tickers unaffected; CLUSTER_1 shrink bounds untouched.
  """
    rs_min_v = kr_dyn_rs_min if kr_dyn_rs_min is not None else _env_float(
        "BULL_RECENCY_01_KR_RS_MIN", _DEFAULT_KR_DYN_RS_MIN, lo=0.0, hi=50.0
    )
    out = copy.deepcopy(dict(brain))
    ml = out.get("LIVE_CLUSTER_TEMPLATES")
    if not isinstance(ml, dict):
        return out, {"enabled": False, "reason": "no_live_templates"}

    touched: List[Dict[str, Any]] = []
    for name, bounds in ml.items():
        if template_mark not in str(name):
            continue
        if not isinstance(bounds, dict):
            continue
        b = dict(bounds)
        b["_bull_recency_01_kr_dyn_rs_min"] = round(rs_min_v, 4)
        ml[name] = b
        touched.append({"template": name, "kr_dyn_rs_min": round(rs_min_v, 4)})

    audit = {
        "enabled": bool(touched),
        "lever": "kr_dyn_rs_min",
        "template_mark": template_mark,
        "kr_dyn_rs_min": round(rs_min_v, 4),
        "templates": touched,
        "note": "KR digit-code tickers only; enforced in time_machine match",
    }
    return out, audit


def mirror_bounds_for_time_machine(bounds: Mapping[str, Any]) -> Dict[str, Any]:
    """RP-1 `_row_matches_template_bounds` reads dyn_* / v_energy_* only — mirror legacy keys."""
    out = dict(bounds)
    for (legacy_lo, legacy_hi), (dyn_lo, dyn_hi) in _TM_BOUNDS_MIRROR:
        if legacy_lo in out or legacy_hi in out:
            if legacy_lo in out:
                out[dyn_lo] = out[legacy_lo]
            if legacy_hi in out:
                out[dyn_hi] = out[legacy_hi]
        elif dyn_lo in out or dyn_hi in out:
            if dyn_lo in out:
                out[legacy_lo] = out[dyn_lo]
            if dyn_hi in out:
                out[legacy_hi] = out[dyn_hi]
    return out


def _axis_keys(
    bounds: Mapping[str, Any],
    min_keys: Sequence[str],
    max_keys: Sequence[str],
) -> Optional[Tuple[str, str, float, float]]:
    lo_key = next((k for k in min_keys if k in bounds), None)
    hi_key = next((k for k in max_keys if k in bounds), None)
    if lo_key is None or hi_key is None:
        return None
    try:
        lo = float(bounds[lo_key])
        hi = float(bounds[hi_key])
    except (TypeError, ValueError):
        return None
    if not (lo <= hi):
        lo, hi = hi, lo
    return lo_key, hi_key, lo, hi


def tighten_axis_range(
    lo: float,
    hi: float,
    *,
    shrink: float,
    floor_lift: float = 0.0,
) -> Tuple[float, float]:
    """Shrink [lo, hi] toward midpoint; optional floor lift (fraction of span)."""
    span = hi - lo
    if span <= 0:
        return lo, hi
    mid = (lo + hi) / 2.0
    half = span * (1.0 - shrink) / 2.0
    new_lo = mid - half
    new_hi = mid + half
    if floor_lift > 0:
        bump = span * floor_lift
        new_lo = min(new_lo + bump, mid)
    return new_lo, new_hi


def tighten_template_bounds(
    bounds: Mapping[str, Any],
    *,
    shrink: float = _DEFAULT_SHRINK,
    tb_floor_lift: float = _DEFAULT_TB_FLOOR_LIFT,
    bbe_floor_lift: float = _DEFAULT_BBE_FLOOR_LIFT,
) -> Dict[str, Any]:
    out = dict(bounds)
    changes: List[Dict[str, Any]] = []

    for min_keys, max_keys, axis in _BOUNDS_AXES:
        parsed = _axis_keys(bounds, min_keys, max_keys)
        if parsed is None:
            continue
        lo_key, hi_key, lo, hi = parsed
        floor_lift = 0.0
        if axis == "tb":
            floor_lift = tb_floor_lift
        elif axis == "bbe":
            floor_lift = bbe_floor_lift
        new_lo, new_hi = tighten_axis_range(lo, hi, shrink=shrink, floor_lift=floor_lift)
        if abs(new_lo - lo) < 1e-12 and abs(new_hi - hi) < 1e-12:
            continue
        out[lo_key] = round(new_lo, 4)
        out[hi_key] = round(new_hi, 4)
        changes.append(
            {
                "axis": axis,
                "lo_key": lo_key,
                "hi_key": hi_key,
                "before": [lo, hi],
                "after": [out[lo_key], out[hi_key]],
            }
        )
    out["_bull_recency_01_tightened"] = bool(changes)
    return out


def apply_bull_recency_01_brain_patch(
    brain: Mapping[str, Any],
    *,
    shrink: Optional[float] = None,
    tb_floor_lift: Optional[float] = None,
    bbe_floor_lift: Optional[float] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Return (patched_brain, audit). Only CLUSTER_1_*폭발형* LIVE templates are touched.
    UNDERDOG / other clusters unchanged.
    """
    shrink_v = shrink if shrink is not None else _env_float(
        "BULL_RECENCY_01_SHRINK", _DEFAULT_SHRINK, lo=0.05, hi=0.45
    )
    tb_lift = tb_floor_lift if tb_floor_lift is not None else _env_float(
        "BULL_RECENCY_01_TB_FLOOR_LIFT", _DEFAULT_TB_FLOOR_LIFT, lo=0.0, hi=0.40
    )
    bbe_lift = bbe_floor_lift if bbe_floor_lift is not None else _env_float(
        "BULL_RECENCY_01_BBE_FLOOR_LIFT", _DEFAULT_BBE_FLOOR_LIFT, lo=0.0, hi=0.40
    )

    out = copy.deepcopy(dict(brain))
    ml = out.get("LIVE_CLUSTER_TEMPLATES")
    if not isinstance(ml, dict):
        ml = {}
        out["LIVE_CLUSTER_TEMPLATES"] = ml

    patched: List[Dict[str, Any]] = []
    skipped = 0
    for name, bounds in list(ml.items()):
        if not isinstance(bounds, dict):
            skipped += 1
            continue
        if not is_cluster_1_explosive_template(name):
            continue
        bounds_before = mirror_bounds_for_time_machine(
            {k: v for k, v in bounds.items() if not str(k).startswith("_")}
        )
        tightened = tighten_template_bounds(
            bounds,
            shrink=shrink_v,
            tb_floor_lift=tb_lift,
            bbe_floor_lift=bbe_lift,
        )
        mirrored = mirror_bounds_for_time_machine(
            {k: v for k, v in tightened.items() if not str(k).startswith("_")}
        )
        ml[name] = mirrored
        patched.append(
            {
                "template": name,
                "shrink": shrink_v,
                "tb_floor_lift": tb_lift,
                "bbe_floor_lift": bbe_lift,
                "bounds_before": bounds_before,
                "bounds_after": mirrored,
                "keys_mirrored_for_time_machine": True,
            }
        )

    audit = {
        "patch": "bull_recency_01_cluster_1_bounds",
        "shrink": shrink_v,
        "tb_floor_lift": tb_lift,
        "bbe_floor_lift": bbe_lift,
        "templates_patched": len(patched),
        "templates_skipped_non_dict": skipped,
        "patched": patched,
    }
    if resolve_bull_recency_01_kr_lever():
        out, kr_audit = apply_kr_rs_lever_to_brain(out)
        audit["kr_lever"] = kr_audit
    return out, audit
