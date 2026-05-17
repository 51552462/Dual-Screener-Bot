"""
메타 거버너 상태 소비자 — Kelly·포지션 캡 병합 (스캐너 비사용).

system_config.json 과 분리된 meta_governor_state.json 만 읽는다.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

from meta_governor import load_meta_governor_state, meta_state_path
from toxic_antipattern_core import any_toxic_rule_matches

logger = logging.getLogger(__name__)

_META_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "data": None}


def load_meta_state_resolved(path: Optional[str] = None) -> Dict[str, Any]:
    """파일 mtime 기준 소프트 캐시 (try_add 고빈도 호출 대비)."""
    p = path or meta_state_path()
    try:
        mtime = os.path.getmtime(p) if os.path.isfile(p) else None
    except OSError:
        mtime = None
    if _META_CACHE["path"] == p and _META_CACHE["mtime"] == mtime and isinstance(_META_CACHE["data"], dict):
        return _META_CACHE["data"]
    data = load_meta_governor_state(p)
    _META_CACHE["path"] = p
    _META_CACHE["mtime"] = mtime
    _META_CACHE["data"] = data
    return data


def invalidate_meta_state_cache() -> None:
    _META_CACHE["path"] = None
    _META_CACHE["mtime"] = None
    _META_CACHE["data"] = None


def _flags(meta: Dict[str, Any]) -> Dict[str, Any]:
    f = meta.get("META_OPERATOR_FLAGS")
    return f if isinstance(f, dict) else {}


def _regime_action(meta: Dict[str, Any]) -> Dict[str, Any]:
    ra = meta.get("META_REGIME_ACTION")
    if isinstance(ra, dict):
        return ra
    return {}


def apply_meta_weight_bounds_clamp(
    w_s1: float,
    w_s4: float,
    meta: Optional[Dict[str, Any]],
) -> Tuple[float, float]:
    """
    META_REGIME_ACTION 의 weight_s1_bounds / weight_s4_bounds [lo, hi] 로 클램프.
    메타 키가 없거나 bounds 가 비어 있으면 입력 그대로 반환.
    """
    if not meta:
        return float(w_s1), float(w_s4)
    ra = _regime_action(meta)
    out1, out4 = float(w_s1), float(w_s4)
    b1 = ra.get("weight_s1_bounds")
    if isinstance(b1, (list, tuple)) and len(b1) == 2:
        try:
            lo, hi = float(b1[0]), float(b1[1])
            if lo <= hi:
                out1 = min(max(out1, lo), hi)
        except (TypeError, ValueError):
            pass
    b4 = ra.get("weight_s4_bounds")
    if isinstance(b4, (list, tuple)) and len(b4) == 2:
        try:
            lo, hi = float(b4[0]), float(b4[1])
            if lo <= hi:
                out4 = min(max(out4, lo), hi)
        except (TypeError, ValueError):
            pass
    return out1, out4


def apply_meta_kelly_merge(
    kelly_risk_pct: float,
    meta: Optional[Dict[str, Any]],
    *,
    ns_prefix: str,
    core_group_name: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    entry_facts: Optional[Dict[str, Any]] = None,
    sector_mapped: Optional[str] = None,
) -> float:
    """
    승인된 소비자 병합 규칙 (곱셈 + 캡/플로어):

    effective = base
              * META_GLOBAL_KELLY_MULT
              * META_NS_KELLY_MULT[ns_prefix] (없으면 1)
              * META_GROUP_KELLY_MULT[core_group_name] (이름 있을 때만, 없으면 1)
    그 후 META_REGIME_ACTION.kelly_cap / kelly_floor 로 clamp.
    KILL_SWITCH 가 참이면 0.
    오답노트(bbox) 일치 시 (sys_config+facts+sector 제공 시) 즉시 0 — 스캐너 합격 후에도 자본 차단.
    """
    if (
        sys_config is not None
        and entry_facts is not None
        and sector_mapped is not None
        and any_toxic_rule_matches(sys_config, entry_facts, str(sector_mapped))
    ):
        return 0.0

    if meta is None:
        return float(kelly_risk_pct)

    flags = _flags(meta)
    if bool(flags.get("KILL_SWITCH")):
        return 0.0

    out = float(kelly_risk_pct)
    g = float(meta.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    out *= g

    ns_map = meta.get("META_NS_KELLY_MULT")
    if isinstance(ns_map, dict) and ns_prefix in ns_map:
        try:
            out *= float(ns_map[ns_prefix])
        except (TypeError, ValueError):
            logger.warning("META_NS_KELLY_MULT[%s] invalid, skip", ns_prefix)

    grp_map = meta.get("META_GROUP_KELLY_MULT")
    if core_group_name and isinstance(grp_map, dict) and core_group_name in grp_map:
        try:
            out *= float(grp_map[core_group_name])
        except (TypeError, ValueError):
            logger.warning("META_GROUP_KELLY_MULT[%s] invalid, skip", core_group_name)

    ra = _regime_action(meta)
    cap = ra.get("kelly_cap")
    floor = ra.get("kelly_floor")
    try:
        fval = float(floor) if floor is not None else None
    except (TypeError, ValueError):
        fval = None
    try:
        cval = float(cap) if cap is not None else None
    except (TypeError, ValueError):
        cval = None
    if fval is not None:
        out = max(out, fval)
    if cval is not None:
        out = min(out, cval)
    out = max(out, 0.0)

    return float(out)


def effective_max_position_pct(sys_config: Dict[str, Any], meta: Optional[Dict[str, Any]]) -> float:
    """min(sys MAX_POSITION_PCT, META_MAX_POSITION_PCT) — 메타가 None 이면 sys 만."""
    base = float(sys_config.get("MAX_POSITION_PCT", 0.25) or 0.25)
    if not meta:
        return base
    m = meta.get("META_MAX_POSITION_PCT")
    if m is None:
        return base
    try:
        return min(base, float(m))
    except (TypeError, ValueError):
        return base
