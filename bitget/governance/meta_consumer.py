"""
Bitget MetaGovernor 소비자 — Kelly·포지션 캡·weight bounds (스캐너 비사용).

주식 `meta_governor_consumer.py` API 대칭. 데이터 소스는 **Bitget 전용**:
  - `bitget.governance.meta_sync.load_bitget_meta_resolved()`
  - `bitget.infra.config_manager` (equity system_config.sqlite 미사용)

루트 `meta_governor_consumer` 직접 import 금지 — 이 모듈만 사용.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

from bitget.governance.meta_sync import (
    load_bitget_meta_unified,
    normalize_regime_key,
    resolve_config_regime_key,
)
from bitget.infra.data_paths import meta_governor_state_path

logger = logging.getLogger(__name__)

_META_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "data": None}


def _meta_cache_fingerprint() -> tuple:
    """JSON mtime + Bitget config_kv META_GOVERNOR_STATE 버전."""
    path = meta_governor_state_path()
    try:
        mtime = os.path.getmtime(path) if os.path.isfile(path) else None
    except OSError:
        mtime = None
    kv_ver = None
    try:
        from bitget.infra import config_manager

        raw = config_manager.get_config_value("META_GOVERNOR_STATE")
        if isinstance(raw, dict):
            kv_ver = (
                raw.get("META_GOVERNOR_LAST_RUN_AT"),
                raw.get("META_REGIME_KEY"),
                raw.get("META_SCHEMA_VERSION"),
            )
    except Exception:
        pass
    return (mtime, kv_ver)


def load_meta_state_resolved(path: Optional[str] = None) -> Dict[str, Any]:
    """Bitget unified meta — 소프트 캐시 (try_add 고빈도 호출 대비)."""
    _ = path  # Bitget SSOT path is data_paths; arg kept for API compat
    fp = _meta_cache_fingerprint()
    if _META_CACHE["mtime"] == fp and isinstance(_META_CACHE["data"], dict):
        return _META_CACHE["data"]
    data = load_bitget_meta_unified()
    _META_CACHE["path"] = meta_governor_state_path()
    _META_CACHE["mtime"] = fp
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
    return ra if isinstance(ra, dict) else {}


def resolve_trading_kelly_base(
    sys_config: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    """실매매·try_add — Bitget config + Meta regime action kelly_cap 병합."""
    if meta is None:
        meta = load_meta_state_resolved()
    base = float(sys_config.get("DYNAMIC_KELLY_RISK", 0.01) or 0.01)
    rk_cfg = resolve_config_regime_key(sys_config)
    rk_meta = normalize_regime_key(meta.get("META_REGIME_KEY"))

    ra = _regime_action(meta)
    cap = ra.get("kelly_cap")
    try:
        cval = float(cap) if cap is not None else None
    except (TypeError, ValueError):
        cval = None

    if rk_cfg in ("", "UNKNOWN") and rk_meta not in ("", "UNKNOWN") and cval is not None:
        base = cval
    elif cval is not None:
        base = min(base, cval)

    floor = ra.get("kelly_floor")
    try:
        fval = float(floor) if floor is not None else None
    except (TypeError, ValueError):
        fval = None
    if fval is not None:
        base = max(base, fval)

    return max(0.0, float(base))


def apply_meta_weight_bounds_clamp(
    w_s1: float,
    w_s4: float,
    meta: Optional[Dict[str, Any]],
) -> Tuple[float, float]:
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
    """Meta mult + kelly_cap/floor + KILL_SWITCH (주식 consumer 규칙 대칭)."""
    if (
        sys_config is not None
        and entry_facts is not None
        and sector_mapped is not None
    ):
        try:
            from toxic_antipattern_core import any_toxic_rule_matches

            if any_toxic_rule_matches(sys_config, entry_facts, str(sector_mapped)):
                return 0.0
        except Exception:
            pass

    if meta is None:
        return float(kelly_risk_pct)

    flags = _flags(meta)
    if bool(flags.get("KILL_SWITCH")):
        return 0.0

    out = float(kelly_risk_pct)
    try:
        out *= float(meta.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    except (TypeError, ValueError):
        pass

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
    cap, floor = ra.get("kelly_cap"), ra.get("kelly_floor")
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
    return max(0.0, float(out))


def effective_max_position_pct(
    sys_config: Dict[str, Any],
    meta: Optional[Dict[str, Any]],
) -> float:
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
