"""
Re-Evolution Z-Score EV Verification — 섀도우 분포 대비 실전 정합성 SSOT.

Ramp:  z ≥ ramp_z_floor (기본 -1.5) 또는 ATR 동적 tolerance 일치
Kill:  z ≤ kill_z_floor (기본 -2.0) → shadow_recall
"""
from __future__ import annotations

import logging
import math
import statistics
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

DEFAULT_RAMP_Z_FLOOR = -1.5
DEFAULT_KILL_Z_FLOOR = -2.0
DEFAULT_MIN_SHADOW_STD_PCT = 0.35


def _cfg_bool(cfg: Optional[Dict[str, Any]], key: str, default: bool) -> bool:
    if not isinstance(cfg, dict):
        return default
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def zscore_ev_config(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    block = cfg.get("RE_EVOLUTION_ZSCORE_EV") or {}
    base = block if isinstance(block, dict) else cfg

    def _f(key: str, default: float) -> float:
        try:
            return float(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        "enabled": _cfg_bool(cfg, "ENABLE_RE_EVOLUTION_ZSCORE_EV", True),
        "ramp_z_floor": _f("RE_EVOLUTION_EV_RAMP_Z_FLOOR", DEFAULT_RAMP_Z_FLOOR),
        "kill_z_floor": _f("RE_EVOLUTION_EV_KILL_Z_FLOOR", DEFAULT_KILL_Z_FLOOR),
        "min_shadow_std_pct": _f(
            "RE_EVOLUTION_MIN_SHADOW_STD_PCT", DEFAULT_MIN_SHADOW_STD_PCT
        ),
    }


def compute_shadow_return_distribution(
    returns: Sequence[float],
    *,
    min_std_pct: float = DEFAULT_MIN_SHADOW_STD_PCT,
) -> Dict[str, Any]:
    """섀도우 청산 수익률 분포 — mean · std (표본 부족 시 보수적 std 바닥)."""
    rets = [float(x) for x in returns if x is not None and math.isfinite(float(x))]
    n = len(rets)
    if n == 0:
        return {
            "n": 0,
            "mean_pct": None,
            "std_pct": None,
            "effective_std_pct": None,
        }

    mean = sum(rets) / n
    if n >= 2:
        std = float(statistics.pstdev(rets))
    else:
        std = max(float(min_std_pct), abs(mean) * 0.5)

    eff_std = max(float(min_std_pct), float(std))
    return {
        "n": n,
        "mean_pct": round(mean, 4),
        "std_pct": round(std, 4),
        "effective_std_pct": round(eff_std, 4),
        "returns": [round(r, 4) for r in rets],
    }


def resolve_shadow_ev_distribution(
    warm_record: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """warm-start 레코드 → 섀도우 mean/std."""
    if not isinstance(warm_record, Mapping):
        return {"mean_pct": None, "std_pct": None, "n": 0}

    mean = warm_record.get("shadow_ev_avg_ret_pct")
    std = warm_record.get("shadow_ev_std_ret_pct")
    n = warm_record.get("shadow_ev_n_closed")
    try:
        mean_f = float(mean) if mean is not None else None
    except (TypeError, ValueError):
        mean_f = None
    try:
        std_f = float(std) if std is not None else None
    except (TypeError, ValueError):
        std_f = None
    try:
        n_i = int(n or 0)
    except (TypeError, ValueError):
        n_i = 0

    return {
        "mean_pct": mean_f,
        "std_pct": std_f,
        "effective_std_pct": std_f,
        "n": n_i,
    }


def compute_z_score(
    live_ret_pct: float,
    shadow_mean_pct: float,
    shadow_std_pct: float,
    *,
    min_std_pct: float = DEFAULT_MIN_SHADOW_STD_PCT,
) -> Optional[float]:
    if shadow_mean_pct is None or not math.isfinite(float(shadow_mean_pct)):
        return None
    std = max(float(min_std_pct), float(shadow_std_pct or 0.0))
    if std <= 0:
        return None
    return (float(live_ret_pct) - float(shadow_mean_pct)) / std


def enrich_ev_ramp_config_with_zscore(
    base_cfg: Mapping[str, Any],
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(base_cfg)
    zs = zscore_ev_config(sys_config)
    out["zscore_ev"] = zs
    out["ramp_z_floor"] = zs["ramp_z_floor"]
    out["kill_z_floor"] = zs["kill_z_floor"]
    out["min_shadow_std_pct"] = zs["min_shadow_std_pct"]
    return out


def should_trigger_zscore_kill(
    live_ret_pct: float,
    shadow_mean_pct: Optional[float],
    shadow_std_pct: Optional[float],
    cfg: Mapping[str, Any],
) -> Tuple[bool, str, Dict[str, Any]]:
    """z ≤ kill_z_floor (기본 -2.0) → 가짜 부활 킬스위치."""
    zs_block = cfg.get("zscore_ev") if isinstance(cfg.get("zscore_ev"), dict) else {}
    if not zs_block.get("enabled", cfg.get("zscore_enabled", True)):
        return False, "", {}

    if shadow_mean_pct is None or shadow_std_pct is None:
        return False, "", {"reason": "shadow_distribution_missing"}

    kill_z = float(cfg.get("kill_z_floor") or zs_block.get("kill_z_floor") or DEFAULT_KILL_Z_FLOOR)
    min_std = float(cfg.get("min_shadow_std_pct") or DEFAULT_MIN_SHADOW_STD_PCT)
    std_eff = max(min_std, float(shadow_std_pct))
    z = compute_z_score(
        live_ret_pct, float(shadow_mean_pct), std_eff, min_std_pct=min_std
    )
    detail = {
        "z_score": round(z, 4) if z is not None else None,
        "kill_z_floor": kill_z,
        "shadow_mean_pct": float(shadow_mean_pct),
        "shadow_std_pct": std_eff,
        "live_ret_pct": float(live_ret_pct),
    }
    if z is not None and z <= kill_z:
        return True, f"z_score={z:.2f}<={kill_z}", detail
    return False, "", detail


def evaluate_zscore_ramp_alignment(
    live_rets: List[float],
    shadow_mean_pct: Optional[float],
    shadow_std_pct: Optional[float],
    cfg: Mapping[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Ramp Z 게이트: 실전이 섀도우 평균 대비 ramp_z_floor 이내 (기본 -1.5σ).
    단일 청산 또는 누적 평균 중 하나라도 통과 시 True.
    """
    zs_block = cfg.get("zscore_ev") if isinstance(cfg.get("zscore_ev"), dict) else {}
    if not zs_block.get("enabled", True):
        return False, {"reason": "zscore_disabled"}

    if shadow_mean_pct is None or shadow_std_pct is None or not live_rets:
        return False, {"reason": "insufficient_shadow_distribution"}

    ramp_z = float(cfg.get("ramp_z_floor") or zs_block.get("ramp_z_floor") or DEFAULT_RAMP_Z_FLOOR)
    min_std = float(cfg.get("min_shadow_std_pct") or DEFAULT_MIN_SHADOW_STD_PCT)
    std_eff = max(min_std, float(shadow_std_pct))
    mean = float(shadow_mean_pct)
    rets = [float(x) for x in live_rets]

    per_z: List[Optional[float]] = []
    per_ok: List[bool] = []
    for r in rets:
        z = compute_z_score(r, mean, std_eff, min_std_pct=min_std)
        per_z.append(round(z, 4) if z is not None else None)
        per_ok.append(z is not None and z >= ramp_z)

    detail: Dict[str, Any] = {
        "shadow_mean_pct": mean,
        "shadow_std_pct": std_eff,
        "ramp_z_floor": ramp_z,
        "per_trade_z": per_z,
        "per_trade_z_ok": per_ok,
        "live_rets": rets,
    }

    if any(per_ok):
        detail["match"] = True
        detail["reason"] = "single_trade_within_z_band"
        return True, detail

    detail["match"] = False
    detail["reason"] = "z_score_mismatch"
    return False, detail


def evaluate_combined_live_ev_verification(
    live_rets: List[float],
    shadow_mean_pct: Optional[float],
    shadow_std_pct: Optional[float],
    cfg: Mapping[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    통합 Ramp 게이트 — ATR tolerance OR Z-Score (-1.5σ) 중 하나 통과 시 승인.
    """
    from re_evolution_ev_rampup import evaluate_ev_alignment

    tol_ok, tol_detail = evaluate_ev_alignment(live_rets, shadow_mean_pct, cfg)
    z_ok, z_detail = evaluate_zscore_ramp_alignment(
        live_rets, shadow_mean_pct, shadow_std_pct, cfg
    )

    combined = {
        "tolerance_gate": tol_detail,
        "zscore_gate": z_detail,
        "match": bool(tol_ok or z_ok),
    }
    if tol_ok and z_ok:
        combined["reason"] = "tolerance_and_zscore"
    elif tol_ok:
        combined["reason"] = tol_detail.get("reason", "tolerance_match")
    elif z_ok:
        combined["reason"] = z_detail.get("reason", "zscore_match")
    else:
        combined["reason"] = "ev_and_zscore_mismatch"

    return bool(tol_ok or z_ok), combined
