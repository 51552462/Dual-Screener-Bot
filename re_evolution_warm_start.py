"""
Re-Evolution Warm-Start — 불사조 LIVE 복귀 초기 자본 배분 SSOT.

Architect 철학: 섀도우 검증 통과 직후 0% 근처 최소 비중이 아니라,
정상 Kelly 대비 Base Confidence(기본 40%)를 즉시 부여해 신뢰도를 반영한다.

상태 키: META_RE_EVOLUTION_WARM_START[group_key]

Phase 2 (EV Ramp-up): re_evolution_ev_rampup.py
  · 실전 1~3회 청산 vs 섀도우 EV 매칭 → full_ramp(100%)
  · 괴리/대손실 → shadow_recall(킬스위치)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

WARM_START_PHASE = "warm_start"
FULL_RAMP_PHASE = "full_ramp"
SHADOW_RECALL_PHASE = "shadow_recall"

DEFAULT_BASE_CONFIDENCE = 0.40


def _cfg_float(cfg: Optional[Dict[str, Any]], key: str, default: float) -> float:
    if not isinstance(cfg, dict):
        return default
    block = cfg.get("RE_EVOLUTION_WARM_START") or {}
    base = block if isinstance(block, dict) else cfg
    try:
        return float(base.get(key, cfg.get(key, default)))
    except (TypeError, ValueError):
        return default


def warm_start_config(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "base_confidence": _cfg_float(
            sys_config, "RE_EVOLUTION_WARM_START_BASE_CONFIDENCE", DEFAULT_BASE_CONFIDENCE
        ),
        "enabled": True,
    }


def _warm_start_map(meta: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    raw = (meta or {}).get("META_RE_EVOLUTION_WARM_START")
    return dict(raw) if isinstance(raw, dict) else {}


def resolve_warm_start_record(
    meta: Optional[Mapping[str, Any]],
    market: str,
    group_key: str,
) -> Optional[Dict[str, Any]]:
    """그룹 warm-start 레코드 — market|group 복합키 우선, group_key 폴백."""
    if not isinstance(meta, Mapping):
        return None
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    if not gk:
        return None
    ws = _warm_start_map(meta)
    bucket = f"{mk}|{gk}"
    rec = ws.get(bucket)
    if isinstance(rec, dict):
        return dict(rec)
    rec = ws.get(gk)
    if isinstance(rec, dict) and str(rec.get("market") or mk).upper() == mk:
        return dict(rec)
    return None


def is_warm_start_live_group(
    meta: Optional[Mapping[str, Any]],
    market: str,
    group_key: str,
) -> bool:
    rec = resolve_warm_start_record(meta, market, group_key)
    if not rec:
        return False
    phase = str(rec.get("phase") or "").lower()
    mult = float(rec.get("kelly_mult") or 0.0)
    return phase == WARM_START_PHASE and mult > 0.0 and mult < 1.0


def resolve_warm_start_kelly_mult(
    meta: Optional[Mapping[str, Any]],
    market: str,
    group_key: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> float:
    """
    warm_start → base_confidence(0.4), full_ramp → 1.0, 없음 → 1.0.
    shadow_recall → 0.0 (2번 킬스위치용 예약).
    """
    rec = resolve_warm_start_record(meta, market, group_key)
    if not rec:
        return 1.0
    phase = str(rec.get("phase") or "").lower()
    if phase == SHADOW_RECALL_PHASE:
        return 0.0
    if phase == FULL_RAMP_PHASE:
        return 1.0
    if phase == WARM_START_PHASE:
        try:
            return float(rec.get("kelly_mult") or 0.0)
        except (TypeError, ValueError):
            pass
        return warm_start_config(sys_config)["base_confidence"]
    return 1.0


def apply_warm_start_kelly_scaler(
    kelly_risk_pct: float,
    meta: Optional[Mapping[str, Any]],
    *,
    market: str,
    group_key: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> float:
    """정상 Kelly × warm-start 배수."""
    mult = resolve_warm_start_kelly_mult(
        meta, market, group_key, sys_config=sys_config
    )
    if mult >= 0.999:
        return float(kelly_risk_pct)
    if mult <= 0.0:
        return 0.0
    return float(kelly_risk_pct) * float(mult)


def apply_warm_start_registry_row(
    row: Dict[str, Any],
    *,
    shadow_stats: Optional[Mapping[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    now_iso: Optional[str] = None,
) -> Dict[str, Any]:
    """LIVE 승격 registry — capital_mult = base_confidence."""
    cfg = warm_start_config(sys_config)
    mult = float(cfg["base_confidence"])
    now_iso = now_iso or datetime.now(timezone.utc).isoformat()

    row["state"] = "LIVE"
    row["capital_mult"] = mult
    row["warm_start_mult"] = mult
    row["re_evolution_warm_start"] = True
    row["re_evolution_warm_start_at"] = now_iso
    row["promote_reason"] = "re_evolution_redemption_warm_start"
    row["shadow_ev_avg_ret_pct"] = (shadow_stats or {}).get("avg_ret_pct")
    row["shadow_ev_n_closed"] = (shadow_stats or {}).get("n_closed")
    return row


def apply_warm_start_meta_on_redemption(
    meta: Dict[str, Any],
    *,
    market: str,
    group_key: str,
    strategy_id: str,
    shadow_stats: Optional[Mapping[str, Any]] = None,
    gate_detail: Optional[Mapping[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    now_iso: Optional[str] = None,
) -> Dict[str, Any]:
    """META warm-start 상태 + Kelly overlay 40% 부여."""
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    sid = str(strategy_id or "").strip()
    now_iso = now_iso or datetime.now(timezone.utc).isoformat()
    cfg = warm_start_config(sys_config)
    mult = float(cfg["base_confidence"])
    stats = dict(shadow_stats or {})

    bucket = f"{mk}|{gk}"
    ws = _warm_start_map(meta)
    ws[bucket] = {
        "strategy_id": sid,
        "market": mk,
        "group_key": gk,
        "phase": WARM_START_PHASE,
        "kelly_mult": mult,
        "base_confidence": mult,
        "ramp_target_mult": 1.0,
        "redeemed_at": now_iso,
        "shadow_ev_avg_ret_pct": stats.get("avg_ret_pct"),
        "shadow_ev_std_ret_pct": stats.get("effective_std_ret_pct")
        or stats.get("std_ret_pct"),
        "shadow_ev_n_closed": stats.get("n_closed"),
        "shadow_wr": stats.get("win_rate"),
        "shadow_pf": stats.get("profit_factor"),
        "redemption_gate": dict(gate_detail or {}),
        "live_closures": 0,
        "live_closure_rets": [],
    }
    meta["META_RE_EVOLUTION_WARM_START"] = ws

    re_overlay = dict(meta.get("META_RE_EVOLUTION_KELLY_OVERLAY") or {})
    re_overlay[gk] = mult
    meta["META_RE_EVOLUTION_KELLY_OVERLAY"] = re_overlay

    dm = dict(meta.get("META_DEATHMATCH_KELLY_OVERLAY") or {})
    if gk in dm and float(dm.get(gk) or 0.0) <= 0.0:
        dm.pop(gk, None)
    meta["META_DEATHMATCH_KELLY_OVERLAY"] = dm

    log: list = list(meta.get("META_RE_EVOLUTION_WARM_START_LOG") or [])
    log.append(
        {
            "market": mk,
            "group_key": gk,
            "strategy_id": sid,
            "phase": WARM_START_PHASE,
            "kelly_mult": mult,
            "at": now_iso,
        }
    )
    meta["META_RE_EVOLUTION_WARM_START_LOG"] = log[-50:]

    try:
        from strategy_promotion_engine import resolve_live_ev_verification_tolerance

        dyn_tol = resolve_live_ev_verification_tolerance(mk, meta=meta, sys_config=sys_config)
        ws[bucket]["dynamic_tolerance_at_redeem"] = dyn_tol
        meta["META_RE_EVOLUTION_WARM_START"] = ws
    except Exception as ex:
        logger.debug("warm-start dynamic tolerance snapshot skip: %s", ex)

    logger.info(
        "Re-Evolution warm-start: %s %s kelly_mult=%.2f (shadow_ev=%.3f std=%.3f n=%s)",
        mk,
        gk,
        mult,
        float(stats.get("avg_ret_pct") or 0.0),
        float(stats.get("effective_std_ret_pct") or stats.get("std_ret_pct") or 0.0),
        stats.get("n_closed"),
    )
    return ws[bucket]
