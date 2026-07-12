"""
Re-Evolution EV Ramp-up — Warm-Start 실측 매칭 및 가속 복원 SSOT.

불사조 LIVE 복귀 후:
  · 실전 1~3회 청산 vs 섀도우 EV(avg_ret) 슬리피지 비교
  · 일치 → 즉시 100% Kelly (full_ramp)
  · 섀도우 대비 큰 손실/괴리 → shadow_recall (가짜 부활 킬스위치)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from re_evolution_strike_guard import (
    _load_shadow_set,
    _merge_re_evolution_kelly_overlay,
    extract_core_group_name,
    is_live_capital_closure,
)
from re_evolution_warm_start import (
    FULL_RAMP_PHASE,
    SHADOW_RECALL_PHASE,
    WARM_START_PHASE,
    resolve_warm_start_record,
)

logger = logging.getLogger(__name__)


def _cfg_bool(cfg: Optional[Dict[str, Any]], key: str, default: bool) -> bool:
    if not isinstance(cfg, dict):
        return default
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def ev_rampup_config(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    block = cfg.get("RE_EVOLUTION_EV_RAMPUP") or {}
    base = block if isinstance(block, dict) else cfg

    def _f(key: str, default: float) -> float:
        try:
            return float(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        try:
            return int(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        "enabled": _cfg_bool(cfg, "ENABLE_RE_EVOLUTION_EV_RAMPUP", True),
        "slippage_tolerance_pct": _f("RE_EVOLUTION_EV_SLIPPAGE_TOLERANCE_PCT", 2.5),
        "max_eval_closures": _i("RE_EVOLUTION_EV_RAMP_MAX_CLOSURES", 3),
        "max_trading_days": _i("RE_EVOLUTION_EV_RAMP_MAX_TRADING_DAYS", 3),
        "kill_single_loss_pct": _f("RE_EVOLUTION_EV_KILL_LOSS_PCT", -8.0),
        "kill_divergence_extra_pct": _f("RE_EVOLUTION_EV_KILL_DIVERGENCE_PCT", 5.0),
    }


def _parse_iso_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


def _calendar_days_since(iso_start: Optional[str], now: Optional[datetime] = None) -> Optional[int]:
    start = _parse_iso_dt(iso_start)
    if start is None:
        return None
    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    return max(0, (now_dt - start.astimezone(timezone.utc)).days)


def _warm_bucket(market: str, group_key: str) -> str:
    return f"{str(market or 'KR').upper()}|{str(group_key or '').strip()}"


def should_trigger_kill_switch(
    live_ret_pct: float,
    shadow_ev_pct: Optional[float],
    cfg: Mapping[str, Any],
    *,
    shadow_std_pct: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    가짜 부활 킬스위치:
      · 단일 청산 대손실 (기본 -8%)
      · 섀도우 EV 대비 허용 슬리피지+추가 괴리 초과 하락
      · Z-Score ≤ kill_z_floor (기본 -2.0σ)
    """
    kill_th = float(cfg.get("kill_single_loss_pct") or -8.0)
    slip = float(cfg.get("slippage_tolerance_pct") or 2.5)
    extra = float(cfg.get("kill_divergence_extra_pct") or 5.0)

    if float(live_ret_pct) <= kill_th:
        return True, f"single_loss<={kill_th}%"

    if shadow_ev_pct is not None:
        shadow = float(shadow_ev_pct)
        floor = shadow - slip - extra
        if float(live_ret_pct) < floor:
            return (
                True,
                f"ev_divergence live={live_ret_pct:.2f}% << shadow={shadow:.2f}%",
            )

    if shadow_ev_pct is not None and shadow_std_pct is not None:
        from re_evolution_zscore_ev import should_trigger_zscore_kill

        z_kill, z_reason, _ = should_trigger_zscore_kill(
            float(live_ret_pct),
            float(shadow_ev_pct),
            float(shadow_std_pct),
            cfg,
        )
        if z_kill:
            return True, z_reason
    return False, ""


def evaluate_ev_alignment(
    live_rets: List[float],
    shadow_ev_pct: Optional[float],
    cfg: Mapping[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """실전 청산(1~N) vs 섀도우 EV — 슬리피지 이내 일치 여부."""
    slip = float(cfg.get("slippage_tolerance_pct") or 2.5)
    max_n = int(cfg.get("max_eval_closures") or 3)
    rets = [float(x) for x in (live_rets or [])[:max_n]]
    shadow = float(shadow_ev_pct) if shadow_ev_pct is not None else None

    detail: Dict[str, Any] = {
        "live_rets": rets,
        "shadow_ev_pct": shadow,
        "slippage_tolerance_pct": slip,
        "n_live": len(rets),
    }
    if isinstance(cfg.get("dynamic_tolerance"), Mapping):
        detail["dynamic_tolerance"] = dict(cfg["dynamic_tolerance"])

    if shadow is None or not rets:
        detail["match"] = False
        detail["reason"] = "insufficient_data"
        return False, detail

    per_trade = [abs(r - shadow) <= slip for r in rets]
    detail["per_trade_match"] = per_trade
    if any(per_trade):
        detail["match"] = True
        detail["reason"] = "single_trade_within_slippage"
        return True, detail

    avg = sum(rets) / len(rets)
    detail["live_avg_ret_pct"] = round(avg, 4)
    if abs(avg - shadow) <= slip:
        detail["match"] = True
        detail["reason"] = "running_avg_within_slippage"
        return True, detail

    detail["match"] = False
    detail["reason"] = "ev_mismatch"
    return False, detail


def _append_warm_start_log(meta: Dict[str, Any], entry: Dict[str, Any]) -> None:
    log: List[Dict[str, Any]] = list(meta.get("META_RE_EVOLUTION_WARM_START_LOG") or [])
    log.append(entry)
    meta["META_RE_EVOLUTION_WARM_START_LOG"] = log[-80:]


def apply_full_ramp_promotion(
    meta: Dict[str, Any],
    *,
    market: str,
    group_key: str,
    strategy_id: str,
    ramp_detail: Mapping[str, Any],
    forward_db_path: Optional[str] = None,
    now_iso: Optional[str] = None,
) -> Dict[str, Any]:
    """warm_start → full_ramp: Kelly·capital 100%."""
    from strategy_registry_store import upsert_registry_rows

    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    sid = str(strategy_id or "").strip()
    now_iso = now_iso or datetime.now(timezone.utc).isoformat()
    bucket = _warm_bucket(mk, gk)

    ws = dict(meta.get("META_RE_EVOLUTION_WARM_START") or {})
    rec = dict(ws.get(bucket) or {})
    rec.update(
        {
            "phase": FULL_RAMP_PHASE,
            "kelly_mult": 1.0,
            "full_ramp_at": now_iso,
            "ramp_detail": dict(ramp_detail),
        }
    )
    ws[bucket] = rec
    meta["META_RE_EVOLUTION_WARM_START"] = ws

    re_overlay = dict(meta.get("META_RE_EVOLUTION_KELLY_OVERLAY") or {})
    re_overlay.pop(gk, None)
    meta["META_RE_EVOLUTION_KELLY_OVERLAY"] = re_overlay

    try:
        from re_evolution_redemption_gate import restore_redemption_capital_overlay

        restore_redemption_capital_overlay(meta, gk)
    except Exception as ex:
        logger.warning("full ramp kelly restore skip: %s", ex)

    _append_warm_start_log(
        meta,
        {
            "market": mk,
            "group_key": gk,
            "strategy_id": sid,
            "phase": FULL_RAMP_PHASE,
            "kelly_mult": 1.0,
            "at": now_iso,
            "ramp_detail": dict(ramp_detail),
        },
    )

    reg_row = {
        "strategy_id": sid,
        "market": mk.split("_")[0] if "_" in mk else mk,
        "group_key": gk,
        "display_name": gk,
        "state": "LIVE",
        "capital_mult": 1.0,
        "warm_start_mult": 1.0,
        "re_evolution_warm_start": False,
        "re_evolution_full_ramp_at": now_iso,
        "promote_reason": "re_evolution_ev_full_ramp",
        "updated_at": now_iso,
    }
    upsert_registry_rows([reg_row], forward_db_path)

    logger.info("Re-Evolution EV full ramp: %s %s → 100%% Kelly", mk, gk)
    return {"action": "full_ramp", "market": mk, "group_key": gk, "kelly_mult": 1.0}


def apply_fake_resurrection_recall(
    meta: Dict[str, Any],
    *,
    market: str,
    group_key: str,
    strategy_id: str,
    kill_reason: str,
    live_ret_pct: float,
    forward_db_path: Optional[str] = None,
    now_iso: Optional[str] = None,
) -> Dict[str, Any]:
    """가짜 부활 킬스위치 — LIVE → OBSERVING 섀도우, Kelly=0."""
    from strategy_registry_store import upsert_registry_rows

    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    sid = str(strategy_id or "").strip()
    now_iso = now_iso or datetime.now(timezone.utc).isoformat()
    bucket = _warm_bucket(mk, gk)
    reason = f"re_evolution_fake_resurrection({kill_reason})"

    ws = dict(meta.get("META_RE_EVOLUTION_WARM_START") or {})
    rec = dict(ws.get(bucket) or {})
    rec.update(
        {
            "phase": SHADOW_RECALL_PHASE,
            "kelly_mult": 0.0,
            "recalled_at": now_iso,
            "kill_reason": kill_reason,
            "kill_live_ret_pct": float(live_ret_pct),
        }
    )
    ws[bucket] = rec
    meta["META_RE_EVOLUTION_WARM_START"] = ws

    shadow = sorted(_load_shadow_set(meta) | {gk})
    meta["META_RE_EVOLUTION_SHADOW_GROUPS"] = shadow
    _merge_re_evolution_kelly_overlay(meta, gk)

    demoted_log: List[Dict[str, Any]] = list(meta.get("META_RE_EVOLUTION_DEMOTED") or [])
    demoted_log.append(
        {
            "strategy_id": sid,
            "market": mk,
            "group_key": gk,
            "demoted_at": now_iso,
            "reason": reason,
            "mutation_pending": True,
            "mutation_done": False,
            "source": "ev_kill_switch",
        }
    )
    meta["META_RE_EVOLUTION_DEMOTED"] = demoted_log[-50:]

    _append_warm_start_log(
        meta,
        {
            "market": mk,
            "group_key": gk,
            "strategy_id": sid,
            "phase": SHADOW_RECALL_PHASE,
            "kelly_mult": 0.0,
            "at": now_iso,
            "kill_reason": kill_reason,
            "live_ret_pct": float(live_ret_pct),
        },
    )

    reg_row = {
        "strategy_id": sid,
        "market": mk.split("_")[0] if "_" in mk else mk,
        "group_key": gk,
        "display_name": gk,
        "state": "OBSERVING",
        "capital_mult": 0.0,
        "warm_start_mult": 0.0,
        "re_evolution_warm_start": False,
        "source": "re_evolution_ev_kill",
        "last_demoted_at": now_iso,
        "demote_reason": reason,
        "updated_at": now_iso,
    }
    upsert_registry_rows([reg_row], forward_db_path)

    logger.warning(
        "Re-Evolution fake resurrection recall: %s %s (%s ret=%.2f%%)",
        mk,
        gk,
        kill_reason,
        float(live_ret_pct),
    )
    return {
        "action": "shadow_recall",
        "market": mk,
        "group_key": gk,
        "reason": reason,
        "live_ret_pct": float(live_ret_pct),
    }


def process_warm_start_live_closure(
    *,
    market: str,
    sig_type: str,
    final_ret_pct: float,
    sim_kelly_invest: float = 0.0,
    invest_amount: float = 0.0,
    exit_date: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Warm-Start LIVE 청산 1건 — EV 매칭 가속 또는 킬스위치.
    ledger track_daily_positions 청산 직후 호출.
    """
    base_cfg = ev_rampup_config(sys_config)
    if not base_cfg["enabled"]:
        return {"action": "disabled"}

    if not is_live_capital_closure(
        sig_type,
        sim_kelly_invest=sim_kelly_invest,
        invest_amount=invest_amount,
    ):
        return {"action": "skip_not_live_capital"}

    gk = extract_core_group_name(sig_type)
    if not gk:
        return {"action": "skip_no_group"}

    from meta_governor import save_meta_governor_state_atomic
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved
    from strategy_promotion_engine import stable_strategy_id

    mk = str(market or "KR").upper()
    meta = dict(load_meta_state_resolved())

    from re_evolution_dynamic_tolerance import enrich_ev_ramp_config_with_dynamic_tolerance
    from re_evolution_zscore_ev import (
        enrich_ev_ramp_config_with_zscore,
        evaluate_combined_live_ev_verification,
    )

    cfg = enrich_ev_ramp_config_with_dynamic_tolerance(
        base_cfg, mk, meta=meta, sys_config=sys_config
    )
    cfg = enrich_ev_ramp_config_with_zscore(cfg, sys_config=sys_config)

    rec = resolve_warm_start_record(meta, mk, gk)
    if not rec:
        return {"action": "skip_not_warm_start", "group_key": gk}

    phase = str(rec.get("phase") or "").lower()
    if phase != WARM_START_PHASE:
        return {"action": "skip_phase", "phase": phase, "group_key": gk}

    sid = str(rec.get("strategy_id") or stable_strategy_id(mk, gk))
    shadow_ev = rec.get("shadow_ev_avg_ret_pct")
    shadow_std = rec.get("shadow_ev_std_ret_pct")
    try:
        shadow_ev_f = float(shadow_ev) if shadow_ev is not None else None
    except (TypeError, ValueError):
        shadow_ev_f = None
    try:
        shadow_std_f = float(shadow_std) if shadow_std is not None else None
    except (TypeError, ValueError):
        shadow_std_f = None

    now_iso = datetime.now(timezone.utc).isoformat()
    if exit_date:
        now_iso = str(exit_date).strip() + "T12:00:00+00:00"

    live_rets: List[float] = list(rec.get("live_closure_rets") or [])
    live_rets.append(float(final_ret_pct))
    max_n = int(cfg["max_eval_closures"])
    live_rets = live_rets[-max_n:]

    bucket = _warm_bucket(mk, gk)
    ws = dict(meta.get("META_RE_EVOLUTION_WARM_START") or {})
    ws_rec = dict(ws.get(bucket) or rec)
    ws_rec["live_closure_rets"] = live_rets
    ws_rec["live_closures"] = len(live_rets)
    ws_rec["last_live_closure_at"] = now_iso
    ws_rec["last_live_ret_pct"] = float(final_ret_pct)
    ws_rec["dynamic_tolerance"] = cfg.get("dynamic_tolerance")

    days = _calendar_days_since(rec.get("redeemed_at"))
    ws_rec["trading_days_since_redeem"] = days
    meta["META_RE_EVOLUTION_WARM_START"] = {**ws, bucket: ws_rec}

    kill, kill_reason = should_trigger_kill_switch(
        float(final_ret_pct),
        shadow_ev_f,
        cfg,
        shadow_std_pct=shadow_std_f,
    )
    if kill:
        out = apply_fake_resurrection_recall(
            meta,
            market=mk,
            group_key=gk,
            strategy_id=sid,
            kill_reason=kill_reason,
            live_ret_pct=float(final_ret_pct),
            forward_db_path=forward_db_path,
            now_iso=now_iso,
        )
        save_meta_governor_state_atomic(meta)
        invalidate_meta_state_cache()
        return {**out, "group_key": gk, "shadow_ev_pct": shadow_ev_f}

    if days is not None and days > int(cfg["max_trading_days"]):
        save_meta_governor_state_atomic(meta)
        invalidate_meta_state_cache()
        return {
            "action": "warm_start_hold",
            "group_key": gk,
            "reason": "ramp_window_expired",
            "trading_days": days,
        }

    matched, match_detail = evaluate_combined_live_ev_verification(
        live_rets, shadow_ev_f, shadow_std_f, cfg
    )
    if matched and len(live_rets) <= max_n:
        out = apply_full_ramp_promotion(
            meta,
            market=mk,
            group_key=gk,
            strategy_id=sid,
            ramp_detail={
                **match_detail,
                "trigger_closure_n": len(live_rets),
                "trading_days": days,
            },
            forward_db_path=forward_db_path,
            now_iso=now_iso,
        )
        save_meta_governor_state_atomic(meta)
        invalidate_meta_state_cache()
        return {**out, "group_key": gk, "match_detail": match_detail}

    save_meta_governor_state_atomic(meta)
    invalidate_meta_state_cache()
    return {
        "action": "ev_pending",
        "group_key": gk,
        "live_closures": len(live_rets),
        "max_closures": max_n,
        "shadow_ev_pct": shadow_ev_f,
        "last_ret_pct": float(final_ret_pct),
        "match_detail": match_detail,
        "dynamic_tolerance": cfg.get("dynamic_tolerance"),
    }
