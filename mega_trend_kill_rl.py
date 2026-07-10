"""
Mega-Trend Kill-Switch RL Sensitivity (내부 3번).

주말 피드백 루프 — 킬스위치 발동 후 섹터 후행 수익(기회비용 vs 방어성공)을
forward_trades 장부만으로 평가하여 트리거 민감도를 자율 진화.

연동: exit_ratchet_rl · exit_dynamics · mega_trend_toxic_kill · mega_trend_internal_monitor
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

MEGA_TREND_KILL_RL_STATE_KEY = "MEGA_TREND_KILL_RL_STATE"
MEGA_TREND_KILL_EVENTS_KEY = "kill_events"
MEGA_TREND_SECTOR_DELTAS_KEY = "sector_deltas"
MEGA_TREND_SECTOR_QUARANTINE_KEY = "sector_quarantine"
MEGA_TREND_CONTAMINATION_AUDIT_KEY = "contamination_audit"

CONTAMINATION_FLAG_OK = "ok"
CONTAMINATION_FLAG_HIGH = "high"

# P5 — internal / climax delta keys (global prior + sector overlay 합산)
INTERNAL_DELTA_KEYS = (
    "win_rate_min_delta",
    "mfe_reach_min_delta",
    "bounce_stop_max_delta",
    "pnl_accel_drop_delta",
    "consecutive_loss_delta",
    "defensive_scale_delta",
)
CLIMAX_DELTA_KEYS = (
    "flow_reversal_z_delta",
    "flow_z_drop_min_delta",
    "climax_vol_shrink_delta",
    "scale_out_fraction_delta",
)
ALL_DELTA_KEYS = INTERNAL_DELTA_KEYS + CLIMAX_DELTA_KEYS

# --- Kill type SSOT (P3: internal vs external lane) ---
KILL_TYPE_INTERNAL_MOMENTUM = "internal_momentum"
KILL_TYPE_TOXIC = "toxic_graveyard"
KILL_TYPE_CLIMAX = "climax_external"

KILL_LANE_INTERNAL = "internal"
KILL_LANE_EXTERNAL = "external"

INTERNAL_KILL_TYPES = frozenset({KILL_TYPE_INTERNAL_MOMENTUM, KILL_TYPE_TOXIC})
EXTERNAL_KILL_TYPES = frozenset({KILL_TYPE_CLIMAX})

DEFAULT_KILL_RL_STATE: Dict[str, Any] = {
    # internal lane — 음수 delta → 더 예민 · 양수 delta → 더 둔감
    "win_rate_min_delta": 0.0,
    "mfe_reach_min_delta": 0.0,
    "bounce_stop_max_delta": 0.0,
    "pnl_accel_drop_delta": 0.0,
    "consecutive_loss_delta": 0,
    "defensive_scale_delta": 0.0,
    # external (climax) lane — 양수 flow_reversal_z_delta → 더 예민
    "flow_reversal_z_delta": 0.0,
    "flow_z_drop_min_delta": 0.0,
    "climax_vol_shrink_delta": 0.0,
    "scale_out_fraction_delta": 0.0,
    # 레거시·감사 (internal lane 집계)
    "opportunity_cost_rate": 0.0,
    "defense_success_rate": 0.0,
    "neutral_rate": 0.0,
    # P3 레인별 감사
    "internal_opportunity_cost_rate": 0.0,
    "internal_defense_success_rate": 0.0,
    "climax_opportunity_cost_rate": 0.0,
    "climax_defense_success_rate": 0.0,
    "last_evolve_summary": None,
    "events_evaluated": 0,
    "events_pending": 0,
    "updated_at": None,
}

_OUTCOME_OPPORTUNITY = "opportunity_cost"
_OUTCOME_DEFENSE = "defense_success"
_OUTCOME_NEUTRAL = "neutral"
_OUTCOME_PENDING = "pending"


def classify_kill_lane(kill_type: object) -> str:
    """킬 타입 → RL 레인 (internal 장부기반 / external climax)."""
    kt = str(kill_type or "")
    if kt in EXTERNAL_KILL_TYPES:
        return KILL_LANE_EXTERNAL
    return KILL_LANE_INTERNAL


def kill_rl_config() -> Dict[str, Any]:
    def _f(key: str, default: float) -> float:
        try:
            return float(os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        try:
            return int(os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    return {
        "lookback_days": _i("MEGA_TREND_KILL_RL_LOOKBACK_DAYS", 90),
        "eval_days": _i("MEGA_TREND_KILL_RL_EVAL_DAYS", 5),
        "min_eval_age_days": _i("MEGA_TREND_KILL_RL_MIN_AGE_DAYS", 6),
        "opportunity_ret_pct": _f("MEGA_TREND_KILL_RL_OPPORTUNITY_PCT", 2.5),
        "defense_ret_pct": _f("MEGA_TREND_KILL_RL_DEFENSE_PCT", -2.5),
        "rl_eta": _f("MEGA_TREND_KILL_RL_ETA", 0.03),
        "max_events": _i("MEGA_TREND_KILL_RL_MAX_EVENTS", 120),
        "min_events_to_update": _i("MEGA_TREND_KILL_RL_MIN_EVENTS", 2),
        "min_sector_events": _i("MEGA_TREND_KILL_RL_MIN_SECTOR_EVENTS", 1),
        "sector_eta_scale": _f("MEGA_TREND_KILL_RL_SECTOR_ETA_SCALE", 0.65),
        "sector_guard_enabled": str(
            os.environ.get("MEGA_TREND_KILL_RL_SECTOR_GUARD", "1")
        ).strip().lower()
        in ("1", "true", "yes", "on"),
        "min_sector_purity": _f("MEGA_TREND_KILL_RL_MIN_SECTOR_PURITY", 0.55),
        "overlay_ignition_bind": str(
            os.environ.get("MEGA_TREND_KILL_RL_OVERLAY_IGNITION_BIND", "1")
        ).strip().lower()
        in ("1", "true", "yes", "on"),
        "quarantine_strike_min": _i("MEGA_TREND_KILL_RL_QUARANTINE_STRIKES", 1),
        "mega_trend_filter": str(
            os.environ.get("MEGA_TREND_KILL_RL_MEGATREND_FILTER", "1")
        ).strip().lower()
        in ("1", "true", "yes", "on"),
    }


def mega_trend_rl_filter_enabled() -> bool:
    return bool(kill_rl_config().get("mega_trend_filter"))


def sector_guard_enabled() -> bool:
    return bool(kill_rl_config().get("sector_guard_enabled"))


def normalize_kill_rl_sector(sector: object) -> str:
    """RL sector_deltas 키 — taxonomy 표준 섹터."""
    try:
        from sector_taxonomy import map_standard_sector

        return str(map_standard_sector(sector, market="KR"))
    except Exception:
        return str(sector or "").strip()


def _empty_sector_delta_block() -> Dict[str, Any]:
    return {k: 0.0 for k in ALL_DELTA_KEYS if k != "consecutive_loss_delta"} | {
        "consecutive_loss_delta": 0,
        "internal_n": 0,
        "climax_n": 0,
        "avg_purity": 1.0,
        "bound_ignited_at": None,
        "updated_at": None,
    }


def compute_kill_event_sector_purity(measure: Mapping[str, Any]) -> float:
    """
    P6 — 킬 사후평가 표본 순도 (0~1).

    MegaTrend 태그·언락 필터 통과 비율이 낮으면 타 섹터/베타 노이즈 유입 가능.
    """
    n_mt = int(measure.get("n_trades") or 0)
    n_all = int(measure.get("n_trades_sector_all") or 0)
    n_tagged = int(measure.get("n_trades_tagged") or 0)
    if n_all <= 0:
        return 1.0 if n_mt > 0 else 0.0
    mt_ratio = n_mt / max(1, n_all)
    tag_ratio = n_tagged / max(1, n_mt) if n_mt else 0.0
    return round(min(1.0, mt_ratio * (0.45 + 0.55 * tag_ratio)), 4)


def _normalize_affected_sectors(snapshot: Optional[Mapping[str, Any]]) -> Tuple[str, ...]:
    if not isinstance(snapshot, Mapping):
        return ()
    raw = snapshot.get("sectors") or snapshot.get("affected_sectors") or []
    if not isinstance(raw, (list, tuple)):
        return ()
    out: List[str] = []
    seen: Dict[str, None] = {}
    for item in raw:
        sec = normalize_kill_rl_sector(item)
        if sec and sec not in seen:
            seen[sec] = None
            out.append(sec)
    return tuple(out)


def sanitize_sector_deltas(
    overlays: Optional[Mapping[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """
    P6 — sector_deltas 키를 taxonomy 표준으로 정규화·병합.

    Returns (sanitized_map, audit_notes).
    """
    if not isinstance(overlays, Mapping) or not overlays:
        return {}, []
    merged: Dict[str, Dict[str, Any]] = {}
    notes: List[str] = []
    for raw_key, block in overlays.items():
        if not isinstance(block, Mapping):
            continue
        std_key = normalize_kill_rl_sector(raw_key)
        if not std_key:
            continue
        if str(raw_key) != std_key:
            notes.append(f"rekey:{raw_key}->{std_key}")
        slot = dict(merged.get(std_key) or _empty_sector_delta_block())
        for key in ALL_DELTA_KEYS:
            if key == "consecutive_loss_delta":
                slot[key] = int(slot.get(key) or 0) + int(block.get(key) or 0)
            else:
                slot[key] = round(
                    float(slot.get(key) or 0.0) + float(block.get(key) or 0.0), 4
                )
        for meta_key in ("internal_n", "climax_n"):
            slot[meta_key] = int(slot.get(meta_key) or 0) + int(block.get(meta_key) or 0)
        purity_vals = [
            float(x)
            for x in (slot.get("avg_purity"), block.get("avg_purity"))
            if x is not None
        ]
        if purity_vals:
            slot["avg_purity"] = round(sum(purity_vals) / len(purity_vals), 4)
        bound = str(block.get("bound_ignited_at") or slot.get("bound_ignited_at") or "")[:10]
        if bound:
            slot["bound_ignited_at"] = bound
        updated = str(block.get("updated_at") or slot.get("updated_at") or "")
        if updated:
            slot["updated_at"] = updated
        merged[std_key] = slot
    return merged, notes


def assess_sector_overlay_eligibility(
    block: Optional[Mapping[str, Any]],
    sector_std: str,
    *,
    rl_state: Optional[Mapping[str, Any]] = None,
    ignited_at: Optional[str] = None,
    active_sectors: Optional[Sequence[str]] = None,
) -> Tuple[bool, str]:
    """P6 — sector overlay 적용 가능 여부 (cross-sector 누수 차단)."""
    if not sector_guard_enabled():
        return True, "guard_disabled"
    if not sector_std:
        return False, "missing_sector"
    if not isinstance(block, Mapping) or not block:
        return False, "empty_overlay"

    cfg = kill_rl_config()
    min_n = int(cfg["min_sector_events"])
    sample_n = int(block.get("internal_n") or 0) + int(block.get("climax_n") or 0)
    if sample_n < min_n:
        return False, f"insufficient_n:{sample_n}"

    quarantine = (rl_state or {}).get(MEGA_TREND_SECTOR_QUARANTINE_KEY) or {}
    if isinstance(quarantine, Mapping) and sector_std in quarantine:
        return False, "quarantined"

    if cfg.get("overlay_ignition_bind") and ignited_at:
        bound = str(block.get("bound_ignited_at") or "")[:10]
        cur = str(ignited_at)[:10]
        if bound and cur and bound != cur:
            return False, f"ignition_mismatch:{bound}!={cur}"

    if active_sectors:
        active_std = {normalize_kill_rl_sector(s) for s in active_sectors if s}
        if active_std and sector_std not in active_std:
            return False, "inactive_sector"

    purity = float(block.get("avg_purity") or 1.0)
    if purity < float(cfg["min_sector_purity"]):
        return False, f"low_purity:{purity:.2f}"

    return True, "ok"


def _append_contamination_audit(
    state: Dict[str, Any],
    *,
    sector: str,
    action: str,
    detail: str,
) -> None:
    audit = list(state.get(MEGA_TREND_CONTAMINATION_AUDIT_KEY) or [])
    audit.append(
        {
            "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sector": sector,
            "action": action,
            "detail": detail,
        }
    )
    state[MEGA_TREND_CONTAMINATION_AUDIT_KEY] = audit[-40:]


def _maybe_quarantine_sector(
    state: Dict[str, Any],
    sector_std: str,
    *,
    reason: str,
) -> bool:
    """P6 — 고오염 섹터 overlay 격리."""
    if not sector_guard_enabled() or not sector_std:
        return False
    cfg = kill_rl_config()
    strikes_need = int(cfg.get("quarantine_strike_min") or 1)
    q = dict(state.get(MEGA_TREND_SECTOR_QUARANTINE_KEY) or {})
    entry = dict(q.get(sector_std) or {})
    entry["count"] = int(entry.get("count") or 0) + 1
    reasons = list(entry.get("reasons") or [])
    reasons.append(reason)
    entry["reasons"] = reasons[-5:]
    entry["last_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    quarantined = int(entry["count"]) >= strikes_need
    if quarantined:
        q[sector_std] = entry
        state[MEGA_TREND_SECTOR_QUARANTINE_KEY] = q
        _append_contamination_audit(
            state,
            sector=sector_std,
            action="quarantine",
            detail=reason,
        )
    return quarantined


def _kill_rl_apply_context(
    rl_state: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """P6 — RL 적용 시 ignition·active sectors 컨텍스트."""
    st = dict(rl_state) if isinstance(rl_state, Mapping) else load_kill_rl_state()
    ignited_at: Optional[str] = None
    active_sectors: Tuple[str, ...] = ()
    try:
        from mega_trend_ignition import load_mega_trend_state

        mt = load_mega_trend_state()
        ignited_at = str(mt.get("ignited_at") or "")[:10] or None
        sectors_raw = list(mt.get("sectors") or [])
        primary = mt.get("primary_sector")
        if primary:
            sectors_raw.insert(0, str(primary))
        active_sectors = tuple(
            normalize_kill_rl_sector(s) for s in sectors_raw if normalize_kill_rl_sector(s)
        )
    except Exception:
        pass
    return {
        "rl_state": st,
        "ignited_at": ignited_at,
        "active_sectors": active_sectors,
    }


def _sector_events_avg_purity(
    events: Sequence[Mapping[str, Any]],
    sector_std: str,
) -> float:
    purities: List[float] = []
    for ev in events or []:
        if normalize_kill_rl_sector(ev.get("sector_std") or ev.get("sector")) != sector_std:
            continue
        if ev.get("outcome") == _OUTCOME_PENDING:
            continue
        if ev.get("contamination_flag") == CONTAMINATION_FLAG_HIGH:
            continue
        try:
            purities.append(float(ev.get("sector_purity") or 1.0))
        except (TypeError, ValueError):
            purities.append(1.0)
    if not purities:
        return 1.0
    return round(sum(purities) / len(purities), 4)


def resolve_effective_kill_rl_state(
    rl_state: Optional[Mapping[str, Any]] = None,
    *,
    sector: Optional[str] = None,
    ignited_at: Optional[str] = None,
    active_sectors: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    P5 — global prior + sector overlay 가산 병합.
    P6 — overlay 적용 전 contamination guard (quarantine·ignition·purity).
    """
    st = dict(rl_state or {})
    out = dict(st)
    for key in ALL_DELTA_KEYS:
        base = float(st.get(key) or 0.0) if key != "consecutive_loss_delta" else int(
            st.get(key) or 0
        )
        out[key] = base

    sec_key: Optional[str] = None
    overlay_applied = False
    overlay_block_reason = "no_sector"
    if sector:
        sec_key = normalize_kill_rl_sector(sector)
        overlays = st.get(MEGA_TREND_SECTOR_DELTAS_KEY) or {}
        if isinstance(overlays, Mapping):
            block = overlays.get(sec_key) or {}
            eligible, reason = assess_sector_overlay_eligibility(
                block,
                sec_key,
                rl_state=st,
                ignited_at=ignited_at,
                active_sectors=active_sectors,
            )
            overlay_block_reason = reason
            if isinstance(block, Mapping) and block and eligible:
                overlay_applied = True
                for key in ALL_DELTA_KEYS:
                    if key == "consecutive_loss_delta":
                        out[key] = int(out.get(key) or 0) + int(block.get(key) or 0)
                    else:
                        out[key] = float(out.get(key) or 0.0) + float(block.get(key) or 0.0)

    out["_kill_rl_sector"] = sec_key
    out["_kill_rl_overlay"] = overlay_applied
    out["_kill_rl_overlay_block_reason"] = overlay_block_reason
    out["_kill_rl_contamination_guard"] = sector_guard_enabled()
    out["_kill_rl_effective"] = True
    return out


def load_kill_rl_state(cfg: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    if cfg is None:
        try:
            from config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = None
    base = dict(DEFAULT_KILL_RL_STATE)
    if isinstance(cfg, Mapping):
        st = cfg.get(MEGA_TREND_KILL_RL_STATE_KEY)
        if isinstance(st, dict):
            for k in DEFAULT_KILL_RL_STATE:
                if k in st:
                    base[k] = st[k]
            events = st.get(MEGA_TREND_KILL_EVENTS_KEY)
            if isinstance(events, list):
                base[MEGA_TREND_KILL_EVENTS_KEY] = list(events)
            sector_deltas = st.get(MEGA_TREND_SECTOR_DELTAS_KEY)
            if isinstance(sector_deltas, dict):
                sanitized, _notes = sanitize_sector_deltas(sector_deltas)
                base[MEGA_TREND_SECTOR_DELTAS_KEY] = sanitized
            quarantine = st.get(MEGA_TREND_SECTOR_QUARANTINE_KEY)
            if isinstance(quarantine, dict):
                base[MEGA_TREND_SECTOR_QUARANTINE_KEY] = dict(quarantine)
            audit = st.get(MEGA_TREND_CONTAMINATION_AUDIT_KEY)
            if isinstance(audit, list):
                base[MEGA_TREND_CONTAMINATION_AUDIT_KEY] = list(audit)
    if MEGA_TREND_KILL_EVENTS_KEY not in base:
        base[MEGA_TREND_KILL_EVENTS_KEY] = []
    if MEGA_TREND_SECTOR_DELTAS_KEY not in base:
        base[MEGA_TREND_SECTOR_DELTAS_KEY] = {}
    if MEGA_TREND_SECTOR_QUARANTINE_KEY not in base:
        base[MEGA_TREND_SECTOR_QUARANTINE_KEY] = {}
    if MEGA_TREND_CONTAMINATION_AUDIT_KEY not in base:
        base[MEGA_TREND_CONTAMINATION_AUDIT_KEY] = []
    return base


def _clamp_delta_win(v: float) -> float:
    return max(-0.12, min(0.12, float(v)))


def _clamp_delta_bounce(v: float) -> float:
    return max(-0.10, min(0.10, float(v)))


def _clamp_delta_sector_win(v: float) -> float:
    return max(-0.08, min(0.08, float(v)))


def _clamp_delta_sector_bounce(v: float) -> float:
    return max(-0.06, min(0.06, float(v)))


def apply_kill_rl_threshold_adjustments(
    base: Mapping[str, Any],
    *,
    rl_state: Optional[Mapping[str, Any]] = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    """exit_dynamics / toxic_kill 기본 임계치에 RL delta 적용."""
    ctx = _kill_rl_apply_context(rl_state)
    st = resolve_effective_kill_rl_state(
        ctx["rl_state"],
        sector=sector,
        ignited_at=ctx.get("ignited_at"),
        active_sectors=ctx.get("active_sectors"),
    )
    out = dict(base)

    wr_base = float(out.get("win_rate_min", 0.40))
    wr_delta = float(st.get("win_rate_min_delta") or 0.0)
    out["win_rate_min"] = round(max(0.28, min(0.52, wr_base + wr_delta)), 4)

    mfe_base = float(out.get("mfe_reach_min", 0.35))
    mfe_delta = float(st.get("mfe_reach_min_delta") or 0.0)
    out["mfe_reach_min"] = round(max(0.20, min(0.50, mfe_base + mfe_delta)), 4)

    bounce_base = float(out.get("bounce_stop_max_rate", 0.45))
    bounce_delta = float(st.get("bounce_stop_max_delta") or 0.0)
    out["bounce_stop_max_rate"] = round(
        max(0.30, min(0.60, bounce_base + bounce_delta)), 4
    )

    accel_base = float(out.get("pnl_accel_drop_min", 0.15))
    accel_delta = float(st.get("pnl_accel_drop_delta") or 0.0)
    out["pnl_accel_drop_min"] = round(
        max(0.08, min(0.25, accel_base + accel_delta)), 4
    )

    out["_kill_rl_applied"] = True
    out["_kill_rl_deltas"] = {
        "win_rate_min_delta": wr_delta,
        "mfe_reach_min_delta": mfe_delta,
        "bounce_stop_max_delta": bounce_delta,
        "pnl_accel_drop_delta": accel_delta,
    }
    if sector:
        out["_kill_rl_sector"] = st.get("_kill_rl_sector")
        out["_kill_rl_overlay"] = st.get("_kill_rl_overlay")
        out["_kill_rl_overlay_block_reason"] = st.get("_kill_rl_overlay_block_reason")
    return out


def apply_kill_rl_toxic_adjustments(
    base: Mapping[str, Any],
    *,
    rl_state: Optional[Mapping[str, Any]] = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    """toxic_kill_config 기본값에 RL delta 적용."""
    ctx = _kill_rl_apply_context(rl_state)
    st = resolve_effective_kill_rl_state(
        ctx["rl_state"],
        sector=sector,
        ignited_at=ctx.get("ignited_at"),
        active_sectors=ctx.get("active_sectors"),
    )
    out = dict(base)

    loss_base = int(out.get("consecutive_loss_min", 3))
    loss_delta = int(st.get("consecutive_loss_delta") or 0)
    out["consecutive_loss_min"] = max(2, min(5, loss_base + loss_delta))

    scale_base = float(out.get("defensive_scale_out_min", 0.82))
    scale_delta = float(st.get("defensive_scale_delta") or 0.0)
    out["defensive_scale_out_min"] = round(
        max(0.70, min(0.95, scale_base + scale_delta)), 4
    )

    out["_kill_rl_applied"] = True
    if sector:
        out["_kill_rl_sector"] = st.get("_kill_rl_sector")
        out["_kill_rl_overlay"] = st.get("_kill_rl_overlay")
        out["_kill_rl_overlay_block_reason"] = st.get("_kill_rl_overlay_block_reason")
    return out


def _clamp_delta_flow_z(v: float) -> float:
    return max(-0.50, min(0.50, float(v)))


def _clamp_delta_vol_shrink(v: float) -> float:
    return max(-0.12, min(0.12, float(v)))


def _clamp_delta_scale_out(v: float) -> float:
    return max(-0.15, min(0.15, float(v)))


def apply_kill_rl_climax_adjustments(
    base: Mapping[str, Any],
    *,
    rl_state: Optional[Mapping[str, Any]] = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    """climax_config 기본값에 external-lane RL delta 적용."""
    ctx = _kill_rl_apply_context(rl_state)
    st = resolve_effective_kill_rl_state(
        ctx["rl_state"],
        sector=sector,
        ignited_at=ctx.get("ignited_at"),
        active_sectors=ctx.get("active_sectors"),
    )
    out = dict(base)

    rev_base = float(out.get("flow_reversal_z", 0.0))
    rev_delta = float(st.get("flow_reversal_z_delta") or 0.0)
    out["flow_reversal_z"] = round(max(-1.0, min(1.0, rev_base + rev_delta)), 4)

    drop_base = float(out.get("flow_z_drop_min", 1.5))
    drop_delta = float(st.get("flow_z_drop_min_delta") or 0.0)
    out["flow_z_drop_min"] = round(max(0.5, min(3.0, drop_base - drop_delta)), 4)

    shrink_base = float(out.get("climax_vol_shrink", 0.85))
    shrink_delta = float(st.get("climax_vol_shrink_delta") or 0.0)
    out["climax_vol_shrink"] = round(
        max(0.60, min(0.95, shrink_base + shrink_delta)), 4
    )

    scale_base = float(out.get("scale_out_fraction", 0.75))
    scale_delta = float(st.get("scale_out_fraction_delta") or 0.0)
    out["scale_out_fraction"] = round(
        max(0.50, min(0.95, scale_base + scale_delta)), 4
    )

    out["_kill_rl_applied"] = True
    out["_kill_rl_lane"] = KILL_LANE_EXTERNAL
    out["_kill_rl_deltas"] = {
        "flow_reversal_z_delta": rev_delta,
        "flow_z_drop_min_delta": drop_delta,
        "climax_vol_shrink_delta": shrink_delta,
        "scale_out_fraction_delta": scale_delta,
    }
    if sector:
        out["_kill_rl_sector"] = st.get("_kill_rl_sector")
        out["_kill_rl_overlay"] = st.get("_kill_rl_overlay")
        out["_kill_rl_overlay_block_reason"] = st.get("_kill_rl_overlay_block_reason")
    return out


def record_mega_trend_kill_event(
    cfg: Dict[str, Any],
    *,
    sector: str,
    kill_type: str,
    reason: str = "",
    exit_mode: str = "defensive_exit",
    kill_at: Optional[str] = None,
    ignited_at: Optional[str] = None,
    snapshot: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    킬스위치 발동 시 이벤트 영속화 — 주말 RL 평가 입력.
    P6: sector_std SSOT · affected_sectors 감사 메타.
    """
    st = load_kill_rl_state(cfg)
    events: List[Dict[str, Any]] = list(st.get(MEGA_TREND_KILL_EVENTS_KEY) or [])
    now_s = kill_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    snap = dict(snapshot or {})
    ign = str(ignited_at or snap.get("ignited_at") or "")[:10] or None
    if ign:
        snap["ignited_at"] = ign
    sector_raw = str(sector or "")
    sector_std = normalize_kill_rl_sector(sector_raw)
    affected = _normalize_affected_sectors(snap)
    if sector_std and sector_std not in affected:
        affected = (sector_std,) + tuple(s for s in affected if s != sector_std)
    event = {
        "id": f"{sector_std or sector_raw}_{now_s[:10]}_{kill_type}_{len(events)}",
        "sector": sector_std or sector_raw,
        "sector_std": sector_std or sector_raw,
        "sector_raw": sector_raw,
        "affected_sectors": list(affected),
        "kill_type": str(kill_type),
        "kill_lane": classify_kill_lane(kill_type),
        "reason": str(reason or "")[:500],
        "exit_mode": str(exit_mode),
        "kill_at": now_s,
        "ignited_at": ign,
        "recorded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "outcome": _OUTCOME_PENDING,
        "snapshot": snap,
    }
    events.append(event)
    cfg_limit = kill_rl_config()["max_events"]
    st[MEGA_TREND_KILL_EVENTS_KEY] = events[-int(cfg_limit) :]
    st["events_pending"] = sum(
        1 for e in st[MEGA_TREND_KILL_EVENTS_KEY] if e.get("outcome") == _OUTCOME_PENDING
    )
    cfg[MEGA_TREND_KILL_RL_STATE_KEY] = st
    return event


def _normalize_sector(conn: sqlite3.Connection, code: object, sector_raw: object) -> str:
    try:
        from mega_trend_ignition import resolve_kr_code_sector

        return resolve_kr_code_sector(code, sector_raw)
    except Exception:
        return str(sector_raw or "")


def _extract_trade_return(
    status: object,
    final_ret: object,
    sim_ret: object,
) -> Optional[float]:
    try:
        if str(status or "").upper().startswith("CLOSED") and final_ret is not None:
            return float(final_ret)
        if sim_ret is not None:
            return float(sim_ret)
    except (TypeError, ValueError):
        return None
    return None


def _resolve_event_ignited_at(event: Mapping[str, Any]) -> Optional[str]:
    ign = str(event.get("ignited_at") or "")[:10]
    if ign:
        return ign
    snap = event.get("snapshot")
    if isinstance(snap, Mapping):
        ign2 = str(snap.get("ignited_at") or "")[:10]
        if ign2:
            return ign2
    return None


def measure_post_kill_sector_outcome(
    conn: sqlite3.Connection,
    sector: str,
    kill_at: str,
    *,
    eval_days: Optional[int] = None,
    ignited_at: Optional[str] = None,
    mega_trend_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    킬 이후 eval_days 구간 체결 수익 평균 — 장부 기반.

    P2: mega_trend_only 시 MegaTrend 태그 체결만 집계(섹터 베타 노이즈 제거).
    """
    from mega_trend_trade_filter import (
        is_mega_trend_sig_type,
        is_mega_trend_unlock_trade,
    )

    cfg = kill_rl_config()
    n_days = int(eval_days or cfg["eval_days"])
    use_mt_filter = (
        mega_trend_only
        if mega_trend_only is not None
        else mega_trend_rl_filter_enabled()
    )
    kill_d = str(kill_at or "")[:10]
    if not kill_d:
        return {"avg_ret_pct": None, "n_trades": 0, "reason": "invalid_kill_at"}

    try:
        end_dt = datetime.strptime(kill_d, "%Y-%m-%d") + timedelta(days=n_days)
        end_d = end_dt.strftime("%Y-%m-%d")
    except ValueError:
        return {"avg_ret_pct": None, "n_trades": 0, "reason": "invalid_kill_date"}

    target_std = normalize_kill_rl_sector(sector)
    if not target_std:
        return {"avg_ret_pct": None, "n_trades": 0, "reason": "invalid_sector"}
    since = str(ignited_at or "")[:10] or None
    rets_mt: List[float] = []
    rets_all: List[float] = []
    n_tagged = 0
    n_unlock_window = 0

    try:
        rows = conn.execute(
            """
            SELECT code, sector, sig_type, sim_stat_ret, final_ret, status,
                   entry_date, exit_date
            FROM forward_trades
            WHERE market='KR'
              AND substr(COALESCE(exit_date, entry_date), 1, 10) > ?
              AND substr(COALESCE(exit_date, entry_date), 1, 10) <= ?
            """,
            (kill_d, end_d),
        ).fetchall()
    except Exception as ex:
        return {"avg_ret_pct": None, "n_trades": 0, "reason": f"db_error:{ex}"}

    for row in rows or []:
        code, sec_raw, sig_type, sim_ret, final_ret, status, entry_d, exit_d = row
        sec = normalize_kill_rl_sector(_normalize_sector(conn, code, sec_raw))
        if sec != target_std:
            continue
        ret = _extract_trade_return(status, final_ret, sim_ret)
        if ret is None:
            continue
        rets_all.append(ret)

        if not use_mt_filter:
            rets_mt.append(ret)
            continue

        ed = str(entry_d or "")[:10]
        tagged = is_mega_trend_sig_type(sig_type)
        in_unlock = is_mega_trend_unlock_trade(
            sig_type=sig_type, entry_date=ed, ignited_at=since
        )
        # P2 엄격: MegaTrend 태그 우선. 태그 없으면 ignited_at 이후 언락 구간만 보조 포함.
        if tagged:
            n_tagged += 1
            rets_mt.append(ret)
        elif since and in_unlock and ed > kill_d:
            n_unlock_window += 1
            rets_mt.append(ret)

    base_meta = {
        "eval_window": f"{kill_d}~{end_d}",
        "sector_std": target_std,
        "ignited_at": since,
        "mega_trend_filter": use_mt_filter,
        "n_trades_sector_all": len(rets_all),
        "n_trades_tagged": n_tagged,
        "n_trades_unlock_window": n_unlock_window,
    }

    if not rets_mt:
        return {
            "avg_ret_pct": None,
            "n_trades": 0,
            **base_meta,
            "reason": (
                "no_megatrend_trades_in_window"
                if use_mt_filter
                else "no_sector_trades_in_window"
            ),
        }

    avg = sum(rets_mt) / len(rets_mt)
    return {
        "avg_ret_pct": round(avg, 4),
        "n_trades": len(rets_mt),
        **base_meta,
        "reason": "computed_megatrend" if use_mt_filter else "computed",
    }


def classify_kill_outcome(
    avg_ret_pct: Optional[float],
    *,
    opportunity_pct: Optional[float] = None,
    defense_pct: Optional[float] = None,
) -> str:
    """기회비용(오경보) vs 방어성공(정타) vs 중립."""
    cfg = kill_rl_config()
    opp_thr = float(opportunity_pct if opportunity_pct is not None else cfg["opportunity_ret_pct"])
    def_thr = float(defense_pct if defense_pct is not None else cfg["defense_ret_pct"])

    if avg_ret_pct is None:
        return _OUTCOME_NEUTRAL
    if float(avg_ret_pct) >= opp_thr:
        return _OUTCOME_OPPORTUNITY
    if float(avg_ret_pct) <= def_thr:
        return _OUTCOME_DEFENSE
    return _OUTCOME_NEUTRAL


def _step_internal_lane_deltas(
    block: Mapping[str, Any],
    *,
    opportunity_cost_rate: float,
    defense_success_rate: float,
    eta: float,
    clamp_win: Callable[[float], float] = _clamp_delta_win,
    clamp_bounce: Callable[[float], float] = _clamp_delta_bounce,
) -> Dict[str, Any]:
    """internal lane 1 RL step — block in/out."""
    st = dict(block)
    opp = max(0.0, min(1.0, float(opportunity_cost_rate)))
    defense = max(0.0, min(1.0, float(defense_success_rate)))
    delta = float(eta) * (defense - opp)

    st["win_rate_min_delta"] = round(
        clamp_win(float(st.get("win_rate_min_delta") or 0.0) - delta), 4
    )
    st["mfe_reach_min_delta"] = round(
        clamp_win(float(st.get("mfe_reach_min_delta") or 0.0) - delta * 0.6), 4
    )
    st["bounce_stop_max_delta"] = round(
        clamp_bounce(float(st.get("bounce_stop_max_delta") or 0.0) - delta * 0.5), 4
    )
    st["pnl_accel_drop_delta"] = round(
        clamp_win(float(st.get("pnl_accel_drop_delta") or 0.0) - delta * 0.4), 4
    )

    loss_delta = int(st.get("consecutive_loss_delta") or 0)
    if delta > 0.005:
        loss_delta = max(-2, loss_delta - 1)
    elif delta < -0.005:
        loss_delta = min(2, loss_delta + 1)
    st["consecutive_loss_delta"] = loss_delta

    scale_delta = float(st.get("defensive_scale_delta") or 0.0)
    st["defensive_scale_delta"] = round(
        max(-0.08, min(0.08, scale_delta + delta * 0.3)), 4
    )
    return st


def _step_climax_lane_deltas(
    block: Mapping[str, Any],
    *,
    opportunity_cost_rate: float,
    defense_success_rate: float,
    eta: float,
    clamp_flow: Callable[[float], float] = _clamp_delta_flow_z,
    clamp_shrink: Callable[[float], float] = _clamp_delta_vol_shrink,
    clamp_scale: Callable[[float], float] = _clamp_delta_scale_out,
) -> Dict[str, Any]:
    """climax lane 1 RL step — block in/out."""
    st = dict(block)
    opp = max(0.0, min(1.0, float(opportunity_cost_rate)))
    defense = max(0.0, min(1.0, float(defense_success_rate)))
    delta = float(eta) * (defense - opp)

    st["flow_reversal_z_delta"] = round(
        clamp_flow(float(st.get("flow_reversal_z_delta") or 0.0) + delta * 0.4), 4
    )
    st["flow_z_drop_min_delta"] = round(
        clamp_flow(float(st.get("flow_z_drop_min_delta") or 0.0) + delta * 0.5), 4
    )
    st["climax_vol_shrink_delta"] = round(
        clamp_shrink(float(st.get("climax_vol_shrink_delta") or 0.0) + delta * 0.03), 4
    )
    st["scale_out_fraction_delta"] = round(
        clamp_scale(float(st.get("scale_out_fraction_delta") or 0.0) + delta * 0.05), 4
    )
    return st


def update_internal_kill_sensitivity_rl(
    state: Dict[str, Any],
    *,
    opportunity_cost_rate: float,
    defense_success_rate: float,
    eta: Optional[float] = None,
) -> Dict[str, Any]:
    """
    [internal lane · global] 방어성공 > 기회비용 → 더 예민 · 반대 → 더 둔감.
    """
    cfg = kill_rl_config()
    st = dict(DEFAULT_KILL_RL_STATE)
    if isinstance(state, dict):
        for k in DEFAULT_KILL_RL_STATE:
            if k in state:
                st[k] = state[k]
        if MEGA_TREND_KILL_EVENTS_KEY in state:
            st[MEGA_TREND_KILL_EVENTS_KEY] = state[MEGA_TREND_KILL_EVENTS_KEY]
        if MEGA_TREND_SECTOR_DELTAS_KEY in state:
            st[MEGA_TREND_SECTOR_DELTAS_KEY] = state[MEGA_TREND_SECTOR_DELTAS_KEY]

    learning_rate = float(eta if eta is not None else cfg["rl_eta"])
    stepped = _step_internal_lane_deltas(
        {k: st.get(k) for k in INTERNAL_DELTA_KEYS},
        opportunity_cost_rate=opportunity_cost_rate,
        defense_success_rate=defense_success_rate,
        eta=learning_rate,
    )
    for k, v in stepped.items():
        st[k] = v

    opp = max(0.0, min(1.0, float(opportunity_cost_rate)))
    defense = max(0.0, min(1.0, float(defense_success_rate)))
    st["internal_opportunity_cost_rate"] = round(opp, 4)
    st["internal_defense_success_rate"] = round(defense, 4)
    st["opportunity_cost_rate"] = round(opp, 4)
    st["defense_success_rate"] = round(defense, 4)
    st["neutral_rate"] = round(max(0.0, 1.0 - opp - defense), 4)
    st["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return st


def update_climax_kill_sensitivity_rl(
    state: Dict[str, Any],
    *,
    opportunity_cost_rate: float,
    defense_success_rate: float,
    eta: Optional[float] = None,
) -> Dict[str, Any]:
    """[external lane · global] Climax 킬 사후평가 → flow/trap/scale 민감도 진화."""
    cfg = kill_rl_config()
    st = dict(DEFAULT_KILL_RL_STATE)
    if isinstance(state, dict):
        for k in DEFAULT_KILL_RL_STATE:
            if k in state:
                st[k] = state[k]
        if MEGA_TREND_KILL_EVENTS_KEY in state:
            st[MEGA_TREND_KILL_EVENTS_KEY] = state[MEGA_TREND_KILL_EVENTS_KEY]
        if MEGA_TREND_SECTOR_DELTAS_KEY in state:
            st[MEGA_TREND_SECTOR_DELTAS_KEY] = state[MEGA_TREND_SECTOR_DELTAS_KEY]

    learning_rate = float(eta if eta is not None else cfg["rl_eta"])
    stepped = _step_climax_lane_deltas(
        {k: st.get(k) for k in CLIMAX_DELTA_KEYS},
        opportunity_cost_rate=opportunity_cost_rate,
        defense_success_rate=defense_success_rate,
        eta=learning_rate,
    )
    for k, v in stepped.items():
        st[k] = v

    opp = max(0.0, min(1.0, float(opportunity_cost_rate)))
    defense = max(0.0, min(1.0, float(defense_success_rate)))
    st["climax_opportunity_cost_rate"] = round(opp, 4)
    st["climax_defense_success_rate"] = round(defense, 4)
    st["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return st


def _update_sector_overlay_block(
    state: Dict[str, Any],
    sector: str,
    *,
    lane: str,
    opportunity_cost_rate: float,
    defense_success_rate: float,
    eta: float,
    sample_n: int,
    bound_ignited_at: Optional[str] = None,
    events: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """P5/P6 — sector_deltas overlay 1 step + ignition bind·purity 메타."""
    sec_key = normalize_kill_rl_sector(sector)
    overlays = dict(state.get(MEGA_TREND_SECTOR_DELTAS_KEY) or {})
    block = dict(overlays.get(sec_key) or _empty_sector_delta_block())

    if lane == KILL_LANE_INTERNAL:
        stepped = _step_internal_lane_deltas(
            {k: block.get(k, 0.0) for k in INTERNAL_DELTA_KEYS},
            opportunity_cost_rate=opportunity_cost_rate,
            defense_success_rate=defense_success_rate,
            eta=eta,
            clamp_win=_clamp_delta_sector_win,
            clamp_bounce=_clamp_delta_sector_bounce,
        )
        for k, v in stepped.items():
            block[k] = v
        block["internal_n"] = int(sample_n)
        block["internal_opportunity_cost_rate"] = round(float(opportunity_cost_rate), 4)
        block["internal_defense_success_rate"] = round(float(defense_success_rate), 4)
    elif lane == KILL_LANE_EXTERNAL:
        stepped = _step_climax_lane_deltas(
            {k: block.get(k, 0.0) for k in CLIMAX_DELTA_KEYS},
            opportunity_cost_rate=opportunity_cost_rate,
            defense_success_rate=defense_success_rate,
            eta=eta,
            clamp_flow=_clamp_delta_sector_win,
            clamp_shrink=_clamp_delta_sector_bounce,
            clamp_scale=_clamp_delta_sector_bounce,
        )
        for k, v in stepped.items():
            block[k] = v
        block["climax_n"] = int(sample_n)
        block["climax_opportunity_cost_rate"] = round(float(opportunity_cost_rate), 4)
        block["climax_defense_success_rate"] = round(float(defense_success_rate), 4)

    if bound_ignited_at:
        block["bound_ignited_at"] = str(bound_ignited_at)[:10]
    if events is not None:
        block["avg_purity"] = _sector_events_avg_purity(events, sec_key)

    block["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    overlays[sec_key] = block
    state[MEGA_TREND_SECTOR_DELTAS_KEY] = overlays
    return state


def _collect_sectors_from_events(events: Sequence[Mapping[str, Any]]) -> List[str]:
    seen: Dict[str, None] = {}
    for ev in events or []:
        sec = normalize_kill_rl_sector(ev.get("sector_std") or ev.get("sector"))
        if sec:
            seen[sec] = None
    return sorted(seen.keys())


def update_kill_sensitivity_rl(
    state: Dict[str, Any],
    *,
    opportunity_cost_rate: float,
    defense_success_rate: float,
    eta: Optional[float] = None,
) -> Dict[str, Any]:
    """레거시 별칭 — internal lane RL 업데이트."""
    return update_internal_kill_sensitivity_rl(
        state,
        opportunity_cost_rate=opportunity_cost_rate,
        defense_success_rate=defense_success_rate,
        eta=eta,
    )


def evaluate_pending_kill_events(
    events: Sequence[Mapping[str, Any]],
    conn: sqlite3.Connection,
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """평가 가능한 pending 이벤트에 사후 outcome·순도 메타 부여."""
    cfg = kill_rl_config()
    now = now or datetime.now()
    min_age = int(cfg["min_eval_age_days"])
    min_purity = float(cfg["min_sector_purity"])
    updated: List[Dict[str, Any]] = []

    for raw in events or []:
        ev = dict(raw)
        if ev.get("outcome") != _OUTCOME_PENDING:
            updated.append(ev)
            continue

        kill_at = str(ev.get("kill_at") or "")[:10]
        if not kill_at:
            ev["outcome"] = _OUTCOME_NEUTRAL
            ev["eval_reason"] = "missing_kill_at"
            updated.append(ev)
            continue

        try:
            kill_dt = datetime.strptime(kill_at, "%Y-%m-%d")
        except ValueError:
            ev["outcome"] = _OUTCOME_NEUTRAL
            ev["eval_reason"] = "bad_kill_at"
            updated.append(ev)
            continue

        age_days = (now - kill_dt).days
        if age_days < min_age:
            updated.append(ev)
            continue

        sector_std = normalize_kill_rl_sector(ev.get("sector_std") or ev.get("sector"))
        ev["sector_std"] = sector_std
        ignited_at = _resolve_event_ignited_at(ev)
        measure = measure_post_kill_sector_outcome(
            conn,
            sector_std,
            kill_at,
            eval_days=cfg["eval_days"],
            ignited_at=ignited_at,
        )
        purity = compute_kill_event_sector_purity(measure)
        ev["sector_purity"] = purity
        if purity < min_purity and sector_guard_enabled():
            ev["contamination_flag"] = CONTAMINATION_FLAG_HIGH
        else:
            ev["contamination_flag"] = CONTAMINATION_FLAG_OK
        outcome = classify_kill_outcome(measure.get("avg_ret_pct"))
        ev["outcome"] = outcome
        ev["post_kill"] = measure
        ev["evaluated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        ev["eval_reason"] = measure.get("reason")
        updated.append(ev)

    return updated


def _apply_event_contamination_audit(
    state: Dict[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> List[str]:
    """P6 — 고오염 이벤트 → sector quarantine 갱신."""
    quarantined: List[str] = []
    for ev in events or []:
        if ev.get("contamination_flag") != CONTAMINATION_FLAG_HIGH:
            continue
        sec = normalize_kill_rl_sector(ev.get("sector_std") or ev.get("sector"))
        if not sec:
            continue
        if _maybe_quarantine_sector(
            state,
            sec,
            reason=f"low_purity:{float(ev.get('sector_purity') or 0):.2f}",
        ):
            quarantined.append(sec)
    return quarantined


def compute_kill_feedback_rates(
    events: Sequence[Mapping[str, Any]],
    *,
    lookback_days: Optional[int] = None,
    now: Optional[datetime] = None,
    kill_lane: Optional[str] = None,
    sector: Optional[str] = None,
    exclude_contaminated: bool = True,
) -> Dict[str, Any]:
    """최근 lookback 내 평가 완료 이벤트 → opportunity / defense 비율.

    P3: kill_lane 필터 · P5: sector 필터 · P6: contamination 제외.
    """
    cfg = kill_rl_config()
    now = now or datetime.now()
    lb = int(lookback_days or cfg["lookback_days"])
    cutoff = (now - timedelta(days=lb)).strftime("%Y-%m-%d")
    target_sector = normalize_kill_rl_sector(sector) if sector else None

    evaluated: List[Dict[str, Any]] = []
    for ev in events or []:
        if ev.get("outcome") == _OUTCOME_PENDING:
            continue
        kill_d = str(ev.get("kill_at") or "")[:10]
        if kill_d and kill_d < cutoff:
            continue
        if kill_lane is not None:
            lane = str(ev.get("kill_lane") or classify_kill_lane(ev.get("kill_type")))
            if lane != str(kill_lane):
                continue
        if target_sector is not None:
            if normalize_kill_rl_sector(ev.get("sector_std") or ev.get("sector")) != target_sector:
                continue
        if exclude_contaminated and sector_guard_enabled():
            if ev.get("contamination_flag") == CONTAMINATION_FLAG_HIGH:
                continue
        evaluated.append(dict(ev))

    n = len(evaluated)
    if n == 0:
        return {
            "n": 0,
            "kill_lane": kill_lane,
            "sector": target_sector,
            "opportunity_cost_rate": 0.0,
            "defense_success_rate": 0.0,
            "neutral_rate": 0.0,
            "events": [],
        }

    opp = sum(1 for e in evaluated if e.get("outcome") == _OUTCOME_OPPORTUNITY)
    defense = sum(1 for e in evaluated if e.get("outcome") == _OUTCOME_DEFENSE)
    neutral = n - opp - defense

    return {
        "n": n,
        "kill_lane": kill_lane,
        "sector": target_sector,
        "opportunity_cost_rate": round(opp / n, 4),
        "defense_success_rate": round(defense / n, 4),
        "neutral_rate": round(neutral / n, 4),
        "events": evaluated,
    }


def _strip_rates_for_audit(rates: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        k: rates[k]
        for k in rates
        if k not in ("events",)
    }


def _attach_evolve_audit_summary(
    state: Dict[str, Any],
    *,
    updated: bool,
    lanes_updated: Sequence[str],
    rates_internal: Mapping[str, Any],
    rates_climax: Mapping[str, Any],
    detail: Sequence[str],
    now: datetime,
    sectors_updated: Optional[Sequence[str]] = None,
    sectors_quarantined: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """P4/P5/P6 — 주말 RL 1사이클 감사 스냅샷 영속화(리포트·Telegram 소비)."""
    state["last_evolve_summary"] = {
        "updated": bool(updated),
        "lanes_updated": list(lanes_updated),
        "sectors_updated": list(sectors_updated or []),
        "sectors_quarantined": list(sectors_quarantined or []),
        "contamination_guard": sector_guard_enabled(),
        "evaluated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "rates_internal": _strip_rates_for_audit(rates_internal),
        "rates_climax": _strip_rates_for_audit(rates_climax),
        "detail": list(detail),
    }
    return state


def evolve_mega_trend_kill_sensitivity(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    [3번] 주말 RL 1사이클 — 킬스위치 민감도 자율 진화.
    """
    own_cfg = cfg is None
    if own_cfg:
        try:
            from config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}

    if db_path is None:
        try:
            import auto_forward_tester as aft

            db_path = aft.DB_PATH
        except Exception:
            try:
                from market_db_paths import market_db_read_path

                db_path = market_db_read_path()
            except Exception:
                db_path = None

    now = now or datetime.now()
    old_state = load_kill_rl_state(cfg)
    sanitized, sanitize_notes = sanitize_sector_deltas(
        old_state.get(MEGA_TREND_SECTOR_DELTAS_KEY)
    )
    old_state[MEGA_TREND_SECTOR_DELTAS_KEY] = sanitized
    if sanitize_notes:
        _append_contamination_audit(
            old_state,
            sector="*",
            action="sanitize",
            detail=";".join(sanitize_notes[:5]),
        )
    events = list(old_state.get(MEGA_TREND_KILL_EVENTS_KEY) or [])
    bound_ignited_at: Optional[str] = None
    try:
        from mega_trend_ignition import load_mega_trend_state

        bound_ignited_at = (
            str(load_mega_trend_state(cfg).get("ignited_at") or "")[:10] or None
        )
    except Exception:
        pass

    conn: Optional[sqlite3.Connection] = None
    own_conn = False
    if db_path:
        try:
            uri = str(db_path).replace("\\", "/")
            conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
            own_conn = True
            events = evaluate_pending_kill_events(events, conn, now=now)
        except Exception as ex:
            return {
                "updated": False,
                "reason": f"db_unavailable:{ex}",
                "state": old_state,
            }
        finally:
            if own_conn and conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    rates_all = compute_kill_feedback_rates(events, now=now)
    internal_rates = compute_kill_feedback_rates(
        events, now=now, kill_lane=KILL_LANE_INTERNAL
    )
    climax_rates = compute_kill_feedback_rates(
        events, now=now, kill_lane=KILL_LANE_EXTERNAL
    )
    cfg_rl = kill_rl_config()
    min_ev = int(cfg_rl["min_events_to_update"])
    min_sector_ev = int(cfg_rl["min_sector_events"])
    sector_eta = float(cfg_rl["rl_eta"]) * float(cfg_rl["sector_eta_scale"])

    old_state[MEGA_TREND_KILL_EVENTS_KEY] = events
    old_state["events_pending"] = sum(
        1 for e in events if e.get("outcome") == _OUTCOME_PENDING
    )
    old_state["events_evaluated"] = sum(
        1 for e in events if e.get("outcome") != _OUTCOME_PENDING
    )
    quarantined_now = _apply_event_contamination_audit(old_state, events)

    new_state = dict(old_state)
    updated_internal = False
    updated_climax = False
    sectors_updated: List[str] = []
    skip_reasons: List[str] = []
    if quarantined_now:
        skip_reasons.append(f"quarantined={','.join(quarantined_now)}")

    quarantine_map = dict(old_state.get(MEGA_TREND_SECTOR_QUARANTINE_KEY) or {})

    if int(internal_rates.get("n") or 0) >= min_ev:
        new_state = update_internal_kill_sensitivity_rl(
            new_state,
            opportunity_cost_rate=float(internal_rates["opportunity_cost_rate"]),
            defense_success_rate=float(internal_rates["defense_success_rate"]),
        )
        updated_internal = True
    else:
        skip_reasons.append(f"internal_n={internal_rates.get('n', 0)}")

    if int(climax_rates.get("n") or 0) >= min_ev:
        new_state = update_climax_kill_sensitivity_rl(
            new_state,
            opportunity_cost_rate=float(climax_rates["opportunity_cost_rate"]),
            defense_success_rate=float(climax_rates["defense_success_rate"]),
        )
        updated_climax = True
    else:
        skip_reasons.append(f"climax_n={climax_rates.get('n', 0)}")

    for sec in _collect_sectors_from_events(events):
        if sector_guard_enabled() and sec in quarantine_map:
            skip_reasons.append(f"sector_quarantined:{sec}")
            continue
        sec_int = compute_kill_feedback_rates(
            events, now=now, kill_lane=KILL_LANE_INTERNAL, sector=sec
        )
        if int(sec_int.get("n") or 0) >= min_sector_ev:
            new_state = _update_sector_overlay_block(
                new_state,
                sec,
                lane=KILL_LANE_INTERNAL,
                opportunity_cost_rate=float(sec_int["opportunity_cost_rate"]),
                defense_success_rate=float(sec_int["defense_success_rate"]),
                eta=sector_eta,
                sample_n=int(sec_int["n"]),
                bound_ignited_at=bound_ignited_at,
                events=events,
            )
            if sec not in sectors_updated:
                sectors_updated.append(sec)
        sec_clx = compute_kill_feedback_rates(
            events, now=now, kill_lane=KILL_LANE_EXTERNAL, sector=sec
        )
        if int(sec_clx.get("n") or 0) >= min_sector_ev:
            new_state = _update_sector_overlay_block(
                new_state,
                sec,
                lane=KILL_LANE_EXTERNAL,
                opportunity_cost_rate=float(sec_clx["opportunity_cost_rate"]),
                defense_success_rate=float(sec_clx["defense_success_rate"]),
                eta=sector_eta,
                sample_n=int(sec_clx["n"]),
                bound_ignited_at=bound_ignited_at,
                events=events,
            )
            if sec not in sectors_updated:
                sectors_updated.append(sec)

    if sectors_updated:
        skip_reasons.append(f"sectors_updated={','.join(sectors_updated)}")

    if not updated_internal and not updated_climax and not sectors_updated:
        old_state = _attach_evolve_audit_summary(
            old_state,
            updated=False,
            lanes_updated=[],
            rates_internal=internal_rates,
            rates_climax=climax_rates,
            detail=skip_reasons,
            now=now,
            sectors_updated=[],
            sectors_quarantined=quarantined_now,
        )
        cfg[MEGA_TREND_KILL_RL_STATE_KEY] = old_state
        if persist and own_cfg:
            _persist_kill_rl_state(cfg, old_state)
        return {
            "updated": False,
            "reason": "insufficient_evaluated_events",
            "detail": skip_reasons,
            "rates": rates_all,
            "rates_internal": internal_rates,
            "rates_climax": climax_rates,
            "state": old_state,
        }

    new_state[MEGA_TREND_KILL_EVENTS_KEY] = events
    new_state["events_pending"] = old_state["events_pending"]
    new_state["events_evaluated"] = old_state["events_evaluated"]
    lanes_updated: List[str] = []
    if updated_internal:
        lanes_updated.append("internal")
    if updated_climax:
        lanes_updated.append("climax")
    new_state = _attach_evolve_audit_summary(
        new_state,
        updated=True,
        lanes_updated=lanes_updated,
        rates_internal=internal_rates,
        rates_climax=climax_rates,
        detail=skip_reasons,
        now=now,
        sectors_updated=sectors_updated,
        sectors_quarantined=quarantined_now,
    )
    cfg[MEGA_TREND_KILL_RL_STATE_KEY] = new_state

    if persist and own_cfg:
        _persist_kill_rl_state(cfg, new_state)

    return {
        "updated": True,
        "lanes_updated": lanes_updated,
        "sectors_updated": sectors_updated,
        "sectors_quarantined": quarantined_now,
        "rates": rates_all,
        "rates_internal": internal_rates,
        "rates_climax": climax_rates,
        "old_state": old_state,
        "state": new_state,
    }


def _persist_kill_rl_state(cfg: Dict[str, Any], state: Dict[str, Any]) -> None:
    try:
        from config_manager import update_system_config

        update_system_config({MEGA_TREND_KILL_RL_STATE_KEY: state})
    except Exception:
        pass


def build_kill_rl_brief(result: Dict[str, Any]) -> str:
    st = result.get("state", {})
    rates_int = result.get("rates_internal") or result.get("rates") or {}
    rates_clx = result.get("rates_climax") or {}
    if not result.get("updated"):
        return (
            f"🧬 <b>[Mega-Trend Kill RL]</b> 표본 부족 "
            f"(int={rates_int.get('n', 0)} / clx={rates_clx.get('n', 0)}) — "
            f"WRΔ {st.get('win_rate_min_delta', 0):+.3f} · "
            f"flowZΔ {st.get('flow_reversal_z_delta', 0):+.3f} 유지"
        )
    lanes = ",".join(result.get("lanes_updated") or [])
    sectors = ",".join(result.get("sectors_updated") or [])
    sec_tag = f" sec={sectors}" if sectors else ""
    return (
        f"🧬 <b>[Mega-Trend Kill RL]</b> [{lanes}{sec_tag}] "
        f"int 기회비용 {rates_int.get('opportunity_cost_rate', 0) * 100:.0f}% / "
        f"방어 {rates_int.get('defense_success_rate', 0) * 100:.0f}% → "
        f"WRΔ {st.get('win_rate_min_delta', 0):+.3f} · "
        f"연속손실Δ {st.get('consecutive_loss_delta', 0):+d} · "
        f"clx 방어 {rates_clx.get('defense_success_rate', 0) * 100:.0f}% → "
        f"flowZΔ {st.get('flow_reversal_z_delta', 0):+.3f} · "
        f"trapΔ {st.get('climax_vol_shrink_delta', 0):+.3f} · "
        f"filter=MegaTrend"
    )
