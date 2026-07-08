"""
Fluid Evolution Bridge — Elastic / MAB / DNA Mutator ↔ MetaGovernor·팩토리 연동.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

_SCOUT_MARKERS = (
    "🔭SCOUT",
    "FLUID_SCOUT",
    "COSINE_SCOUT",
    "MLBOX_SCOUT",
    "SCOUT]",
    "ScoutBearShadow",
)


def is_fluid_scout_sig(sig_type: Any, *, trade_source: Any = None) -> bool:
    """정찰병·탄력 스카우트 거래 식별 — 메인 Treasury/Deathmatch 통계에서 제외."""
    src = str(trade_source or "").upper()
    if src in ("FLUID_SCOUT", "SCOUT"):
        return True
    s = str(sig_type or "")
    su = s.upper()
    return any(m in s or m in su for m in _SCOUT_MARKERS)


def normalize_group_key_for_main_stats(sig_type: Any) -> str:
    """정찰 태그 제거 후 본군 그룹 키 (격리된 스카우트는 호출 전 필터)."""
    s = str(sig_type or "").strip()
    s = re.sub(r"\[🔭SCOUT\]\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\[.*?SCOUT.*?\]\s*", "", s, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", s).strip()


def filter_treasury_rows_exclude_scouts(
    rows: Sequence[Tuple[str, str, float, str]],
) -> List[Tuple[str, str, float, str]]:
    """MetaGovernor Treasury 입력 — 정찰병 PnL 을 메인 로직군 wr/PF 에서 격리."""
    out: List[Tuple[str, str, float, str]] = []
    for mp, gk, ret, exd in rows:
        if is_fluid_scout_sig(gk):
            continue
        out.append((mp, gk, ret, exd))
    return out


def blended_overlay(
    deathmatch_overlay: Dict[str, float],
    mab_overlay: Dict[str, float],
    *,
    dm_weight: float = 0.70,
) -> Dict[str, float]:
    """
    DM(Exploit) 70% + MAB(Explore) 30% 선형 블렌드 → META_GROUP_KELLY_MULT 산출용.
    """
    from mab_capital_allocator import blend_deathmatch_and_mab

    return blend_deathmatch_and_mab(
        deathmatch_overlay,
        mab_overlay,
        exploit_weight=float(dm_weight),
    )


def compute_final_group_kelly_mult(
    health_mult: Dict[str, float],
    blended: Dict[str, float],
    *,
    max_mult: float = 1.45,
) -> Dict[str, float]:
    """health × blended_overlay — MetaGovernor 소비 형식."""
    from evolution.deathmatch_allocation import merge_group_kelly_from_overlay

    return merge_group_kelly_from_overlay(health_mult, blended, max_mult=max_mult)


def sync_mab_exploration_overlay(
    *,
    markets: tuple[str, ...] = ("KR", "US"),
    sys_config: Optional[Dict[str, Any]] = None,
    exploit_weight: float = 0.70,
) -> Dict[str, Any]:
    """
    MAB overlay 계산 후 기존 META_DEATHMATCH_KELLY_OVERLAY 와 블렌드 → meta 저장.
    MetaGovernor Treasury 재실행 시 health × blended overlay 가 반영됨.
    """
    from config_manager import load_system_config

    from evolution.deathmatch_allocation import (
        health_to_group_mult,
    )
    from mab_capital_allocator import MABCapitalAllocator
    from meta_governor import save_meta_governor_state_atomic
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved

    cfg = sys_config if isinstance(sys_config, dict) else (load_system_config() or {})
    meta = dict(load_meta_state_resolved())
    allocator = MABCapitalAllocator(cfg)

    combined: Dict[str, float] = {}
    audit: Dict[str, Any] = {"markets": {}}

    for mk in markets:
        mab_res = allocator.compute(mk)
        dm_overlay = dict(meta.get("META_DEATHMATCH_KELLY_OVERLAY") or {})
        if str(meta.get("META_DEATHMATCH_ALLOC_MARKET", "")).upper() != mk:
            dm_overlay = dm_overlay or {}

        blended = blended_overlay(
            dm_overlay,
            mab_res.group_mult,
            dm_weight=exploit_weight,
        )
        for gk, mult in blended.items():
            prev = combined.get(gk, 1.0)
            combined[gk] = min(prev, float(mult)) if gk in combined else float(mult)

        audit["markets"][mk] = {
            "mab_exploit": mab_res.exploit_groups[:8],
            "mab_explore": mab_res.explore_groups[:8],
            "mode": mab_res.mode,
        }

    meta["META_MAB_KELLY_OVERLAY"] = combined
    meta["META_MAB_ALLOC_AUDIT"] = audit
    meta["META_MAB_ALLOC_AS_OF"] = mab_res.as_of if markets else ""

    health_mult = health_to_group_mult(meta.get("META_STRATEGY_HEALTH") or {})
    cap = float(cfg.get("MAB_OVERLAY_CAP", 1.45) or 1.45)
    meta["META_GROUP_KELLY_MULT"] = compute_final_group_kelly_mult(
        health_mult, combined, max_mult=cap
    )

    save_meta_governor_state_atomic(meta)
    invalidate_meta_state_cache()
    return {"overlay_keys": len(combined), "audit": audit}


def refresh_elastic_config_snapshot(
    sys_config: Dict[str, Any],
    *,
    markets: tuple[str, ...] = ("KR", "US"),
) -> Dict[str, Any]:
    from elastic_threshold import ElasticThreshold

    out: Dict[str, Any] = {}
    for mk in markets:
        et = ElasticThreshold.from_system_config(sys_config, market=mk)
        out[mk] = et.persist_snapshot()
    return out


def run_fluid_evolution_weekend_hooks(
    sys_config: Dict[str, Any],
    *,
    closed_df=None,
) -> List[str]:
    """
    system_auto_pilot.run_autonomous_analysis() 말미 — DNA mutation + elastic snapshot.
    """
    from dna_mutator import run_weekend_dna_mutation_cycle

    lines: List[str] = []
    lines.append("\n🧬 <b>[Fluid Evolution · Meta-DNA Mutation]</b>")
    try:
        updated, mut_logs = run_weekend_dna_mutation_cycle(sys_config)
        sys_config.clear()
        sys_config.update(updated)
        lines.extend(mut_logs)
    except Exception as ex:
        lines.append(f"⚠️ DNA mutation skip: {ex}")
        logger.exception("dna mutation failed")

    try:
        snap = refresh_elastic_config_snapshot(sys_config)
        for mk, s in snap.items():
            lines.append(
                f"▪️ {mk} elastic starv={s.get('starvation_index')} "
                f"cos={s.get('cos_cutoff')} scout_gap={s.get('scout_gap')}"
            )
    except Exception as ex:
        lines.append(f"⚠️ elastic snapshot skip: {ex}")

    try:
        sync_mab_exploration_overlay(sys_config=sys_config)
        lines.append("▪️ MAB explore/exploit overlay → meta_governor_state 동기")
    except Exception as ex:
        lines.append(f"⚠️ MAB sync skip: {ex}")

    _ = closed_df
    return lines


def post_meta_governor_fluid_sync(sys_config: Optional[Dict[str, Any]] = None) -> None:
    """meta_governor_sync 직후 — 가벼운 MAB·elastic 스냅샷 (실패 무시)."""
    try:
        from config_manager import load_system_config

        cfg = sys_config if isinstance(sys_config, dict) else (load_system_config() or {})
        refresh_elastic_config_snapshot(cfg)
        sync_mab_exploration_overlay(sys_config=cfg, exploit_weight=0.70)
    except Exception as ex:
        logger.debug("post_meta_governor_fluid_sync: %s", ex)
