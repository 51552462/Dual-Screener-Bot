"""
PIL → MetaGovernor / 데스매치 자본 페널티 · N일 ZOMBIE 시 RETIRED.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import pandas as pd

from practitioner_zombie_streak import (
    streak_key,
    update_zombie_streaks,
    zombie_retire_days_for_market,
)

if TYPE_CHECKING:
    from practitioner_intelligence import PractitionerBrief

logger = logging.getLogger(__name__)


def _penalties_enabled(sys_config: Optional[dict]) -> bool:
    if not isinstance(sys_config, dict):
        return True
    v = sys_config.get("PRACTITIONER_APPLY_PENALTIES", 1)
    return str(v).strip().lower() not in ("0", "false", "no", "off")


def apply_pil_vitality_penalties(
    briefs: List["PractitionerBrief"],
    sys_config: Optional[dict] = None,
    *,
    apply_registry: bool = True,
) -> Dict[str, Any]:
    """
    ZOMBIE 즉시 Kelly=0 · 연속 N일 시 RETIRED + META_PIL_FORCE_RETIRED.
    """
    if not _penalties_enabled(sys_config):
        return {"applied": False, "reason": "disabled"}

    try:
        from factory_recovery_grace import factory_recovery_grace, practitioner_penalties_relaxed

        if practitioner_penalties_relaxed(sys_config):
            from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved
            from meta_governor import save_meta_governor_state_atomic

            meta = dict(load_meta_state_resolved())
            meta["META_PIL_ZOMBIE_STREAK"] = {}
            meta["META_PIL_ZOMBIE_GROUPS"] = []
            meta["META_PIL_PENALTY_AS_OF"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
            meta["META_PIL_PENALTY_MODE"] = "relaxed"
            save_meta_governor_state_atomic(meta)
            invalidate_meta_state_cache()
            for b in briefs:
                b.penalty_action = "none (PIL relaxed)"
                b.force_retired = False
                b.zombie_streak_days = 0
            logger.info("PIL penalties skipped: relaxed/recovery mode (%d briefs)", len(briefs))
            return {"applied": False, "reason": "relaxed", "n_briefs": len(briefs)}
    except ImportError:
        pass

    from evolution.deathmatch_allocation import health_to_group_mult, merge_group_kelly_from_overlay
    from meta_governor import save_meta_governor_state_atomic
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved
    from strategy_promotion_engine import stable_strategy_id

    meta = dict(load_meta_state_resolved())
    overlay = dict(meta.get("META_DEATHMATCH_KELLY_OVERLAY") or {})
    pil_status: Dict[str, Any] = dict(meta.get("META_PIL_GROUP_STATUS") or {})
    prior_streaks = meta.get("META_PIL_ZOMBIE_STREAK")
    retire_candidates: List[Dict[str, Any]] = list(meta.get("META_PIL_RETIRE_CANDIDATES") or [])
    force_retired_list: List[Dict[str, Any]] = list(meta.get("META_PIL_FORCE_RETIRED") or [])
    zombie_groups: List[str] = []
    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")

    streak_entries = [
        {
            "market": b.market,
            "group_key": b.group_key,
            "is_zombie": b.is_zombie,
            "vitality_score": b.vitality_score,
        }
        for b in briefs
    ]
    updated_streaks, force_retire = update_zombie_streaks(
        streak_entries,
        prior_streaks if isinstance(prior_streaks, dict) else None,
        sys_config=sys_config,
    )
    force_retire_by_key = {
        streak_key(str(r.get("market")), str(r.get("group_key"))): r for r in force_retire
    }

    registry_updates: List[Dict[str, Any]] = []
    retired_ids: List[str] = list(meta.get("META_RETIRED_STRATEGY_IDS") or [])

    for b in briefs:
        gk = str(b.group_key or "").strip()
        if not gk:
            continue
        mk = str(b.market or "KR").upper()
        sk = streak_key(mk, gk)
        need_days = zombie_retire_days_for_market(mk, sys_config)
        streak_rec = updated_streaks.get(sk) or {}
        streak_days = int(streak_rec.get("streak_days", 0) or 0)
        b.zombie_streak_days = streak_days
        b.zombie_retire_after_days = need_days
        force_ret = sk in force_retire_by_key
        b.force_retired = force_ret

        st = {
            "market": mk,
            "rank_tier": b.rank_tier,
            "vitality_score": round(float(b.vitality_score), 4),
            "is_zombie": bool(b.is_zombie),
            "zombie_streak_days": streak_days,
            "zombie_retire_after_days": need_days,
            "force_retired": force_ret,
            "wr_trend_pp": b.wr_trend_pp,
            "turnover_30d": b.turnover_30d,
            "active_days": b.active_days,
            "as_of": now,
            "penalty": "none",
        }
        sid = stable_strategy_id(mk, gk)

        if force_ret:
            zombie_groups.append(gk)
            overlay[gk] = 0.0
        elif b.is_zombie:
            zombie_groups.append(gk)

        if b.is_zombie:
            st["penalty"] = "watch" if not force_ret else "retired"
            if force_ret:
                st["penalty"] = "retired"
            b.penalty_action = (
                ("Kelly=0 · 연속 " if force_ret else "관찰 · 연속 ")
                + f"{streak_days}/{need_days}일"
                + (" → RETIRED" if force_ret else " (N일 시 강제 퇴역)")
            )
            if not force_ret:
                cand = {
                    "strategy_id": sid,
                    "market": mk,
                    "group_key": gk,
                    "reason": "PIL_ZOMBIE_VITALITY",
                    "vitality_score": b.vitality_score,
                    "streak_days": streak_days,
                    "as_of": now,
                }
                if not any(c.get("strategy_id") == sid for c in retire_candidates):
                    retire_candidates.append(cand)
                if apply_registry and not force_ret:
                    registry_updates.append(
                        {
                            "strategy_id": sid,
                            "market": mk.split("_")[0] if "_" in mk else mk,
                            "group_key": gk,
                            "state": "COOLED",
                            "display_name": gk,
                            "capital_mult": 0.0,
                            "source": "PIL_ZOMBIE",
                            "demote_reason": f"PIL vitality={b.vitality_score:.2f} streak={streak_days}",
                            "rolling_wr": b.rolling_wr_pct,
                            "n_closed": b.n_closed_window,
                        }
                    )
        elif streak_days == 0:
            b.penalty_action = "none"

        if force_ret:
            fr = force_retire_by_key[sk]
            force_retired_list = [
                x for x in force_retired_list if x.get("strategy_id") != sid
            ]
            force_retired_list.append(
                {
                    "strategy_id": sid,
                    "market": mk,
                    "group_key": gk,
                    "reason": fr.get("reason", "PIL_ZOMBIE_STREAK"),
                    "streak_days": streak_days,
                    "as_of": now,
                }
            )
            if sid not in retired_ids:
                retired_ids.append(sid)
            if apply_registry:
                registry_updates.append(
                    {
                        "strategy_id": sid,
                        "market": mk.split("_")[0] if "_" in mk else mk,
                        "group_key": gk,
                        "state": "RETIRED",
                        "display_name": gk,
                        "capital_mult": 0.0,
                        "source": "PIL_ZOMBIE_STREAK",
                        "demote_reason": f"PIL zombie streak {streak_days}>={need_days}d",
                        "rolling_wr": b.rolling_wr_pct,
                        "n_closed": b.n_closed_window,
                    }
                )

        pil_status[gk] = st

    meta["META_PIL_GROUP_STATUS"] = pil_status
    meta["META_PIL_ZOMBIE_STREAK"] = updated_streaks
    meta["META_PIL_ZOMBIE_GROUPS"] = sorted(set(zombie_groups))
    meta["META_PIL_RETIRE_CANDIDATES"] = retire_candidates[-100:]
    meta["META_PIL_FORCE_RETIRED"] = force_retired_list[-100:]
    meta["META_RETIRED_STRATEGY_IDS"] = retired_ids[-200:]
    meta["META_PIL_PENALTY_AS_OF"] = now
    meta["META_DEATHMATCH_KELLY_OVERLAY"] = overlay

    max_mult = 1.5
    if isinstance(sys_config, dict):
        try:
            from evolution.deathmatch_config import load_deathmatch_config, market_deathmatch_params

            dmc = market_deathmatch_params(load_deathmatch_config(sys_config), "KR")
            max_mult = float(dmc.get("allocation_max_group_mult", 1.5))
        except Exception:
            pass
    health_mult = health_to_group_mult(meta.get("META_STRATEGY_HEALTH") or {})
    meta["META_GROUP_KELLY_MULT"] = merge_group_kelly_from_overlay(
        health_mult, overlay, max_mult=max_mult
    )

    save_meta_governor_state_atomic(meta)
    invalidate_meta_state_cache()

    if registry_updates and apply_registry:
        try:
            from strategy_registry_store import upsert_registry_rows

            upsert_registry_rows(registry_updates)
        except Exception as ex:
            logger.warning("PIL registry upsert failed: %s", ex)

    n_retired = sum(1 for u in registry_updates if u.get("state") == "RETIRED")
    logger.info(
        "PIL penalties: zombie=%d retired=%d force_ret=%d",
        len(zombie_groups),
        n_retired,
        len(force_retire),
    )
    return {
        "applied": True,
        "zombie_groups": zombie_groups,
        "n_force_retired": len(force_retire),
        "n_registry_retired": n_retired,
        "n_retire_candidates": len(retire_candidates),
    }
