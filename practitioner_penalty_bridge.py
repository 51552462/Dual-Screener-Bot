"""
PIL → MetaGovernor / 데스매치 자본 페널티 연동.

ZOMBIE 그룹: META_GROUP_KELLY_MULT overlay=0, 퇴역 후보 등록, (선택) registry COOLED.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import pandas as pd

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
    배치 실무자 리포트 종료 후 1회 호출.
    ZOMBIE → Kelly 0 overlay + META_PIL_* SSOT + (옵션) strategy_registry COOLED.
    """
    if not _penalties_enabled(sys_config):
        return {"applied": False, "reason": "disabled"}

    from deathmatch_allocation import health_to_group_mult, merge_group_kelly_from_overlay
    from meta_governor import save_meta_governor_state_atomic
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved
    from strategy_promotion_engine import stable_strategy_id

    meta = dict(load_meta_state_resolved())
    overlay = dict(meta.get("META_DEATHMATCH_KELLY_OVERLAY") or {})
    pil_status: Dict[str, Any] = dict(meta.get("META_PIL_GROUP_STATUS") or {})
    retire_candidates: List[Dict[str, Any]] = list(meta.get("META_PIL_RETIRE_CANDIDATES") or [])
    zombie_groups: List[str] = []
    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")

    registry_updates: List[Dict[str, Any]] = []

    for b in briefs:
        gk = str(b.group_key or "").strip()
        if not gk:
            continue
        st = {
            "market": b.market,
            "rank_tier": b.rank_tier,
            "vitality_score": round(float(b.vitality_score), 4),
            "is_zombie": bool(b.is_zombie),
            "wr_trend_pp": b.wr_trend_pp,
            "turnover_30d": b.turnover_30d,
            "active_days": b.active_days,
            "as_of": now,
            "penalty": "none",
        }
        sid = stable_strategy_id(b.market, gk)
        if b.is_zombie:
            zombie_groups.append(gk)
            overlay[gk] = 0.0
            st["penalty"] = "kelly_zero"
            cand = {
                "strategy_id": sid,
                "market": b.market,
                "group_key": gk,
                "reason": "PIL_ZOMBIE_VITALITY",
                "vitality_score": b.vitality_score,
                "as_of": now,
            }
            if not any(c.get("strategy_id") == sid for c in retire_candidates):
                retire_candidates.append(cand)
            if apply_registry:
                registry_updates.append(
                    {
                        "strategy_id": sid,
                        "market": b.market,
                        "group_key": gk,
                        "state": "COOLED",
                        "display_name": gk,
                        "capital_mult": 0.0,
                        "source": "PIL_ZOMBIE",
                        "demote_reason": f"PIL vitality={b.vitality_score:.2f}",
                        "rolling_wr": b.rolling_wr_pct,
                        "n_closed": b.n_closed_window,
                    }
                )
        pil_status[gk] = st

    meta["META_PIL_GROUP_STATUS"] = pil_status
    meta["META_PIL_ZOMBIE_GROUPS"] = sorted(set(zombie_groups))
    meta["META_PIL_RETIRE_CANDIDATES"] = retire_candidates[-100:]
    meta["META_PIL_PENALTY_AS_OF"] = now
    meta["META_DEATHMATCH_KELLY_OVERLAY"] = overlay

    max_mult = 1.5
    if isinstance(sys_config, dict):
        try:
            from deathmatch_config import load_deathmatch_config, market_deathmatch_params

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
            logger.warning("PIL registry COOLED upsert failed: %s", ex)

    logger.info(
        "PIL penalties: zombie=%d overlay_zero=%d retire_cand=%d",
        len(zombie_groups),
        sum(1 for g in zombie_groups if overlay.get(g) == 0),
        len(retire_candidates),
    )
    return {
        "applied": True,
        "zombie_groups": zombie_groups,
        "n_retire_candidates": len(retire_candidates),
        "registry_cooled": len(registry_updates),
    }
