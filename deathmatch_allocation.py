"""
P2 — 데스매치 Scorecard → META_GROUP_KELLY_MULT 자본 할당 루프.

Battle Royal 결과(Composite v2 · 절대 허들 · 하위 탈락)를 group_key 단위 overlay 로 변환하고,
MetaGovernor Treasury health mult 와 곱해 meta_governor_state 에 반영한다.
"""
from __future__ import annotations

import logging
import math
import os
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import pandas as pd

from deathmatch_config import load_deathmatch_config, market_deathmatch_params

if TYPE_CHECKING:
    from deathmatch_battle_royale import BattleRoyaleResult, RegistryArmRow
    from deathmatch_report import NWayDeathmatchResult

logger = logging.getLogger(__name__)


def _allocation_flag(sys_config: Optional[dict]) -> bool:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    flag = str(
        cfg.get("DEATHMATCH_APPLY_ALLOCATION", os.environ.get("DEATHMATCH_APPLY_ALLOCATION", "0"))
    ).strip().lower()
    return flag in ("1", "true", "yes", "on")


def health_to_group_mult(health: Dict[str, Any]) -> Dict[str, float]:
    """META_STRATEGY_HEALTH → group_key 단위 방어 승수 (Treasury 와 동일 규칙)."""
    out: Dict[str, float] = {}
    if not isinstance(health, dict):
        return out
    for key, hv in health.items():
        if key == "__meta__" or not isinstance(hv, dict):
            continue
        _, _, gk = str(key).rpartition("|")
        gk_only = gk or str(key)
        try:
            m = float(hv.get("mult", 1.0) or 1.0)
        except (TypeError, ValueError):
            m = 1.0
        out[gk_only] = min(out.get(gk_only, 1.0), m)
    return out


def compute_group_allocation_overlay(
    arms: List["RegistryArmRow"],
    *,
    dmcfg: Dict[str, Any],
    champion_group_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    group_key → overlay 승수 (1.0=중립, 0=STANDBY, >1=챔피언/상위 가중).

    - hurdle 미통과: standby_mult (기본 0)
    - 하위 bottom_pct & 면제 없음: standby_mult
    - 챔피언(허들+eligible): champion_mult
    - 상위 축(하위 제외): top_boost_mult
    """
    bottom_pct = float(dmcfg.get("bottom_pct", 0.20))
    standby_mult = float(dmcfg.get("allocation_standby_mult", 0.0))
    top_boost = float(dmcfg.get("allocation_top_boost_mult", 1.25))
    champion_mult = float(dmcfg.get("allocation_champion_mult", 1.35))
    hurdle_fail_mult = float(dmcfg.get("allocation_hurdle_fail_mult", 0.0))
    neutral_mult = float(dmcfg.get("allocation_neutral_mult", 1.0))

    ranked = [
        a
        for a in arms
        if a.rank < 999 and a.n_valid > 0
    ]
    ranked.sort(key=lambda x: (x.rank, -float(x.composite_score or 0)))

    n = len(ranked)
    if n == 0:
        return {
            "group_mult": {},
            "standby_groups": [],
            "boost_groups": [],
            "champion_group": champion_group_key,
            "eligible_n": 0,
        }

    n_bottom = max(1, int(math.ceil(n * bottom_pct)))
    bottom_ids = {id(a) for a in ranked[-n_bottom:]}

    group_mult: Dict[str, float] = {}
    standby_groups: List[str] = []
    boost_groups: List[str] = []

    for a in ranked:
        gk = str(a.group_key or a.label or "").strip()
        if not gk:
            continue

        if not a.hurdle_passed:
            mult = hurdle_fail_mult
            standby_groups.append(gk)
        elif a.below_floor and not a.relative_exempt:
            mult = standby_mult
            standby_groups.append(gk)
        elif champion_group_key and gk == champion_group_key and a.champion_eligible:
            mult = champion_mult
            boost_groups.append(gk)
        elif id(a) not in bottom_ids:
            mult = top_boost
            boost_groups.append(gk)
        else:
            mult = neutral_mult

        group_mult[gk] = float(mult)

    return {
        "group_mult": group_mult,
        "standby_groups": standby_groups,
        "boost_groups": boost_groups,
        "champion_group": champion_group_key,
        "eligible_n": n,
        "bottom_pct": bottom_pct,
    }


def merge_group_kelly_from_overlay(
    health_mult: Dict[str, float],
    overlay: Dict[str, float],
    *,
    max_mult: float = 1.5,
) -> Dict[str, float]:
    """Treasury health × 데스매치 overlay → META_GROUP_KELLY_MULT."""
    cap = max(1.0, float(max_mult))
    keys = set(health_mult) | set(overlay)
    merged: Dict[str, float] = {}
    for gk in keys:
        h = float(health_mult.get(gk, 1.0))
        o = float(overlay.get(gk, 1.0))
        merged[gk] = min(max(0.0, h * o), cap)
    return merged


def proposal_to_config_audit(proposal: Dict[str, Any]) -> Dict[str, Any]:
    """system_config DEATHMATCH_ALLOCATION_PROPOSAL 호환."""
    gm = proposal.get("group_mult") or {}
    standby = list(proposal.get("standby_groups") or [])
    boost = list(proposal.get("boost_groups") or [])
    weight_mult = {gk: gm[gk] for gk in gm}
    return {
        "standby_labels": standby,
        "boost_labels": boost,
        "weight_mult": weight_mult,
        "group_mult": dict(gm),
        "champion_group": proposal.get("champion_group"),
        "eligible_n": proposal.get("eligible_n", 0),
        "bottom_pct": proposal.get("bottom_pct"),
    }


def apply_deathmatch_allocation_to_meta(
    proposal: Dict[str, Any],
    *,
    market: str,
    sys_config: Optional[dict] = None,
    save_config_audit: bool = True,
) -> Dict[str, Any]:
    """
    meta_governor_state:
      - META_DEATHMATCH_KELLY_OVERLAY (group_key → overlay, Treasury 재실행 시 유지)
      - META_GROUP_KELLY_MULT (health × overlay 병합본)
    """
    from meta_governor import save_meta_governor_state_atomic
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved

    meta = dict(load_meta_state_resolved())
    overlay = dict(proposal.get("group_mult") or {})
    meta["META_DEATHMATCH_KELLY_OVERLAY"] = overlay
    meta["META_DEATHMATCH_ALLOC_AS_OF"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    meta["META_DEATHMATCH_ALLOC_MARKET"] = str(market or "KR").upper()

    dmcfg = market_deathmatch_params(
        load_deathmatch_config(sys_config if isinstance(sys_config, dict) else {}),
        market,
    )
    max_mult = float(dmcfg.get("allocation_max_group_mult", 1.5))
    health_mult = health_to_group_mult(meta.get("META_STRATEGY_HEALTH") or {})
    meta["META_GROUP_KELLY_MULT"] = merge_group_kelly_from_overlay(
        health_mult, overlay, max_mult=max_mult
    )

    save_meta_governor_state_atomic(meta)
    invalidate_meta_state_cache()

    if save_config_audit and isinstance(sys_config, dict):
        from deathmatch_report import apply_allocation_proposal_to_config

        apply_allocation_proposal_to_config(
            sys_config, proposal_to_config_audit(proposal), save=True
        )

    logger.info(
        "Deathmatch P2 allocation applied market=%s groups=%d standby=%d boost=%d",
        market,
        len(overlay),
        len(proposal.get("standby_groups") or []),
        len(proposal.get("boost_groups") or []),
    )
    return {
        "overlay": overlay,
        "merged_group_mult": dict(meta["META_GROUP_KELLY_MULT"]),
        "market": str(market).upper(),
    }


def maybe_apply_deathmatch_allocation_p2(
    br: "BattleRoyaleResult",
    dm: Optional["NWayDeathmatchResult"] = None,
    sys_config: Optional[dict] = None,
    *,
    market: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """DEATHMATCH_APPLY_ALLOCATION=1 일 때만 Meta 연동."""
    if not _allocation_flag(sys_config):
        return None

    mk = str(market or getattr(br, "market", None) or "KR").upper()
    dmcfg = market_deathmatch_params(
        load_deathmatch_config(sys_config if isinstance(sys_config, dict) else {}),
        mk,
    )
    champ_gk = None
    if br.champion:
        champ_gk = str(br.champion.group_key or "").strip() or None

    proposal = compute_group_allocation_overlay(
        br.arms,
        dmcfg=dmcfg,
        champion_group_key=champ_gk,
    )
    if proposal.get("eligible_n", 0) < 1:
        return None

    return apply_deathmatch_allocation_to_meta(
        proposal, market=mk, sys_config=sys_config, save_config_audit=True
    )
