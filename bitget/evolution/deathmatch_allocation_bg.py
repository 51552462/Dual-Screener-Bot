"""
Bitget P2 deathmatch allocation — ``bitget_meta_governor_state`` / config_kv 전용.

주식 ``meta_governor_state.json`` 을 건드리지 않고 ``META_GROUP_KELLY_MULT`` 를 갱신한다.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, TYPE_CHECKING

import pandas as pd

from bitget.governance.meta_sync import load_bitget_meta_unified, save_bitget_meta_unified
from evolution.deathmatch_allocation import (
    compute_group_allocation_overlay,
    health_to_group_mult,
    merge_group_kelly_from_overlay,
    proposal_to_config_audit,
)
from evolution.deathmatch_config import load_deathmatch_config, market_deathmatch_params

if TYPE_CHECKING:
    from evolution.deathmatch_battle_royale import BattleRoyaleResult
    from evolution.deathmatch_report import NWayDeathmatchResult

logger = logging.getLogger(__name__)


def _allocation_flag(sys_config: Optional[dict]) -> bool:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    flag = str(
        cfg.get("DEATHMATCH_APPLY_ALLOCATION", os.environ.get("DEATHMATCH_APPLY_ALLOCATION", "1"))
    ).strip().lower()
    return flag in ("1", "true", "yes", "on")


def apply_bitget_deathmatch_allocation_to_meta(
    proposal: Dict[str, Any],
    *,
    market: str,
    sys_config: Optional[dict] = None,
    save_config_audit: bool = True,
) -> Dict[str, Any]:
    meta = dict(load_bitget_meta_unified())
    overlay = dict(proposal.get("group_mult") or {})
    meta["META_DEATHMATCH_KELLY_OVERLAY"] = overlay
    meta["META_DEATHMATCH_ALLOC_AS_OF"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    meta["META_DEATHMATCH_ALLOC_MARKET"] = str(market or "SPOT").upper()

    dmcfg = market_deathmatch_params(
        load_deathmatch_config(sys_config if isinstance(sys_config, dict) else {}),
        market,
    )
    max_mult = float(dmcfg.get("allocation_max_group_mult", 1.5))
    health_mult = health_to_group_mult(meta.get("META_STRATEGY_HEALTH") or {})
    meta["META_GROUP_KELLY_MULT"] = merge_group_kelly_from_overlay(
        health_mult, overlay, max_mult=max_mult
    )

    save_bitget_meta_unified(meta)

    if save_config_audit and isinstance(sys_config, dict):
        from bitget.infra.config_manager import save_system_config

        cfg = dict(sys_config)
        audit = proposal_to_config_audit(proposal)
        cfg["DEATHMATCH_ALLOCATION_PROPOSAL"] = {
            "standby_labels": list(audit.get("standby_labels") or []),
            "boost_labels": list(audit.get("boost_labels") or []),
            "weight_mult": dict(audit.get("weight_mult") or {}),
        }
        cfg["DEATHMATCH_ALLOCATION_AS_OF"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        save_system_config(cfg)

    logger.info(
        "Bitget deathmatch P2 allocation applied market=%s groups=%d standby=%d boost=%d",
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


def maybe_apply_bitget_deathmatch_allocation(
    br: "BattleRoyaleResult",
    dm: Optional["NWayDeathmatchResult"] = None,
    sys_config: Optional[dict] = None,
    *,
    market_type: str,
) -> Optional[Dict[str, Any]]:
    del dm  # P2는 battle_royale arms 기준; 시그니처는 주식 maybe_apply 와 동형
    if not _allocation_flag(sys_config):
        return None

    from bitget.infra.market_keys import to_deathmatch_key

    mk = to_deathmatch_key(market_type)
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

    return apply_bitget_deathmatch_allocation_to_meta(
        proposal, market=mk, sys_config=sys_config, save_config_audit=True
    )
