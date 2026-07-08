"""
AceEvolution → 신규 가상매매(Forward) 진입 시점 파라미터 반영.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from evolution.ace_exit_bridge import (
    _logic_matches,
    _rule_match_bonus,
    evolution_live_enabled,
)
from evolution.ace_evolution_clamp import compute_ace_evolution_multiplier
from evolution.ace_evolution_schema import flow_tag_prefix
from evolution.ace_evolution_live import ace_evolution_live_eligible, promote_playbook_for_live
from evolution.ace_evolution_store import load_playbook


@dataclass(frozen=True)
class AceEntryAdjustments:
    active: bool = False
    kelly_mult: float = 1.0
    cos_relax_mult: float = 1.0
    ml_relax_mult: float = 1.0
    sl_atr_mult: float = 1.0
    flow_tag: str = ""
    reason: str = ""


def ace_entry_adjustments(
    row: Any,
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> AceEntryAdjustments:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not evolution_live_enabled(cfg, market):
        return AceEntryAdjustments(reason="evolution_disabled")

    mkt = str(market).upper()
    pb = load_playbook(mkt, cfg)
    if ace_evolution_live_eligible(cfg, mkt):
        pb = promote_playbook_for_live(pb, cfg, mkt)
    logic_core = str(pb.get("logic_core") or "").strip()
    if not logic_core or not _logic_matches(row, logic_core):
        return AceEntryAdjustments(reason="logic_mismatch")

    bonus = _rule_match_bonus(row, pb)
    mult, meta = compute_ace_evolution_multiplier(
        pb, rule_match_bonus=bonus, sys_config=cfg
    )
    if meta.get("observe_only") or mult <= 1.0 + 1e-6:
        return AceEntryAdjustments(
            kelly_mult=1.0,
            reason=str(meta.get("reason") or "observe_only"),
        )

    # 진입: 켈리·컷오프·손절 ATR 배수에 진화 DNA 반영
    kelly_mult = float(min(1.35, max(1.0, mult)))
    # P0: BEAR/HIGH_VOL — Kelly booster cap 1.0 (V_RECOVERY analog 예외)
    try:
        from bear_defense_booster_guard import (
            clamp_bear_attack_booster_mult,
            resolve_meta_regime_key,
        )

        _rk = resolve_meta_regime_key(cfg)
        _k_raw = kelly_mult
        kelly_mult = clamp_bear_attack_booster_mult(_k_raw, _rk, cfg, cap=1.0)
    except Exception:
        pass
    if kelly_mult <= 1.0 + 1e-6:
        return AceEntryAdjustments(
            kelly_mult=1.0,
            reason="bear_booster_cap_or_neutral",
        )
    relax = float(min(0.92, max(0.82, 1.0 - (mult - 1.0) * 0.35)))
    sl_mult = float(min(1.15, max(0.85, 2.0 - mult)))
    tag = flow_tag_prefix(mkt)
    return AceEntryAdjustments(
        active=True,
        kelly_mult=kelly_mult,
        cos_relax_mult=relax,
        ml_relax_mult=relax,
        sl_atr_mult=sl_mult,
        flow_tag=tag,
        reason="ace_evolution_entry",
    )


def apply_ace_entry_sl_atr(
    market: str,
    base_sl_atr: float,
    ace: AceEntryAdjustments,
    sys_config: Optional[Dict[str, Any]] = None,
) -> float:
    if not ace.active:
        return base_sl_atr
    cfg = sys_config if isinstance(sys_config, dict) else {}
    floor = float(cfg.get("ACE_EVOLUTION_ENTRY_SL_ATR_FLOOR", 1.2))
    cap = float(cfg.get("ACE_EVOLUTION_ENTRY_SL_ATR_CAP", 3.5))
    return float(min(cap, max(floor, base_sl_atr * ace.sl_atr_mult)))
