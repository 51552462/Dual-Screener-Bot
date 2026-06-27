"""
AceEvolution → 실전 청산(Exit) Hook — 보유 연장·MFE 완화·flow_tags.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from evolution.ace_deathmatch_bridge import _logic_matches
from evolution.ace_evolution_clamp import compute_ace_evolution_multiplier
from evolution.ace_evolution_schema import flow_tag_prefix
from evolution.ace_evolution_store import load_playbook


@dataclass(frozen=True)
class AceExitOverrides:
    active: bool = False
    multiplier: float = 1.0
    time_stop_mult: float = 1.0
    min_hold_bars_extra: int = 0
    mfe_tp_relax_pct: float = 0.0
    flow_tag: str = ""
    reason: str = ""


def evolution_live_enabled(sys_config: Optional[Dict[str, Any]]) -> bool:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not bool(cfg.get("ENABLE_ACE_EVOLUTION_WEIGHTING", False)):
        return False
    if bool(cfg.get("ACE_EVOLUTION_FORCE_OBSERVE", False)):
        return False
    return True


def _row_scalar(row: Any, col: str, default: float = 0.0) -> float:
    try:
        if hasattr(row, "get"):
            v = row.get(col)
        else:
            v = row[col] if col in row else None
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError, KeyError):
        return default


def _rule_match_bonus(row: Any, playbook: Dict[str, Any]) -> float:
    bonus = 0.0
    sector = str(row.get("sector") if hasattr(row, "get") else "").strip()
    sig = str(row.get("sig_type") if hasattr(row, "get") else "")
    for rule in playbook.get("rules") or []:
        if not isinstance(rule, dict):
            continue
        rt = str(rule.get("type") or "")
        try:
            rb = float(rule.get("bonus", 0.0))
        except (TypeError, ValueError):
            rb = 0.0
        if rt == "feature_band":
            col = str(rule.get("column") or "")
            op = str(rule.get("op") or "gte")
            try:
                thr = float(rule.get("value"))
            except (TypeError, ValueError):
                continue
            val = _row_scalar(row, col, float("nan"))
            if val != val:
                continue
            ok = val >= thr if op == "gte" else val <= thr
            if ok:
                bonus += rb
        elif rt == "theme_token":
            tokens = rule.get("tokens") or []
            if isinstance(tokens, str):
                tokens = re.split(r"[,·/|]", tokens)
            for tok in tokens:
                t = str(tok).strip()
                if t and t in sector:
                    bonus += rb
                    break
        elif rt == "logic_match":
            pat = str(rule.get("pattern") or "")
            if pat and pat in re.sub(r"\[.*?\]", "", sig):
                bonus += rb
    return bonus


def ace_exit_overrides(
    row: Any,
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> AceExitOverrides:
    """
    에이스 DNA playbook 일치 종목 → time_stop·MFE 완화 배수.
    observe_only / 관측 모드면 no-op.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not evolution_live_enabled(cfg):
        return AceExitOverrides(reason="evolution_disabled")

    mkt = str(market).upper()
    pb = load_playbook(mkt, cfg)
    logic_core = str(pb.get("logic_core") or "").strip()
    if not logic_core or not _logic_matches(row, logic_core):
        return AceExitOverrides(reason="logic_mismatch")

    bonus = _rule_match_bonus(row, pb)
    mult, meta = compute_ace_evolution_multiplier(
        pb, rule_match_bonus=bonus, sys_config=cfg
    )
    if meta.get("observe_only") or mult <= 1.0 + 1e-6:
        return AceExitOverrides(
            multiplier=mult,
            reason=str(meta.get("reason") or "observe_only"),
        )

    extra_days = min(5, max(1, int(round((mult - 1.0) * 12))))
    relax = min(8.0, max(0.5, (mult - 1.0) * 50.0))
    tag = flow_tag_prefix(mkt)
    return AceExitOverrides(
        active=True,
        multiplier=mult,
        time_stop_mult=mult,
        min_hold_bars_extra=extra_days,
        mfe_tp_relax_pct=relax,
        flow_tag=tag,
        reason="ace_evolution_live",
    )
