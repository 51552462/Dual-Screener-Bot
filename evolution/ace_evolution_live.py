"""
AceEvolution 실전(live) 승격 — 복구된 DNA·playbook·표본 수에 따른 observe_only 해제.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from evolution.ace_evolution_store import load_playbook


def config_has_evolved_dna_templates(
    sys_config: Optional[Dict[str, Any]],
    market: str,
) -> bool:
    """관제탑 DNA_SUPERNOVA_* / DNA_ALPHA_* 가 비어 있지 않으면 실전 백본으로 간주."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    mkt = str(market or "KR").upper()
    multi = cfg.get(f"DNA_SUPERNOVA_{mkt}_MULTI")
    if isinstance(multi, dict) and len(multi) >= 1:
        return True
    mfe = cfg.get("DNA_SUPERNOVA_MFE_WEIGHTED")
    if isinstance(mfe, dict) and any(k in mfe for k in ("cpv", "tb", "bbe")):
        return True
    for key, val in cfg.items():
        if not isinstance(val, dict):
            continue
        if "DNA_ALPHA" in str(key) or key.startswith("NEW_EVOLUTION_"):
            if any(k in val for k in ("cpv", "tb", "bbe", "shape")):
                return True
    inc = cfg.get("INCUBATOR_TEMPLATES")
    if isinstance(inc, dict) and len(inc) >= 1:
        return True
    return False


def ace_evolution_live_eligible(
    sys_config: Optional[Dict[str, Any]],
    market: str,
) -> bool:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not bool(cfg.get("ENABLE_ACE_EVOLUTION_WEIGHTING", True)):
        return False
    if bool(cfg.get("ACE_EVOLUTION_FORCE_OBSERVE", False)):
        return False
    return config_has_evolved_dna_templates(cfg, market)


def resolve_synthesis_observe_only(
    fact_pack: Dict[str, Any],
    *,
    observe_only_requested: bool,
    sys_config: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    True → playbook observe_only. n_ace<3 이더라도 DNA 백본·logic_core 있으면 live 허용.
    """
    if observe_only_requested:
        return True
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if bool(cfg.get("ACE_EVOLUTION_FORCE_OBSERVE", False)):
        return True
    if not bool(cfg.get("ENABLE_ACE_EVOLUTION_WEIGHTING", True)):
        return True
    mkt = str(fact_pack.get("market") or "KR").upper()
    n_ace = int(fact_pack.get("n_ace") or 0)
    if n_ace >= 3:
        return False
    if config_has_evolved_dna_templates(cfg, mkt):
        return False
    if n_ace >= 1 and str(fact_pack.get("logic_core") or "").strip():
        return False
    return True


def promote_playbook_for_live(
    playbook: Dict[str, Any],
    sys_config: Optional[Dict[str, Any]],
    market: str,
) -> Dict[str, Any]:
    """observe_only 플래그 해제 + 최소 confidence 보정."""
    if not isinstance(playbook, dict):
        return playbook
    if not ace_evolution_live_eligible(sys_config, market):
        return playbook
    out = dict(playbook)
    out["observe_only"] = False
    try:
        conf = float(out.get("confidence", 0.0))
    except (TypeError, ValueError):
        conf = 0.0
    out["confidence"] = max(conf, 0.55)
    return out
