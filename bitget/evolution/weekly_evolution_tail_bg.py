"""Bitget weekly evolution tail — meta_learner, γ evolution, ratchet κ, regime vector."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def run_weekly_evolution_tail(
    *,
    pri_blend_z: Optional[float] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": True}

    cfg = sys_config
    if not isinstance(cfg, dict):
        try:
            from bitget.infra.config_manager import load_system_config

            cfg = load_system_config() or {}
        except Exception:
            cfg = {}

    try:
        from bitget.evolution.coin_regime_vector import build_current_coin_regime_vector

        vec = build_current_coin_regime_vector(cfg)
        out["regime_vector"] = vec.get("vector_map")
    except Exception as ex:
        out["regime_vector_error"] = str(ex)

    try:
        from bitget.meta_learner_bg import run_bitget_meta_learning_cycle

        out["meta_learner"] = run_bitget_meta_learning_cycle(sys_config=cfg)
    except Exception as ex:
        out["meta_learner_error"] = str(ex)
        logger.warning("bitget meta_learner skip: %s", ex)

    try:
        from doomsday_dampener import evolve_gamma

        out["doomsday_gamma"] = evolve_gamma(sys_config=cfg, db_path=None, persist=True)
    except Exception as ex:
        out["doomsday_gamma_error"] = str(ex)
        logger.warning("bitget evolve_gamma skip: %s", ex)

    try:
        from bitget.forward.shared import DB_PATH
        from exit_ratchet_rl import evolve_ratchet_kappa

        out["ratchet_kappa"] = evolve_ratchet_kappa(cfg, db_path=DB_PATH)
    except Exception as ex:
        out["ratchet_kappa_error"] = str(ex)
        logger.warning("bitget ratchet_kappa skip: %s", ex)

    return out
