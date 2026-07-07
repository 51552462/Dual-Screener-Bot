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
        from bitget.evolution.regime_analog_bg import compute_coin_regime_analog

        out["regime_analog"] = compute_coin_regime_analog(
            cfg, persist=True, pri_blend_z=pri_blend_z
        )
    except Exception as ex:
        out["regime_analog_error"] = str(ex)
        logger.warning("bitget regime_analog skip: %s", ex)

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
        from bitget.evolution.exit_ratchet_rl_bg import evolve_bitget_ratchet_kappa

        out["ratchet_kappa"] = evolve_bitget_ratchet_kappa(cfg, persist=True)
    except Exception as ex:
        out["ratchet_kappa_error"] = str(ex)
        logger.warning("bitget ratchet_kappa skip: %s", ex)

    return out
