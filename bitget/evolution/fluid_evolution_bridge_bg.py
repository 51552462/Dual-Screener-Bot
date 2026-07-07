"""Bitget post-meta fluid sync — exploration budget + regime vector (주식 SSOT 미참조)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def post_bitget_meta_governor_fluid_sync(sys_config: Optional[Dict[str, Any]] = None) -> None:
    """meta_governor_sync 직후 — Bitget 전용 경량 fluid 훅."""
    cfg = sys_config
    if not isinstance(cfg, dict):
        try:
            from bitget.infra.config_manager import load_system_config

            cfg = load_system_config() or {}
        except Exception:
            cfg = {}

    try:
        from bitget.governance.exploration_budget import refresh_exploration_budget_state

        refresh_exploration_budget_state()
    except Exception as ex:
        logger.debug("bitget fluid: exploration_budget skip: %s", ex)

    try:
        from bitget.evolution.coin_regime_vector import append_coin_regime_vector_history

        append_coin_regime_vector_history(cfg)
    except Exception as ex:
        logger.debug("bitget fluid: regime vector skip: %s", ex)
