"""Bitget weekly regime archive — coin regime vector + BTC regime refresh."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def run_weekly_coin_regime_archive(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """주식 weekly_proprietary_regime + regime_deep_archive 의 코인 대응."""
    cfg: Dict[str, Any]
    if isinstance(sys_config, dict):
        cfg = sys_config
    else:
        try:
            from bitget.infra.config_manager import load_system_config

            cfg = load_system_config() or {}
        except Exception:
            cfg = {}

    out: Dict[str, Any] = {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}

    try:
        from bitget.auto_pilot import detect_coin_regime

        regime = detect_coin_regime(cfg)
        out["regime_key"] = regime
    except Exception as ex:
        out["regime_error"] = str(ex)
        logger.warning("weekly_coin_regime: detect_coin_regime failed: %s", ex)

    try:
        from bitget.evolution.weekly_proprietary_regime_bg import compute_weekly_coin_pri

        pri = compute_weekly_coin_pri()
        out["pri"] = pri.get("blended")
    except Exception as ex:
        out["pri_error"] = str(ex)
        logger.warning("weekly_coin_regime: PRI failed: %s", ex)

    try:
        from bitget.evolution.coin_regime_vector import (
            append_coin_regime_vector_history,
            build_current_coin_regime_vector,
            load_vector_history,
        )

        append_coin_regime_vector_history(cfg)
        vec = build_current_coin_regime_vector(cfg)
        hist = load_vector_history(cfg)
        out["vector"] = vec.get("vector_map")
        out["history_len"] = len(hist)
    except Exception as ex:
        out["vector_error"] = str(ex)
        logger.warning("weekly_coin_regime: vector archive failed: %s", ex)

    try:
        from bitget.infra.config_manager import update_config_value

        archive_entry = {
            "ts": out["ts"],
            "regime_key": out.get("regime_key"),
            "vector": out.get("vector"),
            "history_len": out.get("history_len", 0),
            "pri": out.get("pri"),
        }

        def _modifier(old: Any) -> Any:
            buf = old if isinstance(old, list) else []
            buf.append(archive_entry)
            return buf[-52:]

        update_config_value("WEEKLY_REGIME_ARCHIVE_BG", _modifier)
        out["archive"] = "ok"
    except Exception as ex:
        out["archive_error"] = str(ex)

    return out
