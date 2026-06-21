"""
US Fluid Upstream Bridge — daily-us / health / track / toxic / spillover 통합.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def run_us_health_fluid_prelude(*, context: str = "daily") -> Dict[str, Any]:
    """us_health_gate 직후 — 앵커·독성 decay·zero-sample 준비."""
    from config_manager import load_system_config, save_system_config
    from factory_us_health import assess_us_pipeline_health, format_us_health_log_line
    from fluid_time_anchor import persist_anchor_state, resolve_us_with_db_fallback
    from toxic_decay_bandit import sync_decayed_toxic_to_config

    cfg = load_system_config() or {}
    anchor = resolve_us_with_db_fallback(cfg)
    persist_anchor_state(cfg, anchor)

    toxic_sync = sync_decayed_toxic_to_config(cfg)
    try:
        save_system_config(cfg)
    except Exception as ex:
        logger.debug("fluid prelude config save: %s", ex)

    health = assess_us_pipeline_health()
    health["fluid_anchor"] = {
        "mode": anchor.mode,
        "session": anchor.session_date,
        "reason": anchor.reason,
        "lag_bd": anchor.lag_business_days,
    }
    health["toxic_decay_sync"] = toxic_sync
    health["context"] = context
    print(
        f"🌊 [US Fluid] anchor={anchor.mode} session={anchor.session_date} "
        f"({anchor.reason}) | {format_us_health_log_line(health)}"
    )
    return health


def run_post_us_incremental_upstream(*, context: str = "daily") -> Dict[str, Any]:
    """
    us_data_incremental 직후 — zero-sample spillover + cross_market publish.
    """
    import os
    import sqlite3

    from config_manager import load_system_config
    from market_db_paths import MARKET_DATA_DB_PATH
    from zero_sample_spillover import apply_zero_sample_spillover, publish_zero_sample_cross_market

    cfg = load_system_config() or {}
    spill = apply_zero_sample_spillover(cfg, force_if_closed_zero=True)
    out: Dict[str, Any] = {"spillover": spill, "context": context}

    closed = 0
    if os.path.isfile(MARKET_DATA_DB_PATH):
        try:
            conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=10)
            try:
                closed = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM forward_trades WHERE market='US' AND status LIKE 'CLOSED%'"
                    ).fetchone()[0]
                    or 0
                )
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    if spill.get("applied") or closed == 0:
        try:
            ssot = publish_zero_sample_cross_market(cfg)
            out["cross_market"] = {"mode": ssot.get("mode"), "sector": ssot.get("us_sector_raw")}
        except Exception as ex:
            out["cross_market_error"] = str(ex)
            logger.warning("zero_sample publish: %s", ex)

    return out


def finalize_us_track_session(sys_config: Dict[str, Any], anchor) -> None:
    """track_daily_positions US 종료 시 session watermark."""
    if anchor is not None and hasattr(anchor, "mark_tracked"):
        anchor.mark_tracked(sys_config)
        try:
            from config_manager import save_system_config

            save_system_config(sys_config)
        except Exception:
            pass
