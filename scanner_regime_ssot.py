"""
장중 스캐너 SSOT — Meta↔config 국면 정렬 + Graceful Kelly 베이스 주입.

supernova_hunter / bitget_supernova_hunter 등 try_add 직전 config 에 반영.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def hydrate_intraday_scanner_config(
    config: Optional[Dict[str, Any]],
    *,
    market: str = "",
    persist_kelly: bool = False,
) -> Dict[str, Any]:
    """
    1) ensure_config_regime_aligned (Meta→config_kv)
    2) DYNAMIC_KELLY_RISK ← resolve_trading_kelly_base (Graceful fail-safe)
    반환: 동일 dict (in-place 갱신).
    """
    cfg: Dict[str, Any] = dict(config) if isinstance(config, dict) else {}
    meta: Dict[str, Any] = {}
    try:
        from meta_state_store import ensure_config_regime_aligned

        ensure_config_regime_aligned()
    except Exception as e:
        logger.debug("scanner_regime_ssot: regime align skip: %s", e)

    try:
        from config_manager import load_system_config

        merged = load_system_config()
        if isinstance(merged, dict):
            for k, v in merged.items():
                if k not in cfg or cfg.get(k) in (None, "", {}):
                    cfg[k] = v
    except Exception:
        pass

    try:
        from meta_governor_consumer import load_meta_state_resolved, resolve_trading_kelly_base

        meta = load_meta_state_resolved()
        base_k = resolve_trading_kelly_base(cfg, meta)
        cfg["DYNAMIC_KELLY_RISK"] = round(float(base_k), 4)
        cfg["_SCANNER_KELLY_SSOT"] = {
            "market": str(market or "").upper(),
            "base_kelly": cfg["DYNAMIC_KELLY_RISK"],
            "meta_regime": str(meta.get("META_REGIME_KEY") or ""),
        }
        if persist_kelly:
            try:
                from config_manager import update_system_config

                update_system_config({"DYNAMIC_KELLY_RISK": cfg["DYNAMIC_KELLY_RISK"]})
            except Exception as ex:
                logger.debug("scanner_regime_ssot: persist kelly skip: %s", ex)
    except Exception as e:
        logger.warning("scanner_regime_ssot: kelly hydrate failed: %s", e)

    return cfg
