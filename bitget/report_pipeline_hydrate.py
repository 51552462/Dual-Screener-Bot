"""
Bitget daily report 직전 데이터 hydration (주식 report_pipeline_hydrate 패턴).

기본: DB 존재·경로 확인만 (4GB 서버 부하 방지).
전체 OHLCV 갱신은 `BITGET_REPORT_HYDRATE_FULL=1` 일 때만.
"""
from __future__ import annotations

import os
from typing import Any, Dict

from bitget.infra.logging_setup import get_logger

logger = get_logger("bitget.report_pipeline_hydrate")


def ensure_bitget_report_pipeline_data(
    *,
    refresh_ohlcv: bool = True,
) -> Dict[str, Any]:
    """daily_audit prelude 이후·track 직전 호출."""
    from bitget.infra.data_paths import market_data_db_path

    out: Dict[str, Any] = {"market": "CRYPTO", "ohlcv": "skipped", "db": market_data_db_path()}

    try:
        from bitget.macro_hydrate_bg import refresh_bitget_macro_daily
        from bitget.infra.config_manager import update_system_config

        macro = refresh_bitget_macro_daily()
        out["macro"] = macro
        src = str(macro.get("source") or "degraded")
        update_system_config({"BITGET_MACRO_FRESHNESS": src, "MACRO_DAILY_FRESHNESS": src})
    except Exception as ex:
        logger.warning("bitget report hydrate macro skip: %s", ex)
        out["macro"] = {"ok": False, "source": "degraded", "error": str(ex)}

    if not refresh_ohlcv:
        logger.info("report_pipeline_hydrate: %s", out)
        return out

    full = str(os.environ.get("BITGET_REPORT_HYDRATE_FULL", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if full:
        try:
            from bitget.mtf_data_updater import run_mtf_update

            run_mtf_update()
            out["ohlcv"] = "full_ok"
        except Exception as ex:
            logger.warning("bitget report hydrate OHLCV skip: %s", ex)
            out["ohlcv"] = f"degraded:{ex}"
    else:
        out["ohlcv"] = "light_ok" if os.path.isfile(market_data_db_path()) else "no_db"

    logger.info("report_pipeline_hydrate: %s", out)
    return out
