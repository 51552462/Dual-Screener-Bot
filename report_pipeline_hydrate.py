"""
Bitget daily report 직전 데이터 hydration (주식 report_pipeline_hydrate 패턴).

기본: DB 존재·경로 확인만 (4GB 서버 부하 방지).
전체 OHLCV 갱신은 `BITGET_REPORT_HYDRATE_FULL=1` 일 때만.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


def ensure_bitget_report_pipeline_data(
    *,
    refresh_ohlcv: bool = True,
) -> Dict[str, Any]:
    """daily_audit prelude 이후·track 직전 호출."""
    from bitget.infra.data_paths import market_data_db_path

    out: Dict[str, Any] = {"market": "CRYPTO", "ohlcv": "skipped", "db": market_data_db_path()}

    if not refresh_ohlcv:
        print(f"🛰️ [Bitget] report_pipeline_hydrate: {out}")
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

    print(f"🛰️ [Bitget] report_pipeline_hydrate: {out}")
    return out
