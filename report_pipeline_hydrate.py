"""
0~9번 리포트 직전 데이터 Hydration SSOT.

매크로·벤치마크·스냅샷 갱신 실패를 삼키지 않고 로그·반환 dict로 노출.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def refresh_macro_daily(*, force: bool = False) -> Dict[str, Any]:
    """alt_data.sqlite macro_daily — factory_data_dir 경로에 upsert."""
    try:
        from legacy_archive import alt_data_miner as adm

        row = adm.run_once()
        if row:
            return {"ok": True, "date": row.get("date"), "row": row}
        return {"ok": False, "error": "empty_row"}
    except Exception as ex:
        logger.warning("refresh_macro_daily failed: %s", ex, exc_info=True)
        return {"ok": False, "error": str(ex)}


def refresh_kr_benchmarks() -> Dict[str, Any]:
    try:
        from data_updater import run_kr_benchmark_refresh

        return run_kr_benchmark_refresh()
    except Exception as ex:
        logger.warning("refresh_kr_benchmarks failed: %s", ex, exc_info=True)
        return {"ok": False, "error": str(ex)}


def refresh_us_incremental() -> Dict[str, Any]:
    try:
        from data_updater import run_us_incremental_db_update

        return run_us_incremental_db_update()
    except Exception as ex:
        logger.warning("refresh_us_incremental failed: %s", ex, exc_info=True)
        return {"ok": False, "error": str(ex)}


def ensure_report_pipeline_data(
    *,
    market: Optional[str] = None,
    refresh_macro: bool = True,
    refresh_ohlcv: bool = True,
) -> Dict[str, Any]:
    """
    daily-kr / daily-us / comprehensive 리포트 직전 호출.
    market: 'KR' | 'US' | None(둘 다)
    """
    mk = str(market or "").upper()
    out: Dict[str, Any] = {"market": mk or "BOTH"}

    if refresh_macro:
        out["macro"] = refresh_macro_daily()

    if refresh_ohlcv:
        if mk in ("", "BOTH", "KR"):
            out["kr_benchmarks"] = refresh_kr_benchmarks()
        if mk in ("", "BOTH", "US"):
            out["us_incremental"] = refresh_us_incremental()

    try:
        from cross_market_ssot import hydrate_kr_runtime_from_ssot

        out["kr_hydrate"] = hydrate_kr_runtime_from_ssot()
    except Exception as ex:
        logger.warning("ensure_report_pipeline_data kr_hydrate: %s", ex)
        out["kr_hydrate_error"] = str(ex)

    print(f"💧 [ReportHydrate] {out}")
    return out
