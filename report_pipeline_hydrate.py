"""
0~9번 리포트 직전 데이터 Hydration SSOT.

매크로·벤치마크·스냅샷 갱신 실패 시 lookback 폴백 + 명시적 예외( silent fail 금지 ).
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_MACRO_LOOKBACK_DAYS = 2


def _normalize_date_key(raw: Any) -> str:
    s = str(raw or "").strip().replace("T", " ").replace("/", "-")
    return s[:10] if len(s) >= 10 else s


def _load_macro_row_lookback(*, days: int = _MACRO_LOOKBACK_DAYS) -> Optional[Dict[str, Any]]:
    """alt_data.sqlite — 오늘 실패 시 최근 N일 중 최신 행."""
    try:
        from factory_data_paths import alt_data_db_path

        path = alt_data_db_path()
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM macro_daily ORDER BY date DESC LIMIT ?",
                (max(1, int(days)),),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            today = datetime.now().strftime("%Y-%m-%d")
            for row in rows:
                d = _normalize_date_key(row["date"])
                if d and d <= today:
                    return {k: row[k] for k in row.keys()}
            return {k: rows[0][k] for k in rows[0].keys()}
        finally:
            conn.close()
    except Exception as ex:
        logger.error("macro lookback load failed: %s", ex, exc_info=True)
        return None


def refresh_macro_daily(*, force: bool = False) -> Dict[str, Any]:
    """alt_data.sqlite macro_daily — factory_data_dir 경로에 upsert."""
    live_err: Optional[str] = None
    try:
        from legacy_archive import alt_data_miner as adm

        row = adm.run_once()
        if row:
            return {"ok": True, "source": "live", "date": row.get("date"), "row": row}
        live_err = "empty_row"
    except Exception as ex:
        live_err = str(ex)
        logger.error("refresh_macro_daily live fetch failed: %s", ex, exc_info=True)

    stale = _load_macro_row_lookback(days=_MACRO_LOOKBACK_DAYS)
    if stale:
        d = _normalize_date_key(stale.get("date"))
        logger.warning(
            "refresh_macro_daily: live failed (%s); using lookback macro_daily date=%s",
            live_err,
            d,
        )
        return {
            "ok": True,
            "source": "lookback",
            "fallback_reason": live_err,
            "date": d,
            "row": stale,
        }

    err = live_err or "no_lookback_row"
    logger.error("refresh_macro_daily FAILED — no live row and no lookback within %s days", _MACRO_LOOKBACK_DAYS)
    return {"ok": False, "error": err}


def refresh_kr_benchmarks() -> Dict[str, Any]:
    try:
        from data_updater import run_kr_benchmark_refresh

        out = run_kr_benchmark_refresh()
        if isinstance(out, dict) and out.get("ok") is False:
            logger.error("refresh_kr_benchmarks returned failure: %s", out)
        return out
    except Exception as ex:
        logger.error("refresh_kr_benchmarks failed: %s", ex, exc_info=True)
        return {"ok": False, "error": str(ex)}


def refresh_us_incremental() -> Dict[str, Any]:
    try:
        from data_updater import run_us_incremental_db_update

        out = run_us_incremental_db_update()
        if isinstance(out, dict) and out.get("ok") is False:
            logger.error("refresh_us_incremental returned failure: %s", out)
        return out
    except Exception as ex:
        logger.error("refresh_us_incremental failed: %s", ex, exc_info=True)
        return {"ok": False, "error": str(ex)}


def _step_failed(step: Dict[str, Any]) -> bool:
    if not isinstance(step, dict):
        return True
    if step.get("ok") is False:
        return True
    if step.get("error"):
        return True
    return False


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
    errors: List[str] = []

    if refresh_macro:
        out["macro"] = refresh_macro_daily()
        if _step_failed(out["macro"]):
            errors.append(f"macro:{out['macro'].get('error', 'failed')}")

    if refresh_ohlcv:
        if mk in ("", "BOTH", "KR"):
            out["kr_benchmarks"] = refresh_kr_benchmarks()
            if _step_failed(out["kr_benchmarks"]):
                errors.append(f"kr_benchmarks:{out['kr_benchmarks'].get('error', 'failed')}")
        if mk in ("", "BOTH", "US"):
            out["us_incremental"] = refresh_us_incremental()
            if _step_failed(out["us_incremental"]):
                errors.append(f"us_incremental:{out['us_incremental'].get('error', 'failed')}")

    try:
        from cross_market_ssot import hydrate_kr_runtime_from_ssot

        out["kr_hydrate"] = hydrate_kr_runtime_from_ssot()
    except Exception as ex:
        logger.error("ensure_report_pipeline_data kr_hydrate failed: %s", ex, exc_info=True)
        out["kr_hydrate_error"] = str(ex)
        errors.append(f"kr_hydrate:{ex}")

    print(f"💧 [ReportHydrate] {out}")
    if errors:
        msg = "ReportHydrate failed: " + "; ".join(errors)
        logger.error(msg)
        raise RuntimeError(msg)
    return out
