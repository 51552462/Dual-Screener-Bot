"""
0~9번 리포트 직전 데이터 Hydration SSOT.

매크로·벤치마크·스냅샷 갱신 실패 시 lookback 폴백 — live API empty_row 는 파이프라인 중단 금지.
"""
from __future__ import annotations

import logging
import math
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# macro_daily 전체 역사에서 최신 유효 행 탐색 (기존 2행 LIMIT 제거)
_MACRO_LOOKBACK_MAX_ROWS = 365

_MACRO_FLOAT_KEYS = (
    "usd_krw",
    "us_10y_yield",
    "vix_index",
    "btc_close",
    "t10y2y",
    "dfii10",
    "walcl",
    "cnn_fear_greed",
    "put_call_ratio",
)


def _normalize_date_key(raw: Any) -> str:
    s = str(raw or "").strip().replace("T", " ").replace("/", "-")
    return s[:10] if len(s) >= 10 else s


def _coerce_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _row_has_usable_macro_data(row: Optional[Dict[str, Any]]) -> bool:
    """적어도 하나의 매크로 지표가 숫자로 존재하면 usable."""
    if not isinstance(row, dict) or not row:
        return False
    for key in _MACRO_FLOAT_KEYS:
        if _coerce_float(row.get(key)) is not None:
            return True
    return False


def _row_from_sqlite(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _load_macro_row_lookback(
    *,
    max_rows: int = _MACRO_LOOKBACK_MAX_ROWS,
) -> Optional[Dict[str, Any]]:
    """
    alt_data.sqlite macro_daily — 최신 행부터 끝까지 탐색.
    숫자 지표가 있는 첫 행 우선, 없으면 최신 date 행이라도 반환.
    """
    try:
        from factory_data_paths import alt_data_db_path

        path = alt_data_db_path()
        conn = sqlite3.connect(path, timeout=30)
        try:
            try:
                from legacy_archive.alt_data_miner import _migrate_extra_columns

                _migrate_extra_columns(conn)
            except Exception:
                pass
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM macro_daily ORDER BY date DESC LIMIT ?",
                (max(1, int(max_rows)),),
            )
            rows = cur.fetchall()
            if not rows:
                return None

            best_sparse: Optional[Dict[str, Any]] = None
            today = datetime.now().strftime("%Y-%m-%d")
            for row in rows:
                dct = _row_from_sqlite(row)
                d = _normalize_date_key(dct.get("date"))
                if not d:
                    continue
                if d > today:
                    continue
                if best_sparse is None:
                    best_sparse = dct
                if _row_has_usable_macro_data(dct):
                    return dct
            if best_sparse is not None:
                return best_sparse
            return _row_from_sqlite(rows[0])
        finally:
            conn.close()
    except Exception as ex:
        logger.error("macro lookback load failed: %s", ex, exc_info=True)
        return None


def _load_macro_row_lookback_via_miner() -> Optional[Dict[str, Any]]:
    """alt_data_miner._load_last_row — 동일 DB SSOT 보조 경로."""
    try:
        from legacy_archive.alt_data_miner import _load_last_row

        row = _load_last_row()
        if isinstance(row, dict) and row:
            return row
    except Exception as ex:
        logger.debug("macro lookback via miner skip: %s", ex)
    return None


def _resolve_macro_lookback(live_err: Optional[str]) -> Optional[Dict[str, Any]]:
    """DB 전체 lookback + miner 보조 — live 실패 시 무조건 시도."""
    stale = _load_macro_row_lookback()
    if stale is None:
        stale = _load_macro_row_lookback_via_miner()
    if stale:
        d = _normalize_date_key(stale.get("date"))
        logger.warning(
            "refresh_macro_daily: live failed (%s); using lookback macro_daily date=%s "
            "(usable_metrics=%s)",
            live_err,
            d,
            _row_has_usable_macro_data(stale),
        )
        return {
            "ok": True,
            "source": "lookback",
            "fallback_reason": live_err,
            "date": d,
            "row": stale,
            "usable_metrics": _row_has_usable_macro_data(stale),
        }
    return None


def refresh_macro_daily(*, force: bool = False) -> Dict[str, Any]:
    """alt_data.sqlite macro_daily — live 실패·empty_row 시 lookback, 없으면 degraded 완주."""
    live_err: Optional[str] = None
    try:
        from legacy_archive import alt_data_miner as adm

        row = adm.run_once()
        if isinstance(row, dict) and row:
            if _row_has_usable_macro_data(row):
                return {
                    "ok": True,
                    "source": "live",
                    "date": _normalize_date_key(row.get("date")),
                    "row": row,
                }
            live_err = "empty_metrics"
            logger.warning(
                "refresh_macro_daily: live row returned but all metrics null — trying lookback"
            )
        else:
            live_err = "empty_row"
            logger.warning("refresh_macro_daily: run_once returned empty — trying lookback")
    except Exception as ex:
        live_err = str(ex)
        logger.error("refresh_macro_daily live fetch failed: %s", ex, exc_info=True)

    lookback = _resolve_macro_lookback(live_err)
    if lookback is not None:
        return lookback

    err = live_err or "no_lookback_row"
    logger.error(
        "refresh_macro_daily: no live row and no macro_daily in DB — "
        "continuing pipeline in degraded mode (%s)",
        err,
    )
    return {
        "ok": True,
        "source": "degraded",
        "degraded": True,
        "fallback_reason": err,
        "date": None,
        "row": None,
        "warning": err,
    }


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
    if step.get("degraded"):
        return False
    if step.get("ok") is False:
        return True
    if step.get("error"):
        return True
    return False


def _macro_step_failed(step: Dict[str, Any]) -> bool:
    """매크로는 lookback/degraded 도 완주 허용 — hard fail 만 차단."""
    if not isinstance(step, dict):
        return True
    if step.get("ok") is True:
        if step.get("source") in ("live", "lookback", "degraded"):
            return False
    return _step_failed(step)


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
        macro = out["macro"]
        # [Mission 3] 신선도(live/lookback/degraded)를 SSOT 에 영속화 → 모든 [1/9] 렌더가 태그 표시.
        try:
            from config_manager import update_system_config

            update_system_config({"MACRO_DAILY_FRESHNESS": str(macro.get("source") or "")})
        except Exception as _mf_ex:
            logger.debug("MACRO_DAILY_FRESHNESS persist skip: %s", _mf_ex)
        if macro.get("source") == "lookback":
            logger.warning(
                "ReportHydrate macro lookback active date=%s reason=%s",
                macro.get("date"),
                macro.get("fallback_reason"),
            )
        elif macro.get("degraded"):
            logger.warning(
                "ReportHydrate macro degraded (no DB row) reason=%s — pipeline continues",
                macro.get("fallback_reason"),
            )
        elif _macro_step_failed(macro):
            errors.append(f"macro:{macro.get('error', macro.get('warning', 'failed'))}")

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
