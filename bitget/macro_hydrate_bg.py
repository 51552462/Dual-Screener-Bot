"""Bitget macro refresh for report_pipeline_hydrate — alt_data.sqlite lookback SSOT."""
from __future__ import annotations

import logging
import math
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_MACRO_LOOKBACK_MAX_ROWS = 365
_MACRO_FLOAT_KEYS = (
    "btc_dominance",
    "eth_btc_ratio",
    "total_market_cap_usd",
    "market_cap_change_24h",
    "btc_price_usd",
    "eth_price_usd",
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
    if not isinstance(row, dict) or not row:
        return False
    return any(_coerce_float(row.get(k)) is not None for k in _MACRO_FLOAT_KEYS)


def _load_macro_row_lookback(*, max_rows: int = _MACRO_LOOKBACK_MAX_ROWS) -> Optional[Dict[str, Any]]:
    try:
        from bitget.infra.data_paths import alt_data_db_path

        path = alt_data_db_path()
        if not path or not os.path.isfile(path):
            return None
        conn = sqlite3.connect(path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT * FROM macro_daily ORDER BY date DESC LIMIT ?",
                (max(1, int(max_rows)),),
            ).fetchall()
            if not rows:
                return None
            today = datetime.utcnow().strftime("%Y-%m-%d")
            best_sparse = None
            for row in rows:
                dct = {k: row[k] for k in row.keys()}
                d = _normalize_date_key(dct.get("date"))
                if not d or d > today:
                    continue
                if best_sparse is None:
                    best_sparse = dct
                if _row_has_usable_macro_data(dct):
                    return dct
            return best_sparse or {k: rows[0][k] for k in rows[0].keys()}
        finally:
            conn.close()
    except Exception as ex:
        logger.error("bitget macro lookback failed: %s", ex)
        return None


def refresh_bitget_macro_daily(*, force: bool = False) -> Dict[str, Any]:
    """CoinGecko live → lookback → degraded (파이프라인 중단 금지)."""
    live_err: Optional[str] = None
    try:
        from bitget.alt_data_miner import run_once

        row = run_once()
        if isinstance(row, dict) and row and _row_has_usable_macro_data(row):
            return {
                "ok": True,
                "source": "live",
                "date": _normalize_date_key(row.get("date")),
                "row": row,
            }
        live_err = "empty_metrics" if row else "empty_row"
    except Exception as ex:
        live_err = str(ex)
        logger.warning("refresh_bitget_macro_daily live failed: %s", ex)

    stale = _load_macro_row_lookback()
    if stale:
        return {
            "ok": True,
            "source": "lookback",
            "fallback_reason": live_err,
            "date": _normalize_date_key(stale.get("date")),
            "row": stale,
        }
    return {
        "ok": True,
        "source": "degraded",
        "degraded": True,
        "fallback_reason": live_err or "no_lookback_row",
        "date": None,
        "row": None,
    }
