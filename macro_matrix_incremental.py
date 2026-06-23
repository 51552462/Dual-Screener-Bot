"""
MACRO_EVOLUTION_MATRIX — forward_trades 증분 갱신 (주말 5초 컷).

유동성 국면(UP/DOWN/SIDEWAYS) × 전략 arm 별 누적 통계만 유지.
풀스캔 금지: watermark 이후 신규 CLOSED 만 덧셈.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from factory_data_paths import factory_data_dir, market_data_db_path

logger = logging.getLogger(__name__)

MATRIX_FILENAME = "MACRO_EVOLUTION_MATRIX.json"
SCHEMA_VERSION = 1
LOOKBACK_YEARS = 3
MIN_CELL_N = 3


def matrix_path() -> str:
    return os.path.join(factory_data_dir(), MATRIX_FILENAME)


def _empty_matrix() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "last_exit_date_watermark": "",
        "last_updated_at": "",
        "lookback_years": LOOKBACK_YEARS,
        "cells": {},
        "regime_by_date": {},
        "totals": {"n": 0, "sum_ret": 0.0},
    }


def load_macro_matrix(path: Optional[str] = None) -> Dict[str, Any]:
    p = path or matrix_path()
    if not os.path.isfile(p):
        return _empty_matrix()
    try:
        with open(p, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else _empty_matrix()
    except (OSError, json.JSONDecodeError) as ex:
        logger.warning("macro matrix load failed: %s", ex)
        return _empty_matrix()


def save_macro_matrix(matrix: Dict[str, Any], path: Optional[str] = None) -> str:
    p = path or matrix_path()
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    tmp = f"{p}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(matrix, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, p)
    return p


def _cell_key(regime: str, arm: str) -> str:
    return f"{str(regime).upper()}|{str(arm)}"


def _touch_cell(cells: Dict[str, Any], key: str) -> Dict[str, Any]:
    if key not in cells or not isinstance(cells[key], dict):
        cells[key] = {"n": 0, "sum_ret": 0.0, "sum_ret_sq": 0.0}
    return cells[key]


def _add_observation(cell: Dict[str, Any], ret_pct: float) -> None:
    r = float(ret_pct)
    if not np.isfinite(r):
        return
    cell["n"] = int(cell.get("n", 0)) + 1
    cell["sum_ret"] = float(cell.get("sum_ret", 0.0)) + r
    cell["sum_ret_sq"] = float(cell.get("sum_ret_sq", 0.0)) + r * r


def _normalize_exit_date(val: Any) -> str:
    s = str(val or "").strip()[:10]
    return s if len(s) == 10 and s[4] == "-" else ""


def _fetch_closed_since(
    conn: sqlite3.Connection,
    since_exclusive: str,
    *,
    lookback_floor: str,
) -> pd.DataFrame:
    floor = lookback_floor if lookback_floor else since_exclusive
    q = """
        SELECT exit_date, trade_date, sig_type, final_ret, market, status
        FROM forward_trades
        WHERE status LIKE 'CLOSED%'
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
          AND COALESCE(NULLIF(TRIM(exit_date), ''), NULLIF(TRIM(trade_date), '')) > ?
          AND COALESCE(NULLIF(TRIM(exit_date), ''), NULLIF(TRIM(trade_date), '')) >= ?
        ORDER BY exit_date ASC
    """
    df = pd.read_sql(q, conn, params=(since_exclusive, floor))
    if df.empty:
        return df
    df = df.copy()
    df["_exit"] = df.apply(
        lambda r: _normalize_exit_date(r.get("exit_date"))
        or _normalize_exit_date(r.get("trade_date")),
        axis=1,
    )
    return df.loc[df["_exit"] != ""].copy()


def _resolve_regime_for_date(
    exit_date: str,
    regime_by_date: Dict[str, str],
    default: str = "SIDEWAYS",
) -> str:
    r = regime_by_date.get(exit_date)
    if r in ("UP", "DOWN", "SIDEWAYS"):
        return r
    return default


def update_macro_matrix_incremental(
    *,
    db_path: Optional[str] = None,
    regime_by_date: Optional[Dict[str, str]] = None,
    bootstrap_if_missing: bool = True,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    증분 갱신. 반환: (matrix, stats).
    stats: {elapsed_sec, new_trades, cells_touched, mode}
    """
    t0 = time.perf_counter()
    matrix = load_macro_matrix()
    cells: Dict[str, Any] = matrix.setdefault("cells", {})
    if not isinstance(cells, dict):
        cells = {}
        matrix["cells"] = cells

    if regime_by_date:
        merged_regime = dict(matrix.get("regime_by_date") or {})
        merged_regime.update(regime_by_date)
        # 최근 500일만 유지 (RAM·파일 크기)
        if len(merged_regime) > 500:
            keys = sorted(merged_regime.keys())[-500:]
            merged_regime = {k: merged_regime[k] for k in keys}
        matrix["regime_by_date"] = merged_regime
    regime_map: Dict[str, str] = dict(matrix.get("regime_by_date") or {})

    watermark = str(matrix.get("last_exit_date_watermark") or "").strip()
    lookback_floor = (
        datetime.now(timezone.utc) - timedelta(days=365 * LOOKBACK_YEARS)
    ).strftime("%Y-%m-%d")

    if bootstrap_if_missing and not watermark:
        watermark = lookback_floor
        mode = "bootstrap"
    else:
        mode = "incremental"

    db = db_path or market_data_db_path()
    if not db or not os.path.isfile(db):
        stats = {
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "new_trades": 0,
            "cells_touched": 0,
            "mode": "skip_no_db",
        }
        return matrix, stats

    from evolution.deathmatch_report import classify_strategy_arm

    conn = sqlite3.connect(db, timeout=60)
    try:
        df_new = _fetch_closed_since(conn, watermark, lookback_floor=lookback_floor)
    finally:
        conn.close()

    new_n = 0
    touched: set[str] = set()
    max_exit = watermark

    for _, row in df_new.iterrows():
        exit_d = str(row.get("_exit") or "")
        if not exit_d:
            continue
        arm = classify_strategy_arm(row.get("sig_type"))
        if not arm:
            continue
        try:
            ret = float(pd.to_numeric(row.get("final_ret"), errors="coerce"))
        except (TypeError, ValueError):
            continue
        if not np.isfinite(ret):
            continue

        regime = _resolve_regime_for_date(exit_d, regime_map)
        ck = _cell_key(regime, arm)
        cell = _touch_cell(cells, ck)
        _add_observation(cell, ret)
        touched.add(ck)
        new_n += 1

        tot = matrix.setdefault("totals", {"n": 0, "sum_ret": 0.0})
        tot["n"] = int(tot.get("n", 0)) + 1
        tot["sum_ret"] = float(tot.get("sum_ret", 0.0)) + ret

        if exit_d > max_exit:
            max_exit = exit_d

    if max_exit and max_exit != watermark:
        matrix["last_exit_date_watermark"] = max_exit
    matrix["last_updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    matrix["schema_version"] = SCHEMA_VERSION

    save_macro_matrix(matrix)
    stats = {
        "elapsed_sec": round(time.perf_counter() - t0, 3),
        "new_trades": new_n,
        "cells_touched": len(touched),
        "mode": mode,
        "watermark": matrix.get("last_exit_date_watermark"),
    }
    return matrix, stats


def cell_mean_ret(cell: Optional[Dict[str, Any]]) -> Optional[float]:
    if not cell or not isinstance(cell, dict):
        return None
    n = int(cell.get("n", 0))
    if n < MIN_CELL_N:
        return None
    return float(cell.get("sum_ret", 0.0)) / n
