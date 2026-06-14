"""
Scan load benchmark — table/symbol counts and optional timed scan probe.
"""
from __future__ import annotations

import os
import sqlite3
import time
from typing import Any

from bitget.infra.data_paths import market_db_read_path


def _parse_table_names(rows: list[tuple]) -> list[str]:
    return [r[0] for r in rows if r and "__tmp" not in str(r[0])]


def _unique_symbols(table_names: list[str]) -> set[str]:
    syms: set[str] = set()
    for t in table_names:
        if not str(t).startswith("BITGET_"):
            continue
        parts = str(t).split("_")
        if len(parts) < 4:
            continue
        syms.add("_".join(parts[2:-1]))
    return syms


def run_load_test(
    *,
    min_symbols: int = 500,
    min_timeframes: int = 4,
    max_elapsed_sec: float = 600.0,
    probe_tables: int = 200,
) -> dict[str, Any]:
    """
    Measures read-path DB scan capacity. Full run_scan is optional via env
    BITGET_LOAD_TEST_RUN_SCAN=1 (not run by default — side effects).
    """
    db = market_db_read_path()
    if not os.path.isfile(db):
        return {"ok": False, "passed": False, "reason": "db_missing", "db": db}

    t0 = time.perf_counter()
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=60)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_%'"
        ).fetchall()
        table_names = _parse_table_names(rows)
        symbols = _unique_symbols(table_names)
        tfs = set()
        for t in table_names:
            if "_" in t:
                tfs.add(str(t).rsplit("_", 1)[-1].upper())

        probed = 0
        for t in table_names[: max(0, int(probe_tables))]:
            try:
                conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()
                probed += 1
            except sqlite3.Error:
                pass
    finally:
        conn.close()
    elapsed = time.perf_counter() - t0

    sym_ok = len(symbols) >= int(min_symbols)
    tf_ok = len(tfs) >= int(min_timeframes)
    time_ok = elapsed <= float(max_elapsed_sec)
    passed = sym_ok and tf_ok and time_ok

    result: dict[str, Any] = {
        "ok": True,
        "passed": passed,
        "elapsed_sec": round(elapsed, 3),
        "max_elapsed_sec": float(max_elapsed_sec),
        "table_count": len(table_names),
        "symbol_count": len(symbols),
        "min_symbols": int(min_symbols),
        "timeframe_count": len(tfs),
        "min_timeframes": int(min_timeframes),
        "tables_probed": probed,
        "db": db,
        "checks": {
            "symbols": sym_ok,
            "timeframes": tf_ok,
            "elapsed": time_ok,
        },
    }

    if os.environ.get("BITGET_LOAD_TEST_RUN_SCAN", "").strip() in ("1", "true", "yes"):
        t1 = time.perf_counter()
        try:
            from bitget.master_scanner import run_scan

            run_scan(market_filter="spot")
            result["scan_spot_sec"] = round(time.perf_counter() - t1, 3)
        except Exception as e:
            result["scan_spot_error"] = str(e)

    return result
