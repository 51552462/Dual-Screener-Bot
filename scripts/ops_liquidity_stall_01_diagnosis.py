"""
OPS-LIQUIDITY-STALL-01 — LIQUIDITY 100% stall 원인 4분류 (read-only).

Mirrors supernova_hunter LIQUIDITY gates (read-only; no threshold changes):
  KR Close < 1000, US Close < 0.5,
  5d mean Volume < floor (US: max(2000, 300000/close)).

    python scripts/ops_liquidity_stall_01_diagnosis.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from market_data_fetcher import fetch_market_data
from market_db_paths import MARKET_DATA_DB_PATH

STALL_SINCE = "2026-08-17 15:10"
SAMPLE_N = 20
DEFECT_SHARE_ALERT = 0.30  # (c)+(d) >= 30% => data/code defect hypothesis


def _connect() -> sqlite3.Connection:
    if not os.path.isfile(MARKET_DATA_DB_PATH):
        raise SystemExit(f"FAIL DB missing: {MARKET_DATA_DB_PATH}")
    conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _sample_codes(conn: sqlite3.Connection, market: str) -> list[tuple[str, str]]:
    """Return list of (code, ts) up to SAMPLE_N for LIQUIDITY drops since stall."""
    rows = conn.execute(
        """
        SELECT code, ts
        FROM scan_funnel_drop_event
        WHERE market = ?
          AND UPPER(TRIM(reason)) = 'LIQUIDITY'
          AND ts >= ?
          AND code IS NOT NULL
          AND TRIM(code) != ''
        ORDER BY ts DESC, id DESC
        """,
        (market, STALL_SINCE),
    ).fetchall()
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        code = str(r["code"]).strip()
        if code in seen:
            continue
        seen.add(code)
        out.append((code, str(r["ts"])))
        if len(out) >= SAMPLE_N:
            break
    return out


def _min_vol_floor(market: str, close: float) -> float:
    """Same formula as supernova_hunter (read-only mirror)."""
    _min_vol = 50_000.0
    if market == "US":
        _us_dollar_floor = 300_000.0
        _min_vol = max(2_000.0, _us_dollar_floor / max(float(close), 0.01))
    return float(_min_vol)


def _classify_row(
    market: str, df: Optional[pd.DataFrame]
) -> tuple[str, dict[str, Any]]:
    """
    (a) price cut fires normally
    (b) volume/turnover cut fires normally
    (c) Volume/Close missing/0/anomaly or fetch fail
    (d) would pass both gates -> threshold application suspicion
    """
    meta: dict[str, Any] = {
        "close": None,
        "vol5": None,
        "min_vol": None,
        "fetch": "ok" if df is not None and not df.empty else "fail",
    }
    if df is None or df.empty:
        return "c", meta
    try:
        if "Close" not in df.columns or "Volume" not in df.columns:
            meta["fetch"] = "bad_cols"
            return "c", meta
        closes = pd.to_numeric(df["Close"], errors="coerce")
        vols = pd.to_numeric(df["Volume"], errors="coerce")
        if len(closes) < 1 or pd.isna(closes.iloc[-1]):
            return "c", meta
        close = float(closes.iloc[-1])
        meta["close"] = close
        tail = vols.iloc[-5:] if len(vols) >= 1 else vols
        if tail.empty or bool(tail.isna().all()) or float(np.nansum(tail.values)) <= 0:
            meta["vol5"] = float(np.nanmean(tail.values)) if len(tail) else None
            return "c", meta
        vol5 = float(np.nanmean(tail.values))
        meta["vol5"] = vol5
        if vol5 <= 0 or not np.isfinite(vol5):
            return "c", meta

        # (a) price floors
        if market == "KR" and close < 1000:
            return "a", meta
        if market == "US" and close < 0.5:
            return "a", meta

        min_vol = _min_vol_floor(market, close)
        meta["min_vol"] = min_vol
        if vol5 < min_vol:
            return "b", meta

        # Would pass both gates but was logged LIQUIDITY
        return "d", meta
    except Exception as ex:  # noqa: BLE001
        meta["fetch"] = f"exc:{type(ex).__name__}"
        return "c", meta


def _window_for_ts(ts: str) -> tuple[str, str]:
    """~15 calendar days ending at drop date (enough for 5 sessions)."""
    day = str(ts)[:10]
    try:
        end = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        end = datetime.utcnow()
        day = end.strftime("%Y-%m-%d")
    start = (end - timedelta(days=21)).strftime("%Y-%m-%d")
    return start, day


def diagnose_market(conn: sqlite3.Connection, market: str) -> Counter:
    print(f"\n=== {market} sample (LIQUIDITY since {STALL_SINCE}, N<={SAMPLE_N}) ===")
    samples = _sample_codes(conn, market)
    if not samples:
        print("  (no LIQUIDITY codes with non-null code in window)")
        return Counter()
    counts: Counter = Counter()
    for code, ts in samples:
        start, end = _window_for_ts(ts)
        df = fetch_market_data(code, market, start, end)
        label, meta = _classify_row(market, df)
        counts[label] += 1
        print(
            f"  {label} code={code[:12]:12} ts={ts[:16]} "
            f"close={meta.get('close')} vol5={meta.get('vol5')} "
            f"min_vol={meta.get('min_vol')} fetch={meta.get('fetch')}"
        )
    n = sum(counts.values()) or 1
    print(f"  summary {market}: " + ", ".join(f"{k}={counts[k]}" for k in "abcd" if counts[k]))
    defect = counts["c"] + counts["d"]
    print(
        f"  defect_share (c+d)={defect}/{n} = {100.0 * defect / n:.1f}% "
        f"(alert>={100 * DEFECT_SHARE_ALERT:.0f}%)"
    )
    return counts


def main() -> int:
    print(f"DB: {MARKET_DATA_DB_PATH}")
    print(f"STALL_SINCE={STALL_SINCE} SAMPLE_N={SAMPLE_N}")
    print("NOTE: Step3 drop_event vs survivors gap -> L-DATA-ALARM-01 backlog (out of scope)")
    conn = _connect()
    try:
        total: Counter = Counter()
        for mk in ("KR", "US"):
            total.update(diagnose_market(conn, mk))
        n = sum(total.values())
        print("\n=== CLASSIFICATION ===")
        if n == 0:
            print("FAIL: empty sample - check drop_event codes / STALL_SINCE")
            return 2
        for k in "abcd":
            print(f"  ({k}) n={total[k]} ({100.0 * total[k] / n:.1f}%)")
        defect = total["c"] + total["d"]
        share = defect / n
        if share >= DEFECT_SHARE_ALERT:
            print(
                f"VERDICT: DATA/CODE DEFECT HYPOTHESIS "
                f"(c+d={share:.0%} >= {DEFECT_SHARE_ALERT:.0%})"
            )
        else:
            print(
                "VERDICT: GATE WORKING + low-liquidity concentration "
                f"(c+d={share:.0%} < {DEFECT_SHARE_ALERT:.0%})"
            )
        print("OBS-HOLD: no config/cutoff/threshold changes applied.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
