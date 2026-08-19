"""
OPS-LIQ-FORK-01 Phase 1 — quality-band liquidity percentile (read-only).

Reuses LIQUIDITY stall sample (since 2026-08-17 15:10, N<=20/market).
Adds avg_vol_5d, avg_dollar_vol_5d, market_percentile_dollar_vol vs DB universe.
No threshold / config / funnel logic changes.

    python scripts/ops_liq_fork_01_quality_band_phase1.py
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
# Display buckets only (not gate cutoffs) — Handoff: no invented policy thresholds
PCT_LOW = 33.333
PCT_HIGH = 66.667

_SKIP_SUFFIX = {
    "KOSPI_IDX",
    "KOSDAQ_IDX",
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
}


def _connect() -> sqlite3.Connection:
    if not os.path.isfile(MARKET_DATA_DB_PATH):
        raise SystemExit(f"FAIL DB missing: {MARKET_DATA_DB_PATH}")
    conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    return conn


def _sample_codes(conn: sqlite3.Connection, market: str) -> list[tuple[str, str]]:
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
    _min_vol = 50_000.0
    if market == "US":
        _min_vol = max(2_000.0, 300_000.0 / max(float(close), 0.01))
    return float(_min_vol)


def _classify_abd(
    market: str, close: Optional[float], vol5: Optional[float]
) -> str:
    if close is None or vol5 is None or not np.isfinite(close) or not np.isfinite(vol5):
        return "c"
    if vol5 <= 0:
        return "c"
    if market == "KR" and close < 1000:
        return "a"
    if market == "US" and close < 0.5:
        return "a"
    if vol5 < _min_vol_floor(market, close):
        return "b"
    return "d"


def _metrics_from_df(df: Optional[pd.DataFrame]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "close": None,
        "avg_vol_5d": None,
        "avg_dollar_vol_5d": None,
        "fetch": "fail",
    }
    if df is None or df.empty:
        return out
    try:
        closes = pd.to_numeric(df["Close"], errors="coerce")
        vols = pd.to_numeric(df["Volume"], errors="coerce")
        tail_c = closes.iloc[-5:]
        tail_v = vols.iloc[-5:]
        if tail_c.empty or bool(tail_c.isna().all()):
            return out
        close = float(tail_c.iloc[-1])
        vol5 = float(np.nanmean(tail_v.values))
        dollar = float(np.nanmean((tail_c * tail_v).values))
        out.update(
            {
                "close": close,
                "avg_vol_5d": vol5,
                "avg_dollar_vol_5d": dollar,
                "fetch": "ok",
            }
        )
    except Exception as ex:  # noqa: BLE001
        out["fetch"] = f"exc:{type(ex).__name__}"
    return out


def _window(ts: str) -> tuple[str, str]:
    day = str(ts)[:10]
    try:
        end = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        end = datetime.utcnow()
        day = end.strftime("%Y-%m-%d")
    start = (end - timedelta(days=21)).strftime("%Y-%m-%d")
    return start, day


def _table_code(market: str, table: str) -> Optional[str]:
    prefix = f"{market}_"
    if not table.startswith(prefix):
        return None
    suffix = table[len(prefix) :]
    if suffix in _SKIP_SUFFIX or suffix.endswith("_IDX"):
        return None
    return suffix


def _universe_dollar_vols(
    conn: sqlite3.Connection, market: str, asof: str
) -> list[float]:
    """Proxy for scan universe: all KR_/US_ OHLCV tables in prod DB."""
    like = f"{market}_%"
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (like,),
        ).fetchall()
    ]
    vals: list[float] = []
    for i, tname in enumerate(tables):
        if _table_code(market, tname) is None:
            continue
        try:
            rows = conn.execute(
                f'SELECT Close, Volume FROM "{tname}" '
                f"WHERE Date <= ? ORDER BY Date DESC LIMIT 5",
                (asof,),
            ).fetchall()
        except Exception:
            continue
        if not rows:
            continue
        dollars: list[float] = []
        for cl, vol in rows:
            try:
                c = float(cl)
                v = float(vol)
            except (TypeError, ValueError):
                continue
            if not np.isfinite(c) or not np.isfinite(v) or v < 0:
                continue
            dollars.append(c * v)
        if not dollars:
            continue
        vals.append(float(np.mean(dollars)))
        if (i + 1) % 800 == 0:
            print(f"  ... universe scan {market} {i+1}/{len(tables)} (n_ok={len(vals)})")
    return vals


def _percentile(value: float, universe: list[float]) -> Optional[float]:
    if not universe or not np.isfinite(value):
        return None
    arr = np.asarray(universe, dtype=float)
    # percent of universe strictly below value (empirical CDF)
    return float(100.0 * np.mean(arr < value))


def _bucket(pct: Optional[float]) -> str:
    if pct is None:
        return "unknown"
    if pct < PCT_LOW:
        return "low"
    if pct < PCT_HIGH:
        return "mid"
    return "high"


def diagnose_market(conn: sqlite3.Connection, market: str) -> Counter:
    print(f"\n=== {market} Phase1 quality-band (LIQUIDITY sample) ===")
    samples = _sample_codes(conn, market)
    if not samples:
        print("  (no sample codes)")
        return Counter()

    # universe as-of: most common sample day (or first)
    days = [ts[:10] for _, ts in samples]
    asof = max(set(days), key=days.count)
    print(f"  universe dollar-vol asof<={asof} ...")
    universe = _universe_dollar_vols(conn, market, asof)
    print(f"  universe_n={len(universe)}")

    buckets: Counter = Counter()
    abd: Counter = Counter()
    for code, ts in samples:
        start, end = _window(ts)
        df = fetch_market_data(code, market, start, end)
        m = _metrics_from_df(df)
        label = _classify_abd(market, m["close"], m["avg_vol_5d"])
        abd[label] += 1
        pct = (
            _percentile(float(m["avg_dollar_vol_5d"]), universe)
            if m["avg_dollar_vol_5d"] is not None
            else None
        )
        b = _bucket(pct)
        buckets[b] += 1
        pct_s = f"{pct:.1f}" if pct is not None else "-"
        print(
            f"  {label} bucket={b:7} pct={pct_s:>6} "
            f"code={code[:12]:12} close={m['close']} "
            f"vol5={m['avg_vol_5d']} dvol5={m['avg_dollar_vol_5d']} "
            f"fetch={m['fetch']}"
        )

    n = sum(buckets.values()) or 1
    print(f"  abd: " + ", ".join(f"{k}={abd[k]}" for k in "abcd"))
    print(
        f"  percentile buckets: low(<{PCT_LOW:.0f})={buckets['low']} "
        f"mid={buckets['mid']} high(>={PCT_HIGH:.0f})={buckets['high']} "
        f"unknown={buckets['unknown']}"
    )
    mid_high = buckets["mid"] + buckets["high"]
    print(
        f"  mid+high share={100.0 * mid_high / n:.1f}% "
        f"low share={100.0 * buckets['low'] / n:.1f}%"
    )
    return buckets


def main() -> int:
    print(f"DB: {MARKET_DATA_DB_PATH}")
    print(f"STALL_SINCE={STALL_SINCE} SAMPLE_N={SAMPLE_N}")
    print("NOTE: Phase1 measurement only — no threshold changes")
    conn = _connect()
    try:
        total: Counter = Counter()
        for mk in ("KR", "US"):
            total.update(diagnose_market(conn, mk))
        n = sum(total[k] for k in ("low", "mid", "high", "unknown")) or 1
        mid_high = total["mid"] + total["high"]
        low = total["low"]
        print("\n=== PHASE1 BRANCH HINT (not a policy change) ===")
        print(
            f"  low={total['low']} mid={total['mid']} high={total['high']} "
            f"unknown={total['unknown']}"
        )
        if mid_high >= low and mid_high > 0:
            print(
                "HINT: MID_HIGH_CONCENTRATION -> Phase2 candidate "
                "(director numbers required; junk-open loosen FORBIDDEN)"
            )
        else:
            print(
                "HINT: LOW_TAIL_CONCENTRATION -> (B) observe-extend "
                "(quality band retune not indicated by this sample)"
            )
        print("OBS-HOLD: no config/cutoff/threshold changes applied.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
