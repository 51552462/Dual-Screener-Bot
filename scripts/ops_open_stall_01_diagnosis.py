"""
OPS-OPEN-STALL-01 — survivors≈0 원인 진단 (read-only).

C-FUNNEL-02 테이블만 SELECT. schema/config/cutoff 비접촉.

    python scripts/ops_open_stall_01_diagnosis.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from market_db_paths import MARKET_DATA_DB_PATH

# Handoff 예외 슬롯 (survivors=5)
EXCEPTION_SLOT_PREFIX = "2026-08-17 14:15"
EXCEPTION_MARKET = "KR"


def _connect() -> sqlite3.Connection:
    if not os.path.isfile(MARKET_DATA_DB_PATH):
        raise SystemExit(f"FAIL DB missing: {MARKET_DATA_DB_PATH}")
    conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def step0_inventory(conn: sqlite3.Connection) -> int:
    print("=== Step 0: scan_funnel_drop_event inventory ===")
    print(f"DB: {MARKET_DATA_DB_PATH}")
    if not _table_exists(conn, "scan_funnel_drop_event"):
        print("FAIL table scan_funnel_drop_event missing - C-FUNNEL-02 wiring check (out of scope)")
        return 0
    rows = conn.execute(
        """
        SELECT market, COUNT(*) AS n, MIN(ts) AS min_ts, MAX(ts) AS max_ts,
               COUNT(DISTINCT substr(ts, 1, 10)) AS days
        FROM scan_funnel_drop_event
        GROUP BY market
        ORDER BY market
        """
    ).fetchall()
    if not rows:
        print("FAIL 0 rows in scan_funnel_drop_event - stop; C-FUNNEL-02 wiring recheck")
        return 0
    total = 0
    for r in rows:
        total += int(r["n"])
        print(
            f"  {r['market']}: n={r['n']} min={r['min_ts']} max={r['max_ts']} days={r['days']}"
        )
    print(f"  TOTAL n={total}")
    return total


def _stall_window_start(conn: sqlite3.Connection) -> Optional[str]:
    """Earliest ts in the most recent contiguous survivors=0 run (per market min)."""
    if not _table_exists(conn, "scan_funnel_snapshot"):
        return None
    snaps = conn.execute(
        """
        SELECT ts, market, survivors
        FROM scan_funnel_snapshot
        ORDER BY ts DESC
        LIMIT 200
        """
    ).fetchall()
    if not snaps:
        return None
    # Walk newest→oldest; while survivors==0 keep going; first non-zero ends window.
    oldest_zero: Optional[str] = None
    for r in snaps:
        try:
            surv = int(r["survivors"] or 0)
        except (TypeError, ValueError):
            surv = 0
        if surv <= 0:
            oldest_zero = str(r["ts"])
        else:
            break
    return oldest_zero


def step1_reason_mix(conn: sqlite3.Connection, since_ts: Optional[str]) -> list[sqlite3.Row]:
    print("\n=== Step 1: market × reason counts (stall window) ===")
    if since_ts:
        print(f"  window since ~ {since_ts} (contiguous survivors=0 from newest)")
        rows = conn.execute(
            """
            SELECT market, reason, COUNT(*) AS n
            FROM scan_funnel_drop_event
            WHERE ts >= ?
            GROUP BY market, reason
            ORDER BY market, n DESC
            """,
            (since_ts,),
        ).fetchall()
    else:
        print("  window: all rows (stall start unresolved)")
        rows = conn.execute(
            """
            SELECT market, reason, COUNT(*) AS n
            FROM scan_funnel_drop_event
            GROUP BY market, reason
            ORDER BY market, n DESC
            """
        ).fetchall()
    if not rows:
        print("  (no rows)")
        return []
    by_mkt: dict[str, int] = {}
    for r in rows:
        by_mkt[str(r["market"])] = by_mkt.get(str(r["market"]), 0) + int(r["n"])
    for r in rows:
        m = str(r["market"])
        n = int(r["n"])
        tot = by_mkt.get(m) or 1
        pct = 100.0 * n / tot
        print(f"  {m:2} {str(r['reason']):20} n={n:6} ({pct:5.1f}%)")
    return rows


def step2_nearmiss(conn: sqlite3.Connection, since_ts: Optional[str]) -> None:
    print("\n=== Step 2: near-miss top5 per market×reason (|score-cutoff|) ===")
    params: list[object] = []
    where = "WHERE final_score IS NOT NULL"
    if since_ts:
        where += " AND ts >= ?"
        params.append(since_ts)
    sql = f"""
        SELECT market, reason, code, ts, final_score, eff_cos_cutoff, eff_ml_cutoff,
               regime_key, rank_in_slot,
               CASE
                 WHEN eff_cos_cutoff IS NOT NULL THEN ABS(final_score - eff_cos_cutoff)
                 WHEN eff_ml_cutoff IS NOT NULL THEN ABS(final_score - eff_ml_cutoff)
                 ELSE NULL
               END AS dist
        FROM scan_funnel_drop_event
        {where}
        ORDER BY market, reason,
                 CASE WHEN (
                   CASE
                     WHEN eff_cos_cutoff IS NOT NULL THEN ABS(final_score - eff_cos_cutoff)
                     WHEN eff_ml_cutoff IS NOT NULL THEN ABS(final_score - eff_ml_cutoff)
                     ELSE NULL
                   END
                 ) IS NULL THEN 1 ELSE 0 END,
                 CASE
                   WHEN eff_cos_cutoff IS NOT NULL THEN ABS(final_score - eff_cos_cutoff)
                   WHEN eff_ml_cutoff IS NOT NULL THEN ABS(final_score - eff_ml_cutoff)
                   ELSE NULL
                 END ASC,
                 CASE WHEN rank_in_slot IS NULL THEN 1 ELSE 0 END,
                 rank_in_slot ASC
    """
    rows = conn.execute(sql, params).fetchall()
    shown: dict[tuple[str, str], int] = {}
    for r in rows:
        key = (str(r["market"]), str(r["reason"]))
        c = shown.get(key, 0)
        if c >= 5:
            continue
        shown[key] = c + 1
        dist = r["dist"]
        dist_s = f"{float(dist):.6f}" if dist is not None else "-"
        print(
            f"  {key[0]:2} {key[1]:16} #{shown[key]} "
            f"code={str(r['code'] or '-')[:12]:12} "
            f"score={r['final_score']} cos={r['eff_cos_cutoff']} ml={r['eff_ml_cutoff']} "
            f"dist={dist_s} regime={r['regime_key']} ts={r['ts']}"
        )
    if not shown:
        print("  (no scored near-miss rows in window)")


def step3_exception_slot(conn: sqlite3.Connection) -> None:
    print(
        f"\n=== Step 3: exception slot {EXCEPTION_MARKET} ts LIKE '{EXCEPTION_SLOT_PREFIX}%' ==="
    )
    rows = conn.execute(
        """
        SELECT reason, COUNT(*) AS n,
               AVG(final_score) AS avg_score,
               AVG(eff_cos_cutoff) AS avg_cos,
               AVG(eff_ml_cutoff) AS avg_ml
        FROM scan_funnel_drop_event
        WHERE market = ? AND ts LIKE ?
        GROUP BY reason
        ORDER BY n DESC
        """,
        (EXCEPTION_MARKET, EXCEPTION_SLOT_PREFIX + "%"),
    ).fetchall()
    if not rows:
        # broaden: same calendar day hour window
        print("  exact prefix miss - try day+hour substring")
        rows = conn.execute(
            """
            SELECT reason, COUNT(*) AS n,
                   AVG(final_score) AS avg_score,
                   AVG(eff_cos_cutoff) AS avg_cos,
                   AVG(eff_ml_cutoff) AS avg_ml
            FROM scan_funnel_drop_event
            WHERE market = ? AND substr(ts, 1, 13) = '2026-08-17 14'
            GROUP BY reason
            ORDER BY n DESC
            """,
            (EXCEPTION_MARKET,),
        ).fetchall()
    if not rows:
        print("  (no drop_event rows for exception slot)")
        return
    for r in rows:
        print(
            f"  {str(r['reason']):20} n={r['n']:5} "
            f"avg_score={r['avg_score']} avg_cos={r['avg_cos']} avg_ml={r['avg_ml']}"
        )


def classify(rows: list[sqlite3.Row]) -> str:
    if not rows:
        return "c"
    totals: dict[str, int] = {}
    grand = 0
    for r in rows:
        reason = str(r["reason"] or "").upper()
        n = int(r["n"])
        grand += n
        if "LIQUIDITY" in reason:
            totals["LIQUIDITY"] = totals.get("LIQUIDITY", 0) + n
        elif "DNA" in reason:
            totals["DNA_FAIL"] = totals.get("DNA_FAIL", 0) + n
        else:
            totals["OTHER"] = totals.get("OTHER", 0) + n
    # (c) absolute count abnormally low vs expected near-miss (cap 50 × slots × days)
    if grand < 20:
        return "c"
    liq = totals.get("LIQUIDITY", 0)
    dna = totals.get("DNA_FAIL", 0)
    if liq >= grand * 0.55 and liq >= dna:
        return "a"
    if dna >= grand * 0.35 or dna >= liq:
        return "b"
    if liq > dna:
        return "a"
    return "b"


def main() -> int:
    conn = _connect()
    try:
        n0 = step0_inventory(conn)
        if n0 <= 0:
            print("\nVERDICT: STOP - Step 0 failed (no drop_event data)")
            print("CLASS: (wire) C-FUNNEL-02 wiring - out of Handoff scope")
            return 2
        since = _stall_window_start(conn)
        reason_rows = step1_reason_mix(conn, since)
        step2_nearmiss(conn, since)
        step3_exception_slot(conn)
        label = classify(reason_rows)
        labels = {
            "a": "(a) LIQUIDITY dominant -> CAT-B liquidity/OHLCV recheck",
            "b": "(b) DNA_FAIL/cutoff near-miss dominant -> DEFENSE cutoff candidate (Claude policy later)",
            "c": "(c) drop_event absolute count abnormally low -> CAT-B universe upstream",
        }
        print("\n=== CLASSIFICATION ===")
        print(labels[label])
        print("OBS-HOLD: no config/cutoff changes applied.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
