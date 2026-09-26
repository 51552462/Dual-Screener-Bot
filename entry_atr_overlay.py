"""ENTRY-ATR-OVERLAY-01 Phase 1 — US observe-only Kelly shrink counterfactual.

Does not change live Kelly or position size. KR is never evaluated.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from typing import Any, Mapping, Sequence

US_WIPEOUT_START = date(2026, 8, 24)
MIN_US_ATR_HIST = 20

# Candidate shrink schedules — Phase 1 records all; none is selected as policy.
# A: p90 → ×0.50 · B: p80 → ×0.70 · C: p70 → ×0.85
OVERLAY_PLANS: tuple[dict[str, Any], ...] = (
    {"id": "A", "min_pct": 0.90, "mult": 0.50},
    {"id": "B", "min_pct": 0.80, "mult": 0.70},
    {"id": "C", "min_pct": 0.70, "mult": 0.85},
)

_SKIP_SIG_FRAGMENTS = ("OBSERVE", "인버스", "INVERSE", "기각/관찰용")


def _parse_iso_date(raw: Any) -> date | None:
    text = str(raw or "").strip()[:10]
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def in_us_wipeout_window(market: str, entry_date: Any) -> bool:
    if str(market or "").strip().upper() != "US":
        return False
    parsed = _parse_iso_date(entry_date)
    if parsed is None:
        return False
    return parsed >= US_WIPEOUT_START


def empirical_percentile(value: float, hist: Sequence[float]) -> float | None:
    vals = [float(x) for x in hist if x is not None]
    if len(vals) < MIN_US_ATR_HIST:
        return None
    n = len(vals)
    rank = sum(1 for x in vals if x <= float(value))
    return rank / float(n)


def plan_multipliers(pct: float | None) -> dict[str, float]:
    out: dict[str, float] = {}
    if pct is None:
        for plan in OVERLAY_PLANS:
            out[str(plan["id"])] = 1.0
        return out
    for plan in OVERLAY_PLANS:
        pid = str(plan["id"])
        out[pid] = float(plan["mult"]) if pct >= float(plan["min_pct"]) else 1.0
    return out


def would_be_from_plans(plan_mults: Mapping[str, float]) -> float:
    if not plan_mults:
        return 1.0
    return float(min(float(v) for v in plan_mults.values()))


def _neutral_stamp(*, reason: str, wipeout: bool = False) -> dict[str, Any]:
    plan_mults = plan_multipliers(None)
    return {
        "would_be_kelly_mult": 1.0,
        "actual_kelly_mult": 1.0,
        "applied_kelly_mult": 1.0,
        "entry_atr_pct": None,
        "entry_atr_wipeout_window": 1 if wipeout else 0,
        "entry_atr_plan_json": json.dumps(plan_mults, separators=(",", ":")),
        "reason": reason,
        "hist_n": 0,
    }


def fetch_us_entry_atr_hist(cursor: sqlite3.Cursor) -> list[float]:
    cursor.execute(
        """
        SELECT entry_atr FROM forward_trades
        WHERE UPPER(TRIM(market)) = 'US'
          AND entry_atr IS NOT NULL
          AND entry_atr > 0
          AND IFNULL(sig_type, '') NOT LIKE '%OBSERVE%'
          AND IFNULL(sig_type, '') NOT LIKE '%INVERSE%'
          AND IFNULL(sig_type, '') NOT LIKE '%인버스%'
          AND IFNULL(sig_type, '') NOT LIKE '%기각/관찰용%'
        """
    )
    out: list[float] = []
    for row in cursor.fetchall():
        try:
            val = float(row[0])
        except (TypeError, ValueError):
            continue
        if val > 0:
            out.append(val)
    return out


def filter_hist_for_observe(hist: Sequence[float], sig_types: Sequence[str] | None = None) -> list[float]:
    """Keep numeric ATRs. Optional parallel sig_types drops observe/inverse rows."""
    if not sig_types:
        return [float(x) for x in hist if x is not None and float(x) > 0]
    kept: list[float] = []
    for atr, sig in zip(hist, sig_types):
        blob = str(sig or "").upper()
        if any(frag.upper() in blob or frag in str(sig or "") for frag in _SKIP_SIG_FRAGMENTS):
            continue
        try:
            val = float(atr)
        except (TypeError, ValueError):
            continue
        if val > 0:
            kept.append(val)
    return kept


def observe_entry_atr_kelly(
    *,
    market: str,
    entry_atr: float,
    entry_date: Any,
    cursor: sqlite3.Cursor | None = None,
    hist: Sequence[float] | None = None,
) -> dict[str, Any]:
    """Counterfactual shrink only. actual/applied stay 1.0 forever in Phase 1."""
    mkt = str(market or "").strip().upper()
    wipeout = in_us_wipeout_window(mkt, entry_date)
    if mkt != "US":
        return _neutral_stamp(reason="kr_excluded", wipeout=False)

    series: list[float]
    if hist is not None:
        series = [float(x) for x in hist if x is not None and float(x) > 0]
    elif cursor is not None:
        series = fetch_us_entry_atr_hist(cursor)
    else:
        series = []

    pct = empirical_percentile(float(entry_atr), series)
    if pct is None:
        stamp = _neutral_stamp(reason="insufficient_us_atr_hist", wipeout=wipeout)
        stamp["hist_n"] = len(series)
        return stamp

    plan_mults = plan_multipliers(pct)
    would_be = would_be_from_plans(plan_mults)
    return {
        "would_be_kelly_mult": would_be,
        "actual_kelly_mult": 1.0,
        "applied_kelly_mult": 1.0,
        "entry_atr_pct": round(float(pct), 6),
        "entry_atr_wipeout_window": 1 if wipeout else 0,
        "entry_atr_plan_json": json.dumps(plan_mults, separators=(",", ":")),
        "reason": "observe_us",
        "hist_n": len(series),
    }


def identity_ok(stamp: Mapping[str, Any], kelly_before: float, kelly_after: float) -> bool:
    """Phase 1 identity: overlay must not move Kelly; actual/applied must be 1.0.

    would_be may differ from actual when ATR is high — that is the observation,
    not a bug. Handoff 'would_be == actual else bug' is the *applied* identity.
    """
    if abs(float(kelly_before) - float(kelly_after)) > 1e-15:
        return False
    if abs(float(stamp.get("actual_kelly_mult") or 0.0) - 1.0) > 1e-15:
        return False
    if abs(float(stamp.get("applied_kelly_mult") or 0.0) - 1.0) > 1e-15:
        return False
    return True
