"""UNIVERSE-BT-U3 — L0 quantitative report (metric 4 excluded).

Banner (fixed):
L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지

Read-only against bitget_universe_bt.sqlite. No CAT-J pipeline registration.
"""
from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, Optional

from bitget.analysis.universe_bt.paths import universe_bt_db_path
from bitget.analysis.universe_bt.store import ensure_results_schema

L0_BANNER = "L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지"
METRIC4_NA = "N/A — 별도 Handoff 대기"


def _safe_div(num: float, den: float) -> Optional[float]:
    """§2: denominator 0 → null (no fake 100%)."""
    if den is None or float(den) == 0.0:
        return None
    return float(num) / float(den)


def _normalize_mt(market_type: str) -> str:
    mt = str(market_type or "").strip().lower()
    if mt in ("fut", "linear"):
        return "futures"
    if mt not in ("spot", "futures"):
        raise ValueError(f"market_type must be spot|futures, got {market_type!r}")
    return mt


def _agg_counts(db_path: str, run_id: str, market_type: str) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_bars,
                COALESCE(SUM(candidate_generated), 0) AS candidates,
                COALESCE(SUM(gate_passed), 0) AS gate_passed,
                COALESCE(SUM(virtual_entry), 0) AS virtual_entries,
                COALESCE(SUM(CASE WHEN virtual_entry=1 AND UPPER(COALESCE(side,''))='LONG' THEN 1 ELSE 0 END), 0) AS long_ve,
                COALESCE(SUM(CASE WHEN virtual_entry=1 AND UPPER(COALESCE(side,''))='SHORT' THEN 1 ELSE 0 END), 0) AS short_ve
            FROM bitget_universe_bt_results
            WHERE run_id=? AND market_type=?
            """,
            (run_id, market_type),
        ).fetchone()
        # regime buckets for FUTURES side asymmetry
        regime_rows = conn.execute(
            """
            SELECT
                COALESCE(regime_label, 'UNKNOWN') AS regime,
                COALESCE(SUM(CASE WHEN virtual_entry=1 AND UPPER(COALESCE(side,''))='LONG' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN virtual_entry=1 AND UPPER(COALESCE(side,''))='SHORT' THEN 1 ELSE 0 END), 0)
            FROM bitget_universe_bt_results
            WHERE run_id=? AND market_type=?
            GROUP BY COALESCE(regime_label, 'UNKNOWN')
            """,
            (run_id, market_type),
        ).fetchall()
    finally:
        conn.close()
    total, cand, gp, ve, long_ve, short_ve = row
    by_regime = {
        str(r[0]): {"long_virtual_entries": int(r[1]), "short_virtual_entries": int(r[2])}
        for r in regime_rows
    }
    return {
        "total_bars_scanned": int(total or 0),
        "candidates_generated": int(cand or 0),
        "gate_passed_candidates": int(gp or 0),
        "virtual_entries": int(ve or 0),
        "long_virtual_entries": int(long_ve or 0),
        "short_virtual_entries": int(short_ve or 0),
        "by_regime": by_regime,
    }


def _side_asymmetry(market_type: str, counts: Dict[str, Any]) -> Any:
    if market_type == "spot":
        return None  # §2 SPOT footnote
    out: Dict[str, Optional[float]] = {}
    for regime, bucket in (counts.get("by_regime") or {}).items():
        out[regime] = _safe_div(
            bucket.get("long_virtual_entries", 0),
            bucket.get("short_virtual_entries", 0),
        )
    return out


def generate_universe_bt_u3_report(
    market_type: str,
    run_id: str,
    *,
    db_path: Optional[str] = None,
) -> dict:
    """Build L0 metrics dict for one market_type (no metric-4 numerics)."""
    mt = _normalize_mt(market_type)
    path = ensure_results_schema(db_path or universe_bt_db_path())
    counts = _agg_counts(path, run_id, mt)
    metrics = {
        "hit_rate": _safe_div(
            counts["candidates_generated"], counts["total_bars_scanned"]
        ),
        "gate_pass_rate": _safe_div(
            counts["gate_passed_candidates"], counts["candidates_generated"]
        ),
        "virtual_entry_rate": _safe_div(
            counts["virtual_entries"], counts["gate_passed_candidates"]
        ),
        "crash_window_forced_exit_rate": METRIC4_NA,
        "side_asymmetry_ratio": _side_asymmetry(mt, counts),
    }
    return {
        "banner": L0_BANNER,
        "run_id": run_id,
        "market_type": mt,
        "counts": counts,
        "metrics": metrics,
        "metric4_status": METRIC4_NA,
    }


def build_u3_side_by_side_report(
    run_id: str, *, db_path: Optional[str] = None
) -> dict:
    """§4: SPOT and FUTURES side-by-side — no pooled sum."""
    return {
        "banner": L0_BANNER,
        "run_id": run_id,
        "markets": {
            "spot": generate_universe_bt_u3_report("spot", run_id, db_path=db_path),
            "futures": generate_universe_bt_u3_report(
                "futures", run_id, db_path=db_path
            ),
        },
        "metric4_status": METRIC4_NA,
    }


def _fmt_rate(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, str):
        return v
    return f"{float(v):.6f}"


def _metrics_table_md(label: str, report: dict) -> str:
    m = report.get("metrics") or {}
    c = report.get("counts") or {}
    lines = [
        f"### {label}",
        "",
        "| metric | value |",
        "|--------|-------|",
        f"| hit_rate | {_fmt_rate(m.get('hit_rate'))} |",
        f"| gate_pass_rate | {_fmt_rate(m.get('gate_pass_rate'))} |",
        f"| virtual_entry_rate | {_fmt_rate(m.get('virtual_entry_rate'))} |",
        f"| crash_window_forced_exit_rate | {METRIC4_NA} |",
        f"| side_asymmetry_ratio | {_fmt_rate(m.get('side_asymmetry_ratio')) if not isinstance(m.get('side_asymmetry_ratio'), dict) else '(see regime table)'} |",
        "",
        "| count | n |",
        "|-------|---|",
        f"| total_bars_scanned | {c.get('total_bars_scanned', 0)} |",
        f"| candidates_generated | {c.get('candidates_generated', 0)} |",
        f"| gate_passed_candidates | {c.get('gate_passed_candidates', 0)} |",
        f"| virtual_entries | {c.get('virtual_entries', 0)} |",
        f"| long_virtual_entries | {c.get('long_virtual_entries', 0)} |",
        f"| short_virtual_entries | {c.get('short_virtual_entries', 0)} |",
        "",
    ]
    sar = m.get("side_asymmetry_ratio")
    if isinstance(sar, dict):
        lines += [
            "| regime | side_asymmetry_ratio |",
            "|--------|---------------------|",
        ]
        for regime, val in sorted(sar.items()):
            lines.append(f"| {regime} | {_fmt_rate(val)} |")
        lines.append("")
    elif sar is None and report.get("market_type") == "spot":
        lines.append("_side_asymmetry_ratio = null (SPOT SHORT=0 footnote)_")
        lines.append("")
    return "\n".join(lines)


def render_u3_report_md(report: dict) -> str:
    """L0 banner + quantitative tables only — no free-form narrative."""
    banner = str(report.get("banner") or L0_BANNER)
    parts = [banner, "", f"run_id: `{report.get('run_id', '')}`", ""]
    markets = report.get("markets")
    if isinstance(markets, dict):
        # side-by-side sections (not summed)
        for key in ("spot", "futures"):
            if key in markets:
                parts.append(_metrics_table_md(key.upper(), markets[key]))
    else:
        mt = str(report.get("market_type") or "").upper()
        parts.append(_metrics_table_md(mt or "MARKET", report))
    parts.append(f"crash_window_forced_exit_rate: {METRIC4_NA}")
    parts.append("")
    return "\n".join(parts)


def write_u3_report_file(
    report: dict, *, reports_dir: Optional[str] = None
) -> str:
    """Write markdown under analysis/universe_bt/reports/ (outside CAT-J)."""
    base = reports_dir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "reports"
    )
    os.makedirs(base, exist_ok=True)
    run_id = str(report.get("run_id") or "unknown")
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run_id)
    path = os.path.join(base, f"u3_{safe}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(render_u3_report_md(report))
    return path
