"""SIDE-ALPHA-01 stage-1 trade-level diag from RP-1 matrix snapshot (read-only).

Same procedure as scripts/bull_recency_01_trade_diag.py — SIDE_02/03 NEAR_MISS(B)
plus SIDEWAYS PASS controls (SIDE_01/04/05).

Usage (VPS, where matrix_*.pkl exists):
  python scripts/side_alpha_01_trade_diag.py \\
    --snapshot reports/regime_panel/matrix_cache/matrix_<digest>.pkl

Does NOT mutate config_kv / Phase A / BULL bounds.
Holding days are exit-type proxies only (SL=-3.5 / TP=+10 / TIME≈15d).
"""
from __future__ import annotations

import argparse
import json
import pickle
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

ROOT = Path(__file__).resolve().parents[1]

# primary windows from REGIME_PERIODS SSOT (SIDEWAYS bucket)
TARGET_WINDOWS = {
    "SIDE_02_2015횡보": ("2015-06-01", "2016-06-30"),  # NEAR_MISS(B)
    "SIDE_03_2021-22혼조": ("2021-12-01", "2022-12-31"),  # NEAR_MISS(B)
    "SIDE_01_2023횡보": ("2023-05-01", "2023-08-31"),  # PASS control
    "SIDE_04_2024여름횡보": ("2024-04-01", "2024-08-31"),  # PASS control
    "SIDE_05_2019횡보": ("2019-04-01", "2019-12-31"),  # PASS control
}


def _wkey(start: str, end: str) -> str:
    return f"{start}|{end}"


def _exit_type(final_ret: float) -> str:
    if abs(final_ret - (-3.5)) < 1e-9:
        return "SL"
    if abs(final_ret - 10.0) < 1e-9:
        return "TP"
    return "TIME"


def _summarize(trades: Sequence[Dict[str, Any]], *, label: str, start: str, end: str) -> Dict[str, Any]:
    n = len(trades)
    if n == 0:
        return {
            "regime_name": label,
            "start": start,
            "end": end,
            "total_trades": 0,
            "note": "empty window — check snapshot keys",
        }

    rets = [float(t.get("final_ret") or 0.0) for t in trades]
    wins = [r for r in rets if r > 0]
    loses = [r for r in rets if r <= 0]
    win_rate = 100.0 * len(wins) / n
    avg_pnl = sum(rets) / n
    pf = (sum(wins) / (abs(sum(loses)) + 0.1)) if loses else 99.9

    tpl = Counter(str(t.get("template") or "") for t in trades)
    exit_c = Counter(_exit_type(float(t.get("final_ret") or 0.0)) for t in trades)

    by_month: Dict[str, List[float]] = {}
    for t in trades:
        d = str(t.get("date") or "")[:7]
        if not d:
            continue
        by_month.setdefault(d, []).append(float(t.get("final_ret") or 0.0))
    monthly = []
    for m in sorted(by_month):
        xs = by_month[m]
        monthly.append(
            {
                "month": m,
                "n": len(xs),
                "win_rate": round(100.0 * sum(1 for x in xs if x > 0) / len(xs), 2),
                "avg_pnl": round(sum(xs) / len(xs), 4),
            }
        )

    kr_rets = [float(t["final_ret"]) for t in trades if str(t.get("code", "")).isdigit()]
    us_rets = [float(t["final_ret"]) for t in trades if not str(t.get("code", "")).isdigit()]

    def _bucket(xs: List[float]) -> Dict[str, Any]:
        if not xs:
            return {"n": 0}
        return {
            "n": len(xs),
            "win_rate": round(100.0 * sum(1 for x in xs if x > 0) / len(xs), 2),
            "avg_pnl": round(sum(xs) / len(xs), 4),
        }

    top_tpl = [
        {
            "template": name,
            "n": cnt,
            "share_pct": round(100.0 * cnt / n, 2),
        }
        for name, cnt in tpl.most_common(15)
    ]

    tpl_edge = []
    for name, _ in tpl.most_common(10):
        xs = [float(t["final_ret"]) for t in trades if str(t.get("template") or "") == name]
        if not xs:
            continue
        tpl_edge.append(
            {
                "template": name,
                "n": len(xs),
                "win_rate": round(100.0 * sum(1 for x in xs if x > 0) / len(xs), 2),
                "avg_pnl": round(sum(xs) / len(xs), 4),
            }
        )

    exit_mix = {k: {"n": v, "share_pct": round(100.0 * v / n, 2)} for k, v in exit_c.items()}
    time_share = exit_mix.get("TIME", {}).get("share_pct", 0.0)

    return {
        "regime_name": label,
        "start": start,
        "end": end,
        "total_trades": n,
        "win_rate": round(win_rate, 4),
        "avg_pnl": round(avg_pnl, 6),
        "pf": round(float(pf), 6),
        "exit_type_mix": exit_mix,
        "holding_proxy": {
            "TIME_share_pct": time_share,
            "note": "TIME≈15 bars; SL/TP exact bars unknown without re-sim",
        },
        "template_top15": top_tpl,
        "template_edge_top10": tpl_edge,
        "monthly": monthly,
        "market_split": {"KR_digit_code": _bucket(kr_rets), "US_other": _bucket(us_rets)},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True, help="Path to matrix_*.pkl")
    ap.add_argument(
        "--out",
        default="",
        help="Output JSON path (default reports/regime_panel/side_alpha_01_trade_diag_{date}.json)",
    )
    args = ap.parse_args()

    snap_path = Path(args.snapshot)
    with snap_path.open("rb") as fh:
        matrix = pickle.load(fh)
    if not isinstance(matrix, dict):
        raise SystemExit(f"bad snapshot type: {type(matrix)}")

    available = sorted(matrix.keys())
    rows = []
    for label, (start, end) in TARGET_WINDOWS.items():
        key = _wkey(start, end)
        bucket = matrix.get(key) or {}
        trades = bucket.get("trades") if isinstance(bucket, dict) else None
        if trades is None:
            rows.append(
                {
                    "regime_name": label,
                    "start": start,
                    "end": end,
                    "total_trades": 0,
                    "missing_key": key,
                    "available_keys_sample": available[:8],
                }
            )
            continue
        rows.append(_summarize(list(trades), label=label, start=start, end=end))

    near_rows = [r for r in rows if r["regime_name"].startswith(("SIDE_02", "SIDE_03"))]
    ctrl_rows = [r for r in rows if r["regime_name"].startswith(("SIDE_01", "SIDE_04", "SIDE_05"))]

    def top_names(r: Dict[str, Any]) -> set:
        return {x["template"] for x in r.get("template_top15", [])[:5]}

    overlap_note = None
    if all(r.get("template_top15") for r in near_rows) and len(near_rows) == 2:
        a, b = near_rows
        inter = top_names(a) & top_names(b)
        union = top_names(a) | top_names(b)
        overlap_note = {
            "near_top5_jaccard": round(len(inter) / max(len(union), 1), 4),
            "shared_top5": sorted(inter),
        }

    out = {
        "schema": "side_alpha_01_trade_diag.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshot": str(snap_path),
        "baseline_json": "reports/regime_panel/rp1_bull_recency_01_20260813.json",
        "windows": rows,
        "controls": [r["regime_name"] for r in ctrl_rows],
        "targets": [r["regime_name"] for r in near_rows],
        "template_overlap_near": overlap_note,
        "aggregate_prior": {
            "near_miss_cause": "B",
            "source": "rp1_bull_recency_01_20260813.json stage1.periods",
            "note": "n≫20 · avg_pnl<0 · PF≈1.0 — cause B (edge shortfall), not A/C",
        },
    }

    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_path = Path(args.out) if args.out else (
        ROOT / "reports" / "regime_panel" / f"side_alpha_01_trade_diag_{date}.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"wrote": str(out_path), "n_windows": len(rows)}, ensure_ascii=False))
    for r in rows:
        et = r.get("exit_type_mix") or {}
        print(
            f"  {r.get('regime_name')}: n={r.get('total_trades')} "
            f"wr={r.get('win_rate')} avg={r.get('avg_pnl')} "
            f"SL%={et.get('SL', {}).get('share_pct')} "
            f"TP%={et.get('TP', {}).get('share_pct')} "
            f"TIME%={et.get('TIME', {}).get('share_pct')}"
        )


if __name__ == "__main__":
    main()
