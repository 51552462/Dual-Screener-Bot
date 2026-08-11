#!/usr/bin/env python3
"""BULL-RECENCY-01 step-2 — CLUSTER_1 bounds patch + 15-period RP-1 re-sim.

VPS (brain + OHLCV parquet cache required):
  export BULL_RECENCY_01_PATCH=1
  export RP1_SKIP_STAGE2=1
  unset RP1_METRICS_ONLY
  python scripts/run_bull_recency_01_rp1.py \\
    --baseline reports/regime_panel/rp1_20260811_v233.json

Optional tuning (defaults: shrink=0.20, tb/bbe floor lift=0.15):
  BULL_RECENCY_01_SHRINK=0.20
  BULL_RECENCY_01_TB_FLOOR_LIFT=0.15
  BULL_RECENCY_01_BBE_FLOOR_LIFT=0.15
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("BULL_RECENCY_01_PATCH", "1")
os.environ.setdefault("RP1_SKIP_STAGE2", "1")
# Bounds change invalidates trade snapshots — never metrics-only on baseline pkl.
if os.environ.get("RP1_METRICS_ONLY", "").strip().lower() in ("1", "true", "yes"):
    os.environ.pop("RP1_METRICS_ONLY", None)

from regime_panel_rp1 import run_regime_panel_rp1  # noqa: E402
from regime_panel_rp1_runner import build_rp1_universe, log_rp1  # noqa: E402

BULL_TARGETS = ("BULL_03_최근상승", "BULL_05_글로벌리플레이")


def _load_baseline(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    return data.get("stage1") or data


def _period_map(stage1: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    rows = stage1.get("periods") or []
    return {str(r.get("name")): dict(r) for r in rows if isinstance(r, dict)}


def compare_dod(
    baseline: Mapping[str, Any],
    patched: Mapping[str, Any],
) -> Dict[str, Any]:
    base_by = _period_map(baseline)
    new_by = _period_map(patched)
    regressions: List[Dict[str, Any]] = []
    bull_target: List[Dict[str, Any]] = []
    mdd_ok = True
    n_ok = True

    for name, brow in base_by.items():
        nrow = new_by.get(name)
        if nrow is None:
            regressions.append({"name": name, "issue": "missing_in_patched"})
            continue
        b_verdict = str(brow.get("verdict") or "")
        n_verdict = str(nrow.get("verdict") or "")
        if name not in BULL_TARGETS and b_verdict != n_verdict:
            regressions.append(
                {
                    "name": name,
                    "baseline_verdict": b_verdict,
                    "patched_verdict": n_verdict,
                }
            )
        if name in BULL_TARGETS:
            bull_target.append(
                {
                    "name": name,
                    "baseline_verdict": b_verdict,
                    "patched_verdict": n_verdict,
                    "baseline_period_ret": brow.get("period_return_pct"),
                    "patched_period_ret": nrow.get("period_return_pct"),
                    "patched_n": nrow.get("total_trades"),
                }
            )
        tier_mdd = float(nrow.get("mdd_pct_tier") or nrow.get("mdd_pct") or 0.0)
        if tier_mdd > 10.0:
            mdd_ok = False
        if int(nrow.get("total_trades") or 0) < 20:
            n_ok = False

    bull_ok = all(
        str(x.get("patched_verdict")) in ("PASS", "NEAR_MISS")
        for x in bull_target
    )

    return {
        "dod_1_bull_03_05_near_miss_plus": bull_ok,
        "dod_2_other_verdict_unchanged": len(regressions) == 0,
        "dod_3_tier_mdd_le_10": mdd_ok,
        "dod_4_n_ge_20": n_ok,
        "regressions": regressions,
        "bull_targets": bull_target,
        "mdd_crosscheck": patched.get("mdd_crosscheck"),
        "all_pass": bull_ok and not regressions and mdd_ok and n_ok,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--baseline",
        default="",
        help="Baseline RP-1 JSON (v2.3.3) for DoD regression compare",
    )
    ap.add_argument(
        "--output-dir",
        default="",
        help="Override reports/regime_panel output dir",
    )
    args = ap.parse_args()

    log_rp1("[BULL-RECENCY-01] start — CLUSTER_1 bounds patch + 15-period re-sim")
    universe = build_rp1_universe()
    report = run_regime_panel_rp1(
        universe,
        run_stage2=False,
        output_dir=args.output_dir or None,
    )
    s1 = report["stage1"]
    out_path = report.get("output_path", "")
    log_rp1(f"[BULL-RECENCY-01] wrote {out_path}")
    log_rp1(f"schema={s1.get('schema')} overall={s1.get('overall_verdict')}")

    if args.baseline:
        baseline_path = Path(args.baseline)
        if not baseline_path.is_file():
            raise SystemExit(f"baseline not found: {baseline_path}")
        baseline = _load_baseline(baseline_path)
        dod = compare_dod(baseline, s1)
        dod_path = Path(out_path).with_name(
            Path(out_path).stem + "_dod.json"
        )
        with dod_path.open("w", encoding="utf-8") as fh:
            json.dump(dod, fh, ensure_ascii=False, indent=2)
        log_rp1(f"[BULL-RECENCY-01] DoD -> {dod_path} all_pass={dod.get('all_pass')}")
        for item in dod.get("bull_targets") or []:
            log_rp1(
                f"  {item['name']}: {item['baseline_verdict']} -> "
                f"{item['patched_verdict']} period_ret={item.get('patched_period_ret')}"
            )


if __name__ == "__main__":
    main()
