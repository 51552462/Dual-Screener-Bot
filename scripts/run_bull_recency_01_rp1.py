#!/usr/bin/env python3
"""BULL-RECENCY-01 — CLUSTER_1 bounds patch + optional KR lever + 15-period RP-1 re-sim.

VPS (brain + OHLCV parquet cache required):
  export BULL_RECENCY_01_PATCH=1
  export BULL_RECENCY_01_SHRINK=0.45
  export RP1_SKIP_STAGE2=1
  unset RP1_METRICS_ONLY
  python scripts/preflight_bull_recency_rp1.py              # static (~5s)
  python scripts/preflight_bull_recency_rp1.py --smoke      # required before full (~15m)
  python scripts/run_bull_recency_01_rp1.py \\
    --baseline reports/regime_panel/rp1_20260811.json

Regenerate DoD only (no re-sim):
  PYTHONPATH=. python scripts/run_bull_recency_01_rp1.py \\
    --dod-only \\
    --baseline reports/regime_panel/rp1_20260811.json \\
    --patched-json reports/regime_panel/rp1_bull_recency_01_20260813.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("BULL_RECENCY_01_PATCH", "1")
os.environ.setdefault("RP1_SKIP_STAGE2", "1")
if os.environ.get("RP1_METRICS_ONLY", "").strip().lower() in ("1", "true", "yes"):
    os.environ.pop("RP1_METRICS_ONLY", None)

from regime_panel_rp1 import run_regime_panel_rp1  # noqa: E402
from regime_panel_rp1_runner import build_rp1_universe, log_rp1  # noqa: E402

BULL_TARGETS = ("BULL_03_최근상승", "BULL_05_글로벌리플레이")
BULL_03_TARGET = "BULL_03_최근상승"
BULL_05_TARGET = "BULL_05_글로벌리플레이"


def _load_baseline(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    return data.get("stage1") or data


def _period_key(row: Mapping[str, Any]) -> str:
    return str(row.get("regime_name") or row.get("name") or "")


def _period_map(stage1: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    rows = stage1.get("periods") or []
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        key = _period_key(r)
        if key:
            out[key] = dict(r)
    return out


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

    bull_05_row = new_by.get(BULL_05_TARGET)
    bull_03_row = new_by.get(BULL_03_TARGET)
    bull_05_ok = str((bull_05_row or {}).get("verdict") or "") in ("PASS", "NEAR_MISS")
    bull_03_maintained = str((bull_03_row or {}).get("verdict") or "") in (
        "PASS",
        "NEAR_MISS",
    )

    for name, brow in base_by.items():
        nrow = new_by.get(name)
        if nrow is None:
            regressions.append({"name": name, "issue": "missing_in_patched"})
            continue
        b_verdict = str(brow.get("verdict") or "")
        n_verdict = str(nrow.get("verdict") or "")
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
        if name == BULL_03_TARGET and str(n_verdict) not in ("PASS", "NEAR_MISS"):
            regressions.append(
                {
                    "name": name,
                    "baseline_verdict": b_verdict,
                    "patched_verdict": n_verdict,
                    "issue": "bull_03_below_near_miss",
                }
            )
        elif name not in BULL_TARGETS and b_verdict != n_verdict:
            regressions.append(
                {
                    "name": name,
                    "baseline_verdict": b_verdict,
                    "patched_verdict": n_verdict,
                }
            )
        tier_mdd = float(nrow.get("mdd_pct_tier") or nrow.get("mdd_pct") or 0.0)
        if tier_mdd > 10.0:
            mdd_ok = False
        if int(nrow.get("total_trades") or 0) < 20:
            n_ok = False

    other_unchanged = not [
        r for r in regressions if r.get("name") not in BULL_TARGETS
    ]

    return {
        "dod_1_bull_05_near_miss_plus": bull_05_ok,
        "dod_2_bull_03_verdict_unchanged": bull_03_maintained,
        "dod_3_other_verdict_unchanged": other_unchanged,
        "dod_4_tier_mdd_le_10": mdd_ok,
        "dod_5_n_ge_20": n_ok,
        "dod_6_period_map_keys_valid": bool(new_by) and bool(base_by),
        "dod_1_bull_03_05_near_miss_plus": bull_05_ok and bull_03_maintained,
        "dod_2_other_verdict_unchanged": other_unchanged,
        "dod_3_tier_mdd_le_10": mdd_ok,
        "dod_4_n_ge_20": n_ok,
        "regressions": regressions,
        "bull_targets": bull_target,
        "mdd_crosscheck": patched.get("mdd_crosscheck"),
        "all_pass": (
            bull_05_ok
            and bull_03_maintained
            and other_unchanged
            and mdd_ok
            and n_ok
            and bool(new_by)
        ),
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
    ap.add_argument(
        "--dod-only",
        action="store_true",
        help="Regenerate _dod.json from existing patched JSON (no re-sim)",
    )
    ap.add_argument(
        "--patched-json",
        default="",
        help="Patched RP-1 JSON for --dod-only",
    )
    args = ap.parse_args()

    if args.dod_only:
        if not args.baseline or not args.patched_json:
            raise SystemExit("--dod-only requires --baseline and --patched-json")
        baseline = _load_baseline(Path(args.baseline))
        with Path(args.patched_json).open(encoding="utf-8") as fh:
            patched = json.load(fh)
        s1 = patched.get("stage1") or patched
        dod = compare_dod(baseline, s1)
        dod_path = Path(args.patched_json).with_name(
            Path(args.patched_json).stem + "_dod.json"
        )
        with dod_path.open("w", encoding="utf-8") as fh:
            json.dump(dod, fh, ensure_ascii=False, indent=2)
        log_rp1(f"[BULL-RECENCY-01] DoD -> {dod_path} all_pass={dod.get('all_pass')}")
        for item in dod.get("bull_targets") or []:
            log_rp1(
                f"  {item['name']}: {item['baseline_verdict']} -> "
                f"{item['patched_verdict']} period_ret={item.get('patched_period_ret')}"
            )
        return

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
        dod_path = Path(out_path).with_name(Path(out_path).stem + "_dod.json")
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
