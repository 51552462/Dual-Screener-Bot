#!/usr/bin/env python3
"""SIDE-ALPHA-01 stage-2 — CAT-E SIDEWAYS MAE_SL overlay + 15-period RP-1 metrics.

VPS (matrix snapshot + OHLCV parquet cache required):
  export SIDE_ALPHA_01_EXIT=1
  export RP1_SKIP_STAGE2=1
  unset BULL_RECENCY_01_PATCH   # must stay off
  export RP1_MATRIX_SNAPSHOT_PATH=reports/regime_panel/matrix_cache/matrix_ab52b174195da604adc8.pkl
  PYTHONPATH=. python3 scripts/run_side_alpha_01_rp1.py \\
    --baseline reports/regime_panel/rp1_bull_recency_01_20260813.json

DoD only (no re-sim):
  PYTHONPATH=. python3 scripts/run_side_alpha_01_rp1.py --dod-only \\
    --baseline reports/regime_panel/rp1_bull_recency_01_20260813.json \\
    --patched-json reports/regime_panel/rp1_side_alpha_01_<stamp>.json
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

os.environ.setdefault("SIDE_ALPHA_01_EXIT", "1")
os.environ.setdefault("RP1_SKIP_STAGE2", "1")
# Hard isolation from BULL-RECENCY entry patch
if os.environ.get("BULL_RECENCY_01_PATCH", "").strip():
    os.environ.pop("BULL_RECENCY_01_PATCH", None)
if os.environ.get("RP1_METRICS_ONLY", "").strip().lower() in ("1", "true", "yes"):
    os.environ.pop("RP1_METRICS_ONLY", None)

from regime_panel_rp1 import run_regime_panel_rp1  # noqa: E402
from regime_panel_rp1_runner import build_rp1_universe, log_rp1  # noqa: E402
from side_alpha_01_exit import SIDE_TARGETS  # noqa: E402

NEAR_OR_BETTER = ("PASS", "NEAR_MISS")


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
    """SIDE-ALPHA-01 DoD #1–5 vs 8/13 SSOT baseline."""
    base_by = _period_map(baseline)
    new_by = _period_map(patched)
    regressions: List[Dict[str, Any]] = []
    side_targets: List[Dict[str, Any]] = []
    mdd_ok = True
    n_ok = True

    for name in SIDE_TARGETS:
        brow = base_by.get(name) or {}
        nrow = new_by.get(name) or {}
        n_verdict = str(nrow.get("verdict") or "")
        side_targets.append(
            {
                "name": name,
                "baseline_verdict": brow.get("verdict"),
                "patched_verdict": n_verdict,
                "baseline_period_ret": brow.get("period_return_pct"),
                "patched_period_ret": nrow.get("period_return_pct"),
                "baseline_avg_pnl": brow.get("avg_pnl"),
                "patched_avg_pnl": nrow.get("avg_pnl"),
                "patched_n": nrow.get("total_trades"),
                "near_or_better": n_verdict in NEAR_OR_BETTER,
                "avg_pnl_improved": (
                    float(nrow.get("avg_pnl") or 0.0) > float(brow.get("avg_pnl") or 0.0)
                ),
            }
        )

    dod1_side_ok = all(t.get("near_or_better") for t in side_targets) and len(
        side_targets
    ) == len(SIDE_TARGETS)
    # Conditional: period_ret NEAR ok but avg_pnl still negative → provisional
    avg_pnl_ok = all(
        float(t.get("patched_avg_pnl") or 0.0) >= 0.0
        or bool(t.get("avg_pnl_improved"))
        for t in side_targets
    )
    dod1_provisional = bool(dod1_side_ok and not avg_pnl_ok)

    for name, brow in base_by.items():
        nrow = new_by.get(name)
        if nrow is None:
            regressions.append({"name": name, "issue": "missing_in_patched"})
            continue
        b_verdict = str(brow.get("verdict") or "")
        n_verdict = str(nrow.get("verdict") or "")
        if name not in SIDE_TARGETS and b_verdict != n_verdict:
            regressions.append(
                {
                    "name": name,
                    "baseline_verdict": b_verdict,
                    "patched_verdict": n_verdict,
                    "issue": "verdict_changed",
                }
            )
        tier_mdd = float(nrow.get("mdd_pct_tier") or nrow.get("mdd_pct") or 0.0)
        if tier_mdd > 10.0:
            mdd_ok = False
        if int(nrow.get("total_trades") or 0) < 20:
            n_ok = False

    other_unchanged = not any(
        r.get("issue") == "verdict_changed" for r in regressions
    )
    mdd_badge = str((patched.get("mdd_crosscheck") or {}).get("badge") or "")
    mdd_cross_ok = mdd_badge == "MDD_OK" or not bool(
        (patched.get("mdd_crosscheck") or {}).get("mdd_cap_violation")
    )

    return {
        "dod_1_side_02_03_near_miss_plus": dod1_side_ok,
        "dod_1_provisional_avg_pnl": dod1_provisional,
        "dod_2_other_verdict_unchanged": other_unchanged,
        "dod_3_tier_mdd_le_10": mdd_ok and mdd_cross_ok,
        "dod_4_n_ge_20": n_ok,
        "dod_5_artifacts_written": True,
        "side_targets": side_targets,
        "regressions": regressions,
        "mdd_crosscheck": patched.get("mdd_crosscheck"),
        "all_pass": bool(
            dod1_side_ok and other_unchanged and mdd_ok and mdd_cross_ok and n_ok
        ),
        "all_pass_strict_avg_pnl": bool(
            dod1_side_ok
            and avg_pnl_ok
            and other_unchanged
            and mdd_ok
            and mdd_cross_ok
            and n_ok
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--baseline",
        default="reports/regime_panel/rp1_bull_recency_01_20260813.json",
        help="8/13 SSOT baseline for DoD",
    )
    ap.add_argument("--output-dir", default="")
    ap.add_argument("--dod-only", action="store_true")
    ap.add_argument("--patched-json", default="")
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
        log_rp1(f"[SIDE-ALPHA-01] DoD -> {dod_path} all_pass={dod.get('all_pass')}")
        return

    snap = os.environ.get("RP1_MATRIX_SNAPSHOT_PATH", "").strip()
    if not snap:
        # Prefer diag-era snapshot used in stage-1
        guess = ROOT / "reports/regime_panel/matrix_cache/matrix_ab52b174195da604adc8.pkl"
        if guess.is_file():
            os.environ["RP1_MATRIX_SNAPSHOT_PATH"] = str(guess)

    log_rp1(
        "[SIDE-ALPHA-01] start — SIDEWAYS MAE_SL overlay + 15-period metrics "
        f"(snapshot={os.environ.get('RP1_MATRIX_SNAPSHOT_PATH', '')})"
    )
    universe = build_rp1_universe()
    report = run_regime_panel_rp1(
        universe,
        run_stage2=False,
        output_dir=args.output_dir or None,
    )
    s1 = report["stage1"]
    out_path = report.get("output_path", "")
    # Rename stamp for clarity when possible
    log_rp1(f"[SIDE-ALPHA-01] wrote {out_path}")
    log_rp1(f"schema={s1.get('schema')} overall={s1.get('overall_verdict')}")

    baseline_path = Path(args.baseline)
    if baseline_path.is_file():
        baseline = _load_baseline(baseline_path)
        dod = compare_dod(baseline, s1)
        dod_path = Path(out_path).with_name(Path(out_path).stem + "_dod.json")
        with dod_path.open("w", encoding="utf-8") as fh:
            json.dump(dod, fh, ensure_ascii=False, indent=2)
        log_rp1(
            f"[SIDE-ALPHA-01] DoD -> {dod_path} all_pass={dod.get('all_pass')} "
            f"provisional={dod.get('dod_1_provisional_avg_pnl')}"
        )
        for item in dod.get("side_targets") or []:
            log_rp1(
                f"  {item['name']}: {item['baseline_verdict']} -> "
                f"{item['patched_verdict']} period_ret={item.get('patched_period_ret')} "
                f"avg_pnl={item.get('patched_avg_pnl')}"
            )


if __name__ == "__main__":
    main()
