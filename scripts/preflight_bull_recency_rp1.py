#!/usr/bin/env python3
"""BULL-RECENCY-01 full RP-1 preflight — fail fast before 3h matrix prime.

Static (seconds):
  PYTHONPATH=. python3 scripts/preflight_bull_recency_rp1.py

Dynamic smoke (~10–20 min, RP1_FAST BULL_03 only) — required before full rerun:
  PYTHONPATH=. python3 scripts/preflight_bull_recency_rp1.py --smoke
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("BULL_RECENCY_01_PATCH", "1")


def _static_gate() -> int:
    from regime_panel_rp1_runner import (
        _brain_templates_and_factors,
        clear_rp1_brain_cache,
        get_rp1_brain_patch_audit,
        load_rp1_brain_cached,
    )

    clear_rp1_brain_cache()
    brain = load_rp1_brain_cached(force_reload=True)
    audit = get_rp1_brain_patch_audit() or {}
    ml_n = len(brain.get("LIVE_CLUSTER_TEMPLATES") or {})
    ud_n = len(brain.get("UNDERDOG_CLUSTER_TEMPLATES") or {})
    tp = int(audit.get("templates_patched") or 0)
    if ml_n < 1:
        print(f"FATAL: LIVE_CLUSTER_TEMPLATES={ml_n}")
        return 1
    if tp < 2:
        print(f"FATAL: templates_patched={tp} (expected 2 CLUSTER_1 폭발형)")
        return 1

    patched = audit.get("patched") or []
    if patched:
        before = (patched[0].get("bounds_before") or {})
        if not before.get("dyn_cpv_min") and not before.get("cpv_min"):
            print("FATAL: patched template missing cpv/dyn_cpv bounds — SSOT overlay failed?")
            return 1
        ssot_lo = before.get("dyn_cpv_min")
        try:
            if ssot_lo is not None and float(ssot_lo) > -0.2:
                print(f"FATAL: bounds_before dyn_cpv_min={ssot_lo} — expected ~-0.51 SSOT")
                return 1
        except (TypeError, ValueError):
            pass
        after = (patched[0].get("bounds_after") or {})
        try:
            lo_before = float(before.get("dyn_cpv_min", before.get("cpv_min", 0)))
            lo_after = float(after.get("dyn_cpv_min", after.get("cpv_min", 0)))
            if abs(lo_after - lo_before) < 1e-6:
                print(
                    "FATAL: bounds_after == bounds_before — shrink not applied "
                    "(mirror_bounds regression? git pull 351b404+)"
                )
                return 1
            if lo_after > -0.2:
                print(f"FATAL: bounds_after dyn_cpv_min={lo_after} — expected ~-0.1703")
                return 1
        except (TypeError, ValueError):
            print("FATAL: bounds_after dyn_cpv_min not numeric")
            return 1

    sim_tpl, _ = _brain_templates_and_factors(brain)
    sim_live = sum(
        1 for name in sim_tpl if name in (brain.get("LIVE_CLUSTER_TEMPLATES") or {})
    )
    if sim_live != tp:
        print(
            f"FATAL: sim_live={sim_live} != patched={tp} "
            f"(fallthrough path? sim_templates={len(sim_tpl)} brain_ml={ml_n})"
        )
        return 1
    if len(sim_tpl) >= ml_n:
        print(
            f"FATAL: sim_templates={len(sim_tpl)} >= brain_ml={ml_n} "
            "(scope not applied — CLUSTER_2/3 fallthrough risk)"
        )
        return 1
    first_tpl = next(iter(sim_tpl), "")
    if "260628" not in str(first_tpl):
        print(f"FATAL: first sim template={first_tpl!r} — expected 260628 binding first")
        return 1

    print(
        f"OK static: brain_ml={ml_n} underdog={ud_n} patched={tp} "
        f"sim_templates={len(sim_tpl)} sim_live={sim_live} first={first_tpl!r} "
        f"DB={os.environ.get('DB_STORAGE_PATH', '(unset)')}"
    )
    return 0


def _smoke_gate() -> int:
    from bull_recency_01_bounds import BR01_SMOKE_PERIOD, validate_br01_smoke_trades
    from regime_panel_rp1_runner import (
        build_rp1_universe,
        clear_rp1_brain_cache,
        clear_rp1_matrix_cache,
        default_run_backtest_for_period,
        prime_rp1_matrix_cache,
    )

    rc = _static_gate()
    if rc != 0:
        return rc

    # 8/13 SSOT run had no KR RS lever — smoke validates patch path only.
    os.environ["BULL_RECENCY_01_KR_LEVER"] = "0"
    os.environ["RP1_FAST"] = "1"
    clear_rp1_brain_cache()
    clear_rp1_matrix_cache()

    regime_name, start_dt, end_dt = BR01_SMOKE_PERIOD
    print(f"[smoke] RP1_FAST BULL_03 matrix prime + {regime_name} ...")
    universe = build_rp1_universe()
    meta = prime_rp1_matrix_cache(universe)
    total = int(meta.get("total_trades") or 0)
    print(f"[smoke] matrix total_trades={total}")
    floor = max(800, len(universe) * 80)
    if total < floor:
        print(
            f"FATAL smoke: matrix_total={total} < {floor} "
            "(collapsed scope / shrink too tight?)"
        )
        return 1

    pack = default_run_backtest_for_period(regime_name, universe, start_dt, end_dt)
    trades = pack.get("trades") or []
    ok, msg = validate_br01_smoke_trades(trades)
    if not ok:
        print(f"FATAL smoke: {msg}")
        return 1
    print(f"OK smoke: {msg} matrix_total={total}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="BULL-RECENCY-01 RP-1 preflight")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="RP1_FAST BULL_03 dynamic gate (~10–20 min) — required before full rerun",
    )
    args = parser.parse_args()
    if args.smoke:
        return _smoke_gate()
    return _static_gate()


if __name__ == "__main__":
    raise SystemExit(main())
