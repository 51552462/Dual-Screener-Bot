#!/usr/bin/env python3
"""BULL-RECENCY-01 full RP-1 preflight — fail fast before 3h matrix prime."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("BULL_RECENCY_01_PATCH", "1")


def main() -> int:
    from bull_recency_01_bounds import (
        apply_bull_recency_01_brain_patch,
        scope_live_templates_for_br01,
    )
    from regime_panel_rp1_runner import _brain_templates_and_factors
    from time_machine_backtester import load_factory_brain_readonly

    brain = load_factory_brain_readonly()
    ml_n = len(brain.get("LIVE_CLUSTER_TEMPLATES") or {})
    if ml_n < 1:
        print(f"FATAL: LIVE_CLUSTER_TEMPLATES={ml_n}")
        return 1

    patched, audit = apply_bull_recency_01_brain_patch(brain)
    tp = int(audit.get("templates_patched") or 0)
    if tp < 1:
        print(f"FATAL: templates_patched={tp}")
        return 1

    scoped, scope_audit = scope_live_templates_for_br01(
        patched.get("LIVE_CLUSTER_TEMPLATES") or {}
    )
    if not scoped:
        print("FATAL: CLUSTER_1 폭발형 scope empty after patch")
        return 1

    sim_tpl, _ = _brain_templates_and_factors(patched)
    if len(sim_tpl) < len(scoped):
        print(f"FATAL: sim template count {len(sim_tpl)} < scoped {len(scoped)}")
        return 1

    print(
        f"OK preflight: brain_ml={ml_n} patched={tp} "
        f"scoped={scope_audit.get('live_out')}/{scope_audit.get('live_in')} "
        f"sim_templates={len(sim_tpl)} "
        f"DB={os.environ.get('DB_STORAGE_PATH', '(unset)')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
