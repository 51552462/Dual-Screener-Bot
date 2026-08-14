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
    if tp < 1:
        print(f"FATAL: templates_patched={tp}")
        return 1

    patched = audit.get("patched") or []
    if patched:
        before = (patched[0].get("bounds_before") or {})
        if not before.get("dyn_cpv_min") and not before.get("cpv_min"):
            print("FATAL: patched template missing cpv/dyn_cpv bounds — repo brain?")
            return 1

    sim_tpl, _ = _brain_templates_and_factors(brain)
    if len(sim_tpl) < tp:
        print(f"FATAL: sim_templates={len(sim_tpl)} < patched={tp}")
        return 1

    print(
        f"OK preflight: brain_ml={ml_n} underdog={ud_n} patched={tp} "
        f"sim_templates={len(sim_tpl)} "
        f"DB={os.environ.get('DB_STORAGE_PATH', '(unset)')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
