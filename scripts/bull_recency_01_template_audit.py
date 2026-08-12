"""BULL-RECENCY-01 — template order + match-path audit (read-only).

Answers: do CLUSTER_1 폭발형 templates actually bind trades in BULL_03/05?
Run on VPS where factory brain + matrix snapshot exist.

  python scripts/bull_recency_01_template_audit.py
  python scripts/bull_recency_01_template_audit.py \\
    --snapshot reports/regime_panel/matrix_cache/matrix_<digest>.pkl
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BULL_WINDOWS = {
    "BULL_03_최근상승": ("2024-10-01", "2025-03-31"),
    "BULL_05_글로벌리플레이": ("2016-06-01", "2016-11-30"),
}

_DYN_KEYS = ("dyn_cpv_min", "dyn_cpv_max", "dyn_tb_min", "dyn_tb_max", "v_energy_min", "v_energy_max")


def _wkey(start: str, end: str) -> str:
    return f"{start}|{end}"


def _effective_dyn_box(bounds: Dict[str, Any]) -> Dict[str, float]:
    """What time_machine actually uses (missing dyn keys → wide defaults)."""
    return {
        "dyn_cpv_min": float(bounds.get("dyn_cpv_min", -99)),
        "dyn_cpv_max": float(bounds.get("dyn_cpv_max", 99)),
        "dyn_tb_min": float(bounds.get("dyn_tb_min", -99)),
        "dyn_tb_max": float(bounds.get("dyn_tb_max", 999)),
        "v_energy_min": float(bounds.get("v_energy_min", -99)),
        "v_energy_max": float(bounds.get("v_energy_max", 999)),
    }


def _box_width(box: Dict[str, float]) -> Dict[str, float]:
    return {
        "cpv_span": box["dyn_cpv_max"] - box["dyn_cpv_min"],
        "tb_span": box["dyn_tb_max"] - box["dyn_tb_min"],
        "v_energy_span": box["v_energy_max"] - box["v_energy_min"],
    }


def _template_rows(
    ml: Dict[str, Any],
    ud: Dict[str, Any],
    *,
    patched_ml: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Iteration order matches RP-1: {**LIVE, **UNDERDOG}."""
    effective_ml = patched_ml if patched_ml is not None else ml
    merged = {**effective_ml, **ud}
    rows: List[Dict[str, Any]] = []
    for idx, name in enumerate(merged):
        bounds = merged[name]
        if not isinstance(bounds, dict):
            continue
        from bull_recency_01_bounds import is_cluster_1_explosive_template

        box = _effective_dyn_box(bounds)
        rows.append(
            {
                "order": idx + 1,
                "template": name,
                "source": "UNDERDOG" if name in ud and name not in effective_ml else "LIVE",
                "cluster_1_explosive": is_cluster_1_explosive_template(name),
                "has_dyn_keys": any(k in bounds for k in _DYN_KEYS),
                "dyn_box": box,
                "dyn_span": _box_width(box),
                "legacy_cpv": (bounds.get("cpv_min"), bounds.get("cpv_max")),
            }
        )
    return rows


def _window_template_counts(matrix: Dict[str, Any], start: str, end: str) -> Dict[str, Any]:
    key = _wkey(start, end)
    bucket = matrix.get(key) or {}
    trades = bucket.get("trades") if isinstance(bucket, dict) else []
    if not trades:
        return {"key": key, "total_trades": 0, "top_templates": []}
    tpl = Counter(str(t.get("template") or "") for t in trades)
    top = [
        {"template": n, "n": c, "share_pct": round(100.0 * c / len(trades), 2)}
        for n, c in tpl.most_common(20)
    ]
    explosive = sum(c for n, c in tpl.items() if "CLUSTER_1" in n and "폭발" in n)
    return {
        "key": key,
        "total_trades": len(trades),
        "explosive_cluster1_n": explosive,
        "explosive_cluster1_share_pct": round(100.0 * explosive / len(trades), 2),
        "top_templates": top,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", default="", help="matrix_*.pkl for per-window template counts")
    ap.add_argument("--out", default="", help="Output JSON path")
    ap.add_argument("--apply-patch", action="store_true", help="Also show post-patch dyn boxes")
    args = ap.parse_args()

    from time_machine_backtester import load_factory_brain_readonly

    brain = load_factory_brain_readonly()
    ml = brain.get("LIVE_CLUSTER_TEMPLATES") or {}
    ud = brain.get("UNDERDOG_CLUSTER_TEMPLATES") or {}

    patched_ml = None
    patch_audit: Optional[Dict[str, Any]] = None
    if args.apply_patch:
        from bull_recency_01_bounds import apply_bull_recency_01_brain_patch

        patched_brain, patch_audit = apply_bull_recency_01_brain_patch(brain)
        patched_ml = patched_brain.get("LIVE_CLUSTER_TEMPLATES") or {}

    rows = _template_rows(ml, ud, patched_ml=patched_ml)
    explosive = [r for r in rows if r["cluster_1_explosive"]]

    windows: Dict[str, Any] = {}
    if args.snapshot:
        with Path(args.snapshot).open("rb") as fh:
            matrix = pickle.load(fh)
        for label, (s, e) in BULL_WINDOWS.items():
            windows[label] = _window_template_counts(matrix, s, e)

    # first-match risk: any non-explosive LIVE template before first explosive with wider dyn span
    first_explosive_order = explosive[0]["order"] if explosive else None
    shadow_candidates = []
    if first_explosive_order:
        for r in rows:
            if r["order"] >= first_explosive_order:
                break
            if r["dyn_span"]["cpv_span"] >= 150 or r["dyn_span"]["v_energy_span"] >= 500:
                shadow_candidates.append(r)

    report: Dict[str, Any] = {
        "schema": "bull_recency_01_template_audit.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "template_count": {"live": len(ml), "underdog": len(ud), "merged": len(rows)},
        "cluster_1_explosive": explosive,
        "first_match_shadow_before_explosive": shadow_candidates,
        "iteration_order_sample": rows[:25],
        "note": (
            "time_machine uses first matching template in merged dict order; "
            "dyn_* missing → defaults (-99..99 / -99..999). "
            "Jaccard 1.0 in stage-1 = top5 template name overlap between fail windows, not 100% trade share."
        ),
        "windows": windows,
        "patch_audit_summary": (
            {
                "templates_patched": patch_audit.get("templates_patched"),
                "shrink": patch_audit.get("shrink"),
            }
            if patch_audit
            else None
        ),
    }

    out = Path(args.out) if args.out else (
        ROOT / "reports" / "regime_panel" / f"bull_recency_01_template_audit_{datetime.now().strftime('%Y%m%d')}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
