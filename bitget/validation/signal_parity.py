"""
Signal parity — compare scan hit keys (master sent_log) vs baseline snapshot.
"""
from __future__ import annotations

import json
import os
from typing import Any

from bitget.infra.clock import utc_datetime_str_tz
from bitget.infra.data_paths import logs_dir, validation_state_dir

SIGNAL_BASELINE_NAME = "signal_baseline.json"
DEFAULT_MAX_DIFF_PCT = 1.0


def _sent_log_path() -> str:
    return os.path.join(logs_dir(), "sent_log_bitget_master.txt")


def read_scan_hit_keys() -> set[str]:
    path = _sent_log_path()
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        if len(lines) <= 1:
            return set()
        return {ln.strip() for ln in lines[1:] if ln.strip()}
    except OSError:
        return set()


def baseline_path() -> str:
    return os.path.join(validation_state_dir(), SIGNAL_BASELINE_NAME)


def save_signal_baseline(*, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    keys = sorted(read_scan_hit_keys())
    payload = {
        "recorded_at_utc": utc_datetime_str_tz(),
        "hit_keys": keys,
        "hit_count": len(keys),
        "extra": extra or {},
    }
    path = baseline_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def load_signal_baseline() -> dict[str, Any] | None:
    path = baseline_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def compare_signal_parity(
    *,
    max_diff_pct: float = DEFAULT_MAX_DIFF_PCT,
    baseline: dict[str, Any] | None = None,
    current_keys: set[str] | None = None,
) -> dict[str, Any]:
    base = baseline if baseline is not None else load_signal_baseline()
    if not base:
        return {
            "ok": False,
            "passed": False,
            "reason": "no_baseline",
            "message": f"Run record_baseline first ({baseline_path()})",
        }
    old_keys = set(base.get("hit_keys") or [])
    cur = current_keys if current_keys is not None else read_scan_hit_keys()
    union = old_keys | cur
    if not union:
        return {
            "ok": True,
            "passed": True,
            "diff_pct": 0.0,
            "baseline_count": 0,
            "current_count": 0,
            "only_in_baseline": [],
            "only_in_current": [],
            "message": "empty baseline and current (skip)",
        }
    sym_diff = len(old_keys ^ cur)
    diff_pct = (sym_diff / len(union)) * 100.0
    passed = diff_pct <= float(max_diff_pct)
    return {
        "ok": True,
        "passed": passed,
        "diff_pct": round(diff_pct, 4),
        "max_diff_pct": float(max_diff_pct),
        "baseline_count": len(old_keys),
        "current_count": len(cur),
        "only_in_baseline": sorted(old_keys - cur)[:50],
        "only_in_current": sorted(cur - old_keys)[:50],
        "baseline_recorded_at": base.get("recorded_at_utc"),
        "message": "PASS" if passed else f"signal diff {diff_pct:.2f}% > {max_diff_pct}%",
    }
