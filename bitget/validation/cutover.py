"""
Cutover readiness — pipeline SSOT vs legacy main/sentinel paths.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any

from bitget.infra.data_paths import validation_state_dir

PARALLEL_STATE_NAME = "parallel_run_state.json"


def _env_truthy(name: str, default: str = "0") -> bool:
    return str(os.environ.get(name, default)).strip().lower() in ("1", "true", "yes", "on")


def parallel_state_path() -> str:
    return os.path.join(validation_state_dir(), PARALLEL_STATE_NAME)


def start_parallel_run(*, mode: str = "pipeline", note: str = "") -> dict[str, Any]:
    payload = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": str(mode),
        "note": note,
        "target_hours": float(os.environ.get("BITGET_PARALLEL_RUN_HOURS", "48")),
    }
    with open(parallel_state_path(), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def load_parallel_state() -> dict[str, Any] | None:
    path = parallel_state_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def parallel_run_status() -> dict[str, Any]:
    st = load_parallel_state()
    if not st:
        return {"active": False, "elapsed_hours": 0.0, "ready_for_cutover": False}
    try:
        started = datetime.fromisoformat(str(st["started_at_utc"]).replace("Z", "+00:00"))
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        elapsed_h = (datetime.now(timezone.utc) - started).total_seconds() / 3600.0
    except Exception:
        elapsed_h = 0.0
    target = float(st.get("target_hours") or 48.0)
    return {
        "active": True,
        "started_at_utc": st.get("started_at_utc"),
        "mode": st.get("mode"),
        "elapsed_hours": round(elapsed_h, 2),
        "target_hours": target,
        "ready_for_cutover": elapsed_h >= target,
    }


def _legacy_main_process_detected() -> bool:
    if sys.platform == "win32":
        return False
    try:
        out = subprocess.run(
            ["pgrep", "-f", "bitget.main"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return out.returncode == 0 and bool(out.stdout.strip())
    except Exception:
        return False


def check_cutover_readiness() -> dict[str, Any]:
    from bitget.validation.architecture_checks import run_architecture_checks

    pipeline_ssot = _env_truthy("BITGET_PIPELINE_SSOT", "0")
    parallel = parallel_run_status()
    legacy_main = _legacy_main_process_detected()
    architecture = run_architecture_checks()
    checks = {
        "pipeline_ssot_env": pipeline_ssot,
        "parallel_run_ready": parallel.get("ready_for_cutover", False),
        "no_legacy_main_process": not legacy_main,
        "async_telegram": _env_truthy("BITGET_ASYNC_TELEGRAM", "0"),
        "architecture_ok": architecture.get("passed", False),
    }
    if pipeline_ssot:
        passed = all(checks.values())
    else:
        passed = False
    return {
        "ok": True,
        "passed": passed,
        "checks": checks,
        "architecture": architecture,
        "parallel_run": parallel,
        "legacy_main_running": legacy_main,
        "message": (
            "cutover ready - set BITGET_PIPELINE_SSOT=1 and complete 48h parallel run"
            if not passed
            else "cutover checks PASS"
        ),
        "recommendation": (
            "Use systemd dante-bitget-* + bitget.sh cron; deprecate python -m bitget.main"
        ),
    }
