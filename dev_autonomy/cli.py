"""CLI entry for local dev autonomy P0."""

from __future__ import annotations

import argparse
import json
import sys

from dev_autonomy.orchestrator import P0Orchestrator
from dev_autonomy.process_lock import OrchestratorLockError, orchestrator_lock
from dev_autonomy.types import RunMode, Track


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local Autonomous Development P0")
    parser.add_argument(
        "--mode",
        choices=[m.value for m in RunMode],
        default=RunMode.STATUS.value,
        help="Run mode (default STATUS)",
    )
    parser.add_argument(
        "--track",
        choices=[t.value for t in Track],
        default=Track.A.value,
        help="Track A (KR/US), B (Bitget), IV",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON only",
    )
    args = parser.parse_args(argv)

    track = Track(args.track)
    mode = RunMode(args.mode)
    orch = P0Orchestrator()

    try:
        with orchestrator_lock():
            if mode == RunMode.STATUS:
                result = orch.run_status(track)
            elif mode == RunMode.SHADOW:
                result = orch.run_shadow(track)
            elif mode == RunMode.SAFE_SINGLE_CYCLE:
                result = orch.run_safe_single_cycle(track)
            elif mode == RunMode.LOCAL_P0_LOOP:
                result = orch.run_local_p0_loop(track)
            else:
                result = {"error": "unknown mode"}
    except OrchestratorLockError as exc:
        result = {"phase": "STOPPED", "reason": str(exc)}

    if args.json:
        payload = json.dumps(result, ensure_ascii=True, indent=2)
    else:
        payload = json.dumps(result, ensure_ascii=True, indent=2)
    try:
        sys.stdout.write(payload + "\n")
    except UnicodeEncodeError:
        sys.stdout.write(json.dumps(result, ensure_ascii=True, indent=2) + "\n")

    phase = result.get("phase", "")
    if phase in ("FAILED_REQUIRES_REVIEW", "SAFETY_BLOCKED", "STOPPED"):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
