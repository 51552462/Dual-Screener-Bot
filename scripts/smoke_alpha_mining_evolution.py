#!/usr/bin/env python3
"""Manual E2E smoke — alpha mining self-evolution pipeline (no Telegram by default)."""
from __future__ import annotations

import argparse
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test self-evolution pipeline")
    parser.add_argument(
        "--full-mining",
        action="store_true",
        help="Run full run_alpha_mining_pipeline (heavy; default: self-evolution only)",
    )
    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Send Telegram digest (default: dry-run print only)",
    )
    args = parser.parse_args()

    if args.full_mining:
        from alpha_mining_orchestrator import run_alpha_mining_pipeline

        result = run_alpha_mining_pipeline()
    else:
        from alpha_mining_orchestrator import run_self_evolution_pipeline

        send_fn = None
        if not args.telegram:
            captured: list[str] = []

            def _print_send(msg: str) -> bool:
                captured.append(msg)
                print("--- TELEGRAM PREVIEW ---")
                try:
                    print(msg)
                except UnicodeEncodeError:
                    print(msg.encode("utf-8", errors="replace").decode("utf-8"))
                return True

            send_fn = _print_send

        result = run_self_evolution_pipeline(
            persist=False,
            send_telegram=True,
            send_fn=send_fn,
        )

    payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    try:
        print(payload)
    except UnicodeEncodeError:
        print(json.dumps(result, ensure_ascii=True, indent=2, default=str))
    return 0 if result.get("ok", False) else 1


if __name__ == "__main__":
    raise SystemExit(main())
