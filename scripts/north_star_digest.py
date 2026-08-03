#!/usr/bin/env python3
"""듀얼 북극성 진행장부 다이제스트 CLI — cron / factory.sh 용."""
from __future__ import annotations

import argparse
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def main() -> int:
    parser = argparse.ArgumentParser(description="Dual North Star progress digest")
    parser.add_argument(
        "--cadence",
        choices=("daily", "weekly", "monthly", "yearly"),
        default="daily",
    )
    parser.add_argument("--dry-run", action="store_true", help="텔레그램 미발송, stdout JSON")
    parser.add_argument("--no-persist", action="store_true", help="ledger 미저장")
    args = parser.parse_args()

    from dual_north_star_telegram import send_north_star_digest

    out = send_north_star_digest(
        cadence=args.cadence,
        persist=not args.no_persist,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        print(out.get("html", ""))
        print("---")
        print(json.dumps(out.get("snap"), ensure_ascii=False, indent=2, default=str))
    if out.get("error"):
        print(out["error"], file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
