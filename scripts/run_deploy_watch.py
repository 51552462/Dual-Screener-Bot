#!/usr/bin/env python3
"""L-OBS-01 — 배포 관측 CLI (cron / factory.sh)."""
from __future__ import annotations

import argparse
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT in sys.path:
    sys.path.remove(_ROOT)
sys.path.insert(0, _ROOT)


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy watch — PASS/WARN/BREAK")
    parser.add_argument(
        "--phase",
        default=os.environ.get("DEPLOY_WATCH_PHASE", "post_f_gate_01"),
        help="배포 단계 SSOT (예: post_f_gate_01, post_f_retire_02, post_bear_underdog_01)",
    )
    parser.add_argument(
        "--funnel-baseline",
        default=os.environ.get("DEPLOY_WATCH_FUNNEL_BASELINE_TS", "2026-07-02"),
        help="C-FUNNEL-02 MAX(ts) 비교 기준일",
    )
    parser.add_argument("--db-path", default="", help="market DB override")
    parser.add_argument("--dry-run", action="store_true", help="JSON stdout only")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--no-persist", action="store_true")
    args = parser.parse_args()

    from deploy_watch import run_deploy_watch

    report = run_deploy_watch(
        phase=args.phase,
        db_path=args.db_path or None,
        funnel_baseline_ts=args.funnel_baseline,
        send_telegram=not args.no_telegram and not args.dry_run,
        persist=not args.no_persist,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if report.get("overall") == "BREAK":
        return 2
    if report.get("overall") == "WARN":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
