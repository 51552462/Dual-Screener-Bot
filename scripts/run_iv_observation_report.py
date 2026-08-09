#!/usr/bin/env python3
"""IV 관측 리포트 CLI — 주간 텔레그램 + Cursor 붙여넣기 블록."""
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
    parser = argparse.ArgumentParser(
        description="IV observation report — weekly telegram + ---CURSOR--- block"
    )
    parser.add_argument("--db-path", default="", help="market DB override")
    parser.add_argument("--dry-run", action="store_true", help="JSON stdout, no telegram")
    parser.add_argument("--no-persist", action="store_true")
    parser.add_argument(
        "--force-telegram",
        action="store_true",
        help="항상 텔레그램 발송 (주간 기본은 readiness 변경·7일차·WARN 시)",
    )
    args = parser.parse_args()

    from iv_observation_report import run_iv_observation_report

    report = run_iv_observation_report(
        db_path=args.db_path or None,
        send_telegram=not args.dry_run,
        persist=not args.no_persist,
        force_telegram=args.force_telegram,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    readiness = (report.get("v2") or {}).get("readiness")
    if readiness == "READY":
        return 0
    if readiness == "NOT_READY":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
