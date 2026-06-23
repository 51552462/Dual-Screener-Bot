#!/usr/bin/env python3
"""
forward_trades 식별자(종목미상) 역추적 · 백필 — Ubuntu/로컬 운영 CLI.

예:
  python scripts/diag_forward_trade_identity_gaps.py --market KR
  python scripts/diag_forward_trade_identity_gaps.py --market ALL --repair
  python scripts/diag_forward_trade_identity_gaps.py --market US --json --repair-all
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from market_db_paths import MARKET_DATA_DB_PATH


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="forward_trades 종목명 공백 진단·백필")
    p.add_argument("--market", choices=("KR", "US", "ALL"), default="ALL")
    p.add_argument("--db", default=MARKET_DATA_DB_PATH, help="market_data.sqlite 경로")
    p.add_argument("--rolling-days", type=int, default=90)
    p.add_argument("--row-limit", type=int, default=40)
    p.add_argument("--json", action="store_true", help="JSON stdout")
    p.add_argument(
        "--repair",
        action="store_true",
        help="롤링 윈도우+OPEN 이름 백필 적용 (기본 dry-run)",
    )
    p.add_argument(
        "--repair-all",
        action="store_true",
        help="전체 이력 이름 백필 (--repair 필수)",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="실제 UPDATE (없으면 dry-run)",
    )
    return p.parse_args()


def main() -> int:
    import sqlite3

    from forward.forward_trade_identity import (
        backfill_forward_trade_names,
        diagnose_forward_trade_identity,
        diagnostic_report_to_dict,
        format_diagnostic_report_text,
        format_repair_log_line,
    )

    args = _parse_args()
    markets = ("KR", "US") if args.market == "ALL" else (args.market,)
    dry_run = not args.apply
    only_window = not args.repair_all

    if args.repair_all and not args.repair:
        print("--repair-all requires --repair", file=sys.stderr)
        return 2

    exit_code = 0
    payload: dict = {"markets": {}}

    conn = sqlite3.connect(args.db, timeout=60)
    try:
        for mkt in markets:
            diag = diagnose_forward_trade_identity(
                conn,
                mkt,
                rolling_days=args.rolling_days,
                db_path=args.db,
                row_limit=args.row_limit,
            )
            backfill = None
            if args.repair:
                backfill = backfill_forward_trade_names(
                    conn,
                    mkt,
                    dry_run=dry_run,
                    db_path=args.db,
                    only_window=only_window,
                    rolling_days=args.rolling_days,
                )

            if args.json:
                entry = diagnostic_report_to_dict(diag)
                if backfill:
                    entry["backfill"] = {
                        "dry_run": backfill.dry_run,
                        "candidates": backfill.candidates,
                        "updated": backfill.updated,
                        "skipped_no_lookup": backfill.skipped_no_lookup,
                        "skipped_already_ok": backfill.skipped_already_ok,
                        "sample_updates": backfill.sample_updates,
                    }
                payload["markets"][mkt] = entry
            else:
                print(format_diagnostic_report_text(diag))
                if backfill:
                    print(format_repair_log_line(diag, backfill))
                    if backfill.sample_updates:
                        print("sample_updates (id, code, old, new):")
                        for row in backfill.sample_updates:
                            print(f"  {row}")
                print()

            if diag.verdict.startswith("PIPELINE_STALL"):
                exit_code = max(exit_code, 2)
            elif diag.n_gap_window > 0 and not args.repair:
                exit_code = max(exit_code, 1)
    finally:
        conn.close()

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
