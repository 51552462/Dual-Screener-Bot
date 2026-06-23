#!/usr/bin/env python3
"""forward_trades OPEN 장부 ↔ 리포트 정합 진단 (Ubuntu/로컬)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> int:
    from forward.forward_book_integrity import diagnose_open_book_from_db
    from reports.report_timekeeper import ReportTimekeeper

    p = argparse.ArgumentParser(description="OPEN 장부 정합 진단")
    p.add_argument("--market", choices=("KR", "US", "ALL"), default="ALL")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    markets = ("KR", "US") if args.market == "ALL" else (args.market,)

    out = {}
    exit_code = 0
    for mkt in markets:
        tk = ReportTimekeeper.for_market(mkt)
        st = diagnose_open_book_from_db(mkt, session_anchor=tk.session_anchor)
        out[mkt] = st.as_dict()
        if st.open_raw > 0 and st.open_valid == 0:
            exit_code = 1
        if st.open_raw == 0 and st.integrity_note.startswith("OPEN_EMPTY"):
            exit_code = max(exit_code, 2)

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        for mkt, d in out.items():
            print(f"=== {mkt} OPEN book ===")
            for k, v in d.items():
                print(f"  {k}: {v}")
            print()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
