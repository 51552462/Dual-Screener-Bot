#!/usr/bin/env python3
"""S5 페이퍼 게이트 스모크 — read-only JSON 산출."""
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
    parser = argparse.ArgumentParser(description="S5 defense contribution log (read-only)")
    parser.add_argument("--start", default="2026-08-17")
    parser.add_argument("--end", default="")
    parser.add_argument("--market", default="", help="KR | US | empty=both")
    parser.add_argument("--forward-db", default="")
    parser.add_argument("--short-db", default="")
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--as-of", default="")
    args = parser.parse_args()

    from datetime import datetime, timezone

    from reports.s5_defense_contribution import (
        compute_s5_defense_contribution_log,
        write_s5_contribution_json,
    )

    end = args.end.strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    market = args.market.strip().upper() or None
    payload = compute_s5_defense_contribution_log(
        args.start,
        end,
        market=market,
        forward_db_path=args.forward_db.strip() or None,
        short_db_path=args.short_db.strip() or None,
    )
    as_of = args.as_of.strip() or datetime.now(timezone.utc).strftime("%Y%m%d")
    path = write_s5_contribution_json(
        payload,
        as_of=as_of,
        out_dir=args.out_dir.strip() or None,
    )
    text = json.dumps({"wrote": path, "report": payload}, ensure_ascii=False, indent=2, default=str)
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
