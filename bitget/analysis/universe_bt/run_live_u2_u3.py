"""One-shot: inspect Bitget OHLCV then run UNIVERSE-BT U2 + U3 (L0)."""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Optional

# file: bitget/analysis/universe_bt/run_live_u2_u3.py → repo root = parents[3]
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault("BITGET_UNIVERSE_BT_OHLCV_ONLY", "1")  # no live exchange required


def main() -> int:
    from bitget.infra.data_paths import bitget_data_dir, market_data_db_path, market_db_read_path
    from bitget.analysis.universe_bt.paths import universe_bt_db_path
    from bitget.analysis.universe_bt.u2 import count_paper_forward_trades, run_universe_bt_u2
    from bitget.analysis.universe_bt.u3_report import (
        build_u3_side_by_side_report,
        write_u3_report_file,
    )
    from bitget.analysis.universe_bt.universe import list_ohlcv_symbols

    print("=== UNIVERSE-BT live run (L0) ===")
    print("data_dir:", bitget_data_dir())
    md = market_data_db_path()
    rd = market_db_read_path()
    market = rd if os.path.isfile(rd) else md
    print("market_db:", market, "exists=", os.path.isfile(market))
    if not os.path.isfile(market):
        print("FAIL: no OHLCV sqlite — cannot run historical verification")
        return 2

    conn = sqlite3.connect(market)
    try:
        spot_n = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_SPOT_%_1D'"
        ).fetchone()[0]
        fut_n = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_FUT_%_1D'"
        ).fetchone()[0]
    finally:
        conn.close()
    print(f"SPOT_1D tables={spot_n} FUT_1D tables={fut_n}")
    spot_syms = list_ohlcv_symbols("spot", db_path=market)
    fut_syms = list_ohlcv_symbols("futures", db_path=market)
    print(f"OHLCV symbols spot={len(spot_syms)} futures={len(fut_syms)}")
    if not spot_syms and not fut_syms:
        print(
            "FAIL: no BITGET_SPOT_*/BITGET_FUT_* OHLCV tables. "
            "Run on coin VPS with BITGET_DB_STORAGE_PATH pointing at real data "
            "(e.g. /var/lib/quant-bitget/data). Local empty sqlite cannot verify history."
        )
        return 2

    # Cap: prefer majors + 1D depth; 0/all = no cap. Default 30 for VPS first-pass.
    raw_max = (os.environ.get("BITGET_UNIVERSE_BT_MAX_SYMBOLS") or "30").strip()
    max_sym: Optional[int]
    if raw_max in ("0", "all", "ALL", "-1"):
        max_sym = None
    else:
        max_sym = int(raw_max)
    run_id = "live-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results = universe_bt_db_path()
    paper_before = count_paper_forward_trades()
    print(f"run_id={run_id} max_symbols={max_sym} results_db={results}")
    print(f"paper_forward_trades before={paper_before}")

    summaries = []
    for mt in ("spot", "futures"):
        print(f"--- run_universe_bt_u2 market={mt} ---")
        out = run_universe_bt_u2(
            mt,
            run_id,
            resume=True,
            market_db=market,
            results_db=results,
            max_symbols=max_sym,
        )
        summaries.append(out)
        print(
            f"  rows_written={out.get('rows_written')} "
            f"symbols={out.get('symbols')} "
            f"paper_ok={out.get('paper_invariant_ok')}"
        )

    paper_after = count_paper_forward_trades()
    print(f"paper_forward_trades after={paper_after} delta={paper_after - paper_before}")
    if paper_after != paper_before:
        print("FAIL: paper DB drifted")
        return 3

    dual = build_u3_side_by_side_report(run_id, db_path=results)
    path = write_u3_report_file(dual)
    print("U3 report:", path)
    print("--- banner + metrics preview ---")
    from bitget.analysis.universe_bt.u3_report import render_u3_report_md

    md_text = render_u3_report_md(dual)
    try:
        print(md_text[:2500])
    except UnicodeEncodeError:
        sys.stdout.buffer.write((md_text[:2500] + "\n").encode("utf-8", errors="replace"))
    print("=== DONE ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
