#!/usr/bin/env bash
# FULL-BT-HIST-1/2 pilot — max_symbols=10, SPOT then FUTURES (paper untouched).
# HIST-2: batch out includes diag (engine_hit / gate_reject) from full_bt_diag.
# Usage (coin VPS):
#   cd ~/dante_bots/Dual-Screener-Bot
#   git pull
#   export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
#   bash bitget/deploy/run_full_bt_hist_pilot.sh
# Optional:
#   BITGET_FULL_BT_MAX_SYMBOLS=10   # Handoff default (U3 reuse); do not invent
#   BITGET_FULL_BT_MAX_SYMBOLS=3    # HIST-2 dry (소표본)
#   BITGET_FULL_BT_START_DATE=YYYY-MM-DD  # RUN-2: COUNT first bar (no hardcode)
set -euo pipefail

ROOT="${BITGET_INSTALL_ROOT:-${INSTALL_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}}"
cd "$ROOT"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:$PYTHONPATH}"

PY="${ROOT}/venv/bin/python"
[[ -x "$PY" ]] || PY="${ROOT}/.venv/bin/python"
[[ -x "$PY" ]] || PY="$(command -v python3 || true)"
if [[ -z "${PY}" || ! -x "${PY}" ]]; then
  echo "ERROR: venv/python not found under ${ROOT}/venv or ${ROOT}/.venv" >&2
  exit 1
fi

if [[ -z "${BITGET_DB_STORAGE_PATH:-}" ]]; then
  if [[ -d /var/lib/quant-bitget/data ]]; then
    export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
  fi
fi

export BITGET_FULL_BT_MAX_SYMBOLS="${BITGET_FULL_BT_MAX_SYMBOLS:-10}"

echo "[full-bt-pilot] ROOT=$ROOT"
echo "[full-bt-pilot] PY=$PY"
echo "[full-bt-pilot] BITGET_DB_STORAGE_PATH=${BITGET_DB_STORAGE_PATH:-unset}"
echo "[full-bt-pilot] MAX_SYMBOLS=${BITGET_FULL_BT_MAX_SYMBOLS}"

"$PY" - <<'PY'
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

ROOT = os.environ.get("BITGET_INSTALL_ROOT") or os.getcwd()
sys.path.insert(0, ROOT)

from bitget.full_bt.batch import count_paper_forward_trades, run_full_bt_batch
from bitget.full_bt.paths import full_bt_db_path
from bitget.full_bt.report import (
    L1_BANNER,
    generate_full_bt_l1_report,
    render_full_bt_l1_report_md,
)
from bitget.infra.data_paths import market_data_db_path, market_db_read_path


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _cols(db: str, table: str) -> list[str]:
    if not os.path.isfile(db):
        return []
    conn = sqlite3.connect(db)
    try:
        return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
    finally:
        conn.close()


def _count(db: str) -> int:
    if not os.path.isfile(db):
        return 0
    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_forward_trades'"
        ).fetchone()
        if not row:
            return 0
        return int(conn.execute("SELECT COUNT(*) FROM bitget_forward_trades").fetchone()[0])
    finally:
        conn.close()


def _one(mt: str, run_id: str, *, market_db: str, results_db: str, paper_db: str, max_sym: int, start_date=None) -> dict:
    paper_before = count_paper_forward_trades(paper_db)
    table_before = _count(results_db)
    out = run_full_bt_batch(
        mt,
        run_id,
        resume=True,
        results_db=results_db,
        market_db=market_db,
        paper_db=paper_db,
        max_symbols=max_sym,
        start_date=start_date,
    )
    paper_after = count_paper_forward_trades(paper_db)
    table_after = _count(results_db)
    rep = generate_full_bt_l1_report(mt, run_id, db_path=results_db, market_db=market_db)
    md = render_full_bt_l1_report_md(rep)
    q = rep.get("quantitative") or {}
    clues = rep.get("clues") or {}
    return {
        "market_type": mt,
        "run_id": run_id,
        "batch": {
            "symbol_count": out.get("symbol_count"),
            "batches_run": out.get("batches_run"),
            "batches_skipped": out.get("batches_skipped"),
        },
        "diag": out.get("diag"),
        "paper_before": paper_before,
        "paper_after": paper_after,
        "results_table_before": table_before,
        "results_table_after": table_after,
        "trade_count": q.get("trade_count"),
        "total_return_pct": q.get("total_return_pct"),
        "mdd_pct": q.get("mdd_pct"),
        "gate_bottleneck_by_step": clues.get("gate_bottleneck_by_step"),
        "side_asymmetry": clues.get("side_asymmetry"),
        "banner_ok": md.startswith(L1_BANNER),
        "warnings": rep.get("warnings"),
    }


def main() -> int:
    from datetime import date as date_cls

    market_db = (os.environ.get("BITGET_FULL_BT_MARKET_DB") or "").strip() or market_db_read_path()
    paper_db = market_data_db_path()
    results_db = full_bt_db_path()
    max_sym = int(os.environ.get("BITGET_FULL_BT_MAX_SYMBOLS", "10") or "10")
    only_mt = (os.environ.get("BITGET_FULL_BT_ONLY_MT") or "").strip().lower()
    start_raw = (os.environ.get("BITGET_FULL_BT_START_DATE") or "").strip()
    start_date = None
    if start_raw:
        start_date = date_cls.fromisoformat(start_raw)
    stamp = _ts()
    spot_id = f"pilot-spot-{stamp}"
    fut_id = f"pilot-fut-{stamp}"

    print(f"[full-bt-pilot] market_db={market_db}")
    print(f"[full-bt-pilot] paper_db={paper_db}")
    print(f"[full-bt-pilot] results_db={results_db}")
    print(f"[full-bt-pilot] only_mt={only_mt or 'spot+futures'}")
    print(f"[full-bt-pilot] start_date={start_date.isoformat() if start_date else 'None'}")
    if not os.path.isfile(market_db):
        print("FAIL: market DB missing", file=sys.stderr)
        return 2

    summary = {
        "results_db": results_db,
        "market_db": market_db,
        "paper_db": paper_db,
        "only_mt": only_mt or "spot+futures",
        "start_date": start_date.isoformat() if start_date else None,
        "result_table_columns": _cols(results_db, "bitget_forward_trades"),
        "run_id_column_present": "run_id" in _cols(results_db, "bitget_forward_trades"),
    }
    if only_mt not in ("futures", "fut"):
        summary["spot"] = _one(
            "spot",
            spot_id,
            market_db=market_db,
            results_db=results_db,
            paper_db=paper_db,
            max_sym=max_sym,
            start_date=start_date,
        )
    else:
        summary["spot"] = {"skipped": True, "reason": "BITGET_FULL_BT_ONLY_MT=futures"}
    if only_mt not in ("spot",):
        summary["futures"] = _one(
            "futures",
            fut_id,
            market_db=market_db,
            results_db=results_db,
            paper_db=paper_db,
            max_sym=max_sym,
            start_date=start_date,
        )
    else:
        summary["futures"] = {"skipped": True, "reason": "BITGET_FULL_BT_ONLY_MT=spot"}

    cols = summary["result_table_columns"]
    summary["run_id_column_present"] = "run_id" in cols
    out_path = os.path.join(
        os.environ.get("BITGET_DB_STORAGE_PATH") or os.path.dirname(results_db) or ".",
        f"full_bt_pilot_summary_{stamp}.json",
    )
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    # ASCII-safe console dump
    print(json.dumps(summary, ensure_ascii=True, indent=2, default=str))
    print(f"WROTE {out_path}")
    return 0


raise SystemExit(main())
PY
