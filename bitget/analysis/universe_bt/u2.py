"""UNIVERSE-BT-U2 orchestrator — shards / window batches / checkpoint resume.

Reuses U1 replay_symbol_window / write_bt_results / resolve_historical_regime.
Constants (no new values — rule 5):
  shard_size  = TIME_MACHINE_MAX_TABLES (300)
  batch_size  = TIME_MACHINE_MAX_BARS_PER_TABLE (5000)
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from bitget.analysis.universe_bt.checkpoint import (
    load_checkpoint,
    result_keys_existing,
    save_checkpoint,
)
from bitget.analysis.universe_bt.replay import (
    _U1_MIN_BARS,
    _U1_TIMEFRAME,
    _bar_ts_from_date,
    _load_ohlcv,
    replay_symbol_window,
)
from bitget.analysis.universe_bt.store import write_bt_results
from bitget.analysis.universe_bt.universe import (
    resolve_run_timeframe,
    resolve_universe_snapshot,
    select_run_symbols,
)
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import (
    TIME_MACHINE_MAX_BARS_PER_TABLE,
    TIME_MACHINE_MAX_TABLES,
)
from bitget.infra.shared_db_connector import get_connection

logger = get_logger("bitget.analysis.universe_bt.u2")

# Reuse-only report line for OUTBOX (rule 5 — no new constants)
REUSED_TIME_MACHINE = {
    "TIME_MACHINE_MAX_TABLES": TIME_MACHINE_MAX_TABLES,
    "TIME_MACHINE_MAX_BARS_PER_TABLE": TIME_MACHINE_MAX_BARS_PER_TABLE,
    "source": "bitget.infra.memory_policy",
}


def build_universe_shards(symbols: list[str], shard_size: int) -> list[list[str]]:
    size = max(1, int(shard_size))
    syms = list(symbols)
    return [syms[i : i + size] for i in range(0, len(syms), size)]


def get_symbol_window_batches(
    symbol: str,
    market_type: str,
    batch_size: int,
    *,
    db_path: Optional[str] = None,
    timeframe: str = _U1_TIMEFRAME,
) -> list[tuple[int, int]]:
    """Split eligible bar endpoints into (start_ts, end_ts) batches.

    batch_size = TIME_MACHINE_MAX_BARS_PER_TABLE (reused). Each batch covers up to
    ``batch_size`` consecutive window endpoints. U1 per-symbol window≤5 is bypassed
    by calling replay once per bar inside the orchestrator (U1 file untouched).
    """
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        mt = "futures"
    tf = str(timeframe or _U1_TIMEFRAME).strip().upper()
    df = _load_ohlcv(symbol, mt, db_path=db_path, timeframe=tf)
    if df is None or len(df) < _U1_MIN_BARS:
        return []
    # Cap load length by TIME_MACHINE_MAX_BARS_PER_TABLE (reuse)
    max_bars = max(1, int(TIME_MACHINE_MAX_BARS_PER_TABLE))
    if len(df) > max_bars:
        df = df.iloc[-max_bars:].reset_index(drop=True)

    endpoints: List[int] = []
    for i in range(_U1_MIN_BARS - 1, len(df)):
        endpoints.append(_bar_ts_from_date(df["Date"].iloc[i]))
    if not endpoints:
        return []

    size = max(1, int(batch_size))
    batches: List[Tuple[int, int]] = []
    for i in range(0, len(endpoints), size):
        chunk = endpoints[i : i + size]
        # Inclusive window for replay: start just before first bar, end at last
        start_ts = int(chunk[0]) - 1
        end_ts = int(chunk[-1])
        batches.append((start_ts, end_ts))
    return batches


def count_paper_forward_trades(paper_db: Optional[str] = None) -> int:
    path = paper_db or market_data_db_path()
    if not os.path.isfile(path):
        return 0
    conn = get_connection(path, read_only=True)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_forward_trades'"
        ).fetchone()
        if not row:
            return 0
        return int(conn.execute("SELECT COUNT(*) FROM bitget_forward_trades").fetchone()[0])
    finally:
        conn.close()


def _normalize_mt(market_type: str) -> str:
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        return "futures"
    if mt not in ("spot", "futures"):
        raise ValueError(f"market_type must be spot|futures, got {market_type!r}")
    return mt


def run_universe_bt_u2(
    market_type: str,
    run_id: str,
    resume: bool = True,
    *,
    results_db: Optional[str] = None,
    market_db: Optional[str] = None,
    scratch_path: Optional[str] = None,
    paper_db: Optional[str] = None,
    max_symbols: Optional[int] = None,
) -> Dict[str, Any]:
    """Shard + checkpoint orchestrator. U1 engines/TF/C3 regime policy unchanged."""
    mt = _normalize_mt(market_type)
    shard_size = int(TIME_MACHINE_MAX_TABLES)
    batch_size = int(TIME_MACHINE_MAX_BARS_PER_TABLE)

    paper_before = count_paper_forward_trades(paper_db)
    paper_log: List[Dict[str, Any]] = [{"event": "start", "paper_count": paper_before}]

    tf, tf_reason = resolve_run_timeframe(mt, min_bars=_U1_MIN_BARS, db_path=market_db)
    symbols = resolve_universe_snapshot(mt, db_path=market_db, timeframe=tf)
    if max_symbols is not None:
        # Prefer majors + TF depth ≥ U1 min bars (alpha-slice / shallow 1D caused FUT=0)
        symbols = select_run_symbols(
            mt,
            symbols,
            max_symbols=max_symbols,
            min_bars=_U1_MIN_BARS,
            timeframe=tf,
            db_path=market_db,
        )

    shards = build_universe_shards(symbols, shard_size)
    ckpt = load_checkpoint(run_id, mt, db_path=results_db) if resume else None
    completed = set(ckpt["completed"]) if ckpt else set()

    rows_written = 0
    shards_done = 0

    for shard_index, shard in enumerate(shards):
        for symbol in shard:
            batches = get_symbol_window_batches(
                symbol, mt, batch_size, db_path=market_db, timeframe=tf
            )
            existing = result_keys_existing(
                run_id, mt, symbol, db_path=results_db
            )
            for batch_idx, (start_ts, end_ts) in enumerate(batches):
                if resume and (symbol, batch_idx) in completed:
                    continue

                # One bar per replay call → bypasses U1 window≤5 without editing U1
                df = _load_ohlcv(symbol, mt, db_path=market_db, timeframe=tf)
                batch_rows: List[dict] = []
                if df is not None and len(df) >= _U1_MIN_BARS:
                    max_bars = max(1, int(TIME_MACHINE_MAX_BARS_PER_TABLE))
                    if len(df) > max_bars:
                        df = df.iloc[-max_bars:].reset_index(drop=True)
                    for i in range(_U1_MIN_BARS - 1, len(df)):
                        bts = _bar_ts_from_date(df["Date"].iloc[i])
                        if bts <= start_ts or bts > end_ts:
                            continue
                        if bts in existing:
                            continue
                        part = replay_symbol_window(
                            symbol,
                            mt,
                            bts - 1,
                            bts,
                            run_id=run_id,
                            db_path=market_db,
                            scratch_path=scratch_path,
                            timeframe=tf,
                        )
                        for r in part:
                            # C3 inheritance
                            r["exit_trigger"] = None
                            r["regime_label"] = r.get("regime_label") or "UNKNOWN"
                            r["timeframe"] = tf
                        batch_rows.extend(part)
                        existing.add(bts)

                n = write_bt_results(batch_rows, db_path=results_db)
                rows_written += n
                save_checkpoint(
                    run_id,
                    mt,
                    symbol,
                    batch_idx,
                    shard_index=shard_index,
                    db_path=results_db,
                )
                completed.add((symbol, batch_idx))

        paper_now = count_paper_forward_trades(paper_db)
        paper_log.append(
            {
                "event": f"shard_{shard_index}",
                "paper_count": paper_now,
                "delta": paper_now - paper_before,
            }
        )
        if paper_now != paper_before:
            logger.error(
                "paper DB drift after shard %s: before=%s now=%s",
                shard_index,
                paper_before,
                paper_now,
            )
            raise RuntimeError(
                f"paper bitget_forward_trades count changed: {paper_before} → {paper_now}"
            )
        shards_done += 1

    paper_after = count_paper_forward_trades(paper_db)
    paper_log.append({"event": "end", "paper_count": paper_after})
    summary = {
        "run_id": run_id,
        "market_type": mt,
        "timeframe": tf,
        "timeframe_reason": tf_reason,
        "symbols": len(symbols),
        "shards": len(shards),
        "shards_done": shards_done,
        "rows_written": rows_written,
        "resume": bool(resume),
        "reused_time_machine": REUSED_TIME_MACHINE,
        "paper_counts": paper_log,
        "paper_invariant_ok": paper_after == paper_before,
        "policy": "C3_inherited",
        "banner": "L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지",
    }
    logger.info("run_universe_bt_u2 done: %s", summary)
    return summary
