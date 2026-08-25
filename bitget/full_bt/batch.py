"""FULL-BT-2 batch orchestrator — shards / window batches / checkpoint resume.

Reuses FULL-BT-1 ``harness.run_replay`` only (no CAT-C/D/E rewrite).
Constants (rule 5 — reuse only):
  shard_size  = TIME_MACHINE_MAX_TABLES
  batch_size  = TIME_MACHINE_MAX_BARS_PER_TABLE
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from bitget.full_bt.checkpoint import load_full_bt_checkpoint, save_full_bt_checkpoint
from bitget.full_bt.harness import REUSED_SCANNER_TIMEFRAMES, run_replay
from bitget.full_bt.paths import full_bt_db_path
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import (
    TIME_MACHINE_MAX_BARS_PER_TABLE,
    TIME_MACHINE_MAX_TABLES,
)
from bitget.infra.shared_db_connector import get_connection

logger = get_logger("bitget.full_bt.batch")

REUSED_TIME_MACHINE = {
    "TIME_MACHINE_MAX_TABLES": TIME_MACHINE_MAX_TABLES,
    "TIME_MACHINE_MAX_BARS_PER_TABLE": TIME_MACHINE_MAX_BARS_PER_TABLE,
    "source": "bitget.infra.memory_policy",
}


def build_full_bt_shards(symbols: list[str], shard_size: int) -> list[list[str]]:
    size = max(1, int(shard_size))
    syms = list(symbols)
    return [syms[i : i + size] for i in range(0, len(syms), size)]


def _normalize_mt(market_type: str) -> str:
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        return "futures"
    if mt not in ("spot", "futures"):
        raise ValueError(f"market_type must be spot|futures, got {market_type!r}")
    return mt


def _ts_to_date(ts: int) -> date:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()


def get_full_bt_window_batches(
    symbol: str,
    market_type: str,
    batch_size: int,
    *,
    db_path: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> list[tuple[int, int]]:
    """Split OHLCV bar timeline into (start_ts, end_ts) batches.

    ``batch_size`` = TIME_MACHINE_MAX_BARS_PER_TABLE (reused). TF = FULL-BT-1 list[0].
    OHLCV loader: universe_bt read-only helper (CAT-B read; no write).
    """
    from bitget.analysis.universe_bt.replay import _bar_ts_from_date, _load_ohlcv

    mt = _normalize_mt(market_type)
    tf = str(timeframe or REUSED_SCANNER_TIMEFRAMES[0]).strip().upper()
    df = _load_ohlcv(symbol, mt, db_path=db_path, timeframe=tf)
    if df is None or len(df) == 0:
        return []

    max_bars = max(1, int(TIME_MACHINE_MAX_BARS_PER_TABLE))
    if len(df) > max_bars:
        df = df.iloc[-max_bars:].reset_index(drop=True)

    endpoints: List[int] = [_bar_ts_from_date(df["Date"].iloc[i]) for i in range(len(df))]
    endpoints = [e for e in endpoints if e > 0]
    if not endpoints:
        return []

    size = max(1, int(batch_size))
    batches: List[Tuple[int, int]] = []
    for i in range(0, len(endpoints), size):
        chunk = endpoints[i : i + size]
        batches.append((int(chunk[0]) - 1, int(chunk[-1])))
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


def _default_symbols(market_type: str, *, market_db: Optional[str], max_symbols: Optional[int]) -> list[str]:
    from bitget.analysis.universe_bt.universe import (
        resolve_universe_snapshot,
        select_run_symbols,
    )

    mt = _normalize_mt(market_type)
    tf = REUSED_SCANNER_TIMEFRAMES[0]
    symbols = resolve_universe_snapshot(mt, db_path=market_db, timeframe=tf)
    if max_symbols is not None:
        symbols = select_run_symbols(
            mt,
            symbols,
            max_symbols=max_symbols,
            min_bars=1,
            timeframe=tf,
            db_path=market_db,
        )
    return list(symbols)


def run_full_bt_batch(
    market_type: str,
    run_id: str,
    resume: bool = True,
    *,
    results_db: Optional[str] = None,
    market_db: Optional[str] = None,
    paper_db: Optional[str] = None,
    symbols: Optional[list[str]] = None,
    max_symbols: Optional[int] = None,
    engine: str = "MASTER",
    replay_fn: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    """Shard + checkpoint orchestrator over FULL-BT-1 ``run_replay``."""
    mt = _normalize_mt(market_type)
    shard_size = int(TIME_MACHINE_MAX_TABLES)
    batch_size = int(TIME_MACHINE_MAX_BARS_PER_TABLE)
    full_db = results_db or full_bt_db_path()
    replay = replay_fn or run_replay

    paper_before = count_paper_forward_trades(paper_db)
    paper_log: List[Dict[str, Any]] = [{"event": "start", "paper_count": paper_before}]

    syms = list(symbols) if symbols is not None else _default_symbols(
        mt, market_db=market_db, max_symbols=max_symbols
    )
    shards = build_full_bt_shards(syms, shard_size)
    ckpt = load_full_bt_checkpoint(run_id, mt, db_path=full_db) if resume else None
    completed = set(ckpt["completed"]) if ckpt else set()

    batches_run = 0
    batches_skipped = 0

    for shard_index, shard in enumerate(shards):
        for symbol in shard:
            batches = get_full_bt_window_batches(
                symbol, mt, batch_size, db_path=market_db
            )
            for batch_idx, (start_ts, end_ts) in enumerate(batches):
                if resume and (symbol, batch_idx) in completed:
                    batches_skipped += 1
                    continue
                start_d = _ts_to_date(start_ts if start_ts > 0 else end_ts)
                end_d = _ts_to_date(end_ts)
                replay(
                    mt,
                    symbol,
                    engine,
                    start_d,
                    end_d,
                    full_db,
                    market_db=market_db,
                    run_id=run_id,
                )
                save_full_bt_checkpoint(
                    run_id,
                    mt,
                    symbol,
                    batch_idx,
                    shard_index=shard_index,
                    db_path=full_db,
                )
                completed.add((symbol, batch_idx))
                batches_run += 1

        paper_now = count_paper_forward_trades(paper_db)
        paper_log.append(
            {
                "event": "shard_done",
                "shard_index": shard_index,
                "paper_count": paper_now,
            }
        )
        if paper_now != paper_before:
            raise RuntimeError(
                f"paper bitget_forward_trades changed after shard {shard_index}: "
                f"{paper_before} -> {paper_now}"
            )

    paper_after = count_paper_forward_trades(paper_db)
    paper_log.append({"event": "end", "paper_count": paper_after})
    if paper_after != paper_before:
        raise RuntimeError(
            f"paper bitget_forward_trades invariant broken: {paper_before} -> {paper_after}"
        )

    from bitget.full_bt.harness import summarize_diag

    out = {
        "run_id": run_id,
        "market_type": mt,
        "symbol_count": len(syms),
        "shard_count": len(shards),
        "batches_run": batches_run,
        "batches_skipped": batches_skipped,
        "paper_before": paper_before,
        "paper_after": paper_after,
        "paper_log": paper_log,
        "reused_time_machine": dict(REUSED_TIME_MACHINE),
        "tf_reused": list(REUSED_SCANNER_TIMEFRAMES),
        "diag": summarize_diag(full_db, run_id, mt),
    }
    logger.info("full_bt_batch done %s", {k: out[k] for k in out if k != "paper_log"})
    return out
