"""U1 sequential replay — CAT-C engines import-only; results → isolated DB."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from bitget.analysis.universe_bt.gate_adapter import dry_try_add_virtual_position
from bitget.analysis.universe_bt.regime import resolve_historical_regime
from bitget.analysis.universe_bt.store import write_bt_results
from bitget.analysis.universe_bt.universe import (
    resolve_universe_snapshot,
    select_run_symbols,
)
from bitget.infra.data_paths import market_data_db_path, market_db_read_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import OHLCV_SIGNAL_BAR_LIMIT
from bitget.infra.shared_db_connector import get_connection
import memory_bounds

logger = get_logger("bitget.analysis.universe_bt.replay")

# U1 scope: single TF; full multi-TF batch = U2
_U1_TIMEFRAME = "1D"
_U1_MIN_BARS = 240
# Cap end-points per symbol in U1 (full bar walk = U2)
_U1_MAX_WINDOWS_PER_SYMBOL = 5


def _market_prefix(market_type: str) -> str:
    mt = str(market_type or "").strip().lower()
    if mt in ("futures", "fut", "linear"):
        return "BITGET_FUT_"
    return "BITGET_SPOT_"


def _ohlcv_table(symbol: str, market_type: str, timeframe: str = _U1_TIMEFRAME) -> str:
    return f"{_market_prefix(market_type)}{symbol}_{timeframe}"


def _bar_ts_from_date(val: Any) -> int:
    ts = pd.Timestamp(val)
    if pd.isna(ts):
        return 0
    return int(ts.timestamp())


def _load_ohlcv(
    symbol: str,
    market_type: str,
    *,
    db_path: Optional[str] = None,
    timeframe: str = _U1_TIMEFRAME,
) -> Optional[pd.DataFrame]:
    path = db_path or market_db_read_path()
    if not os.path.isfile(path):
        path = db_path or market_data_db_path()
    tbl = _ohlcv_table(symbol, market_type, timeframe)
    conn = get_connection(path, read_only=True)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (tbl,),
        ).fetchone()
        if not exists:
            return None
        tail = memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_SIGNAL_BAR_LIMIT)
        df = pd.read_sql(
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}"{tail}',
            conn,
        )
    finally:
        conn.close()
    if df is None or df.empty:
        return None
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    return df


def _load_benchmark_close(
    market_type: str,
    *,
    db_path: Optional[str] = None,
    timeframe: str = _U1_TIMEFRAME,
) -> pd.Series:
    df = _load_ohlcv("BTC_USDT", market_type, db_path=db_path, timeframe=timeframe)
    if df is None or df.empty:
        return pd.Series(dtype=float)
    s = df.set_index("Date")["Close"].astype(float)
    s.index = pd.to_datetime(s.index)
    return s


def _engine_pool_u1():
    """Import CAT-C engines via master_scanner pool helper — originals unmodified."""
    from bitget.master_scanner import _build_engine_pool

    # U1: master+ema5 only (full practitioner pool = U2 cost). Still original imports.
    return _build_engine_pool("master") + _build_engine_pool("ema5")


def _facts_from_dbg(dbg: dict) -> dict:
    return {
        "v_cpv": dbg.get("v_cpv", 0.0),
        "v_yang": dbg.get("v_yang", 0.0),
        "v_energy": dbg.get("v_energy", 0.0),
        "v_rs": dbg.get("v_rs", 0.0),
        "dyn_rs": dbg.get("dyn_rs_score", 0.0),
        "dyn_cpv": dbg.get("dyn_cpv_score", 0.0),
        "dyn_tb": dbg.get("dyn_tb_score", 0.0),
        "sn_score": dbg.get("sn_score", 0.0),
        "dtw_score": dbg.get("dtw_score", 0.0),
        "trade_value_24h": float(dbg.get("trade_value_24h", 0.0) or 0.0),
        "marcap_eok": float(dbg.get("marcap_eok", 0.0) or 0.0),
        "is_top_dna": bool(dbg.get("is_top_dna", False)),
        "is_worst_dna": bool(dbg.get("is_worst_dna", False)),
        "is_death_combo": bool(dbg.get("is_death_combo", False)),
        "is_tenbagger": bool(dbg.get("is_tenbagger", False)),
    }


def replay_symbol_window(
    symbol: str,
    market_type: str,
    start_ts: int,
    end_ts: int,
    *,
    run_id: Optional[str] = None,
    db_path: Optional[str] = None,
    scratch_path: Optional[str] = None,
) -> list[dict]:
    """Replay one symbol on bars with start_ts < bar_ts <= end_ts (U1 capped)."""
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        mt = "futures"
    if mt not in ("spot", "futures"):
        raise ValueError(f"market_type must be spot|futures, got {market_type!r}")

    df = _load_ohlcv(symbol, mt, db_path=db_path)
    if df is None or len(df) < _U1_MIN_BARS:
        return []

    idx_close = _load_benchmark_close(mt, db_path=db_path)
    engines = _engine_pool_u1()
    rid = run_id or uuid.uuid4().hex[:12]

    # Candidate end indices inside window
    ends: List[int] = []
    for i in range(_U1_MIN_BARS - 1, len(df)):
        bts = _bar_ts_from_date(df["Date"].iloc[i])
        if start_ts and bts <= int(start_ts):
            continue
        if end_ts and bts > int(end_ts):
            continue
        ends.append(i)
    if not ends:
        return []
    if len(ends) > _U1_MAX_WINDOWS_PER_SYMBOL:
        step = max(1, len(ends) // _U1_MAX_WINDOWS_PER_SYMBOL)
        ends = ends[::step][:_U1_MAX_WINDOWS_PER_SYMBOL]

    rows: List[dict] = []
    for i in ends:
        window = df.iloc[: i + 1].copy()
        bar_ts = _bar_ts_from_date(window["Date"].iloc[-1])
        regime = resolve_historical_regime(symbol, mt, bar_ts)

        # Align benchmark to window dates
        if not idx_close.empty:
            bench = idx_close.reindex(window["Date"]).ffill().bfill()
        else:
            bench = window["Close"].astype(float)

        candidate = False
        gate_ok = False
        entered = False
        side_out: Optional[str] = None

        for engine_name, engine in engines:
            try:
                hit, sig_type, out_df, dbg = engine(window, bench, _U1_TIMEFRAME)
            except Exception as ex:
                logger.debug("engine %s skip %s: %s", engine_name, symbol, ex)
                continue
            if not hit:
                continue
            candidate = True
            dbg = dbg if isinstance(dbg, dict) else {}
            side = str(dbg.get("side", "LONG")).upper()
            if side not in ("LONG", "SHORT"):
                side = "LONG"
            last_close = float(window["Close"].iloc[-1])
            entry = float(dbg.get("last_close", last_close) or last_close)
            entry_high = float(dbg.get("entry_high", window["High"].iloc[-1]) or entry)
            score = float(dbg.get("score", 0.0) or 0.0)
            sig_for_db = f"[STANDARD][{engine_name}] {sig_type}"
            facts = _facts_from_dbg(dbg)

            ok, _msg = dry_try_add_virtual_position(
                market_type=mt,
                symbol=symbol,
                timeframe=_U1_TIMEFRAME,
                sig_type=sig_for_db,
                score=score,
                entry_price=entry,
                facts=facts,
                side=side,
                entry_high=entry_high,
                scratch_path=scratch_path,
            )
            if ok:
                gate_ok = True
                entered = True
                side_out = side
                break
            # Structural reject still counts as candidate; gate_pass stays false unless ok
            # Spot SHORT hard-reject: candidate True, gate False — correct asymmetry signal
            if side_out is None:
                side_out = side
            # If reject reason is not hard structural, leave gate_ok False
            del out_df

        rows.append(
            {
                "run_id": rid,
                "market_type": mt,
                "symbol": symbol,
                "bar_ts": bar_ts,
                "regime_label": regime,
                "candidate_generated": int(candidate),
                "gate_passed": int(gate_ok),
                "virtual_entry": int(entered),
                "side": side_out,
                "exit_trigger": None,  # C3 deferred
            }
        )
    return rows


def run_universe_bt_u1(
    market_type: str,
    *,
    results_db: Optional[str] = None,
    market_db: Optional[str] = None,
    scratch_path: Optional[str] = None,
    max_symbols: Optional[int] = None,
) -> Dict[str, Any]:
    """Sequential U1 over §1 snapshot. Batch/shard/checkpoint = U2."""
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        mt = "futures"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    symbols = resolve_universe_snapshot(mt, db_path=market_db)
    if max_symbols is not None:
        symbols = select_run_symbols(
            mt,
            symbols,
            max_symbols=max_symbols,
            min_bars=_U1_MIN_BARS,
            db_path=market_db,
        )

    # Default window: all bars available (per-symbol cap inside replay)
    start_ts, end_ts = 0, 2_147_483_647

    all_rows: List[dict] = []
    for sym in symbols:
        part = replay_symbol_window(
            sym,
            mt,
            start_ts,
            end_ts,
            run_id=run_id,
            db_path=market_db,
            scratch_path=scratch_path,
        )
        all_rows.extend(part)

    n = write_bt_results(all_rows, db_path=results_db)
    summary = {
        "run_id": run_id,
        "market_type": mt,
        "symbols": len(symbols),
        "rows_written": n,
        "policy": "C3",
        "exit_trigger": "deferred",
        "banner": "L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지",
    }
    logger.info("run_universe_bt_u1 done: %s", summary)
    return summary
