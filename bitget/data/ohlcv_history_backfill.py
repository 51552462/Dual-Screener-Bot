"""FUT/SPOT OHLCV history depth backfill — pilot Adapter (CAT-B).

Default ``mtf_data_updater`` remains tail-only (limit, no since).
This module paginates ``fetch_ohlcv(..., since=)`` and merges into SQLite
so short refreshes do not erase deeper history.

Pilot scope: explicit symbol list only — never full universe.
FULL-BT / IV L1 reference only — not LIVE promotion evidence.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, List, Optional, Sequence, Tuple

import pandas as pd

from bitget.infra.logging_setup import get_logger
from bitget.infra.shared_db_connector import get_connection
from bitget.mtf_data_updater import (
    create_exchange,
    normalize_symbol,
    save_ohlcv,
    table_name,
)

logger = get_logger("bitget.data.ohlcv_history_backfill")

# Warmup SSOT reused by FULL-BT (_U1_MIN_BARS=240); pilot target ≥ SPOT-like depth
DEFAULT_TARGET_BARS = 300
DEFAULT_SINCE_UTC = "2024-01-01"
DEFAULT_FUT_SYMBOLS = (
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
)
_MS_PER_DAY = 86_400_000
_MAX_PAGES = 80


def _parse_since_ms(since_utc: str) -> int:
    s = str(since_utc).strip()[:10]
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_ohlcv_paginated(
    exchange: Any,
    symbol: str,
    timeframe: str,
    *,
    since_ms: int,
    target_bars: int,
    until_ms: Optional[int] = None,
    page_limit: int = 200,
    sleep_sec: float = 0.12,
    fetch_fn: Optional[Callable[..., list]] = None,
) -> List[list]:
    """Walk forward from since_ms until near ``until_ms`` (default: now).

    Bitget swap 1D often returns ~90 bars/page even when limit=200.
    After paging, keep the **most recent** ``target_bars`` (FULL-BT walk needs
    current tail depth, not a stale mid-history window).
    """
    fetch = fetch_fn or (
        lambda sym, tf, since, lim: exchange.fetch_ohlcv(
            symbol=sym, timeframe=tf, since=since, limit=lim
        )
    )
    end_ms = int(until_ms) if until_ms is not None else int(time.time() * 1000)
    out: List[list] = []
    seen: set = set()
    cursor = int(since_ms)
    pages = 0
    while pages < _MAX_PAGES and cursor < end_ms:
        rows = fetch(symbol, timeframe, cursor, int(page_limit)) or []
        pages += 1
        if not rows:
            break
        new = 0
        for row in rows:
            ts = int(row[0])
            if ts in seen:
                continue
            seen.add(ts)
            out.append(list(row))
            new += 1
        last_ts = int(rows[-1][0])
        nxt = last_ts + _MS_PER_DAY
        if nxt <= cursor:
            break
        cursor = nxt
        if new == 0:
            break
        if last_ts >= end_ms:
            break
        if sleep_sec > 0:
            time.sleep(float(sleep_sec))
    out.sort(key=lambda r: int(r[0]))
    if len(out) > int(target_bars):
        out = out[-int(target_bars) :]
    return out


def _load_existing_ohlcv_rows(
    db_path: str, market_type: str, symbol: str, timeframe: str
) -> List[list]:
    """Return [[ts_ms, o, h, l, c, v], ...] from existing table (if any)."""
    tbl = table_name(market_type, symbol, timeframe)
    if not os.path.isfile(db_path):
        return []
    conn = get_connection(db_path, read_only=True)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (tbl,),
        ).fetchone()
        if not exists:
            return []
        df = pd.read_sql(
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}"',
            conn,
        )
    finally:
        conn.close()
    if df is None or df.empty:
        return []
    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    rows: List[list] = []
    for _, r in df.iterrows():
        ts = int(r["Date"].timestamp() * 1000)
        rows.append(
            [
                ts,
                float(r["Open"]),
                float(r["High"]),
                float(r["Low"]),
                float(r["Close"]),
                float(r["Volume"]),
            ]
        )
    return rows


def merge_ohlcv_rows(existing: Sequence[list], fetched: Sequence[list]) -> List[list]:
    """Union by timestamp; prefer fetched values on conflict."""
    by_ts: dict = {}
    for row in existing:
        by_ts[int(row[0])] = list(row)
    for row in fetched:
        by_ts[int(row[0])] = list(row)
    return [by_ts[k] for k in sorted(by_ts.keys())]


def backfill_symbol_1d(
    market_type: str,
    symbol: str,
    *,
    db_path: str,
    since_utc: str = DEFAULT_SINCE_UTC,
    target_bars: int = DEFAULT_TARGET_BARS,
    exchange: Any = None,
) -> dict:
    """Fetch+merge+save one symbol 1D. Returns diagnostic dict."""
    mt = "spot" if str(market_type).lower() in ("spot",) else "futures"
    ex = exchange or create_exchange("spot" if mt == "spot" else "swap")
    since_ms = _parse_since_ms(since_utc)
    fetched = fetch_ohlcv_paginated(
        ex,
        symbol,
        "1d",
        since_ms=since_ms,
        target_bars=int(target_bars),
    )
    existing = _load_existing_ohlcv_rows(db_path, mt, symbol, "1d")
    merged = merge_ohlcv_rows(existing, fetched)
    # Keep at least target depth from the end if oversize merge
    if len(merged) > max(int(target_bars) * 2, int(target_bars)):
        merged = merged[-max(int(target_bars) * 2, 600) :]
    conn = get_connection(db_path)
    try:
        ok = save_ohlcv(conn, mt, symbol, "1d", merged)
    finally:
        conn.close()
    first = last = None
    if merged:
        first = datetime.fromtimestamp(merged[0][0] / 1000, tz=timezone.utc).date().isoformat()
        last = datetime.fromtimestamp(merged[-1][0] / 1000, tz=timezone.utc).date().isoformat()
    return {
        "market_type": mt,
        "symbol": symbol,
        "table": table_name(mt, symbol, "1d"),
        "fetched": len(fetched),
        "existing_before": len(existing),
        "merged": len(merged),
        "saved": bool(ok),
        "first": first,
        "last": last,
        "target_bars": int(target_bars),
        "warmup_ok": len(merged) >= 240,
    }


def run_fut_1d_depth_pilot(
    *,
    db_path: Optional[str] = None,
    symbols: Optional[Sequence[str]] = None,
    since_utc: str = DEFAULT_SINCE_UTC,
    target_bars: int = DEFAULT_TARGET_BARS,
) -> dict:
    """Pilot: FUT 1D depth for explicit symbols only (default BTC/ETH/SOL)."""
    from bitget.infra.data_paths import market_data_db_path

    # Write target (Claude Ask 조건1):
    # - default: market_data_db_path() == BITGET_DB_STORAGE_PATH 프로덕션 OHLCV (직접 merge-write)
    # - staging: BITGET_FUT_DEPTH_DB=/path/to/staging.sqlite 로 격리 가능
    staging = (os.environ.get("BITGET_FUT_DEPTH_DB") or "").strip()
    path = db_path or staging or market_data_db_path()
    if db_path:
        write_mode = "explicit_db_path"
    elif staging:
        write_mode = "staging"
    else:
        write_mode = "production"

    syms = list(symbols) if symbols is not None else list(DEFAULT_FUT_SYMBOLS)
    env_syms = (os.environ.get("BITGET_FUT_DEPTH_SYMBOLS") or "").strip()
    if env_syms and symbols is None:
        syms = [s.strip() for s in env_syms.split(",") if s.strip()]
    target = int(os.environ.get("BITGET_FUT_DEPTH_TARGET_BARS") or target_bars)
    since = (os.environ.get("BITGET_FUT_DEPTH_SINCE") or since_utc).strip()

    logger.info(
        "fut_1d_depth_pilot db=%s write_mode=%s symbols=%s target=%s since=%s",
        path,
        write_mode,
        syms,
        target,
        since,
    )
    if write_mode == "production":
        logger.warning(
            "FUT depth pilot writes PRODUCTION OHLCV tables (atomic replace of merged 1D). "
            "Pause mtf data-refresh during run; later tail-only refresh can wipe depth. "
            "Set BITGET_FUT_DEPTH_DB for staging."
        )
    ex = create_exchange("swap")
    results = []
    for sym in syms:
        results.append(
            backfill_symbol_1d(
                "futures",
                sym,
                db_path=path,
                since_utc=since,
                target_bars=target,
                exchange=ex,
            )
        )
    return {
        "db_path": path,
        "write_mode": write_mode,
        "target_bars": target,
        "since_utc": since,
        "results": results,
        "all_warmup_ok": all(r.get("warmup_ok") for r in results),
    }
