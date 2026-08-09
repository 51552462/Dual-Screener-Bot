"""RP-1 L0: FDR fetch with timeout + local parquet cache (isolated from CAT-B live fetch)."""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Optional, Tuple

import pandas as pd

_CACHE_EXECUTOR: Optional[ThreadPoolExecutor] = None


def _log(msg: str) -> None:
    print(msg, flush=True)


def rp1_ohlcv_cache_dir() -> str:
    override = os.environ.get("RP1_OHLCV_CACHE_DIR", "").strip()
    if override:
        return override
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "rp1_ohlcv")


def resolve_rp1_fetch_timeout() -> int:
    raw = os.environ.get("RP1_FETCH_TIMEOUT", "30").strip()
    try:
        return max(5, min(300, int(raw)))
    except ValueError:
        return 30


def _ticker_cache_path(ticker: str) -> str:
    safe = (
        str(ticker)
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("*", "_")
    )
    return os.path.join(rp1_ohlcv_cache_dir(), f"{safe}.parquet")


def _normalize_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.sort_index()
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index)
    if out.index.has_duplicates:
        out = out[~out.index.duplicated(keep="last")]
    return out


def _parse_ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(str(value)[:10])


def _covers_range(df: pd.DataFrame, start: str, end: str) -> bool:
    if df is None or df.empty:
        return False
    lo = _parse_ts(start)
    hi = _parse_ts(end)
    return df.index.min() <= lo and df.index.max() >= hi


def _read_cache(path: str) -> Optional[pd.DataFrame]:
    if not os.path.isfile(path):
        return None
    try:
        return _normalize_ohlcv_df(pd.read_parquet(path))
    except Exception as exc:
        _log(f"[RP-1-OHLCV] cache corrupt {path}: {exc} — will refetch")
        return None


def _write_cache(path: str, df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _normalize_ohlcv_df(df).to_parquet(path, index=True)


def _fetch_fdr(ticker: str, start: str, end: str) -> pd.DataFrame:
    import FinanceDataReader as fdr

    return fdr.DataReader(ticker, start, end)


def _get_executor() -> ThreadPoolExecutor:
    global _CACHE_EXECUTOR
    if _CACHE_EXECUTOR is None:
        _CACHE_EXECUTOR = ThreadPoolExecutor(max_workers=1)
    return _CACHE_EXECUTOR


def fetch_ohlcv_cached(
    ticker: str,
    start: str,
    end: str,
    *,
    timeout_sec: Optional[int] = None,
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Return (DataFrame, gate).
    gate: cache_hit | fetch_ok | timeout | fetch_error | skip_empty
    Timeout/error → (None, gate) — caller must skip ticker, not abort batch.
    """
    if timeout_sec is None:
        timeout_sec = resolve_rp1_fetch_timeout()

    path = _ticker_cache_path(ticker)
    cached = _read_cache(path)
    if cached is not None and _covers_range(cached, start, end):
        return cached, "cache_hit"

    try:
        fut = _get_executor().submit(_fetch_fdr, ticker, start, end)
        df = fut.result(timeout=float(timeout_sec))
    except FuturesTimeoutError:
        _log(f"[RP-1-OHLCV] WARN timeout {timeout_sec}s ticker={ticker} — SKIP")
        return None, "timeout"
    except Exception as exc:
        _log(f"[RP-1-OHLCV] WARN fetch_error ticker={ticker}: {exc} — SKIP")
        return None, "fetch_error"

    if df is None or getattr(df, "empty", True):
        return None, "skip_empty"

    df = _normalize_ohlcv_df(df)
    if cached is not None and not cached.empty:
        df = _normalize_ohlcv_df(pd.concat([cached, df]))
    try:
        _write_cache(path, df)
    except Exception as exc:
        _log(f"[RP-1-OHLCV] WARN cache write failed ticker={ticker}: {exc}")

    if not _covers_range(df, start, end):
        return df, "fetch_ok"
    return df, "fetch_ok"
