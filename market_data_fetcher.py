"""
시세 수집 단일 파이프라인.

- Chain of Responsibility: 시장별 순차 시도 후 다음 핸들러로 위임.
- 지수 백오프: 429 / Timeout / 일시 오류 시 소량 재시도.

체인
- KR: FinanceDataReader → yfinance(.KS / .KQ) → 로컬 SQLite (읽기 경로는 market_db_read_path)
- US: yfinance → FinanceDataReader → 로컬 SQLite

체인
- KR: FinanceDataReader → yfinance(.KS / .KQ) → 로컬 SQLite (읽기 경로는 market_db_read_path)
- US: yfinance → FinanceDataReader → 로컬 SQLite

배치(`fetch_market_data_batch`): 심볼마다 위 단일 체인을 유지하며 ThreadPoolExecutor 로 병렬 수집.

스키마: 이 파일은 테이블을 생성하지 않는다. `market_data.sqlite` 테이블 정의·컬럼 추가는
소유 모듈에서 `CREATE TABLE IF NOT EXISTS` 후 `sqlite_schema_guard.apply_column_migrations` 패턴으로
기존 데이터를 유지한다.
"""
from __future__ import annotations

import logging
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

import low_ram_sqlite_pragmas
from market_db_paths import market_db_read_path

logger = logging.getLogger(__name__)

_MAX_TRIES = 3

# 저 RAM / 공인 IP 환경: 업스트림(FDR·yfinance) 동시 연결 폭주로 429·빈 응답·차단 방지
_MAX_CONCURRENT_UPSTREAM = 2
_upstream_http_sem = threading.BoundedSemaphore(_MAX_CONCURRENT_UPSTREAM)

# 배치 스캐너용: 청크 단위로 끊어 호출 후 휴지 (IP 레이트리밋·OOM 완화)
_BATCH_CHUNK_SIZE = 50
_BATCH_CHUNK_SLEEP_SEC = 0.55
_BATCH_MAX_WORKERS_CAP = 4


def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(12.0, 0.45 * (2**attempt)) + random.uniform(0, 0.2))


def _is_transient_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    needles = (
        "429",
        "too many requests",
        "timeout",
        "timed out",
        "temporarily",
        "connection reset",
        "connection aborted",
        "remote end closed",
        "503",
        "502",
    )
    return any(n in msg for n in needles)


def _retrying_call(
    fn: Callable[[], pd.DataFrame], *, label: str
) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    업스트림 호출. 성공 시 (비어 있지 않은 DF, None).
    재시도 후에도 비어 있거나 예외면 (None, 사유 문자열).
    """
    last: Optional[BaseException] = None
    for attempt in range(_MAX_TRIES):
        try:
            df = fn()
            if df is not None and not df.empty:
                return df, None
            if attempt < _MAX_TRIES - 1:
                _sleep_backoff(attempt)
                continue
            return None, f"{label}:empty_or_invalid_after_{_MAX_TRIES}_tries"
        except Exception as e:
            last = e
            if _is_transient_error(e) or attempt < _MAX_TRIES - 1:
                _sleep_backoff(attempt)
                continue
            logger.warning("market_data_fetcher: %s failed after retries: %s", label, e)
            return None, f"{label}:{type(e).__name__}:{e}"
    return None, f"{label}:exhausted:{last!r}"


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "Date" in out.columns and not isinstance(out.index, pd.DatetimeIndex):
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
        out = out.dropna(subset=["Date"]).set_index("Date")
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.duplicated(keep="last")].sort_index()
    cols_req = ("Open", "High", "Low", "Close", "Volume")
    for c in cols_req:
        if c not in out.columns:
            return pd.DataFrame()
    out = out[list(cols_req)].copy()
    for c in cols_req:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out.dropna(how="any")


def _slice_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if df.empty:
        return df
    try:
        s = pd.Timestamp(start_date)
        e = pd.Timestamp(end_date) + pd.Timedelta(days=1)
        return df.loc[(df.index >= s) & (df.index < e)]
    except Exception:
        return df


def _finalize_us_index(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    try:
        if df.index.tzinfo is not None:
            df = df.copy()
            df.index = df.index.tz_convert("America/New_York").tz_localize(None)
    except Exception:
        try:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        except Exception:
            pass
    return df


def _fetch_fdr_kr(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    import FinanceDataReader as fdr

    c = str(code).strip().zfill(6)
    with _upstream_http_sem:
        raw = fdr.DataReader(c, start_date, end_date)
        time.sleep(random.uniform(0.04, 0.12))
    return _slice_date_range(_normalize_ohlcv(raw), start_date, end_date)


def _fetch_fdr_us(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    import FinanceDataReader as fdr

    sym = str(code).strip().replace("-", ".")
    with _upstream_http_sem:
        raw = fdr.DataReader(sym, start_date, end_date)
        time.sleep(random.uniform(0.04, 0.12))
    return _finalize_us_index(_slice_date_range(_normalize_ohlcv(raw), start_date, end_date))


def _fetch_yf(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    import yfinance as yf

    from yf_download_flatten import flatten_yf_download_df

    end_excl = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    with _upstream_http_sem:
        raw = yf.download(
            symbol,
            start=start_date,
            end=end_excl,
            interval="1d",
            progress=False,
            threads=False,
        )
        time.sleep(random.uniform(0.05, 0.14))
    if raw is None or raw.empty:
        return pd.DataFrame()
    flat = flatten_yf_download_df(raw)
    return _normalize_ohlcv(flat)


def _fetch_yf_us(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    sym = str(code).strip()
    df = _fetch_yf(sym, start_date, end_date)
    return _finalize_us_index(_slice_date_range(df, start_date, end_date))


def _kr_yf_candidates(code: str) -> list[str]:
    c = str(code).strip().zfill(6)
    return [f"{c}.KS", f"{c}.KQ"]


def _fetch_yf_kr(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    for sym in _kr_yf_candidates(code):
        df = _fetch_yf(sym, start_date, end_date)
        df = _slice_date_range(df, start_date, end_date)
        if not df.empty:
            return df
    return pd.DataFrame()


def _sqlite_table_name(code: str, market: str) -> str:
    m = market.upper()
    if m == "KR":
        return f"KR_{str(code).strip().zfill(6)}"
    return f"US_{str(code).strip()}"


def _fetch_sqlite(code: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
    table = _sqlite_table_name(code, market)
    if not (table.startswith("KR_") or table.startswith("US_")) or len(table) > 120:
        return pd.DataFrame()
    path = market_db_read_path()
    try:
        conn = sqlite3.connect(path, timeout=30.0)
        try:
            low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
            raw = pd.read_sql(f'SELECT * FROM "{table}"', conn)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("market_data_fetcher: sqlite read failed table=%s: %s", table, e)
        return pd.DataFrame()
    df = _normalize_ohlcv(raw)
    df = _slice_date_range(df, start_date, end_date)
    if market.upper() == "US":
        df = _finalize_us_index(df)
    return df


@dataclass
class _FetchHandler:
    name: str
    fn: Callable[[str, str, str, str], pd.DataFrame]
    next_handler: Optional["_FetchHandler"] = None

    def handle_with_path(
        self, code: str, market: str, start_date: str, end_date: str
    ) -> tuple[Optional[pd.DataFrame], list[str], Optional[str]]:
        """성공 시 (df, 시도한 핸들러 이름 순서, None). 전부 실패 시 (None, 경로, 사유)."""
        path: list[str] = [self.name]
        df, err = _retrying_call(
            lambda: self.fn(code, market, start_date, end_date),
            label=f"{market}:{self.name}",
        )
        if df is not None and not df.empty:
            return df, path, None
        if self.next_handler is not None:
            df2, tail, err2 = self.next_handler.handle_with_path(code, market, start_date, end_date)
            full_path = path + tail
            if df2 is not None and not df2.empty:
                return df2, full_path, None
            msg = err2 or err or f"chain_exhausted:{','.join(full_path)}"
            logger.warning(
                "market_data_fetcher: no OHLCV code=%s market=%s path=%s detail=%s",
                code,
                market,
                "->".join(full_path),
                msg,
            )
            return None, full_path, msg
        msg = err or f"handler_failed:{self.name}"
        logger.warning(
            "market_data_fetcher: no OHLCV code=%s market=%s last_handler=%s detail=%s",
            code,
            market,
            self.name,
            msg,
        )
        return None, path, msg

    def handle(self, code: str, market: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        df, _, err = self.handle_with_path(code, market, start_date, end_date)
        if err:
            return None
        return df


def fetch_market_data_with_trace(
    code: str, market: str, start_date: str, end_date: str
) -> tuple[Optional[pd.DataFrame], list[str], Optional[str]]:
    """OHLCV + 체인 시도 경로 + 실패 시 사유(성공이면 err=None)."""
    m = (market or "").strip().upper()
    if m not in ("KR", "US"):
        logger.warning("market_data_fetcher: invalid market %r for code=%s", market, code)
        return None, [], f"invalid_market:{market!r}"

    if m == "KR":
        chain = _FetchHandler("FDR", lambda c, _mk, s, e: _fetch_fdr_kr(c, s, e), None)
        chain.next_handler = _FetchHandler("YFinance_KR", lambda c, _mk, s, e: _fetch_yf_kr(c, s, e), None)
        chain.next_handler.next_handler = _FetchHandler(
            "SQLite", lambda c, mk, s, e: _fetch_sqlite(c, mk, s, e), None
        )
    else:
        chain = _FetchHandler("YFinance_US", lambda c, _mk, s, e: _fetch_yf_us(c, s, e), None)
        chain.next_handler = _FetchHandler("FDR_US", lambda c, _mk, s, e: _fetch_fdr_us(c, s, e), None)
        chain.next_handler.next_handler = _FetchHandler(
            "SQLite", lambda c, mk, s, e: _fetch_sqlite(c, mk, s, e), None
        )

    return chain.handle_with_path(str(code).strip(), m, start_date, end_date)


def fetch_market_data(
    code: str, market: str, start_date: str, end_date: str
) -> Optional[pd.DataFrame]:
    """
    OHLCV DataFrame (DatetimeIndex, 컬럼 Open/High/Low/Close/Volume).
    전부 실패 시 None (빈 DataFrame으로 통신 실패를 위장하지 않음).
    """
    m = (market or "").strip().upper()
    if m not in ("KR", "US"):
        logger.warning("market_data_fetcher: fetch_market_data invalid market %r code=%s", market, code)
        return None

    df, path, err = fetch_market_data_with_trace(str(code).strip(), m, start_date, end_date)
    if err:
        logger.warning(
            "market_data_fetcher: fetch failed code=%s market=%s path=%s err=%s",
            code,
            m,
            "->".join(path) if path else "(none)",
            err,
        )
    return df


def fetch_market_data_batch(
    codes: list[str],
    market: str,
    start_date: str,
    end_date: str,
    *,
    max_workers: int = 24,
) -> dict[str, pd.DataFrame]:
    """
    심볼 리스트에 대해 fetch_market_data 를 병렬 호출.
    각 심볼은 FDR→YF→SQLite 체인을 그대로 탄다.
    반환: {원본 코드 문자열: OHLCV DataFrame} (빈 DF 는 생략)

    대량 심볼 시 `_BATCH_CHUNK_SIZE` 단위로 끊어 처리하고 청크 사이에 짧은 휴지를 둔다.
    """
    raw = [str(c).strip() for c in codes if str(c).strip()]
    if not raw:
        return {}
    m = (market or "").strip().upper()
    if m not in ("KR", "US"):
        return {}
    w = max(1, min(int(max_workers), len(raw), _BATCH_MAX_WORKERS_CAP))

    def _one(sym: str) -> tuple[str, Optional[pd.DataFrame], float, int, Optional[str]]:
        t0 = time.perf_counter()
        df, path, err = fetch_market_data_with_trace(sym, m, start_date, end_date)
        wall = time.perf_counter() - t0
        fb = max(0, len(path) - 1)
        return sym, df, wall, fb, err

    out: dict[str, pd.DataFrame] = {}
    t_batch = time.perf_counter()
    walls: list[float] = []
    fallbacks: list[int] = []
    for i in range(0, len(raw), _BATCH_CHUNK_SIZE):
        chunk = raw[i : i + _BATCH_CHUNK_SIZE]
        with ThreadPoolExecutor(max_workers=min(w, len(chunk))) as ex:
            futs = {ex.submit(_one, c): c for c in chunk}
            for fut in as_completed(futs):
                try:
                    sym, df, wall, fb, err = fut.result()
                    walls.append(float(wall))
                    fallbacks.append(int(fb))
                    if err:
                        logger.warning(
                            "market_data_fetcher: batch symbol failed sym=%s market=%s err=%s",
                            sym,
                            m,
                            err,
                        )
                    if df is not None and not df.empty:
                        out[sym] = df
                except Exception as e:
                    logger.exception("market_data_fetcher: batch worker future failed: %s", e)
        if i + _BATCH_CHUNK_SIZE < len(raw):
            time.sleep(_BATCH_CHUNK_SLEEP_SEC + random.uniform(0, 0.15))
    batch_wall = time.perf_counter() - t_batch
    try:
        import ops_logger

        sw = sorted(walls) if walls else []
        p95 = sw[min(len(sw) - 1, max(0, int(0.95 * (len(sw) - 1))))] if sw else 0.0

        ops_logger.record_gauge_snapshot(
            "market_data_fetcher",
            {
                "batch_market": m,
                "batch_symbols_requested": len(raw),
                "batch_symbols_hit": len(out),
                "batch_wall_sec": round(batch_wall, 4),
                "per_symbol_wall_max_sec": round(max(walls), 4) if walls else 0.0,
                "per_symbol_wall_p95_sec": round(float(p95), 4),
                "fallback_steps_sum": int(sum(fallbacks)),
                "fallback_steps_max": int(max(fallbacks)) if fallbacks else 0,
            },
        )
    except Exception as e:
        logger.warning("market_data_fetcher: ops_logger gauge skipped: %s", e)
    return out


def us_benchmark_close_series(
    start_date: str,
    end_date: str,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """SPY / QQQ / ^VIX 종가 시계열 (미국장 스캐너 벤치마크용)."""
    m = fetch_market_data_batch(["SPY", "QQQ", "^VIX"], "US", start_date, end_date, max_workers=3)

    def _close(sym: str) -> pd.Series:
        df = m.get(sym)
        if df is None or df.empty:
            return pd.Series(dtype=float)
        s = df["Close"].copy()
        if s.index.tzinfo is not None:
            try:
                s.index = s.index.tz_convert("America/New_York").tz_localize(None)
            except Exception:
                try:
                    s.index = s.index.tz_localize(None)
                except Exception:
                    pass
        return s[~s.index.duplicated(keep="last")]

    return _close("SPY"), _close("QQQ"), _close("^VIX")
