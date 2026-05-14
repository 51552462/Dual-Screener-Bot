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


def _retrying_call(fn: Callable[[], pd.DataFrame], *, label: str) -> pd.DataFrame:
    last: Optional[BaseException] = None
    for attempt in range(_MAX_TRIES):
        try:
            df = fn()
            if df is not None and not df.empty:
                return df
            # 빈 DataFrame: 일부 차단/일시 장애가 예외 없이 빈 패널만 돌려주는 경우 → 백오프 재시도
            if attempt < _MAX_TRIES - 1:
                _sleep_backoff(attempt)
                continue
        except Exception as e:
            last = e
            if _is_transient_error(e) or attempt < _MAX_TRIES - 1:
                _sleep_backoff(attempt)
                continue
            break
    return pd.DataFrame()


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
    except Exception:
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
    ) -> tuple[pd.DataFrame, list[str]]:
        """성공 시 (df, 시도한 핸들러 이름 순서). 실패 시 빈 DF + 전체 시도 경로."""
        path: list[str] = [self.name]
        df = _retrying_call(
            lambda: self.fn(code, market, start_date, end_date),
            label=f"{market}:{self.name}",
        )
        if df is not None and not df.empty:
            return df, path
        if self.next_handler is not None:
            df2, tail = self.next_handler.handle_with_path(code, market, start_date, end_date)
            return df2, path + tail
        return pd.DataFrame(), path

    def handle(self, code: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
        df, _ = self.handle_with_path(code, market, start_date, end_date)
        return df


def fetch_market_data_with_trace(
    code: str, market: str, start_date: str, end_date: str
) -> tuple[pd.DataFrame, list[str]]:
    """fetch_market_data + 체인 시도 경로(폴백 판별용)."""
    m = (market or "").strip().upper()
    if m not in ("KR", "US"):
        return pd.DataFrame(), []

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


def fetch_market_data(code: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    OHLCV DataFrame (DatetimeIndex, 컬럼 Open/High/Low/Close/Volume).
    전부 실패 시 빈 DataFrame.
    """
    m = (market or "").strip().upper()
    if m not in ("KR", "US"):
        return pd.DataFrame()

    df, _ = fetch_market_data_with_trace(str(code).strip(), m, start_date, end_date)
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

    def _one(sym: str) -> tuple[str, pd.DataFrame, float, int]:
        t0 = time.perf_counter()
        df, path = fetch_market_data_with_trace(sym, m, start_date, end_date)
        wall = time.perf_counter() - t0
        # 폴백 횟수: 1차 소스 외 추가 핸들러 시도 횟수 (성공/전부 실패 공통으로 path 길이 기반)
        fb = max(0, len(path) - 1)
        return sym, df, wall, fb

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
                    sym, df, wall, fb = fut.result()
                    walls.append(float(wall))
                    fallbacks.append(int(fb))
                    if df is not None and not df.empty:
                        out[sym] = df
                except Exception:
                    pass
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
    except Exception:
        pass
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
