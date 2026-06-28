"""
Mission 9 — 콜드 스타트 방어 & 과거 데이터 자율 수집 (Cold-Start Hydration).

[문제] 라이브 DB(market_data.sqlite)에는 1~2개월 치만 있어 1~2년 단기/10년 장기 탐색 시
       Empty DataFrame·표본부족으로 시스템이 뻗을 수 있다.

[해결]
  1) 동적 룩백(Elastic Lookback): DB MIN(date) 를 먼저 확인해 요청 기간보다 데이터가 적으면
     에러 대신 '보유 최대 기간'으로 자동 축소(regime_analog_engine.clamp_lookback_window).
  2) 주말 자율 수집(Weekend Hydration): 2008/2020 등 과거 에피소드 데이터가 없으면 포기하지 말고
     yfinance(US/매크로)·pykrx(KR)·ccxt(코인) 로 백그라운드(Priority 3) 다운로드.
  3) DB 분리 보관(Cold Storage): 방대한 과거 데이터는 라이브 DB 가 아니라 오프라인 학습 전용
     `deep_archive_history.sqlite` 에 격리 저장(라이브 속도 무영향).
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

ARCHIVE_TABLE = "archive_ohlcv"


# ---------------------------------------------------------------------------
# 콜드 스토리지 경로 / 스키마
# ---------------------------------------------------------------------------
def cold_storage_db_path() -> str:
    env = (os.environ.get("DEEP_ARCHIVE_DB_PATH") or "").strip()
    if env:
        return os.path.abspath(os.path.expanduser(env))
    try:
        from factory_data_paths import factory_data_dir

        return os.path.join(factory_data_dir(), "deep_archive_history.sqlite")
    except Exception:
        return os.path.join(os.getcwd(), "deep_archive_history.sqlite")


def init_cold_storage(db_path: Optional[str] = None) -> str:
    path = db_path or cold_storage_db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ARCHIVE_TABLE} (
                symbol  TEXT NOT NULL,
                market  TEXT NOT NULL,
                date    TEXT NOT NULL,
                open    REAL, high REAL, low REAL, close REAL, volume REAL,
                source  TEXT,
                PRIMARY KEY (symbol, date)
            );
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_arch_sym_date ON {ARCHIVE_TABLE}(symbol, date);"
        )
        conn.commit()
    finally:
        conn.close()
    return path


# ---------------------------------------------------------------------------
# 과거 에피소드 달력 (대표 역사 구간 + 심볼)
#   source: yf(yfinance) | pykrx | ccxt
# ---------------------------------------------------------------------------
EPISODE_CALENDAR: Dict[str, Dict[str, Any]] = {
    "EXTREME_CRASH": {
        "start": "2008-09-01", "end": "2009-03-31",
        "symbols": [("^GSPC", "US", "yf"), ("^VIX", "MACRO", "yf"),
                    ("^KS11", "KR", "yf"), ("BTC/USDT", "COIN", "ccxt")],
    },
    "V_RECOVERY": {
        "start": "2020-03-01", "end": "2020-09-30",
        "symbols": [("^GSPC", "US", "yf"), ("^VIX", "MACRO", "yf"),
                    ("^KS11", "KR", "yf"), ("BTC/USDT", "COIN", "ccxt")],
    },
    "MASSIVE_BULL": {
        "start": "2020-11-01", "end": "2021-11-30",
        "symbols": [("^GSPC", "US", "yf"), ("^VIX", "MACRO", "yf"),
                    ("^KS11", "KR", "yf"), ("BTC/USDT", "COIN", "ccxt")],
    },
    "CHOPPY_STAGNANT": {
        "start": "2015-06-01", "end": "2016-02-29",
        "symbols": [("^GSPC", "US", "yf"), ("^VIX", "MACRO", "yf"),
                    ("^KS11", "KR", "yf"), ("BTC/USDT", "COIN", "ccxt")],
    },
}

# 서버 SSD/RAM 보호 — 전 종목을 긁지 않고 '당시 장세를 주도한 대장주'만 타겟 수집(상한).
MAX_LEADERS = 50

# 에피소드별 당시 장세를 주도했던 대장주(curated). US=yfinance, KR=pykrx(6자리).
EPISODE_LEADERS: Dict[str, Dict[str, List[str]]] = {
    "EXTREME_CRASH": {  # 2008 금융위기 — 메가캡/금융 대장
        "US": ["AAPL", "MSFT", "XOM", "GE", "JPM", "BAC", "WFC", "IBM", "GOOG", "PG",
               "JNJ", "CVX", "KO", "WMT", "T"],
        "KR": ["005930", "000660", "005380", "005490", "035420", "012330", "051910",
               "055550", "105560", "066570"],
    },
    "V_RECOVERY": {  # 2020 코로나 V자 반등 — 빅테크/언택트 대장
        "US": ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "NFLX", "AMD",
               "PYPL", "CRM", "ADBE", "QCOM", "AVGO", "SHOP"],
        "KR": ["005930", "000660", "035420", "051910", "207940", "006400", "035720",
               "068270", "036570", "251270"],
    },
    "MASSIVE_BULL": {  # 2021 유동성 강세장 — 성장주 대장
        "US": ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "AMD", "AVGO",
               "CRM", "ADBE", "COST", "LLY", "V", "MA"],
        "KR": ["005930", "000660", "051910", "006400", "035420", "207940", "005380",
               "000270", "012450", "042700"],
    },
    "CHOPPY_STAGNANT": {  # 2015 박스권 — 당시 주도주
        "US": ["AAPL", "AMZN", "GOOGL", "META", "NFLX", "MSFT", "DIS", "NKE", "SBUX",
               "GILD"],
        "KR": ["005930", "000660", "035420", "090430", "128940", "068270", "051910",
               "009150", "011170", "010130"],
    },
}


def _episode_leader_symbols(episode: str) -> List[Tuple[str, str, str]]:
    """에피소드 대장주 (symbol, market, source) 목록을 상한(MAX_LEADERS) 내로 반환."""
    leaders = EPISODE_LEADERS.get(episode) or {}
    out: List[Tuple[str, str, str]] = []
    for code in (leaders.get("US") or []):
        out.append((str(code), "US", "yf"))
    for code in (leaders.get("KR") or []):
        out.append((str(code).zfill(6), "KR", "pykrx"))
    return out[:MAX_LEADERS]


# ---------------------------------------------------------------------------
# 날짜 경계 / 동적 룩백 (regime_analog_engine 재사용)
# ---------------------------------------------------------------------------
def db_date_bounds(
    db_path: Optional[str] = None, *, symbol: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    path = db_path or cold_storage_db_path()
    if not os.path.isfile(path):
        return None, None
    try:
        uri = str(path).replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=15)
    except sqlite3.Error:
        return None, None
    try:
        if symbol:
            row = conn.execute(
                f"SELECT MIN(date), MAX(date) FROM {ARCHIVE_TABLE} WHERE symbol=?",
                (symbol,),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT MIN(date), MAX(date) FROM {ARCHIVE_TABLE}"
            ).fetchone()
    except sqlite3.Error:
        return None, None
    finally:
        conn.close()
    if not row or row[0] is None:
        return None, None
    return str(row[0])[:10], str(row[1])[:10]


def elastic_lookback_window(
    requested_days: int,
    *,
    db_path: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """콜드 스토리지 기준 동적 룩백 — regime_analog_engine.clamp_lookback_window 위임."""
    from regime_analog_engine import clamp_lookback_window

    return clamp_lookback_window(
        db_path or cold_storage_db_path(),
        ARCHIVE_TABLE,
        date_col="date",
        requested_days=requested_days,
        now=now,
    )


# ---------------------------------------------------------------------------
# 다운로드 어댑터 (yfinance / pykrx / ccxt) — 모두 가드 import, 실패 시 빈 결과
# ---------------------------------------------------------------------------
def _download_yf(symbol: str, start: str, end: str):
    try:
        from market_data_fetcher import _fetch_yf  # 정규화된 OHLCV 재사용

        df = _fetch_yf(symbol, start, end)
    except Exception:
        df = None
    return df


def _download_pykrx(symbol: str, start: str, end: str):
    try:
        from pykrx import stock
        import pandas as pd

        s = start.replace("-", "")
        e = end.replace("-", "")
        raw = stock.get_market_ohlcv(s, e, str(symbol).zfill(6))
        if raw is None or raw.empty:
            return None
        raw = raw.rename(
            columns={"시가": "Open", "고가": "High", "저가": "Low",
                     "종가": "Close", "거래량": "Volume"}
        )
        raw.index = pd.to_datetime(raw.index, errors="coerce")
        cols = ["Open", "High", "Low", "Close", "Volume"]
        return raw[[c for c in cols if c in raw.columns]].dropna(how="any")
    except Exception:
        return None


def _download_ccxt(symbol: str, start: str, end: str):
    try:
        import ccxt
        import pandas as pd

        ex = ccxt.binance({"enableRateLimit": True})
        since = ex.parse8601(f"{start}T00:00:00Z")
        end_ms = ex.parse8601(f"{end}T00:00:00Z")
        rows: List[list] = []
        cursor = since
        while cursor is not None and cursor < end_ms:
            batch = ex.fetch_ohlcv(symbol, timeframe="1d", since=cursor, limit=1000)
            if not batch:
                break
            rows.extend(batch)
            cursor = batch[-1][0] + 86_400_000
            if len(batch) < 1000:
                break
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
        df["Date"] = pd.to_datetime(df["ts"], unit="ms")
        df = df.set_index("Date")[["Open", "High", "Low", "Close", "Volume"]]
        return df[df.index < pd.Timestamp(end)]
    except Exception:
        return None


_DOWNLOADERS: Dict[str, Callable[[str, str, str], Any]] = {
    "yf": _download_yf,
    "pykrx": _download_pykrx,
    "ccxt": _download_ccxt,
}


def _store_ohlcv(
    conn: sqlite3.Connection, symbol: str, market: str, source: str, df
) -> int:
    if df is None or getattr(df, "empty", True):
        return 0
    n = 0
    for idx, r in df.iterrows():
        try:
            date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
            conn.execute(
                f"INSERT OR REPLACE INTO {ARCHIVE_TABLE} "
                "(symbol, market, date, open, high, low, close, volume, source) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    symbol, str(market).upper(), date_str,
                    float(r.get("Open", 0.0)), float(r.get("High", 0.0)),
                    float(r.get("Low", 0.0)), float(r.get("Close", 0.0)),
                    float(r.get("Volume", 0.0)), source,
                ),
            )
            n += 1
        except (ValueError, TypeError, AttributeError, sqlite3.Error):
            continue
    conn.commit()
    return n


# ---------------------------------------------------------------------------
# 심볼/에피소드 하이드레이션
# ---------------------------------------------------------------------------
def hydrate_symbol(
    symbol: str,
    market: str,
    start: str,
    end: str,
    *,
    source: str = "yf",
    db_path: Optional[str] = None,
    skip_if_present: bool = True,
) -> Dict[str, Any]:
    """단일 심볼 과거 OHLCV 를 콜드 스토리지에 다운로드/적재. 네트워크/모듈 부재 시 graceful."""
    path = init_cold_storage(db_path)
    if skip_if_present:
        lo, hi = db_date_bounds(path, symbol=symbol)
        if lo is not None and lo <= start and hi is not None and hi >= end[:10]:
            return {"symbol": symbol, "rows": 0, "skipped": "already_present"}

    dl = _DOWNLOADERS.get(source)
    if dl is None:
        return {"symbol": symbol, "rows": 0, "skipped": f"no_downloader:{source}"}
    df = dl(symbol, start, end)
    if df is None or getattr(df, "empty", True):
        return {"symbol": symbol, "rows": 0, "skipped": "download_empty_or_offline"}

    conn = sqlite3.connect(path, timeout=30.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        rows = _store_ohlcv(conn, symbol, market, source, df)
    finally:
        conn.close()
    return {"symbol": symbol, "rows": rows, "source": source}


def hydrate_episode(
    episode: str,
    *,
    db_path: Optional[str] = None,
    throttle_fn: Optional[Callable[[], Any]] = None,
    include_leaders: bool = True,
) -> Dict[str, Any]:
    """
    에피소드 구간(2008/2020 등)의 핵심 거시 지표·지수·코인 + '당시 장세 주도 대장주(상위 50)'
    데이터만 타겟 수집한다(전 종목 스캔 금지 — 서버 SSD/RAM 보호).
    throttle_fn 가 주어지면 심볼마다 호출해 코인 스캔(Priority 1)에 CPU 를 양보(Active Throttle).
    """
    spec = EPISODE_CALENDAR.get(episode)
    if not isinstance(spec, dict):
        return {"episode": episode, "skipped": "unknown_episode"}
    path = init_cold_storage(db_path)
    start, end = spec["start"], spec["end"]

    targets: List[Tuple[str, str, str]] = list(spec.get("symbols", []))
    if include_leaders:
        targets += _episode_leader_symbols(episode)  # 매크로/지수 + 대장주(≤50)

    results: List[Dict[str, Any]] = []
    total_rows = 0
    for symbol, market, source in targets:
        if throttle_fn is not None:
            try:
                throttle_fn()  # 코인 스캔 양보(Zero-Collision)
            except Exception:
                pass
        res = hydrate_symbol(symbol, market, start, end, source=source, db_path=path)
        results.append(res)
        total_rows += int(res.get("rows", 0))
    return {
        "episode": episode, "window": [start, end],
        "targets": len(targets), "total_rows": total_rows, "symbols": results,
        "hydrated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# 콜드 스토리지 → 정규화 국면 인덱스 시퀀스(DTW 입력)
# ---------------------------------------------------------------------------
def load_index_series(
    symbol: str,
    *,
    db_path: Optional[str] = None,
    requested_days: int = 3650,
) -> List[float]:
    """
    콜드 스토리지에서 심볼 종가를 z-정규화한 시퀀스로 반환(동적 룩백 적용). 데이터 없으면 [].
    """
    path = db_path or cold_storage_db_path()
    if not os.path.isfile(path):
        return []
    win = elastic_lookback_window(requested_days, db_path=path)
    if not win.get("has_data"):
        return []
    try:
        uri = str(path).replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=15)
    except sqlite3.Error:
        return []
    try:
        rows = conn.execute(
            f"SELECT close FROM {ARCHIVE_TABLE} WHERE symbol=? AND date>=? "
            "ORDER BY date ASC",
            (symbol, win["start"]),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    closes = [float(r[0]) for r in rows if r and r[0] is not None]
    if len(closes) < 2:
        return closes
    import numpy as np

    arr = np.asarray(closes, dtype=float)
    mu, sd = float(arr.mean()), float(arr.std())
    if sd <= 1e-9:
        return [0.0] * len(closes)
    return [round(float(v), 6) for v in ((arr - mu) / sd)]


def cold_storage_stats(db_path: Optional[str] = None) -> Dict[str, Any]:
    path = db_path or cold_storage_db_path()
    if not os.path.isfile(path):
        return {"exists": False}
    try:
        uri = str(path).replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=15)
    except sqlite3.Error:
        return {"exists": True, "error": "open_failed"}
    try:
        n = conn.execute(f"SELECT COUNT(*) FROM {ARCHIVE_TABLE}").fetchone()[0]
        syms = conn.execute(
            f"SELECT COUNT(DISTINCT symbol) FROM {ARCHIVE_TABLE}"
        ).fetchone()[0]
    except sqlite3.Error:
        return {"exists": True, "error": "no_table"}
    finally:
        conn.close()
    lo, hi = db_date_bounds(path)
    return {"exists": True, "rows": int(n), "symbols": int(syms), "min_date": lo, "max_date": hi}


if __name__ == "__main__":
    print("cold storage:", cold_storage_db_path())
    print("stats:", cold_storage_stats())
