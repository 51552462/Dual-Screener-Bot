"""
Project 4: Alternative Data Miner — 완전 격리 실크로.
- market_data.sqlite / system_config.json 미사용
- 전용 DB: ~/dante_bots/Dual-Screener-Bot/alt_data.sqlite
"""
from __future__ import annotations

import io
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from factory_data_paths import alt_data_db_path, ensure_alt_data_db_initialized


def _alt_db_path() -> str:
    """factory_data_dir SSOT — legacy ~/dante_bots 경로와 분리."""
    return ensure_alt_data_db_initialized(alt_data_db_path())

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

_EXTRA_COLUMNS = [
    ("btc_close", "REAL"),
    ("t10y2y", "REAL"),
    ("dfii10", "REAL"),
    ("walcl", "REAL"),
]

_FLOAT_KEYS = (
    "usd_krw",
    "us_10y_yield",
    "vix_index",
    "btc_close",
    "t10y2y",
    "dfii10",
    "walcl",
    "cnn_fear_greed",
    "put_call_ratio",
)


def _sleep_jitter(a: float = 0.6, b: float = 1.8) -> None:
    time.sleep(random.uniform(a, b))


def _migrate_extra_columns(conn: sqlite3.Connection) -> None:
    for col, sqlt in _EXTRA_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE macro_daily ADD COLUMN {col} {sqlt}")
        except sqlite3.OperationalError:
            pass


def ensure_db() -> None:
    ensure_alt_data_db_initialized(_alt_db_path())


def _load_last_row() -> Optional[dict[str, Any]]:
    path = _alt_db_path()
    if not os.path.exists(path):
        return None
    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM macro_daily ORDER BY date DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        return {k: row[k] for k in row.keys()}
    except Exception:
        return None
    finally:
        conn.close()


def _coalesce_with_previous(
    today: dict[str, Optional[float]], prev: Optional[dict[str, Any]]
) -> dict[str, Optional[float]]:
    """네트워크 실패 시 NULL 대신 직전 영업일 값으로 보간(요청 사양)."""
    if not prev:
        return today
    out = dict(today)
    for k in _FLOAT_KEYS:
        if out.get(k) is None and prev.get(k) is not None:
            try:
                out[k] = float(prev[k])
            except (TypeError, ValueError):
                pass
    return out


def fetch_yfinance_macro() -> tuple[dict[str, Optional[float]], str]:
    """
    KRW=X, ^TNX, ^VIX, BTC-USD 일봉 종가. 반환: (필드 dict, 거래일 YYYY-MM-DD).
    """
    specs = [
        ("KRW=X", "usd_krw"),
        ("^TNX", "us_10y_yield"),
        ("^VIX", "vix_index"),
        ("BTC-USD", "btc_close"),
    ]
    out: dict[str, Optional[float]] = {v: None for _, v in specs}
    last_dates: list[str] = []

    for sym, key in specs:
        try:
            _sleep_jitter(0.35, 1.1)
            t = yf.Ticker(sym)
            hist = t.history(period="10d", interval="1d", auto_adjust=False)
            if hist is None or hist.empty:
                continue
            close = hist["Close"].dropna()
            if close.empty:
                continue
            val = float(close.iloc[-1])
            out[key] = val
            idx = close.index[-1]
            if hasattr(idx, "strftime"):
                last_dates.append(idx.strftime("%Y-%m-%d"))
            else:
                last_dates.append(str(idx)[:10])
        except Exception:
            continue

    if last_dates:
        as_of = max(last_dates)
    else:
        as_of = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return out, as_of


def _fred_latest_from_frame(df: pd.DataFrame, series_map: dict[str, str]) -> dict[str, Optional[float]]:
    """컬럼명 FRED ID → 스키마 키, 마지막 유효 행 기준."""
    out = {v: None for v in series_map.values()}
    if df is None or df.empty:
        return out
    try:
        d = df.sort_index().ffill()
        last = d.iloc[-1]
        for fid, key in series_map.items():
            if fid not in d.columns:
                continue
            v = last[fid]
            if pd.notna(v):
                out[key] = float(v)
    except Exception:
        pass
    return out


def fetch_fred_macro(as_of_date_str: str) -> dict[str, Optional[float]]:
    """
    FRED: T10Y2Y(장단기 금리차), DFII10(실질 10Y), WALCL(연준 대차).
    pandas_datareader 우선, 누락·장애 시 시리즈별 fredgraph.csv 폴백(공개 CSV).
    """
    merged: dict[str, Optional[float]] = {"t10y2y": None, "dfii10": None, "walcl": None}
    series_map = {"T10Y2Y": "t10y2y", "DFII10": "dfii10", "WALCL": "walcl"}
    try:
        end = pd.Timestamp(as_of_date_str[:10])
        start = end - pd.Timedelta(days=400)
    except Exception:
        end = pd.Timestamp.now(tz=timezone.utc).normalize().tz_localize(None)
        start = end - pd.Timedelta(days=400)

    try:
        from pandas_datareader import data as pdr

        _sleep_jitter(0.4, 1.2)
        df = pdr.get_data_fred(list(series_map.keys()), start=start, end=end)
        merged.update(_fred_latest_from_frame(df, series_map))
    except Exception:
        pass

    for fid, key in series_map.items():
        if merged.get(key) is not None:
            continue
        try:
            _sleep_jitter(0.5, 1.5)
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={fid}"
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=35)
            r.raise_for_status()
            raw = pd.read_csv(io.StringIO(r.text))
            if raw.empty or "DATE" not in raw.columns:
                continue
            col = "VALUE" if "VALUE" in raw.columns else raw.columns[-1]
            s = raw[["DATE", col]].copy()
            s["DATE"] = pd.to_datetime(s["DATE"], errors="coerce")
            s[col] = pd.to_numeric(s[col], errors="coerce")
            s = s.dropna(subset=["DATE"])
            s = s[s["DATE"] <= end]
            s = s.dropna(subset=[col])
            if s.empty:
                continue
            merged[key] = float(s.iloc[-1][col])
        except Exception:
            continue
    return merged


def fetch_cnn_fear_greed(headers: dict[str, str]) -> Optional[float]:
    """CNN Fear & Greed — 공개 JSON 엔드포인트(비공식). 실패 시 None."""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        _sleep_jitter(0.5, 1.4)
        r = requests.get(url, headers=headers, timeout=25)
        r.raise_for_status()
        data = r.json()
        fg = data.get("fear_and_greed")
        if isinstance(fg, dict):
            sc = fg.get("score")
            if sc is not None:
                return float(sc)
        hist = data.get("fear_and_greed_historical", {})
        pts = hist.get("data") if isinstance(hist, dict) else None
        if isinstance(pts, list) and pts:
            last = pts[-1]
            if isinstance(last, dict) and last.get("y") is not None:
                return float(last["y"])
    except Exception:
        return None
    return None


def fetch_put_call_ratio(headers: dict[str, str]) -> Optional[float]:
    """
    CBOE / NASDAQ 계열 Put·Call 비율 — HTML 스크래핑(구조 변경 시 None).
    여러 후보 URL을 순차 시도.
    """
    candidates = [
        "https://www.cboe.com/us/options/market_statistics/",
        "https://www.cboe.com/tradable_products/vix/vix_put_call_ratio/",
    ]
    number_re = re.compile(r"put[-_/ ]?call[-_/ ]?ratio[^\d]{0,40}(\d+\.?\d*)", re.I)

    for url in candidates:
        try:
            _sleep_jitter(0.7, 2.0)
            r = requests.get(url, headers=headers, timeout=25)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            text = soup.get_text("\n", strip=True)
            m = number_re.search(text)
            if m:
                v = float(m.group(1))
                if 0.3 < v < 3.0:
                    return v
            for td in soup.find_all(["td", "span", "div"]):
                t = td.get_text(strip=True)
                if re.match(r"^\d+\.\d{2,4}$", t):
                    v = float(t)
                    if 0.3 < v < 3.0:
                        return v
        except Exception:
            continue
    return None


def collect_one_day() -> dict[str, Any]:
    ensure_db()
    prev = _load_last_row()
    headers = dict(DEFAULT_HEADERS)

    yf_vals, yf_date = fetch_yfinance_macro()
    row: dict[str, Optional[float]] = {
        "usd_krw": yf_vals.get("usd_krw"),
        "us_10y_yield": yf_vals.get("us_10y_yield"),
        "vix_index": yf_vals.get("vix_index"),
        "btc_close": yf_vals.get("btc_close"),
        "t10y2y": None,
        "dfii10": None,
        "walcl": None,
        "cnn_fear_greed": None,
        "put_call_ratio": None,
    }

    try:
        fred_part = fetch_fred_macro(yf_date)
        row["t10y2y"] = fred_part.get("t10y2y")
        row["dfii10"] = fred_part.get("dfii10")
        row["walcl"] = fred_part.get("walcl")
    except Exception:
        pass

    try:
        row["cnn_fear_greed"] = fetch_cnn_fear_greed(headers)
    except Exception:
        row["cnn_fear_greed"] = None

    try:
        row["put_call_ratio"] = fetch_put_call_ratio(headers)
    except Exception:
        row["put_call_ratio"] = None

    row = _coalesce_with_previous(row, prev)
    pk_date = yf_date
    return {"date": pk_date, **row}


def upsert_macro_daily(row: dict[str, Any]) -> None:
    path = _alt_db_path()
    conn = sqlite3.connect(path, timeout=30)
    try:
        _migrate_extra_columns(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO macro_daily
            (date, usd_krw, us_10y_yield, vix_index, btc_close, t10y2y, dfii10, walcl,
             cnn_fear_greed, put_call_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["date"],
                row.get("usd_krw"),
                row.get("us_10y_yield"),
                row.get("vix_index"),
                row.get("btc_close"),
                row.get("t10y2y"),
                row.get("dfii10"),
                row.get("walcl"),
                row.get("cnn_fear_greed"),
                row.get("put_call_ratio"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def run_once() -> dict[str, Any]:
    """단일 수집 사이클 — 예외로 프로세스가 죽지 않도록 상위에서도 방어."""
    try:
        row = collect_one_day()
        upsert_macro_daily(row)
        print(
            f"✅ [Alt Data Miner] 저장 완료: date={row['date']} | "
            f"usd_krw={row.get('usd_krw')} | us_10y={row.get('us_10y_yield')} | vix={row.get('vix_index')} | "
            f"btc={row.get('btc_close')} | T10Y2Y={row.get('t10y2y')} | DFII10={row.get('dfii10')} | "
            f"WALCL={row.get('walcl')} | cnn_fg={row.get('cnn_fear_greed')} | pc={row.get('put_call_ratio')}"
        )
        return row
    except Exception as e:
        print(f"🚨 [Alt Data Miner] 수집 중 예외(부분 저장 생략): {e}")
        return {}


if __name__ == "__main__":
    run_once()
