"""
US listing pipeline: FDR → CSV cache → sqlite US_* table names (KR krx_list_survival 대칭).
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
import warnings
from typing import List, Optional, Tuple

import pandas as pd

from market_db_paths import MARKET_DATA_DB_PATH

logger = logging.getLogger(__name__)

US_LIST_CACHE_BASENAME = "us_list_cache.csv"
_CODE_RE = re.compile(r"^US_([A-Z][A-Z0-9.\-]{0,14})$")
_BENCH = frozenset({"US_SPY", "US_QQQ", "US_VIX"})
_MIN_LIVE_ROWS = 400


def default_us_list_cache_path(db_path: str | None = None) -> str:
    base = db_path or MARKET_DATA_DB_PATH
    return os.path.join(os.path.dirname(base), US_LIST_CACHE_BASENAME)


def _safe_write_cache(df: pd.DataFrame, path: str) -> None:
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        df.to_csv(path, index=False)
    except Exception:
        pass


def _read_cache_csv(path: str) -> pd.DataFrame | None:
    if not path or not os.path.isfile(path):
        return None
    try:
        snap = pd.read_csv(path)
    except Exception:
        return None
    if snap is None or snap.empty:
        return None
    return snap


def _normalize_us_columns(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    if "Symbol" in d.columns and "Code" not in d.columns:
        d["Code"] = d["Symbol"]
    if "Code" not in d.columns:
        raise ValueError("Code column missing")
    d["Code"] = (
        d["Code"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(".", "-", regex=False)
    )
    d = d[d["Code"].str.match(r"^[A-Z][A-Z0-9.\-]{0,14}$", na=False)]
    if "Name" not in d.columns:
        d["Name"] = d["Code"]
    if "Market" not in d.columns:
        d["Market"] = "US"
    for src, dst in (
        ("Sector", "Sector"),
        ("Industry", "Industry"),
        ("sector", "Sector"),
        ("industry", "Industry"),
        ("GICS Sector", "Sector"),
        ("GICS Industry", "Industry"),
    ):
        if src in d.columns and dst not in d.columns:
            d[dst] = d[src]
    if "Sector" in d.columns:
        d["Sector"] = d["Sector"].astype(str).str.strip()
        d.loc[d["Sector"].isin(("", "nan", "None", "none")), "Sector"] = pd.NA
    if "Industry" in d.columns:
        d["Industry"] = d["Industry"].astype(str).str.strip()
        d.loc[d["Industry"].isin(("", "nan", "None", "none")), "Industry"] = pd.NA
    return d.drop_duplicates(subset=["Code"], ignore_index=True)


def _stage3_sqlite_codes(db_path: str) -> pd.DataFrame | None:
    try:
        if not db_path or not os.path.isfile(db_path):
            return None
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            tables = pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'US_%'",
                conn,
            )
        finally:
            conn.close()
    except Exception:
        return None
    if tables is None or tables.empty or "name" not in tables.columns:
        return None
    codes: List[str] = []
    for raw in tables["name"].astype(str):
        name = raw.strip()
        if name in _BENCH:
            continue
        m = _CODE_RE.match(name)
        if m:
            codes.append(m.group(1))
    if not codes:
        return None
    uq = sorted(set(codes))
    return pd.DataFrame({"Code": uq, "Name": uq, "Market": "US"})


def _us_list_columns(df: pd.DataFrame) -> List[str]:
    base = ["Code", "Name", "Market"]
    for c in ("Sector", "Industry"):
        if c in df.columns:
            base.append(c)
    return base


def enrich_missing_us_sectors(
    df: pd.DataFrame,
    *,
    max_fetch: int = 120,
) -> pd.DataFrame:
    """Sector/Industry 누락 행에 yfinance 메타 보강 (배치 상한)."""
    if df is None or df.empty or "Code" not in df.columns:
        return df
    out = df.copy()
    if "Sector" not in out.columns:
        out["Sector"] = pd.NA
    if "Industry" not in out.columns:
        out["Industry"] = pd.NA
    miss = out["Sector"].isna() | (out["Sector"].astype(str).str.strip() == "")
    if not miss.any():
        return out
    try:
        from market_data_fetcher import fetch_us_ticker_sector_industry
    except ImportError:
        return out
    n = 0
    for idx in out.index[miss]:
        if n >= max_fetch:
            break
        code = str(out.at[idx, "Code"]).strip()
        if not code:
            continue
        sec, ind = fetch_us_ticker_sector_industry(code)
        if sec:
            out.at[idx, "Sector"] = sec
        if ind and "Industry" in out.columns:
            out.at[idx, "Industry"] = ind
        n += 1
        time.sleep(0.08)
    return out


def _fetch_fdr_us_list(fdr_module) -> pd.DataFrame:
    fdr = fdr_module
    parts: List[pd.DataFrame] = []
    for mkt in ("NASDAQ", "NYSE", "AMEX"):
        try:
            chunk = fdr.StockListing(mkt)
            if chunk is not None and not chunk.empty:
                chunk = chunk.copy()
                if "Market" not in chunk.columns:
                    chunk["Market"] = mkt
                parts.append(chunk)
        except Exception:
            pass
        time.sleep(0.35)
    if not parts:
        return pd.DataFrame(columns=["Code", "Name", "Market"])
    df = pd.concat(parts, ignore_index=True)
    if "Symbol" in df.columns:
        df["Code"] = df["Symbol"]
    return _normalize_us_columns(df)


def collect_us_list_survival(
    *,
    db_path: str | None = None,
    primary_cache_csv: str | None = None,
    min_live_rows: int = _MIN_LIVE_ROWS,
    fdr_module=None,
) -> Tuple[pd.DataFrame, str]:
    """
    Tier 1: FDR NASDAQ/NYSE/AMEX
    Tier 2: us_list_cache.csv
    Tier 3: sqlite US_{ticker} tables
    Returns (DataFrame[Code,Name,Market], source) source in live|cache|sqlite|fail
    """
    warnings.filterwarnings("ignore", category=FutureWarning)
    resolved_db = db_path or MARKET_DATA_DB_PATH
    resolved_cache = primary_cache_csv or default_us_list_cache_path(resolved_db)

    if fdr_module is None:
        try:
            import FinanceDataReader as fdr_module  # type: ignore
        except ImportError:
            fdr_module = None

    if fdr_module is not None:
        try:
            live = _fetch_fdr_us_list(fdr_module)
            if live is not None and len(live) >= min_live_rows:
                live = enrich_missing_us_sectors(live)
                _safe_write_cache(live, resolved_cache)
                return live[_us_list_columns(live)], "live"
        except Exception:
            pass

    cached = _read_cache_csv(resolved_cache)
    if cached is not None and len(cached) >= max(50, min_live_rows // 4):
        try:
            norm = _normalize_us_columns(cached)
            if len(norm) >= 50:
                norm = enrich_missing_us_sectors(norm, max_fetch=80)
                return norm[_us_list_columns(norm)], "cache"
        except Exception:
            pass

    sqlite_df = _stage3_sqlite_codes(resolved_db)
    if sqlite_df is not None and len(sqlite_df) >= 50:
        return sqlite_df[_us_list_columns(sqlite_df)], "sqlite"

    if cached is not None and not cached.empty:
        try:
            norm = enrich_missing_us_sectors(_normalize_us_columns(cached), max_fetch=60)
            return norm[_us_list_columns(norm)], "cache"
        except Exception:
            pass

    return pd.DataFrame(columns=["Code", "Name", "Market"]), "fail"
