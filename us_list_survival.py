"""
US listing pipeline: FDR → CSV cache → sqlite US_* table names (KR krx_list_survival 대칭).
"""
from __future__ import annotations

import os
import re
import sqlite3
import time
import warnings
from typing import List, Optional, Tuple

import pandas as pd

from market_db_paths import MARKET_DATA_DB_PATH

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
                _safe_write_cache(live, resolved_cache)
                return live[["Code", "Name", "Market"]], "live"
        except Exception:
            pass

    cached = _read_cache_csv(resolved_cache)
    if cached is not None and len(cached) >= max(50, min_live_rows // 4):
        try:
            norm = _normalize_us_columns(cached)
            if len(norm) >= 50:
                return norm[["Code", "Name", "Market"]], "cache"
        except Exception:
            pass

    sqlite_df = _stage3_sqlite_codes(resolved_db)
    if sqlite_df is not None and len(sqlite_df) >= 50:
        return sqlite_df[["Code", "Name", "Market"]], "sqlite"

    if cached is not None and not cached.empty:
        try:
            return _normalize_us_columns(cached)[["Code", "Name", "Market"]], "cache"
        except Exception:
            pass

    return pd.DataFrame(columns=["Code", "Name", "Market"]), "fail"
