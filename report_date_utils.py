"""
리포트·장부 공통 날짜 정규화 (KST SSOT).

timezone-aware/naive·문자열 포맷 편차로 CLOSED/진입 표본이 0건으로 증발하는 것을 방지.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz

_KR_TZ = pytz.timezone("Asia/Seoul")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INVALID_TOKENS = frozenset({"", "nan", "none", "nat", "null"})


def kst_today_str() -> str:
    return datetime.now(_KR_TZ).strftime("%Y-%m-%d")


def normalize_date_scalar(value: object) -> str:
    """단일 값 → YYYY-MM-DD 또는 빈 문자열."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if s.lower() in _INVALID_TOKENS:
        return ""
    if _ISO_DATE.match(s[:10]):
        return s[:10]
    try:
        ts = pd.to_datetime(s, errors="coerce", utc=False)
        if pd.isna(ts):
            return ""
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            ts = ts.tz_convert(_KR_TZ) if hasattr(ts, "tz_convert") else ts
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return ""


def normalize_date_series(series: pd.Series) -> pd.Series:
    if series is None or len(series) == 0:
        return pd.Series(dtype=str)
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    formatted = parsed.dt.strftime("%Y-%m-%d")
    fallback = series.astype(str).str.strip().str.slice(0, 10)
    merged = formatted.where(parsed.notna(), fallback)
    merged = merged.str.strip().str.lower().replace(
        {tok: "" for tok in _INVALID_TOKENS if tok}
    )
    return merged.where(merged.str.match(_ISO_DATE, na=False), "")


def closed_event_dates(df: pd.DataFrame) -> pd.Series:
    """CLOSED 윈도우 기준일: exit_date 우선, 없으면 entry_date."""
    if df is None or df.empty:
        return pd.Series(dtype=str)
    exit_d = (
        normalize_date_series(df["exit_date"])
        if "exit_date" in df.columns
        else pd.Series("", index=df.index)
    )
    entry_d = (
        normalize_date_series(df["entry_date"])
        if "entry_date" in df.columns
        else pd.Series("", index=df.index)
    )
    return exit_d.where(exit_d != "", entry_d)


def entry_dates(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty or "entry_date" not in df.columns:
        return pd.Series(dtype=str)
    return normalize_date_series(df["entry_date"])


def in_date_window(days: pd.Series, cutoff: str, anchor: str) -> pd.Series:
    valid = days.str.match(_ISO_DATE, na=False)
    return valid & (days >= str(cutoff)[:10]) & (days <= str(anchor)[:10])


def is_stale_asof(asof: Optional[str], *, max_lag_days: int = 2) -> bool:
    """asof가 KST 오늘보다 max_lag_days 이상 뒤처졌으면 stale."""
    norm = normalize_date_scalar(asof)
    if not norm:
        return True
    try:
        asof_d = datetime.strptime(norm, "%Y-%m-%d").date()
        today_d = datetime.strptime(kst_today_str(), "%Y-%m-%d").date()
        return (today_d - asof_d).days > max(0, int(max_lag_days))
    except ValueError:
        return True
