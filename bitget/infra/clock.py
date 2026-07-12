"""
Bitget UTC clock SSOT — `datetime.utcnow()` deprecation 대체.

24/7 코인 데몬·장부·OMS 전역에서 timezone-aware UTC 만 사용한다.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_date() -> date:
    return utc_now().date()


def utc_date_key(*, anchor: datetime | None = None) -> str:
    """UTC date key (YYYY-MM-DD) — daemon day_key / dedup SSOT."""
    ref = anchor if anchor is not None else utc_now()
    return ref.strftime("%Y-%m-%d")


def utc_date_str() -> str:
    return utc_date_key()


def utc_date_days_ago_str(days: int, *, anchor: datetime | None = None) -> str:
    """UTC 기준 N일 전 날짜 (YYYY-MM-DD) — SQL substr lookback SSOT."""
    ref = anchor if anchor is not None else utc_now()
    return (ref - timedelta(days=int(days))).strftime("%Y-%m-%d")


def utc_hm_key(*, anchor: datetime | None = None) -> str:
    """UTC 분 단위 키 (YYYY-MM-DD HH:MM) — funnel snapshot·daemon dedup SSOT."""
    ref = anchor if anchor is not None else utc_now()
    return ref.strftime("%Y-%m-%d %H:%M")


def utc_now_iso() -> str:
    """Timezone-aware UTC ISO8601 — ops_events ts_utc SSOT."""
    return utc_now().isoformat()


def utc_hours_ago_iso(hours: float, *, anchor: datetime | None = None) -> str:
    """UTC ISO timestamp N hours before anchor — ops query window SSOT."""
    ref = anchor if anchor is not None else utc_now()
    return (ref - timedelta(hours=float(hours))).isoformat()


def parse_utc_iso(s: str) -> datetime | None:
    """Parse ops_events ts_utc / ISO8601 into timezone-aware UTC datetime."""
    if not s or not isinstance(s, str):
        return None
    try:
        t = s.strip()
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        d = datetime.fromisoformat(t)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception:
        return None


def utc_datetime_str() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S")


def utc_datetime_str_tz() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")


def utc_compact_key(*, anchor: datetime | None = None) -> str:
    """UTC compact key (YYYYMMDDTHHMMSS) — backup archive / ops run_id SSOT."""
    ref = anchor if anchor is not None else utc_now()
    return ref.strftime("%Y%m%dT%H%M%S")
