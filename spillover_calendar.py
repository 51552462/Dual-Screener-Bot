"""
한·미 스필오버 캘린더 — ReportTimekeeper SSOT + KST/ET 정렬 버킷.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable, List, Optional, Sequence, Tuple

import pandas as pd
import pytz

from report_timekeeper import (
    ReportTimekeeper,
    business_lag_days,
    previous_business_day,
    us_last_trading_session_date,
)

_KR_TZ = pytz.timezone("Asia/Seoul")


@dataclass(frozen=True)
class AlignedSpilloverDay:
    """KR 리포트 1칸 — KST 세션 + 짝지은 US ET 세션."""

    kst_label: str
    kr_session: str
    us_session: str
    kr_trade_dates: Tuple[str, ...]
    us_trade_dates: Tuple[str, ...]


@dataclass(frozen=True)
class SpilloverCalendarContext:
    """V28 타임라인 SSOT — Timekeeper 강제 상속."""

    calendar_today_kst: str
    kr_anchor: str
    us_anchor: str
    kr_anchor_label: str
    us_anchor_label: str
    us_db_watermark: Optional[str]
    us_lag_business_days: int
    window_days: int
    aligned_days: Tuple[AlignedSpilloverDay, ...]

    @classmethod
    def from_timekeepers(
        cls,
        tk_kr: ReportTimekeeper,
        tk_us: ReportTimekeeper,
        *,
        window_days: int = 7,
    ) -> "SpilloverCalendarContext":
        wd = max(1, int(window_days))
        kr_end = tk_kr.session_anchor_date
        kr_dates = _kr_business_days_ending(kr_end, wd)
        aligned: List[AlignedSpilloverDay] = []
        for kr_d in kr_dates:
            kr_s = kr_d.strftime("%Y-%m-%d")
            us_d = _us_session_for_kr_day(kr_d)
            us_s = us_d.strftime("%Y-%m-%d")
            aligned.append(
                AlignedSpilloverDay(
                    kst_label=kr_s,
                    kr_session=kr_s,
                    us_session=us_s,
                    kr_trade_dates=(kr_s,),
                    us_trade_dates=(us_s,),
                )
            )
        lag = business_lag_days(
            tk_us.db_watermark_exit, tk_us.session_anchor, market="US"
        )
        return cls(
            calendar_today_kst=tk_kr.calendar_today_kst,
            kr_anchor=tk_kr.session_anchor,
            us_anchor=tk_us.session_anchor,
            kr_anchor_label=tk_kr.anchor_label,
            us_anchor_label=tk_us.anchor_label,
            us_db_watermark=tk_us.db_watermark_exit,
            us_lag_business_days=lag,
            window_days=wd,
            aligned_days=tuple(aligned),
        )

    def query_cutoff(self, *, padding_days: int = 30) -> str:
        """forward_trades entry_date 하한 (YYYY-MM-DD)."""
        dates: List[date] = []
        for row in self.aligned_days:
            dates.append(_parse_ymd(row.kr_session))
            dates.append(_parse_ymd(row.us_session))
        if not dates:
            anchor = _parse_ymd(self.kr_anchor)
            return (anchor - timedelta(days=padding_days)).strftime("%Y-%m-%d")
        earliest = min(dates)
        return (earliest - timedelta(days=padding_days)).strftime("%Y-%m-%d")

    def recent_kr_labels(self, n: int = 3) -> Tuple[str, ...]:
        """최근 N KR 세션일 (alignment 카운트용)."""
        labels = [d.kst_label for d in self.aligned_days]
        return tuple(labels[-n:])


def _parse_ymd(s: str) -> date:
    return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()


def _kr_business_days_ending(anchor: date, count: int) -> List[date]:
    out: List[date] = []
    d = anchor
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    for _ in range(count):
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        out.append(d)
        d = previous_business_day(d, market="KR")
    out.reverse()
    return out


def _us_session_for_kr_day(kr_d: date) -> date:
    """KR 영업일 종가 근사 시점 기준 짝지은 US Last Trading Day."""
    ref = _KR_TZ.localize(datetime(kr_d.year, kr_d.month, kr_d.day, 23, 59, 59))
    return us_last_trading_session_date(ref=ref)


def add_norm_day_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    out = df.copy()
    out["norm_day"] = out["entry_date"].astype(str).str.slice(0, 10)
    return out


def dominant_sector_label_for_days(
    trade_dates: Sequence[str],
    df_raw: pd.DataFrame,
    map_standard_sector: Callable[[object], str],
    sector_row_ok: Callable[[object], bool],
) -> str:
    """정렬된 trade_dates 집합에 속하는 진입 행의 주도 섹터."""
    if df_raw is None or df_raw.empty or "norm_day" not in df_raw.columns:
        return "데이터 없음"
    days = {str(d)[:10] for d in trade_dates if d}
    if not days:
        return "데이터 없음"
    sub = df_raw.loc[df_raw["norm_day"].isin(days)]
    if sub.empty:
        return "데이터 없음"
    mapped = sub["sector"].map(lambda x: map_standard_sector(x))
    ok = mapped.map(sector_row_ok)
    if not ok.any():
        return "필터 탈락"
    good = mapped.loc[ok]
    mode_ser = good.mode()
    if mode_ser.empty:
        return "필터 탈락"
    v = str(mode_ser.iloc[0]).strip()
    return v if v else "필터 탈락"
