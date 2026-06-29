"""
ReportTimekeeper SSOT — 시장별 session_anchor · 롤링 컷오프 · DB 워터마크.

KR: KST 달력 최근 영업일.
US: 방금 마감된 US 현지 종가 세션 영업일 (America/New_York, 16:00 ET 근사).
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal, Optional

import pytz

_KR_TZ = pytz.timezone("Asia/Seoul")
_US_ET = pytz.timezone("America/New_York")

MarketCode = Literal["KR", "US"]
ReadSource = Literal["MAIN", "SNAPSHOT"]


@dataclass(frozen=True)
class ReportTimekeeper:
    """딥다이브·최우수 성적표·듀얼트랙 쿼리의 단일 시간 앵커."""

    market: str
    calendar_today_kst: str
    session_anchor: str
    rolling_cutoff: str
    rolling_days: int
    db_watermark_exit: Optional[str]
    read_source: ReadSource
    anchor_label: str

    @property
    def session_anchor_date(self) -> date:
        return datetime.strptime(self.session_anchor, "%Y-%m-%d").date()

    @classmethod
    def for_market(
        cls,
        market: str,
        *,
        rolling_days: int = 90,
        ref_kst: Optional[datetime] = None,
        db_watermark_exit: Optional[str] = None,
        read_source: ReadSource = "MAIN",
    ) -> "ReportTimekeeper":
        mkt = str(market).upper()
        if ref_kst is None:
            ref_kst = datetime.now(_KR_TZ)
        elif ref_kst.tzinfo is None:
            ref_kst = _KR_TZ.localize(ref_kst)
        else:
            ref_kst = ref_kst.astimezone(_KR_TZ)

        cal = ref_kst.date()
        cal_str = cal.strftime("%Y-%m-%d")
        rd = int(rolling_days)
        if rd not in (90, 180):
            rd = 90

        if mkt == "US":
            anchor_d = us_last_trading_session_date(ref=ref_kst)
            label = "US Last Trading Day (ET)"
        else:
            anchor_d = kr_session_anchor_date(ref=cal)
            label = "KST 영업일"

        anchor_str = anchor_d.strftime("%Y-%m-%d")
        cutoff = (anchor_d - timedelta(days=rd)).strftime("%Y-%m-%d")

        return cls(
            market=mkt,
            calendar_today_kst=cal_str,
            session_anchor=anchor_str,
            rolling_cutoff=cutoff,
            rolling_days=rd,
            db_watermark_exit=db_watermark_exit,
            read_source=read_source,
            anchor_label=label,
        )

    def header_watermark_line(self) -> str:
        wm = self.db_watermark_exit or "—"
        src = self.read_source
        return (
            f"리포트일 KST <b>{self.calendar_today_kst}</b> · "
            f"세션앵커({self.anchor_label}) <b>{self.session_anchor}</b> · "
            f"DB청산워터마크 <b>{wm}</b> · 읽기 <b>{src}</b>"
        )


def kr_session_anchor_date(*, ref: Optional[date] = None) -> date:
    """KR: 토·일이면 직전 금요일, 그 외 당일."""
    d = ref or datetime.now(_KR_TZ).date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def us_last_trading_session_date(*, ref: Optional[datetime] = None) -> date:
    """
    US: 방금 마감된 NYSE regular 세션의 현지 영업일.
    KST 화 06:45 실행 → ET 월 16:xx → anchor = 월(US).
    """
    if ref is None:
        ref = datetime.now(_KR_TZ)
    elif ref.tzinfo is None:
        ref = _KR_TZ.localize(ref)
    else:
        ref = ref.astimezone(_KR_TZ)

    et = ref.astimezone(_US_ET)
    session = et.date()
    if et.hour < 16:
        session -= timedelta(days=1)
    while session.weekday() >= 5:
        session -= timedelta(days=1)
    return session


def previous_business_day(d: date, *, market: str) -> date:
    """시장 달력(주말 제외) 기준 직전 영업일."""
    m = str(market).upper()
    p = d - timedelta(days=1)
    while p.weekday() >= 5:
        p -= timedelta(days=1)
    return p


def resolve_data_candle_watermark(
    market: str, sys_config: Optional[dict] = None
) -> Optional[str]:
    """시장 데이터(벤치마크 캔들) 신선도 워터마크 — '청산 공백'과 '진짜 데이터 정체' 분리용 SSOT.

    청산 워터마크와 무관하게 최신 벤치마크 캔들 날짜(SPY/KOSPI_IDX)를 돌려준다.
    DB(세션) 우선 → 실패 시 FLUID_{KR|US}_ANCHOR_STATE.latest_candle 폴백 → None.

    모든 staleness gate 호출부가 이 함수를 공유해야 한 곳만 고치고 다른 곳이 누락되는
    오탐(예: 실무자 리포트 RED 오탐)을 원천 차단한다. fluid_time_anchor 는 순환참조 회피
    위해 함수 내부에서 지연 import.
    """
    mk = str(market or "KR").upper()
    try:
        from fluid_time_anchor import (
            load_kr_kospi_session_from_db,
            load_spy_session_from_db,
        )

        db_candle = (
            load_spy_session_from_db() if mk == "US" else load_kr_kospi_session_from_db()
        )
        if db_candle:
            return str(db_candle)[:10]
    except Exception:
        pass
    try:
        state = (sys_config or {}).get(f"FLUID_{mk}_ANCHOR_STATE")
        if isinstance(state, dict):
            cand = state.get("latest_candle") or state.get("session_date")
            if cand:
                return str(cand)[:10]
    except Exception:
        pass
    return None


def business_lag_days(watermark: Optional[str], anchor: str, *, market: str) -> int:
    """워터마크가 anchor보다 몇 영업일 뒤처졌는지 (0=동일 또는 최신)."""
    if not watermark or not anchor:
        return 99
    try:
        w = datetime.strptime(str(watermark)[:10], "%Y-%m-%d").date()
        a = datetime.strptime(str(anchor)[:10], "%Y-%m-%d").date()
    except ValueError:
        return 99
    if w >= a:
        return 0
    lag = 0
    cur = w
    while cur < a:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            lag += 1
    return lag
