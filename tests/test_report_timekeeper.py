"""ReportTimekeeper · US Last Trading Day · Staleness Gate."""
from __future__ import annotations

from datetime import datetime

import pytz

from reports.report_staleness_gate import evaluate_staleness
from reports.report_timekeeper import (
    ReportTimekeeper,
    business_lag_days,
    kr_session_anchor_date,
    us_last_trading_session_date,
)

_KR = pytz.timezone("Asia/Seoul")


def test_us_anchor_kst_tuesday_morning_is_us_monday():
    # KST 2026-05-26 (화) 06:45 → US ET 월요일 장 마감 직후
    ref = _KR.localize(datetime(2026, 5, 26, 6, 45, 0))
    assert us_last_trading_session_date(ref=ref).strftime("%Y-%m-%d") == "2026-05-25"


def test_kr_anchor_skips_weekend():
    assert kr_session_anchor_date(ref=datetime(2026, 5, 24).date()).weekday() == 4


def test_staleness_red_on_large_lag():
    tk = ReportTimekeeper.for_market(
        "KR",
        rolling_days=90,
        ref_kst=_KR.localize(datetime(2026, 5, 26, 17, 0, 0)),
        db_watermark_exit="2026-05-17",
        read_source="MAIN",
    )
    assert business_lag_days("2026-05-17", tk.session_anchor, market="KR") >= 2
    v = evaluate_staleness(tk, live_row_count=0)
    assert v.grade == "RED"
    assert not v.allow_tier_champion


def test_staleness_green_when_watermark_current():
    tk = ReportTimekeeper.for_market(
        "US",
        rolling_days=90,
        ref_kst=_KR.localize(datetime(2026, 5, 26, 6, 45, 0)),
        db_watermark_exit="2026-05-25",
        read_source="MAIN",
    )
    v = evaluate_staleness(tk, live_row_count=3)
    assert v.grade == "GREEN"


def test_staleness_downgrades_to_yellow_when_candle_fresh():
    """청산 워터마크는 ≥2영업일 지연이나 시장 캔들은 신선 → 데이터 정체 아님(YELLOW)."""
    tk = ReportTimekeeper.for_market(
        "KR",
        rolling_days=90,
        ref_kst=_KR.localize(datetime(2026, 5, 26, 17, 0, 0)),
        db_watermark_exit="2026-05-17",
        read_source="MAIN",
    )
    assert business_lag_days("2026-05-17", tk.session_anchor, market="KR") >= 2
    # 캔들 워터마크 = 세션 앵커 당일 (신선)
    v = evaluate_staleness(
        tk, live_row_count=0, data_candle_watermark=tk.session_anchor
    )
    assert v.grade == "YELLOW"
    assert v.allow_tier_champion  # 최우수 성적표 차단 안 함
    assert not v.fail_safe_html


def test_staleness_stays_red_when_candle_also_stale():
    """청산 워터마크·시장 캔들 모두 지연 → 진짜 데이터 정체(RED)."""
    tk = ReportTimekeeper.for_market(
        "KR",
        rolling_days=90,
        ref_kst=_KR.localize(datetime(2026, 5, 26, 17, 0, 0)),
        db_watermark_exit="2026-05-17",
        read_source="MAIN",
    )
    v = evaluate_staleness(
        tk, live_row_count=0, data_candle_watermark="2026-05-17"
    )
    assert v.grade == "RED"
    assert not v.allow_tier_champion
