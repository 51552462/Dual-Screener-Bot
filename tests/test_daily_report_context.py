"""DailyReportContext — Timekeeper · 헤더."""
from datetime import datetime

import pytz

from reports.daily_report_context import DailyReportContext


def test_build_and_calendar_today():
    ref = datetime(2026, 5, 26, 12, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
    ctx = DailyReportContext.build(ref_kst=ref, rolling_days=90)
    assert ctx.calendar_today_kst == "2026-05-26"
    assert ctx.window_days == 90


def test_global_header_contains_anchor_and_lag():
    ref = datetime(2026, 5, 26, 12, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
    ctx = DailyReportContext.build(ref_kst=ref, rolling_days=90)
    html = ctx.global_header_html()
    assert "KR앵커" in html
    assert "US앵커" in html
    assert "lag KR" in html
    assert "워터마크" in html


def test_market_window_header_sample_counts():
    ref = datetime(2026, 5, 26, 12, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
    ctx = DailyReportContext.build(ref_kst=ref, rolling_days=90)
    hdr = ctx.market_window_header_html("KR", n_real=10, n_closed=7, n_open=3)
    assert "표본 실거래" in hdr
    assert "<b>10</b>" in hdr
    assert "lag" in hdr
