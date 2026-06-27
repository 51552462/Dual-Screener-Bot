"""forward.dna_autopsy — 슬라이스 · 임계 · Fallback."""
from datetime import datetime

import pandas as pd
import pytz

from reports.daily_report_context import DailyReportContext
from forward.dna_autopsy import (
    build_dna_autopsy_slice,
    format_dna_autopsy_section,
    resolve_dna_thresholds,
)


def test_resolve_dna_thresholds_us_default_profile():
    j, d, m = resolve_dna_thresholds("US", {})
    assert j == 4.0
    assert d == -2.5
    assert m == 2


def test_resolve_dna_thresholds_config_override():
    cfg = {"FORWARD_DNA_JACKPOT_PCT_KR": 3.5, "FORWARD_DNA_DISASTER_PCT_KR": -2.0}
    j, d, _ = resolve_dna_thresholds("KR", cfg)
    assert j == 3.5
    assert d == -2.0


def test_build_slice_tier_b_candidates():
    ref = datetime(2026, 5, 26, 12, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
    ctx = DailyReportContext.build(ref_kst=ref, rolling_days=90)
    df = pd.DataFrame({"final_ret": [1.0, -1.5, 2.0]})
    sl = build_dna_autopsy_slice(ctx, "KR", df, sys_config={})
    assert sl.n_closed == 3
    assert sl.n_winners == 0
    assert sl.n_losers == 0


def test_format_tier_a_no_scanner_hint_when_open_exists():
    ref = datetime(2026, 5, 26, 12, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
    ctx = DailyReportContext.build(ref_kst=ref, rolling_days=90)
    sl = build_dna_autopsy_slice(ctx, "KR", pd.DataFrame(), sys_config={})
    html_out = format_dna_autopsy_section(sl, ctx=ctx, n_real=5, n_open=5)
    assert "factory.sh" not in html_out
    assert "스캐너" not in html_out
    assert "청산(CLOSED) <b>0</b>건" in html_out


def test_format_tier_b_message():
    ref = datetime(2026, 5, 26, 12, 0, 0, tzinfo=pytz.timezone("Asia/Seoul"))
    ctx = DailyReportContext.build(ref_kst=ref, rolling_days=90)
    df = pd.DataFrame({"final_ret": [1.0]})
    sl = build_dna_autopsy_slice(ctx, "US", df, sys_config={})
    html_out = format_dna_autopsy_section(sl, ctx=ctx, n_real=1, n_open=0)
    assert "기준에 부합하는 종목이 없습니다" in html_out
