"""report_executive_summary — 3단 Executive Summary."""
from cross_market_ssot import MODE_KR_STANDALONE, MODE_US_ONLINE
from report_executive_summary import (
    build_daily_executive_summary_html,
    build_weekly_executive_summary_html,
)


def _sample_meta() -> dict:
    return {
        "META_REGIME_KEY": "BULL",
        "META_GROUP_KELLY_MULT": {
            "SUPERNOVA_COSINE": 0.75,
            "BEAST": 1.35,
        },
        "META_CHANGELOG": [
            {
                "key": "META_GROUP_KELLY_MULT",
                "old": {"SUPERNOVA_COSINE": 1.0, "BEAST": 1.0},
                "new": {"SUPERNOVA_COSINE": 0.75, "BEAST": 1.35},
                "reason": "treasury_groups",
                "at": "2026-06-10T09:00:00",
            }
        ],
    }


def test_daily_kr_three_sections():
    ssot = {
        "mode": MODE_US_ONLINE,
        "us_sector_std": "AI/반도체",
        "kr_sector_std": "반도체",
        "age_hours": 4.0,
    }
    html = build_daily_executive_summary_html(
        _sample_meta(),
        ssot,
        market="KR",
        sys_config={"DYNAMIC_SUPERNOVA_CUTOFF": 0.72},
    )
    assert "[시장 &amp; 국면]" in html
    assert "[오늘의 시스템 조치]" in html
    assert "[내일의 스탠스]" in html
    assert "BULL" in html
    assert "SUPERNOVA" in html or "COSINE" in html
    assert "AI/반도체" in html


def test_daily_us_spillover():
    ssot = {"mode": MODE_US_ONLINE, "us_sector_std": "Technology"}
    html = build_daily_executive_summary_html(
        {"META_REGIME_KEY": "SIDEWAYS", "META_GROUP_KELLY_MULT": {}},
        ssot,
        market="US",
    )
    assert "🇺🇸" in html
    assert "Technology" in html


def test_daily_kr_standalone():
    ssot = {"mode": MODE_KR_STANDALONE, "degraded_reason": "stale_or_missing"}
    html = build_daily_executive_summary_html(
        {"META_REGIME_KEY": "BEAR", "META_GROUP_KELLY_MULT": {}},
        ssot,
        market="KR",
    )
    assert "KR_STANDALONE" in html or "단독 모멘텀" in html


def test_weekly_executive_summary():
    meta = _sample_meta()
    html = build_weekly_executive_summary_html(
        meta,
        {"DYNAMIC_SUPERNOVA_CUTOFF": 0.72, "CROSS_MARKET_SSOT": {"mode": MODE_US_ONLINE}},
        week_start="2026-06-09",
        week_end="2026-06-13",
        regime_key="BULL",
        lifecycle_n_cooled=2,
        kr_week_pnl=1500000.0,
    )
    assert "[주간 최종 요약" in html
    assert "[다음 주 스탠스]" in html
