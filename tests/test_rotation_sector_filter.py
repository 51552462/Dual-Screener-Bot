"""rotation_sector_filter — Junk Hard Block."""
from rotation_sector_filter import (
    dominant_sector_for_series,
    filter_eligible_daily_series,
    is_rotation_eligible_sector,
)


def test_junk_blocked():
    assert is_rotation_eligible_sector("기타/혼합") is False
    assert is_rotation_eligible_sector("nan") is False
    assert is_rotation_eligible_sector("US/EQUITY", market="US") is False
    assert is_rotation_eligible_sector("반도체/IT") is True


def test_dominant_skips_junk():
    sectors = ["기타/혼합", "기타/혼합", "반도체/IT", "반도체/IT"]
    assert dominant_sector_for_series(sectors) == "반도체/IT"


def test_filter_series():
    raw = [("2026-01-01", "기타/혼합"), ("2026-01-02", "에너지/화학")]
    out = filter_eligible_daily_series(raw)
    assert len(out) == 1
    assert out[0][1] == "에너지/화학"
