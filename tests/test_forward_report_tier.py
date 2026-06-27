"""forward_report_tier — 티어 버킷 · 데스콤보 폴백."""
import pandas as pd

from reports.forward_report_tier import (
    compute_death_combo_flag,
    effective_tier_bucket,
    filter_death_combo_df,
    filter_tier_80_df,
    is_tier_80_bucket,
)


def test_effective_tier_bucket_from_score():
    assert effective_tier_bucket({"total_score": 85}) == "80점대"
    assert effective_tier_bucket({"tier": "90점대", "total_score": 85}) == "90점대"


def test_is_tier_80_bucket_score_range():
    assert is_tier_80_bucket({"total_score": 82.5}) is True
    assert is_tier_80_bucket({"total_score": 75}) is False
    assert is_tier_80_bucket({"tier": "80점대"}) is True


def test_filter_tier_80_df():
    df = pd.DataFrame(
        [
            {"tier": "70점대", "total_score": 85, "final_ret": 1},
            {"tier": "80점대", "total_score": 88, "final_ret": -1},
            {"tier": "90점대", "total_score": 92, "final_ret": 2},
        ]
    )
    out = filter_tier_80_df(df)
    assert len(out) == 2


def test_death_combo_fallback_from_facts():
    row = {"is_death_combo": 0, "dyn_cpv": 0.9, "dyn_rs": -1}
    assert compute_death_combo_flag(row) is True


def test_death_combo_db_flag_priority():
    row = {"is_death_combo": 1, "dyn_cpv": 0.1, "dyn_rs": 5}
    assert compute_death_combo_flag(row) is True


def test_filter_death_combo_df():
    df = pd.DataFrame(
        [
            {"is_death_combo": 0, "dyn_cpv": 0.9, "dyn_rs": -0.5, "final_ret": -2},
            {"is_death_combo": 0, "dyn_cpv": 0.5, "dyn_rs": 1, "final_ret": 3},
        ]
    )
    out = filter_death_combo_df(df, market="KR")
    assert len(out) == 1
