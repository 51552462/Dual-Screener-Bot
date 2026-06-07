import pandas as pd

from report_date_utils import (
    closed_event_dates,
    in_date_window,
    is_stale_asof,
    normalize_date_scalar,
    normalize_date_series,
)


def test_normalize_date_scalar_iso_and_nan():
    assert normalize_date_scalar("2026-06-02") == "2026-06-02"
    assert normalize_date_scalar("2026-06-02T15:30:00+09:00") == "2026-06-02"
    assert normalize_date_scalar("nan") == ""
    assert normalize_date_scalar(None) == ""


def test_closed_event_dates_exit_fallback():
    df = pd.DataFrame(
        {
            "exit_date": ["", "nan", "2026-06-01"],
            "entry_date": ["2026-05-28", "2026-05-29", "2026-05-20"],
        }
    )
    days = closed_event_dates(df)
    assert days.iloc[0] == "2026-05-28"
    assert days.iloc[1] == "2026-05-29"
    assert days.iloc[2] == "2026-06-01"


def test_in_date_window():
    days = pd.Series(["2026-05-01", "2026-06-01", "invalid"])
    mask = in_date_window(days, "2026-05-15", "2026-06-02")
    assert mask.tolist() == [False, True, False]


def test_is_stale_asof():
    assert is_stale_asof("2099-01-01", max_lag_days=2) is False
    assert is_stale_asof("2000-01-01", max_lag_days=2) is True
    assert is_stale_asof("", max_lag_days=2) is True
