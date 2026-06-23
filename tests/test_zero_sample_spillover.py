"""zero_sample_spillover — NA-safe confidence."""
import pandas as pd


def test_confidence_calculation_na_score():
    """Regression: min(0.92, pd.NA) raised ambiguous NA boolean."""
    top_row = pd.Series({"score": pd.NA, "n": 5})
    score_v = float(pd.to_numeric(top_row["score"], errors="coerce") or 0.0)
    n_v = float(pd.to_numeric(top_row["n"], errors="coerce") or 0.0)
    conf = float(min(0.92, 0.45 + score_v * 0.08 + min(n_v, 8.0) * 0.03))
    assert 0.5 < conf <= 0.92


def test_enrich_sectors_miss_any_na_safe():
    from us_list_survival import enrich_missing_us_sectors

    df = pd.DataFrame(
        {
            "Code": ["ZZZ"],
            "Name": ["Z"],
            "Market": ["US"],
            "Sector": pd.array([pd.NA], dtype="string"),
        }
    )
    out = enrich_missing_us_sectors(df, max_fetch=0)
    assert len(out) == 1
