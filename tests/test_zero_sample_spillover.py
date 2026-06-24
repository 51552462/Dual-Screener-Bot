"""zero_sample_spillover — NA-safe confidence · universe coalesce."""
import pandas as pd

from zero_sample_spillover import _first_cell_str, persist_dark_horse_spillover_cfg


def test_confidence_calculation_na_score():
    """Regression: min(0.92, pd.NA) raised ambiguous NA boolean."""
    from reports.forward_report_scalar import scalar_float

    top_row = pd.Series({"score": pd.NA, "n": 5})
    score_v = scalar_float(top_row["score"], 0.0)
    n_v = scalar_float(top_row["n"], 0.0)
    conf = float(min(0.92, 0.45 + score_v * 0.08 + min(n_v, 8.0) * 0.03))
    assert 0.5 < conf <= 0.92


def test_universe_symbol_na_coalesce():
    """Regression: row.get('Symbol') or row.get('Code') on pd.NA."""
    row = pd.Series({"Symbol": pd.NA, "Code": "AAPL", "Sector": pd.NA, "Industry": "Tech"})
    assert _first_cell_str(row, "Symbol", "Code") == "AAPL"
    assert _first_cell_str(row, "Sector", "Industry", default="Unknown") == "Tech"


def test_build_code_to_sector_with_na_rows():
    """infer_dark_horse universe loop — pd.NA Symbol/Sector must not raise."""
    udf = pd.DataFrame(
        {
            "Code": ["AAPL", "MSFT"],
            "Symbol": pd.array([pd.NA, "MSFT"], dtype="string"),
            "Sector": pd.array([pd.NA, "Technology"], dtype="string"),
        }
    )
    code_to_sector: dict[str, str] = {}
    for _, row in udf.iterrows():
        sym = _first_cell_str(row, "Symbol", "Code").upper()
        sec = _first_cell_str(row, "Sector", "Industry", default="Unknown")
        if sym:
            code_to_sector[sym] = sec
    assert code_to_sector["AAPL"] == "Unknown"
    assert code_to_sector["MSFT"] == "Technology"


def test_persist_dark_horse_spillover_cfg():
    cfg: dict = {}
    out = persist_dark_horse_spillover_cfg(
        cfg,
        {"ok": True, "sector_std": "Technology", "method": "sector_aggregate", "confidence": 0.7, "n_tickers": 12},
        save=False,
    )
    assert out["applied"] is True
    assert cfg["US_SPILLOVER_SECTOR"] == "Technology"


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
