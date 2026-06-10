"""DB market SSOT — _normalize_trade_market · _daily_report_trades_for_market."""
import pandas as pd

from forward.shared import _daily_report_trades_for_market, _normalize_trade_market


def test_db_market_us_wins_over_numeric_code():
    assert _normalize_trade_market("005930", "US") == "US"
    assert _normalize_trade_market("123456", "us") == "US"


def test_db_market_kr_wins_over_alpha_code():
    assert _normalize_trade_market("AAPL", "KR") == "KR"


def test_empty_market_falls_back_to_code_shape():
    assert _normalize_trade_market("005930", "") == "KR"
    assert _normalize_trade_market("005930", None) == "KR"
    assert _normalize_trade_market("AAPL", "") == "US"
    assert _normalize_trade_market("AAPL", None) == "US"


def test_daily_report_trades_keeps_us_rows_with_numeric_code():
    df = pd.DataFrame(
        [
            {"id": 1, "code": "005930", "market": "US", "status": "CLOSED"},
            {"id": 2, "code": "AAPL", "market": "US", "status": "CLOSED"},
            {"id": 3, "code": "005930", "market": "KR", "status": "CLOSED"},
        ]
    )
    us = _daily_report_trades_for_market(df, "US")
    assert len(us) == 2
    assert set(us["id"].tolist()) == {1, 2}

    kr = _daily_report_trades_for_market(df, "KR")
    assert len(kr) == 1
    assert kr.iloc[0]["id"] == 3
