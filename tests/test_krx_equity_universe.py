"""krx_equity_universe — MarketId·ETF 제외 집합 필터."""
from __future__ import annotations

import unittest

import pandas as pd

from krx_equity_universe import filter_krx_equity_universe


class TestKrxEquityUniverse(unittest.TestCase):
    def test_market_id_and_etf_code_exclusion(self):
        raw = pd.DataFrame(
            [
                {"Code": "005930", "Name": "삼성전자", "Market": "KOSPI", "MarketId": "STK"},
                {"Code": "252670", "Name": "인버스2X", "Market": "KOSPI", "MarketId": "STK"},
                {"Code": "123456", "Name": "스팩예시", "Market": "KOSDAQ", "MarketId": "KSQ", "Dept": "SPAC(소속부)"},
                {"Code": "999999", "Name": "코넥스", "Market": "KONEX", "MarketId": "KNX"},
                {"Code": "005935", "Name": "삼성전자우", "Market": "KOSPI", "MarketId": "STK"},
            ]
        )
        out = filter_krx_equity_universe(
            raw, derivative_exclude=frozenset({"252670"})
        )
        codes = set(out["Code"].tolist())
        self.assertEqual(codes, {"005930"})

    def test_naver_style_without_market_id_uses_market_column(self):
        raw = pd.DataFrame(
            [
                {"Code": "000660", "Name": "SK하이닉스", "Market": "KOSPI", "Marcap": 0},
                {"Code": "196170", "Name": "알테오젠", "Market": "KOSDAQ GLOBAL", "Marcap": 0},
            ]
        )
        out = filter_krx_equity_universe(raw, derivative_exclude=frozenset())
        self.assertEqual(len(out), 2)


if __name__ == "__main__":
    unittest.main()
