"""SMARTMONEY-PERSIST-FIX-01: Naver/scan flow_map upsert, no silent 0-row skip."""
from __future__ import annotations

import unittest
from unittest.mock import patch

import smart_money_tracker as smt


class TestSmartMoneyPersistFix01(unittest.TestCase):
    def test_trade_date_iso(self):
        self.assertEqual(smt._trade_date_iso("20260911"), "2026-09-11")
        self.assertEqual(smt._trade_date_iso("2026-09-11"), "2026-09-11")

    def test_empty_flow_map_logs_and_skips_pykrx(self):
        with patch.object(smt, "_try_pykrx_flow_leaderboard") as krx:
            n = smt._persist_investor_flow_timeseries(
                ["20260910", "20260911"],
                {},
                source="naver_fallback",
            )
        self.assertEqual(n, 0)
        krx.assert_not_called()

    def test_naver_flow_map_upserts_last_trade_date(self):
        acc = {
            "005930": {"name": "삼성전자", "krw": 1.5e9, "vol": 1000.0},
            "000660": {"name": "SK하이닉스", "krw": 8e8, "vol": 400.0},
        }
        with patch(
            "kr_flow_factor.persist_daily_flow", return_value=2
        ) as persist:
            n = smt._persist_investor_flow_timeseries(
                ["20260910", "20260911"],
                acc,
                source="naver_fallback",
            )
        self.assertEqual(n, 2)
        persist.assert_called_once()
        date_arg, map_arg = persist.call_args[0]
        self.assertEqual(date_arg, "2026-09-11")
        self.assertEqual(map_arg, acc)

    def test_pykrx_not_called_when_flow_map_present(self):
        acc = {"005930": {"name": "삼성전자", "krw": 1.0, "vol": 1.0}}
        with patch.object(smt, "_try_pykrx_flow_leaderboard") as krx:
            with patch("kr_flow_factor.persist_daily_flow", return_value=1):
                smt._persist_investor_flow_timeseries(
                    ["20260911"], acc, source="naver_fallback"
                )
        krx.assert_not_called()

    def test_persist_zero_is_logged_not_swallowed(self):
        acc = {"005930": {"name": "x", "krw": 1.0, "vol": 1.0}}
        with patch("kr_flow_factor.persist_daily_flow", return_value=0):
            n = smt._persist_investor_flow_timeseries(
                ["20260911"], acc, source="naver_fallback"
            )
        self.assertEqual(n, 0)


if __name__ == "__main__":
    unittest.main()
