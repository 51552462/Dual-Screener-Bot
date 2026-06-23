"""OPEN 장부 ↔ 리포트 보유 집계 정합."""
from __future__ import annotations

import unittest

import pandas as pd

from forward.forward_book_integrity import (
    compute_open_book_stats,
    reporter_valid_holding_mask,
)


class TestOpenBookIntegrity(unittest.TestCase):
    def test_valid_holding_sim_kelly_without_shares(self):
        df = pd.DataFrame(
            [
                {
                    "status": "OPEN",
                    "shares": 0,
                    "sim_kelly_invest": 500000.0,
                    "invest_amount": 0,
                    "entry_price": 10000,
                    "sig_type": "RANK_C test",
                }
            ]
        )
        self.assertTrue(bool(reporter_valid_holding_mask(df).iloc[0]))

    def test_exclude_incubator_open(self):
        df = pd.DataFrame(
            [
                {
                    "status": "OPEN",
                    "shares": 10,
                    "sim_kelly_invest": 1.0,
                    "invest_amount": 1.0,
                    "entry_price": 1.0,
                    "sig_type": "[INCUBATOR_x]",
                }
            ]
        )
        self.assertFalse(bool(reporter_valid_holding_mask(df).iloc[0]))

    def test_stats_ghost_detection(self):
        df = pd.DataFrame(
            [
                {
                    "status": "OPEN",
                    "shares": 0,
                    "sim_kelly_invest": 0,
                    "invest_amount": 0,
                    "entry_price": 0,
                    "sig_type": "RANK_A",
                    "entry_date": "2026-06-23",
                },
                {
                    "status": "OPEN",
                    "shares": 0,
                    "sim_kelly_invest": 400000,
                    "invest_amount": 0,
                    "entry_price": 5000,
                    "sig_type": "RANK_B",
                    "entry_date": "2026-06-23",
                },
            ]
        )
        st = compute_open_book_stats(df, market="KR", session_anchor="2026-06-23")
        self.assertEqual(st.open_raw, 2)
        self.assertEqual(st.open_valid, 1)
        self.assertEqual(st.open_ghost, 1)


if __name__ == "__main__":
    unittest.main()
