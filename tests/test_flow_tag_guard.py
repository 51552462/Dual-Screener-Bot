"""Flow tag guard, nan filter, stock chip formatting."""
from __future__ import annotations

import unittest

import pandas as pd

from forward_flow_tag_deep_dive import (
    _is_valid_tag,
    _sanitize_flow_tags_series,
    _stock_chip,
    build_flow_tag_snapshot,
)
from forward_market_guard import MarketContaminationError, enforce_market_frame
from reports.report_staleness_gate import evaluate_staleness
from reports.report_timekeeper import ReportTimekeeper


class TestMarketGuard(unittest.TestCase):
    def test_scrub_us_ticker_from_kr_frame(self):
        df = pd.DataFrame(
            [
                {"code": "005930", "market": "KR", "flow_tags": "#test", "final_ret": 1.0},
                {"code": "KLAC", "market": "KR", "flow_tags": "#test", "final_ret": -4.0},
            ]
        )
        out = enforce_market_frame(df, "KR", context="test")
        self.assertEqual(len(out), 1)
        self.assertEqual(str(out.iloc[0]["code"]), "005930")

    def test_strict_raises(self):
        df = pd.DataFrame([{"code": "KLAC", "market": "KR", "final_ret": 1.0}])
        with self.assertRaises(MarketContaminationError):
            enforce_market_frame(df, "KR", context="test", mode="strict")


class TestFlowTagNanFilter(unittest.TestCase):
    def test_invalid_tags_rejected(self):
        self.assertFalse(_is_valid_tag("nan"))
        self.assertFalse(_is_valid_tag(""))
        self.assertTrue(_is_valid_tag("#상승"))

    def test_explode_nan_tag_excluded(self):
        tk = ReportTimekeeper.for_market("KR", rolling_days=90)
        df = pd.DataFrame(
            [
                {
                    "code": "005930",
                    "market": "KR",
                    "name": "삼성",
                    "flow_tags": "",
                    "final_ret": -4.0,
                    "exit_date": tk.session_anchor,
                },
                {
                    "code": "000660",
                    "market": "KR",
                    "name": "SK",
                    "flow_tags": "#모멘텀",
                    "final_ret": 2.0,
                    "exit_date": tk.session_anchor,
                },
                {
                    "code": "035420",
                    "market": "KR",
                    "name": "NAVER",
                    "flow_tags": "#모멘텀",
                    "final_ret": 3.0,
                    "exit_date": tk.session_anchor,
                },
                {
                    "code": "051910",
                    "market": "KR",
                    "name": "LG화학",
                    "flow_tags": "#모멘텀",
                    "final_ret": 1.0,
                    "exit_date": tk.session_anchor,
                },
            ]
        )
        st = evaluate_staleness(tk, live_row_count=1)
        snap = build_flow_tag_snapshot(df, timekeeper=tk, staleness=st, persist_toxic=False)
        tags = {b.tag for b in snap.blocks}
        self.assertNotIn("nan", tags)

    def test_stock_chip_no_double_minus(self):
        row = pd.Series({"name": None, "code": "KLAC", "final_ret": -4.0, "_fr": -4.0})
        chip = _stock_chip(row)
        self.assertNotIn("—(-", chip)
        self.assertIn("KLAC", chip)
        self.assertIn("-4%", chip)
        self.assertNotIn("(+-", chip)


class TestSanitizeFlowTags(unittest.TestCase):
    def test_sanitize_literal_nan(self):
        s = pd.Series(["nan", "#ok", None])
        out = _sanitize_flow_tags_series(s)
        self.assertEqual(out.iloc[0], "")
        self.assertEqual(out.iloc[1], "#ok")


if __name__ == "__main__":
    unittest.main()
