"""Spillover calendar alignment tests."""
from __future__ import annotations

import unittest
from datetime import date

import pytz

from report_timekeeper import ReportTimekeeper
from spillover_calendar import SpilloverCalendarContext, _us_session_for_kr_day


class TestSpilloverCalendar(unittest.TestCase):
    def test_us_session_for_kr_day_weekday(self):
        kr_d = date(2026, 5, 20)
        us_d = _us_session_for_kr_day(kr_d)
        self.assertIsInstance(us_d, date)
        self.assertLessEqual(us_d.weekday(), 4)

    def test_aligned_window_length(self):
        kr_tz = pytz.timezone("Asia/Seoul")
        ref = kr_tz.localize(__import__("datetime").datetime(2026, 5, 26, 8, 0, 0))
        tk_kr = ReportTimekeeper.for_market("KR", ref_kst=ref)
        tk_us = ReportTimekeeper.for_market("US", ref_kst=ref)
        cal = SpilloverCalendarContext.from_timekeepers(tk_kr, tk_us, window_days=7)
        self.assertEqual(len(cal.aligned_days), 7)
        self.assertEqual(cal.aligned_days[-1].kr_session, tk_kr.session_anchor)
        for row in cal.aligned_days:
            self.assertEqual(row.us_trade_dates[0], row.us_session)


if __name__ == "__main__":
    unittest.main()
