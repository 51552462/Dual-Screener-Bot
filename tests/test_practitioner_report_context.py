"""PIL ReportContext — Timekeeper 활성 필터."""
from __future__ import annotations

import unittest
from datetime import datetime

import pytz

from reports.practitioner_report_context import PractitionerReportContext
from reports.report_timekeeper import ReportTimekeeper

_KR_TZ = pytz.timezone("Asia/Seoul")


class TestPractitionerReportContext(unittest.TestCase):
    def _ctx(self, ref: datetime) -> PractitionerReportContext:
        tk_kr = ReportTimekeeper.for_market(
            "KR", rolling_days=90, ref_kst=ref, db_watermark_exit="2026-05-20"
        )
        tk_us = ReportTimekeeper.for_market(
            "US", rolling_days=90, ref_kst=ref, db_watermark_exit="2026-05-19"
        )
        return PractitionerReportContext(
            tk_kr=tk_kr,
            tk_us=tk_us,
            db_read_path="/tmp/market_data.sqlite",
            read_source_label="MAIN",
        )

    def test_open_always_active(self):
        ref = _KR_TZ.localize(datetime(2026, 5, 26, 10, 0))
        ctx = self._ctx(ref)
        self.assertTrue(ctx.is_row_active("US", "OPEN", ""))

    def test_us_exit_in_anchor_window_active(self):
        ref = _KR_TZ.localize(datetime(2026, 5, 26, 10, 0))
        ctx = self._ctx(ref)
        anchor = ctx.session_anchor_str("US")
        self.assertTrue(ctx.is_row_active("US", "CLOSED", anchor))

    def test_us_exit_before_rolling_cutoff_inactive(self):
        ref = _KR_TZ.localize(datetime(2026, 5, 26, 10, 0))
        ctx = self._ctx(ref)
        self.assertFalse(ctx.is_row_active("US", "CLOSED", "2020-01-01"))

    def test_kst_two_day_cutoff_would_drop_us_but_anchor_window_keeps(self):
        """구 KST-2일 필터: US ET 청산이 3일 전이면 탈락 — 앵커 윈도우는 유지."""
        ref = _KR_TZ.localize(datetime(2026, 5, 26, 10, 0))
        ctx = self._ctx(ref)
        tk = ctx.timekeeper_for("US")
        mid = tk.session_anchor
        self.assertTrue(ctx.is_row_active("US", "CLOSED", mid))

    def test_global_header_contains_anchors(self):
        ref = _KR_TZ.localize(datetime(2026, 5, 26, 10, 0))
        ctx = self._ctx(ref)
        h = ctx.global_timekeeper_header_html()
        self.assertIn("KR앵커", h)
        self.assertIn("US앵커(ET)", h)
        self.assertIn("lag", h)


if __name__ == "__main__":
    unittest.main()
