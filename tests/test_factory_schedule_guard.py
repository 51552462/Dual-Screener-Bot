"""factory_schedule_guard — quiet session skip & cron misalignment."""
from __future__ import annotations

import unittest
from datetime import datetime
from unittest import mock

import pytz

from factory_schedule_guard import is_quiet_scan_session_skip, us_cron_misalignment_hint

_KR = pytz.timezone("Asia/Seoul")


class TestFactoryScheduleGuard(unittest.TestCase):
    def test_us_skip_kst_noon_is_quiet(self):
        noon = _KR.localize(datetime(2026, 6, 23, 12, 30, 0))
        self.assertTrue(
            is_quiet_scan_session_skip("scan_us_ema5", detail="US 장외", now_kst=noon)
        )

    def test_us_misalignment_hint_kst_daytime(self):
        noon = _KR.localize(datetime(2026, 6, 23, 13, 20, 0))
        mis, hint = us_cron_misalignment_hint("scan_us_bowl", now_kst=noon)
        self.assertTrue(mis)
        self.assertIn("install_factory_cron", hint)

    def test_kr_skip_weekend_quiet(self):
        sat = _KR.localize(datetime(2026, 6, 27, 11, 0, 0))
        self.assertTrue(is_quiet_scan_session_skip("scan_kr_supernova", now_kst=sat))


class TestNotifyQuietSkip(unittest.TestCase):
    def test_us_off_hours_no_telegram(self):
        from factory_runtime import FactoryRunReport, notify_factory_run

        sent: list[str] = []
        report = FactoryRunReport(
            mode="scan_us_ema5",
            run_id="t",
            started_at="s",
            finished_at="f",
            skipped_session=True,
            skipped_session_detail="US 장외",
        )
        noon = _KR.localize(datetime(2026, 6, 23, 12, 30, 0))
        with mock.patch("factory_schedule_guard.kst_now", return_value=noon):
            notify_factory_run(report, send_fn=lambda m: sent.append(m))
        self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
