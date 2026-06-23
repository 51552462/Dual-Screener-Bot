"""factory_runtime — 장외 scan 즉시 스킵 (락·prelude 없음 · scan_* 는 스킵 통지)."""
from __future__ import annotations

import unittest
from unittest import mock

from factory_runtime import (
    FactoryRunReport,
    dispatch_factory_mode,
    factory_exit_code,
    notify_factory_run,
)
from factory_runtime import StepSpec


class TestScanSessionSkip(unittest.TestCase):
    def test_off_hours_scan_kr_skips_before_lock(self):
        sent: list[str] = []

        def _send(msg: str) -> None:
            sent.append(msg)

        sat = __import__("pytz").timezone("Asia/Seoul").localize(
            __import__("datetime").datetime(2026, 6, 28, 11, 0, 0)
        )
        with mock.patch(
            "market_session_gate.is_market_open",
            return_value=(False, "KR weekend — 장외"),
        ), mock.patch("factory_schedule_guard.kst_now", return_value=sat):
            report = dispatch_factory_mode(
                "scan_kr",
                [StepSpec("never", lambda: (_ for _ in ()).throw(AssertionError()))],
                send_fn=_send,
            )

        self.assertTrue(report.skipped_session)
        self.assertEqual(report.status_label, "SKIPPED_SESSION")
        self.assertEqual(factory_exit_code(report), 0)
        self.assertEqual(len(sent), 0)

    def test_notify_skips_lock_and_session_for_non_scan(self):
        for label, kwargs in (
            ("SKIPPED_LOCK", {"skipped_lock": True}),
            ("SKIPPED_SESSION", {"skipped_session": True}),
        ):
            with self.subTest(label=label):
                sent: list[str] = []
                report = FactoryRunReport(
                    mode="daily_audit_us",
                    run_id="t",
                    started_at="s",
                    finished_at="f",
                    **kwargs,
                )
                notify_factory_run(report, send_fn=lambda m: sent.append(m))
                self.assertEqual(sent, [])

    def test_notify_scan_us_skipped_session_sends_once(self):
        sent: list[str] = []
        report = FactoryRunReport(
            mode="scan_us_supernova",
            run_id="t",
            started_at="s",
            finished_at="f",
            skipped_session=True,
            skipped_session_detail="US 장외",
        )
        # KST 23:30 (US 장중) — 예상 외 스킵이면 알림
        night = __import__("pytz").timezone("Asia/Seoul").localize(
            __import__("datetime").datetime(2026, 6, 23, 23, 30, 0)
        )
        with mock.patch("factory_schedule_guard.kst_now", return_value=night):
            notify_factory_run(
                report,
                send_fn=lambda m: sent.append(m),
            )
        self.assertEqual(len(sent), 1)
        self.assertIn("SKIPPED_SESSION", sent[0])


if __name__ == "__main__":
    unittest.main()
