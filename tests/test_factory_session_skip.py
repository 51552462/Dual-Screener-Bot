"""factory_runtime — 장외 scan 즉시 스킵 (락·prelude·FAIL 텔레그램 없음)."""
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

        with mock.patch(
            "market_session_gate.is_market_open",
            return_value=(False, "KR 장외 테스트"),
        ):
            report = dispatch_factory_mode(
                "scan_kr",
                [StepSpec("never", lambda: (_ for _ in ()).throw(AssertionError()))],
                send_fn=_send,
            )

        self.assertTrue(report.skipped_session)
        self.assertEqual(report.status_label, "SKIPPED_SESSION")
        self.assertEqual(factory_exit_code(report), 0)
        self.assertEqual(sent, [])

    def test_notify_skips_lock_and_session(self):
        for label, kwargs in (
            ("SKIPPED_LOCK", {"skipped_lock": True}),
            ("SKIPPED_SESSION", {"skipped_session": True}),
        ):
            with self.subTest(label=label):
                sent: list[str] = []
                report = FactoryRunReport(
                    mode="scan_us",
                    run_id="t",
                    started_at="s",
                    finished_at="f",
                    **kwargs,
                )
                notify_factory_run(report, send_fn=lambda m: sent.append(m))
                self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
