"""Zombie pipeline guard — critical step 실패 시 downstream 스킵 (주식 factory_runtime 패리티)."""
from __future__ import annotations

import unittest

from bitget.infra.runtime import StepSpec, bitget_exit_code, dispatch_bitget_mode


class TestBitgetPipelineCriticalAbort(unittest.TestCase):
    def test_critical_failure_skips_downstream(self):
        calls: list[str] = []

        def boom() -> None:
            calls.append("critical")
            raise RuntimeError("meta_governor_sync aborted")

        def overseer() -> None:
            calls.append("overseer")

        pipeline = [
            StepSpec("meta_governor_sync", boom, critical=True),
            StepSpec("ai_overseer", overseer, critical=False),
        ]
        report = dispatch_bitget_mode("daily_audit", pipeline, skip_telegram=True)
        self.assertEqual(calls, ["critical"])
        self.assertFalse(report.all_critical_ok)
        self.assertEqual(report.status_label, "FAIL")
        self.assertEqual(bitget_exit_code(report), 1)
        skipped = [s for s in report.steps if s.name == "ai_overseer"]
        self.assertEqual(len(skipped), 1)
        self.assertFalse(skipped[0].ok)
        self.assertIn("skipped", skipped[0].error or "")

    def test_optional_failure_continues_downstream(self):
        calls: list[str] = []

        def _raise_optional() -> None:
            raise RuntimeError("net")

        pipeline = [
            StepSpec("meta_governor_sync", lambda: calls.append("meta"), critical=True),
            StepSpec("sentiment_mining", _raise_optional, critical=False),
            StepSpec("track_spot", lambda: calls.append("track"), critical=True),
        ]
        report = dispatch_bitget_mode("daily_audit", pipeline, skip_telegram=True)
        self.assertEqual(calls, ["meta", "track"])
        self.assertEqual(report.status_label, "PARTIAL_FAIL")
        self.assertEqual(bitget_exit_code(report), 0)


if __name__ == "__main__":
    unittest.main()
