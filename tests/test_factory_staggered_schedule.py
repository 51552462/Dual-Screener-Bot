"""Staggered factory scan schedule — pipeline modes & cron SSOT."""
from __future__ import annotations

import unittest

from factory_pipelines import FACTORY_MODES, get_pipeline, build_factory_pipelines
from factory_scan_schedule import (
    ALL_SCAN_SLOTS,
    KR_SCAN_SLOTS,
    US_SCAN_SLOTS,
    scan_mode_market,
    slots_for_market,
)


class TestStaggeredScanSchedule(unittest.TestCase):
    def test_kr_slot_count_and_window(self):
        self.assertEqual(len(KR_SCAN_SLOTS), 10)
        hours = [s.hour * 60 + s.minute for s in KR_SCAN_SLOTS]
        self.assertEqual(min(hours), 10 * 60)
        self.assertEqual(max(hours), 14 * 60 + 30)
        for i in range(1, len(hours)):
            self.assertGreaterEqual(hours[i] - hours[i - 1], 30)

    def test_us_slot_count_no_master(self):
        self.assertEqual(len(US_SCAN_SLOTS), 9)
        keys = [s.scanner_key for s in US_SCAN_SLOTS if s.cycle == 1]
        self.assertNotIn("master", keys)

    def test_every_slot_has_pipeline(self):
        pipelines = build_factory_pipelines()
        for slot in ALL_SCAN_SLOTS:
            self.assertIn(slot.mode, pipelines)
            self.assertIn(slot.mode, FACTORY_MODES)
            steps = get_pipeline(slot.mode)
            self.assertGreaterEqual(len(steps), 2)  # guard + scan
            names = [s.name for s in steps]
            self.assertIn("factory_artifact_guard", names)

    def test_supernova_full_prelude_kr(self):
        steps = [s.name for s in get_pipeline("scan_kr_supernova")]
        self.assertIn("meta_governor_sync_scan", steps)
        self.assertIn("supernova_scan_kr", steps)

    def test_ema5_minimal_prelude(self):
        steps = [s.name for s in get_pipeline("scan_kr_ema5")]
        self.assertIn("kr_ema5_scan", steps)
        self.assertNotIn("meta_governor_sync_scan", steps)

    def test_scan_mode_market(self):
        self.assertEqual(scan_mode_market("scan_kr_nulrim_r2"), "KR")
        self.assertEqual(scan_mode_market("scan_us_ema5"), "US")
        self.assertIsNone(scan_mode_market("daily_audit_kr"))

    def test_slots_for_market(self):
        self.assertEqual(len(slots_for_market("KR")), 10)
        self.assertEqual(len(slots_for_market("US")), 9)


if __name__ == "__main__":
    unittest.main()
