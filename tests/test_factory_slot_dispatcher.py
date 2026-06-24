"""factory_slot_dispatcher — ET slot matching without CRON_TZ."""
from __future__ import annotations

import unittest
from datetime import datetime

import pytz

from factory_scan_schedule import US_SCAN_SLOTS
from factory_slot_dispatcher import due_slots

_ET = pytz.timezone("America/New_York")


class TestFactorySlotDispatcher(unittest.TestCase):
    def test_us_supernova_due_at_et_1000(self):
        now = _ET.localize(datetime(2026, 6, 23, 10, 2, 0))  # Mon
        due, _ = due_slots("US", now=now, grace_minutes=4, state={})
        modes = [s.mode for s in due]
        self.assertIn("scan_us_supernova", modes)

    def test_us_not_due_on_weekend(self):
        now = _ET.localize(datetime(2026, 6, 27, 10, 0, 0))  # Sat
        due, _ = due_slots("US", now=now, grace_minutes=4, state={})
        self.assertEqual(due, [])

    def test_us_skips_already_dispatched(self):
        now = _ET.localize(datetime(2026, 6, 23, 10, 1, 0))
        state = {"scan_us_supernova:2026-06-23": "done"}
        due, _ = due_slots("US", now=now, grace_minutes=4, state=state)
        self.assertNotIn("scan_us_supernova", [s.mode for s in due])

    def test_us_bowl_r2_slots_exist(self):
        modes = {s.mode for s in US_SCAN_SLOTS}
        self.assertIn("scan_us_bowl", modes)
        self.assertIn("scan_us_supernova_r2", modes)


if __name__ == "__main__":
    unittest.main()
