"""Staggered factory scan schedule — pipeline modes & cron SSOT."""
from __future__ import annotations

import unittest

from factory_pipelines import FACTORY_MODES, get_pipeline, build_factory_pipelines
from factory_scan_schedule import (
    ALL_SCAN_SLOTS,
    KR_SCAN_SLOTS,
    MARKET_CLOSE_LOCAL,
    US_SCAN_SLOTS,
    scan_mode_market,
    slots_for_market,
)


def _slot_min(slot) -> int:
    return slot.hour * 60 + slot.minute


def _close_min(market: str) -> int:
    h, m = MARKET_CLOSE_LOCAL[market]
    return h * 60 + m


class TestStaggeredScanSchedule(unittest.TestCase):
    def test_kr_cycle1_window_and_spacing(self):
        # 1회차 6종(50분 간격) + 2회차(마감 전에 들어가는 것만)
        c1 = [s for s in KR_SCAN_SLOTS if s.cycle == 1]
        self.assertEqual(len(c1), 6)
        hours = [_slot_min(s) for s in c1]
        self.assertEqual(min(hours), 10 * 60)
        for i in range(1, len(hours)):
            self.assertEqual(hours[i] - hours[i - 1], 50)

    def test_us_cycle1_no_master_spacing(self):
        c1 = [s for s in US_SCAN_SLOTS if s.cycle == 1]
        self.assertEqual(len(c1), 5)
        keys = [s.scanner_key for s in c1]
        self.assertNotIn("master", keys)
        hours = [_slot_min(s) for s in c1]
        for i in range(1, len(hours)):
            self.assertEqual(hours[i] - hours[i - 1], 50)

    def test_no_scan_slot_after_market_close(self):
        # 핵심 불변식: 어떤 슬롯도 정규장 마감 이후로 편성되지 않는다
        # (장외 SKIPPED_SESSION 으로 매일 데이터 정체되던 회귀 방지)
        for s in KR_SCAN_SLOTS:
            self.assertLessEqual(
                _slot_min(s), _close_min("KR"),
                msg=f"KR {s.mode} {s.hour:02d}:{s.minute:02d} > close",
            )
        for s in US_SCAN_SLOTS:
            self.assertLessEqual(
                _slot_min(s), _close_min("US"),
                msg=f"US {s.mode} {s.hour:02d}:{s.minute:02d} > close",
            )

    def test_us_cycle2_all_four_fit(self):
        c2 = [s for s in US_SCAN_SLOTS if s.cycle == 2]
        self.assertEqual(len(c2), 4)  # US 는 마감(16:00) 전 4종 모두 편성

    def test_kr_cycle2_only_those_that_fit(self):
        c2 = [s for s in KR_SCAN_SLOTS if s.cycle == 2]
        # KR 은 15:30 마감 전 들어가는 것만(=supernova_r2, nulrim_r2)
        self.assertGreaterEqual(len(c2), 1)
        modes = {s.mode for s in c2}
        self.assertIn("scan_kr_supernova_r2", modes)

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
        # KR: 1회차 6 + 2회차(마감 전 적합분) / US: 1회차 5 + 2회차 4
        self.assertEqual(len(slots_for_market("KR")), len(KR_SCAN_SLOTS))
        self.assertEqual(len(slots_for_market("US")), 9)
        self.assertEqual(len([s for s in slots_for_market("US") if s.cycle == 2]), 4)

    def test_cron_templates_match_ssot(self):
        import subprocess
        import sys
        from pathlib import Path

        repo = Path(__file__).resolve().parents[1]
        gen = repo / "deploy" / "generate_factory_crontab.py"
        proc = subprocess.run(
            [sys.executable, str(gen), "--check"],
            cwd=str(repo),
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            proc.returncode,
            0,
            msg=proc.stderr or proc.stdout or "cron template drift",
        )


if __name__ == "__main__":
    unittest.main()
