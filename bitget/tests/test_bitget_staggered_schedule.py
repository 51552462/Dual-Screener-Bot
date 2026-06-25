"""Staggered Bitget scan schedule — pipeline modes & cron SSOT."""
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

from bitget.bitget_scan_schedule import (
    ALL_SCAN_SLOTS,
    FUTURES_SCAN_SLOTS,
    SPOT_SCAN_SLOTS,
    scan_mode_market,
    slots_for_market,
)
from bitget.infra.runtime import BITGET_MODES
from bitget.pipelines.bitget_pipelines import PIPELINE_BUILDERS, get_pipeline

_REPO = Path(__file__).resolve().parents[2]


class TestBitgetStaggeredSchedule(unittest.TestCase):
    def test_spot_slot_count_and_24h_spread(self):
        self.assertEqual(len(SPOT_SCAN_SLOTS), 10)
        mins = sorted(s.hour * 60 + s.minute for s in SPOT_SCAN_SLOTS)
        # 24h 분산: 첫 슬롯은 자정대, 마지막은 밤 — 한 윈도우(예: 01–08)에 몰리지 않음.
        self.assertLess(mins[0], 60)
        self.assertGreaterEqual(mins[-1], 20 * 60)
        # 모든 분은 5의 배수가 아니어야 주식/ops 크론과 같은 분에 안 겹친다.
        for s in SPOT_SCAN_SLOTS:
            self.assertNotEqual(s.minute % 5, 0, f"{s.mode} at :{s.minute:02d} is %5")

    def test_futures_slot_count_no_master(self):
        self.assertEqual(len(FUTURES_SCAN_SLOTS), 9)
        keys = [s.scanner_key for s in FUTURES_SCAN_SLOTS if s.cycle == 1]
        self.assertNotIn("master", keys)
        for s in FUTURES_SCAN_SLOTS:
            self.assertNotEqual(s.minute % 5, 0, f"{s.mode} at :{s.minute:02d} is %5")

    def test_no_stock_minute_collision_and_no_simultaneous_markets(self):
        # KR/US 스캔(:00..:50)·오딧(:45) 분 = 5의 배수. bitget 스캔은 전부 비-5배수여야 함.
        for s in ALL_SCAN_SLOTS:
            self.assertNotEqual(s.minute % 5, 0)
        # SPOT/FUTURES 는 같은 (시,분)에 동시 실행되면 안 됨(서버 부하/락 경합).
        times = [(s.hour, s.minute) for s in ALL_SCAN_SLOTS]
        self.assertEqual(len(times), len(set(times)))

    def test_every_slot_has_pipeline(self):
        for slot in ALL_SCAN_SLOTS:
            self.assertIn(slot.mode, PIPELINE_BUILDERS)
            self.assertIn(slot.mode, BITGET_MODES)
            steps = get_pipeline(slot.mode)
            self.assertGreaterEqual(len(steps), 2)
            names = [s.name for s in steps]
            self.assertIn("artifact_guard", names)

    def test_supernova_full_prelude_spot(self):
        steps = [s.name for s in get_pipeline("scan_spot_supernova")]
        self.assertIn("meta_governor_sync_scan", steps)
        self.assertIn("scan_spot_supernova", steps)

    def test_ema5_minimal_prelude(self):
        steps = [s.name for s in get_pipeline("scan_spot_ema5")]
        self.assertIn("scan_spot_ema5", steps)
        self.assertNotIn("meta_governor_sync_scan", steps)

    def test_shadow_tail_spot(self):
        steps = [s.name for s in get_pipeline("scan_spot_shadow")]
        self.assertIn("shadow_eval", steps)
        self.assertIn("track_spot", steps)
        self.assertIn("doomsday_bridge_sync", steps)

    def test_scan_mode_market(self):
        self.assertEqual(scan_mode_market("scan_spot_nulrim_r2"), "SPOT")
        self.assertEqual(scan_mode_market("scan_futures_ema5"), "FUTURES")
        self.assertIsNone(scan_mode_market("daily_audit"))

    def test_slots_for_market(self):
        self.assertEqual(len(slots_for_market("SPOT")), 10)
        self.assertEqual(len(slots_for_market("FUTURES")), 9)

    def test_cron_template_matches_ssot(self):
        gen = _REPO / "bitget" / "deploy" / "generate_bitget_crontab.py"
        proc = subprocess.run(
            [sys.executable, str(gen), "--check"],
            cwd=str(_REPO),
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
