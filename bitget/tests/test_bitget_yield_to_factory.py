"""Bitget yield-to-factory 가드 — 4GB 서버에서 주식 무거운 잡과 동시 실행 방지."""
from __future__ import annotations

import datetime
import os
import unittest
from unittest import mock

import pytz

import bitget.bitget_schedule_guard as g


class TestYieldToFactory(unittest.TestCase):
    def setUp(self) -> None:
        self._lock = g._FACTORY_LOCK_PATH
        self._prev_env = os.environ.get("BITGET_YIELD_TO_FACTORY")
        os.environ.pop("BITGET_YIELD_TO_FACTORY", None)
        # 실제 os.kill 회피(특히 win32 의 CTRL_C 위험) — 기본 '살아있음'으로 가정.
        self._alive = mock.patch.object(g, "_pid_alive", return_value=True)
        self._alive.start()

    def tearDown(self) -> None:
        self._alive.stop()
        if os.path.exists(self._lock):
            os.remove(self._lock)
        if self._prev_env is None:
            os.environ.pop("BITGET_YIELD_TO_FACTORY", None)
        else:
            os.environ["BITGET_YIELD_TO_FACTORY"] = self._prev_env

    def _write_lock(self, mode: str, *, pid: int = 4242, started=None) -> None:
        started = started or datetime.datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
        with open(self._lock, "w", encoding="utf-8") as f:
            f.write(f"{mode}\n{started}\n{pid}\n")

    def test_no_lock_does_not_yield(self):
        if os.path.exists(self._lock):
            os.remove(self._lock)
        self.assertFalse(g.factory_heavy_job_active()[0])
        self.assertFalse(g.evaluate_scan_skip("scan_spot_supernova")[0])

    def test_active_factory_scan_makes_bitget_scan_yield(self):
        os.environ["BITGET_YIELD_TO_FACTORY"] = "1"
        self._write_lock("scan_kr_supernova")
        self.assertTrue(g.factory_heavy_job_active()[0])
        skip, reason = g.evaluate_scan_skip("scan_spot_supernova")
        self.assertTrue(skip)
        self.assertIn("yield_to_factory", reason)

    def test_data_refresh_also_yields(self):
        os.environ["BITGET_YIELD_TO_FACTORY"] = "1"
        self._write_lock("daily_audit_kr")
        skip, _ = g.evaluate_scan_skip("data_refresh")
        self.assertTrue(skip)

    def test_light_ops_never_yield(self):
        self._write_lock("scan_kr_supernova")
        self.assertFalse(g.evaluate_scan_skip("track_positions")[0])
        self.assertFalse(g.evaluate_scan_skip("reconcile")[0])

    def test_light_factory_holder_does_not_trigger_yield(self):
        # track_positions 같은 경량 holder 는 무거운 잡 prefix 아님 → 양보 안 함.
        self._write_lock("track_positions")
        self.assertFalse(g.factory_heavy_job_active()[0])

    def test_dead_pid_does_not_yield(self):
        self._write_lock("scan_kr_supernova")
        with mock.patch.object(g, "_pid_alive", return_value=False):
            self.assertFalse(g.factory_heavy_job_active()[0])

    def test_stale_lock_beyond_max_age_does_not_starve_bitget(self):
        import time

        self._write_lock("scan_kr_supernova")
        old = time.time() - (g._factory_yield_max_age_sec() + 600)
        os.utime(self._lock, (old, old))
        self.assertFalse(g.factory_heavy_job_active()[0])

    def test_opt_out_env_disables_yield(self):
        self._write_lock("scan_kr_supernova")
        os.environ["BITGET_YIELD_TO_FACTORY"] = "0"
        self.assertFalse(g.factory_heavy_job_active()[0])
        self.assertFalse(g.evaluate_scan_skip("scan_spot_supernova")[0])


if __name__ == "__main__":
    unittest.main()
