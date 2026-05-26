"""factory_runtime — lock metadata, PID alive, stale self-heal."""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest import mock

from factory_runtime import (
    LockMetadata,
    _attempt_stale_lock_self_heal,
    _maybe_purge_stale_lock_file,
    _parse_lock_metadata,
    _pid_is_alive,
)


class TestLockMetadata(unittest.TestCase):
    def test_parse_v2_with_pid(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
            f.write("scan_kr\n2026-05-27T10:00:00+09:00\n12345\n")
            path = f.name
        try:
            meta = _parse_lock_metadata(path)
            self.assertIsNotNone(meta)
            assert meta is not None
            self.assertEqual(meta.mode, "scan_kr")
            self.assertEqual(meta.pid, 12345)
        finally:
            os.unlink(path)

    def test_pid_alive_self(self):
        self.assertTrue(_pid_is_alive(os.getpid()))

    def test_pid_alive_dead(self):
        self.assertFalse(_pid_is_alive(999999999))

    def test_purge_dead_pid_lock_file(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".factory_runtime.lock")
            with open(path, "w", encoding="utf-8") as f:
                f.write("daily_audit_us\n2026-05-27T10:00:00+09:00\n999999999\n")
            self.assertTrue(_maybe_purge_stale_lock_file(path))
            self.assertFalse(os.path.isfile(path))


@unittest.skipIf(sys.platform == "win32", "fcntl flock is Linux-only in factory_runtime")
class TestStaleLockHeal(unittest.TestCase):
    def test_heal_dead_pid_acquires(self):
        import fcntl

        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, ".factory_runtime.lock")
            with open(path, "w", encoding="utf-8") as f:
                f.write("scan_kr\n2026-05-27T10:00:00+09:00\n999999999\n")

            lock_f = open(path, "a+", encoding="utf-8")
            try:
                with mock.patch(
                    "factory_runtime._pid_is_alive", return_value=False
                ):
                    healed, _ = _attempt_stale_lock_self_heal(
                        path,
                        lock_f,
                        requesting_mode="scan_kr",
                        fcntl_mod=fcntl,
                    )
                self.assertTrue(healed)
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
            finally:
                lock_f.close()


if __name__ == "__main__":
    unittest.main()
