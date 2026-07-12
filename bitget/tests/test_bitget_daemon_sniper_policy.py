"""Daemon supernova sniper must be opt-in — cron staggered scan is SSOT."""
from __future__ import annotations

import os
import unittest
from unittest import mock

from bitget.pipelines import bitget_auto_pilot as bap


class TestDaemonSniperPolicy(unittest.TestCase):
    def test_sniper_disabled_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("BITGET_DAEMON_SNIPER", None)
            self.assertFalse(bap._daemon_sniper_enabled())

    def test_sniper_enabled_when_opt_in(self):
        with mock.patch.dict(os.environ, {"BITGET_DAEMON_SNIPER": "1"}):
            self.assertTrue(bap._daemon_sniper_enabled())

    def test_main_loop_skips_sniper_thread_without_opt_in(self):
        started: list[str] = []

        class _FakeThread:
            def __init__(self, target=None, args=(), daemon=False, name=None):
                self.name = name
                started.append(name or "")

            def start(self):
                return None

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("BITGET_DAEMON_SNIPER", None)
            with mock.patch.object(bap.threading, "Thread", _FakeThread), mock.patch.object(
                bap, "setup_logging", create=True
            ), mock.patch("bitget.infra.logging_setup.setup_logging"), mock.patch(
                "bitget.infra.ops_logger.install_unhandled_exception_hooks"
            ), mock.patch(
                "bitget.infra.ops_logger.record_heartbeat"
            ), mock.patch(
                "bitget.infra.artifact_guard.ensure_bitget_artifacts",
                return_value={"ok": True},
            ), mock.patch.object(bap, "_oms_cycle"), mock.patch.object(
                bap, "_satellite_cycle"
            ), mock.patch.object(
                bap.time, "sleep", side_effect=KeyboardInterrupt
            ):
                with self.assertRaises(KeyboardInterrupt):
                    bap.system_main_loop()

        self.assertIn("bitget_hb", started)
        self.assertNotIn("bitget_supernova_sniper", started)

    def test_public_ws_disabled_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("BITGET_DAEMON_PUBLIC_WS", None)
            self.assertFalse(bap._daemon_public_ws_enabled())

    def test_public_ws_enabled_when_opt_in(self):
        with mock.patch.dict(os.environ, {"BITGET_DAEMON_PUBLIC_WS": "1"}):
            self.assertTrue(bap._daemon_public_ws_enabled())

    def test_private_ws_disabled_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("BITGET_DAEMON_PRIVATE_WS", None)
            self.assertFalse(bap._daemon_private_ws_enabled())

    def test_private_ws_enabled_when_opt_in(self):
        with mock.patch.dict(os.environ, {"BITGET_DAEMON_PRIVATE_WS": "1"}):
            self.assertTrue(bap._daemon_private_ws_enabled())


if __name__ == "__main__":
    unittest.main()
