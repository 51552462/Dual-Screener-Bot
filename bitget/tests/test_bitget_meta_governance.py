"""Bitget meta_alerts + MetaGovernor cycle invariants."""
from __future__ import annotations

import unittest
from unittest import mock


class TestMetaAlerts(unittest.TestCase):
    def test_send_meta_critical_alert_html_escape(self):
        from bitget.governance.meta_alerts import send_meta_critical_alert

        sent: list[str] = []

        with mock.patch(
            "bitget.forward.shared.send_telegram_msg",
            side_effect=lambda m: sent.append(m),
        ):
            ok = send_meta_critical_alert("Test <title>", "body & detail", prefix="META_BRAIN")

        self.assertTrue(ok)
        self.assertEqual(len(sent), 1)
        self.assertIn("META_BRAIN", sent[0])
        self.assertIn("&lt;title&gt;", sent[0])
        self.assertIn("body &amp; detail", sent[0])

    def test_send_meta_critical_alert_telegram_skip_returns_false(self):
        from bitget.governance.meta_alerts import send_meta_critical_alert

        with mock.patch(
            "bitget.forward.shared.send_telegram_msg",
            side_effect=RuntimeError("no token"),
        ):
            ok = send_meta_critical_alert("x", "y")
        self.assertFalse(ok)


class TestMetaGovernorCycleWiring(unittest.TestCase):
    def test_run_cycle_delegates_to_root_meta_governor(self):
        from bitget.governance import meta_sync

        fake_state = {
            "META_GOVERNOR_LAST_RUN_STATUS": "OK",
            "META_REGIME_KEY": "CHOP",
            "META_REGIME_ACTION": {"kelly_cap": 0.018},
        }
        with mock.patch.object(meta_sync, "_write_bitget_config_snapshot_for_governor", return_value="/tmp/cfg.json"), mock.patch(
            "meta_governor.MetaGovernor"
        ) as Gov, mock.patch.object(meta_sync, "save_bitget_meta_unified") as save_u, mock.patch.object(
            meta_sync, "sync_config_regime_from_meta"
        ) as sync_r, mock.patch(
            "bitget.governance.meta_consumer.invalidate_meta_state_cache"
        ):
            inst = Gov.return_value
            inst.run_governor_cycle.return_value = fake_state
            status = meta_sync._run_bitget_meta_governor_cycle()
        self.assertEqual(status, "OK")
        save_u.assert_called_once_with(fake_state, mock.ANY)
        sync_r.assert_called_once_with(fake_state, force=True)

    def test_rebuild_sends_alert_on_regime_failure(self):
        from bitget.governance import meta_sync

        alerts: list[tuple[str, str]] = []

        def _alert(title: str, body: str, *, prefix: str = "CRITICAL") -> bool:
            alerts.append((title, body))
            return True

        with mock.patch.object(meta_sync, "regime_analysis_stale_or_missing", return_value=True), mock.patch.object(
            meta_sync, "_refresh_coin_regime", side_effect=RuntimeError("regime boom")
        ), mock.patch.object(meta_sync, "load_bitget_meta_unified", return_value={"META_REGIME_KEY": "CHOP", "META_GOVERNOR_LAST_RUN_STATUS": "OK", "META_GOVERNOR_LAST_RUN_AT": "2026-06-24T00:00:00+00:00", "META_REGIME_ACTION": {}}), mock.patch.object(
            meta_sync, "is_bitget_meta_degraded", return_value=False
        ), mock.patch.object(meta_sync, "_run_bitget_meta_governor_cycle", return_value="OK"), mock.patch.object(
            meta_sync, "ensure_config_regime_aligned", return_value={"synced": False}
        ), mock.patch(
            "bitget.governance.meta_alerts.send_meta_critical_alert",
            side_effect=_alert,
        ):
            out = meta_sync.rebuild_bitget_meta_state(force=False, refresh_regime=True)
        self.assertEqual(out.get("regime"), "failed")
        self.assertTrue(any("regime" in t.lower() for t, _ in alerts))


if __name__ == "__main__":
    unittest.main()
