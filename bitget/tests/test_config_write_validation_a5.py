"""A-5 — config write-time reject bounds (DYNAMIC_KELLY_RISK, MAX_LEVERAGE)."""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

from bitget.infra import config_manager as cm


class TestConfigWriteValidationA5(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self._cfg_path = os.path.join(self._td.name, "bitget_system_config.sqlite")
        self._patcher = mock.patch.object(cm, "CONFIG_DB_PATH", self._cfg_path)
        self._patcher.start()
        self.addCleanup(self._patcher.stop)
        self.addCleanup(self._td.cleanup)

    def _seed(self, key: str, value) -> None:
        cm.set_config_value(key, value)

    def test_ops_values_write_successfully(self) -> None:
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            cm.set_config_value("DYNAMIC_KELLY_RISK", 0.01)
            cm.set_config_value("MAX_LEVERAGE", 5)
        self.assertAlmostEqual(
            float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.01
        )
        self.assertAlmostEqual(float(cm.get_config_value("MAX_LEVERAGE")), 5.0)

    def test_kelly_in_range_writes_exact_value_not_clamped(self) -> None:
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            cm.set_config_value("DYNAMIC_KELLY_RISK", 0.025)
        self.assertAlmostEqual(
            float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.025
        )

    def test_kelly_out_of_range_rejected_not_clamped(self) -> None:
        self._seed("DYNAMIC_KELLY_RISK", 0.01)
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            cm.set_config_value("DYNAMIC_KELLY_RISK", 0.9)
        self.assertAlmostEqual(
            float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.01
        )

    def test_kelly_below_min_rejected(self) -> None:
        self._seed("DYNAMIC_KELLY_RISK", 0.01)
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            cm.set_config_value("DYNAMIC_KELLY_RISK", 0.001)
        self.assertAlmostEqual(
            float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.01
        )

    def test_max_leverage_out_of_range_rejected(self) -> None:
        self._seed("MAX_LEVERAGE", 5)
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            cm.set_config_value("MAX_LEVERAGE", 25)
        self.assertAlmostEqual(float(cm.get_config_value("MAX_LEVERAGE")), 5.0)

    def test_unbounded_key_passes_without_validation(self) -> None:
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            cm.set_config_value("TREASURY_SPOT_USDT", 12345.67)
        self.assertAlmostEqual(
            float(cm.get_config_value("TREASURY_SPOT_USDT")), 12345.67
        )

    def test_kill_switch_disables_reject_and_restores_clamp(self) -> None:
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=False,
        ):
            cm.set_config_value("DYNAMIC_KELLY_RISK", 0.9)
        self.assertAlmostEqual(
            float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.05
        )

    def test_meta_sync_kelly_reject_no_crash(self) -> None:
        from bitget.governance.meta_sync import sync_config_regime_from_meta

        self._seed("DYNAMIC_KELLY_RISK", 0.01)
        self._seed("CURRENT_REGIME_KEY", "UNKNOWN")
        meta = {
            "META_REGIME_KEY": "BULL",
            "META_REGIME_ACTION": {"kelly_cap": 0.5},
        }
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            out = sync_config_regime_from_meta(meta, force=True)
        self.assertTrue(out.get("synced"))
        self.assertAlmostEqual(
            float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.01
        )


if __name__ == "__main__":
    unittest.main()
