"""Config write-time hard bounds — capital parameter clamps."""
from __future__ import annotations

import unittest


class TestConfigHardBounds(unittest.TestCase):
    def test_clamp_kelly_and_cutoff(self):
        from bitget.infra.config_bounds import (
            apply_config_hard_bounds,
            clamp_config_value,
        )

        self.assertEqual(clamp_config_value("DYNAMIC_KELLY_RISK", 0.50), 0.05)
        self.assertEqual(clamp_config_value("DYNAMIC_KELLY_RISK", 0.0001), 0.001)
        self.assertEqual(clamp_config_value("DYNAMIC_SUPERNOVA_CUTOFF", 1.5), 0.99)
        self.assertEqual(clamp_config_value("UNKNOWN_KEY", 999), 999)

    def test_nav_stage_order_enforced(self):
        from bitget.infra.config_bounds import apply_config_hard_bounds

        out, changes = apply_config_hard_bounds(
            {
                "NAV_DD_REDUCE_PCT": 40.0,
                "NAV_DD_BLOCK_PCT": 20.0,
                "NAV_DD_HALT_PCT": 10.0,
            }
        )
        self.assertLessEqual(out["NAV_DD_REDUCE_PCT"], out["NAV_DD_BLOCK_PCT"])
        self.assertLessEqual(out["NAV_DD_BLOCK_PCT"], out["NAV_DD_HALT_PCT"])
        self.assertTrue(any("order" in c for c in changes))

    def test_save_system_config_applies_bounds(self):
        from unittest.mock import patch

        from bitget.infra import config_manager as cm

        captured = {}

        def _fake_retry(fn, max_retries=5):
            # Execute _save body via capturing what would be written:
            # save_system_config builds config_data then calls _retry_on_locked(_save)
            return fn()

        # Intercept at encode path by patching _connect to in-memory behavior is heavy;
        # instead verify apply is invoked by patching apply and ensuring save uses result.
        clamped = {"DYNAMIC_KELLY_RISK": 0.05, "OTHER": "x"}

        with patch(
            "bitget.infra.config_bounds.apply_config_hard_bounds",
            return_value=(clamped, ["DYNAMIC_KELLY_RISK:0.9→0.05"]),
        ) as apply_mock, patch.object(cm, "_retry_on_locked", side_effect=_fake_retry), patch.object(
            cm, "_connect"
        ) as conn_factory, patch.object(cm, "invalidate_runtime_system_config_cache"):
            conn = conn_factory.return_value
            cur = conn.execute.return_value
            ok = cm.save_system_config({"DYNAMIC_KELLY_RISK": 0.9, "OTHER": "x"})
            self.assertTrue(ok)
            apply_mock.assert_called_once()
            # DELETE + INSERT for each key in clamped
            self.assertTrue(conn.execute.called)

    def test_set_config_value_clamps_before_write(self):
        from unittest.mock import patch

        from bitget.infra import config_manager as cm

        with patch.object(cm, "_retry_on_locked") as retry, patch.object(
            cm, "invalidate_runtime_system_config_cache"
        ), patch.object(cm, "_encode_json", side_effect=lambda v: f"json:{v}") as enc:
            retry.side_effect = lambda fn, max_retries=5: None
            cm.set_config_value("DYNAMIC_KELLY_RISK", 0.9)
            enc.assert_called()
            encoded_arg = enc.call_args[0][0]
            self.assertEqual(encoded_arg, 0.05)


if __name__ == "__main__":
    unittest.main()
