"""ace_exit_bridge — 실전 Hook 배수."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from evolution.ace_exit_bridge import AceExitOverrides, ace_exit_overrides, evolution_live_enabled


class TestAceExitBridge(unittest.TestCase):
    def test_evolution_disabled_when_observe_forced(self) -> None:
        cfg = {
            "ENABLE_ACE_EVOLUTION_WEIGHTING": True,
            "ACE_EVOLUTION_FORCE_OBSERVE": True,
        }
        self.assertFalse(evolution_live_enabled(cfg))

    def test_evolution_live_when_enabled(self) -> None:
        cfg = {
            "ENABLE_ACE_EVOLUTION_WEIGHTING": True,
            "ACE_EVOLUTION_FORCE_OBSERVE": False,
        }
        self.assertTrue(evolution_live_enabled(cfg))

    @patch("evolution.ace_exit_bridge.load_playbook")
    @patch("evolution.ace_exit_bridge.compute_ace_evolution_multiplier")
    def test_active_overrides(self, mock_mult, mock_pb) -> None:
        mock_pb.return_value = {"logic_core": "TEST_LOGIC", "observe_only": False}
        mock_mult.return_value = (
            1.12,
            {"observe_only": False, "clamped_multiplier": 1.12},
        )
        row = {"sig_type": "TEST_LOGIC S1", "sector": "반도체", "dyn_cpv": 0.8}
        cfg = {
            "ENABLE_ACE_EVOLUTION_WEIGHTING": True,
            "ACE_EVOLUTION_FORCE_OBSERVE": False,
        }
        out = ace_exit_overrides(row, "KR", cfg)
        self.assertIsInstance(out, AceExitOverrides)
        self.assertTrue(out.active)
        self.assertGreater(out.min_hold_bars_extra, 0)
        self.assertIn("ACE_EVOL", out.flow_tag)


if __name__ == "__main__":
    unittest.main()
