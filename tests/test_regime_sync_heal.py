"""Regime sync, Kelly fail-safe, self-heal streak."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from meta_state_store import (
    ensure_config_regime_aligned,
    is_config_regime_misaligned,
    sync_config_regime_from_meta,
)
from regime_kelly_failsafe import resolve_graceful_base_kelly
from regime_self_heal import is_regime_misaligned, tick_regime_mismatch


class TestRegimeSync(unittest.TestCase):
    def test_misaligned_when_config_unknown_meta_bull(self) -> None:
        meta = {"META_REGIME_KEY": "BULL", "META_REGIME_ACTION": {"kelly_cap": 0.028}}
        cfg = {
            "REGIME_ANALYSIS": {"regime_key": "UNKNOWN"},
            "CURRENT_REGIME_KEY": "UNKNOWN",
        }
        self.assertTrue(is_config_regime_misaligned(meta, cfg))

    @patch("config_manager.set_config_value")
    @patch("config_manager.get_config_value")
    @patch("config_manager.invalidate_runtime_system_config_cache")
    def test_sync_writes_both_keys(
        self,
        _inv: MagicMock,
        get_val: MagicMock,
        set_val: MagicMock,
    ) -> None:
        get_val.side_effect = lambda k, default=None: (
            {"regime_key": "UNKNOWN"}
            if k == "REGIME_ANALYSIS"
            else "UNKNOWN"
        )
        meta = {
            "META_REGIME_KEY": "BULL",
            "META_REGIME_CONFIDENCE": 0.8,
            "META_REGIME_ACTION": {"notes": "test"},
        }
        out = sync_config_regime_from_meta(meta, force=True)
        self.assertTrue(out["synced"])
        self.assertEqual(out["regime"], "BULL")
        keys = [c[0][0] for c in set_val.call_args_list]
        self.assertIn("REGIME_ANALYSIS", keys)
        self.assertIn("CURRENT_REGIME_KEY", keys)


class TestKellyFailsafe(unittest.TestCase):
    def test_meta_led_when_config_unknown(self) -> None:
        meta = {"META_REGIME_KEY": "BULL"}
        cfg = {
            "DYNAMIC_KELLY_RISK": 0.01,
            "REGIME_ANALYSIS": {"regime_key": "UNKNOWN"},
            "CURRENT_REGIME_KEY": "UNKNOWN",
        }
        kelly, reason = resolve_graceful_base_kelly(cfg, meta, config_regime_unknown=True)
        self.assertGreater(kelly, 0.01)
        self.assertEqual(reason, "meta_led_config_unknown")

    def test_ma_fallback_from_snapshots(self) -> None:
        cfg = {
            "DYNAMIC_KELLY_RISK": 0.01,
            "REGIME_KELLY_SNAPSHOT": [
                {"date": "2026-05-17", "effective_kelly": 0.018, "regime_key": "BULL"},
                {"date": "2026-05-18", "effective_kelly": 0.016, "regime_key": "BULL"},
            ],
        }
        kelly, reason = resolve_graceful_base_kelly(cfg, None, config_regime_unknown=True)
        self.assertGreaterEqual(kelly, 0.015)
        self.assertEqual(reason, "kelly_ma_fallback")


class TestSelfHeal(unittest.TestCase):
    def test_misaligned_detection(self) -> None:
        meta = {"META_REGIME_KEY": "BULL"}
        cfg = {"CURRENT_REGIME_KEY": "UNKNOWN", "REGIME_ANALYSIS": {"regime_key": "UNKNOWN"}}
        self.assertTrue(is_regime_misaligned(meta, cfg))

    @patch("regime_self_heal.schedule_background_meta_rebuild")
    @patch("regime_self_heal._load_heal_state")
    @patch("regime_self_heal._save_heal_state")
    def test_tick_triggers_rebuild_at_threshold(
        self,
        _save: MagicMock,
        load_st: MagicMock,
        sched: MagicMock,
    ) -> None:
        load_st.return_value = {"mismatch_streak": 2}
        sched.return_value = True
        meta = {"META_REGIME_KEY": "BULL"}
        cfg = {"CURRENT_REGIME_KEY": "UNKNOWN", "REGIME_ANALYSIS": {"regime_key": "UNKNOWN"}}
        out = tick_regime_mismatch(meta, cfg, threshold=3, auto_rebuild=True)
        self.assertEqual(out["mismatch_streak"], 3)
        self.assertTrue(out["rebuild_scheduled"])


if __name__ == "__main__":
    unittest.main()
