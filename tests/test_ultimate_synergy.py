"""Ultimate Synergy — asymmetric treasury · alpha mining · fast-track promotion."""
from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from meta_governor import resolve_asymmetric_treasury_lookback_days
from strategy_lifecycle_config import market_params, load_strategy_lifecycle_config
from strategy_promotion_engine import (
    is_fast_track_group,
    is_group_live_in_registry,
    passes_hard_threshold_auto_promotion,
    run_registry_lifecycle,
)


class TestAsymmetricTreasuryMemory(unittest.TestCase):
    def test_bear_short_window(self):
        days, reason = resolve_asymmetric_treasury_lookback_days("BEAR", base_days=90)
        self.assertGreaterEqual(days, 15)
        self.assertLessEqual(days, 20)
        self.assertIn("BEAR", reason)

    def test_high_vol_short_window(self):
        days, _ = resolve_asymmetric_treasury_lookback_days("HIGH_VOL")
        self.assertLessEqual(days, 20)

    def test_bull_extended_window(self):
        days, reason = resolve_asymmetric_treasury_lookback_days("BULL", base_days=90)
        self.assertGreaterEqual(days, 90)
        self.assertLessEqual(days, 120)
        self.assertIn("BULL", reason)


class TestAlphaMiningOrchestrator(unittest.TestCase):
    def test_lock_prevents_double_acquire(self):
        from alpha_mining_orchestrator import acquire_orchestrator_lock, release_orchestrator_lock

        with tempfile.TemporaryDirectory() as td:
            lock = os.path.join(td, ".lock")
            with patch("alpha_mining_orchestrator._LOCK_PATH", lock):
                self.assertTrue(acquire_orchestrator_lock())
                self.assertFalse(acquire_orchestrator_lock())
                release_orchestrator_lock()

    def test_spawn_returns_background_mode(self):
        from alpha_mining_orchestrator import spawn_weekly_alpha_mining

        with tempfile.TemporaryDirectory() as td:
            lock = os.path.join(td, ".lock")
            log_dir = os.path.join(td, "logs")
            os.makedirs(log_dir, exist_ok=True)
            with patch("alpha_mining_orchestrator._LOCK_PATH", lock):
                with patch("alpha_mining_orchestrator._LOG_DIR", log_dir):
                    with patch("alpha_mining_orchestrator.subprocess.Popen") as popen:
                        popen.return_value.pid = 99999
                        out = spawn_weekly_alpha_mining(tag="test_spawn")
            self.assertTrue(out.get("ok"))
            self.assertEqual(out.get("mode"), "background")


class TestFastTrackPromotion(unittest.TestCase):
    def test_fast_track_group_detection(self):
        self.assertTrue(is_fast_track_group("INCUBATOR_MUTANT_A"))
        self.assertTrue(is_fast_track_group("ACE_PLAYBOOK_X"))
        self.assertFalse(is_fast_track_group("SUPERNOVA_S4"))

    def test_pf_gate(self):
        mp = market_params(load_strategy_lifecycle_config(), "KR")
        hv_ok = {"n": 12, "rolling_pf": 2.1, "mult": 1.0}
        hv_fail = {"n": 12, "rolling_pf": 1.5, "mult": 1.0}
        self.assertTrue(passes_hard_threshold_auto_promotion(hv_ok, mp))
        self.assertFalse(passes_hard_threshold_auto_promotion(hv_fail, mp))

    def test_incubator_promoted_to_live(self):
        from strategy_promotion_engine import stable_strategy_id

        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "m.sqlite")
            open(db, "a").close()
            gk = "INCUBATOR_ALPHA_X"
            sid = stable_strategy_id("KR", gk)
            health = {
                "__meta__": {"window_days_kst": 18},
                "KR|INCUBATOR_ALPHA_X": {
                    "n": 14,
                    "rolling_wr": 0.40,
                    "rolling_pf": 2.5,
                    "mdd_pct": -8,
                    "mult": 1.0,
                },
            }
            prior = [
                {
                    "strategy_id": sid,
                    "market": "KR",
                    "group_key": gk,
                    "state": "CANDIDATE",
                    "capital_mult": 0.0,
                }
            ]
            reg, stats = run_registry_lifecycle(
                prior_registry=prior,
                health=health,
                forward_db_path=db,
                now=datetime(2026, 6, 11, tzinfo=timezone.utc),
            )
            self.assertGreaterEqual(stats.get("fast_track_promoted", 0), 1)
            row = next(r for r in reg if r.get("group_key") == gk)
            self.assertEqual(str(row.get("state")).upper(), "LIVE")
            self.assertEqual(row.get("promote_reason"), "fast_track_pf2")

    def test_registry_live_lookup(self):
        meta = {
            "META_STRATEGY_REGISTRY": [
                {
                    "strategy_id": "s1",
                    "market": "US",
                    "group_key": "ACE_CORE",
                    "state": "LIVE",
                }
            ]
        }
        self.assertTrue(is_group_live_in_registry(meta, "US", "ACE_CORE"))
        self.assertFalse(is_group_live_in_registry(meta, "US", "OTHER"))


if __name__ == "__main__":
    unittest.main()
