"""US fluid upstream — anchor / toxic decay / zero-sample smoke tests."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from fluid_time_anchor import FluidAnchorResult
from toxic_decay_bandit import (
    decay_strength,
    enrich_rules_with_decay,
    evaluate_toxic_ml_gate,
    forgiveness_scout_roll,
)


class TestFluidTimeAnchor(unittest.TestCase):
    def test_carry_over_mode_from_lag(self):
        cfg = {"FLUID_US_MAX_CARRY_LAG_DAYS": 3}
        res = FluidAnchorResult(
            market="US",
            mode="carry_over",
            session_date="2026-06-09",
            calendar_today="2026-06-10",
            latest_candle_date="2026-06-09",
            lag_business_days=1,
            reason="us_carry_over",
        )
        self.assertEqual(res.mode, "carry_over")
        self.assertLessEqual(res.lag_business_days, int(cfg["FLUID_US_MAX_CARRY_LAG_DAYS"]))

    def test_us_session_watermark(self):
        res = FluidAnchorResult(
            market="US",
            mode="live",
            session_date="2026-06-10",
            calendar_today="2026-06-10",
            latest_candle_date="2026-06-10",
            lag_business_days=0,
            reason="us_live",
        )
        cfg = {"FLUID_TRACK_SESSION_US": "2026-06-10"}
        self.assertFalse(res.should_increment_bars(cfg))
        self.assertTrue(res.should_increment_bars({}))

    def test_kr_session_watermark(self):
        res = FluidAnchorResult(
            market="KR",
            mode="carry_over",
            session_date="2026-06-20",
            calendar_today="2026-06-21",
            latest_candle_date="2026-06-20",
            lag_business_days=1,
            reason="kr_carry_over",
        )
        cfg = {"FLUID_TRACK_SESSION_KR": "2026-06-20"}
        self.assertFalse(res.should_increment_bars(cfg))
        self.assertTrue(res.should_increment_bars({}))


class TestToxicDecayBandit(unittest.TestCase):
    def test_decay_strength_halves_over_half_life(self):
        bounds = {
            "recorded_at": (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d"),
            "half_life_days": 45.0,
        }
        ds = decay_strength(bounds)
        self.assertGreater(ds, 0.45)
        self.assertLess(ds, 0.55)

    def test_strong_toxic_blocks(self):
        old = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        cfg = {
            "US_TOXIC_ML_ANTIPATTERNS": {
                "TOXIC_PATTERN_X": {
                    "dyn_cpv_min": 0.0,
                    "dyn_cpv_max": 10.0,
                    "dyn_tb_min": 0.0,
                    "dyn_tb_max": 100.0,
                    "v_energy_min": 0.0,
                    "v_energy_max": 10.0,
                    "recorded_at": old,
                    "half_life_days": 90.0,
                }
            }
        }
        gate = evaluate_toxic_ml_gate(cfg, 1.0, 5.0, 1.0, 100.0, market="US")
        self.assertEqual(gate.action, "block")

    def test_enrich_rules_adds_decay_meta(self):
        rules = enrich_rules_with_decay({"TOXIC_PATTERN_A": {"dyn_cpv_max": 1.0}})
        self.assertIn("_decay_strength", rules["TOXIC_PATTERN_A"])
        self.assertIn("recorded_at", rules["TOXIC_PATTERN_A"])

    def test_forgiveness_roll_respects_config(self):
        with patch("toxic_decay_bandit.random.random", return_value=0.0):
            self.assertTrue(forgiveness_scout_roll({"TOXIC_FORGIVENESS_SCOUT_PCT": 0.5}, decay_str=0.1))


class TestZeroSampleSpillover(unittest.TestCase):
    def test_apply_zero_sample_when_no_hot_sample(self):
        from zero_sample_spillover import apply_zero_sample_spillover

        cfg: dict = {}
        with patch(
            "sector_spillover_refresh.refresh_us_spillover_from_db",
            return_value={"reason": "no_hot_sample"},
        ):
            with patch(
                "zero_sample_spillover.infer_dark_horse_sector_from_ohlcv",
                return_value={
                    "ok": True,
                    "sector": "Technology",
                    "sector_std": "Technology",
                    "confidence": 0.72,
                    "method": "sector_aggregate",
                },
            ):
                with patch("zero_sample_spillover.os.path.isfile", return_value=False):
                    with patch("config_manager.save_system_config"):
                        out = apply_zero_sample_spillover(cfg, force_if_closed_zero=True)
        self.assertTrue(out.get("applied"))
        self.assertEqual(cfg.get("US_SPILLOVER_SECTOR"), "Technology")

    def test_run_post_us_incremental_upstream_na_safe(self):
        from evolution.us_fluid_upstream_bridge import run_post_us_incremental_upstream

        with patch("config_manager.load_system_config", return_value={}):
            with patch(
                "zero_sample_spillover.apply_zero_sample_spillover",
                return_value={"applied": True, "sector": "Technology", "reason": "zero_sample_dark_horse"},
            ):
                with patch(
                    "zero_sample_spillover.publish_zero_sample_cross_market",
                    return_value={"mode": "us_online", "us_sector_raw": "Technology"},
                ):
                    with patch("os.path.isfile", return_value=False):
                        out = run_post_us_incremental_upstream(context="daily")
        self.assertTrue(out.get("spillover", {}).get("applied"))


if __name__ == "__main__":
    unittest.main()
