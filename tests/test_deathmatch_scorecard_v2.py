"""Composite v2 — 지수 MDD · 절대 허들."""
from __future__ import annotations

import math
import unittest

from deathmatch_config import load_deathmatch_config, market_deathmatch_params
from deathmatch_scorecard import (
    ArmScorecard,
    compute_composite_v2,
    exponential_mdd_penalty,
    passes_absolute_hurdle,
)


class TestExponentialMdd(unittest.TestCase):
    def test_zero_below_threshold(self):
        p = exponential_mdd_penalty(-10.0, threshold_pct=-15.0, scale=5.0, exp_base=1.45)
        self.assertEqual(p, 0.0)

    def test_grows_nonlinear(self):
        p_shallow = exponential_mdd_penalty(-16.0, threshold_pct=-15.0, scale=5.0, exp_base=1.45)
        p_deep = exponential_mdd_penalty(-30.0, threshold_pct=-15.0, scale=5.0, exp_base=1.45)
        self.assertGreater(p_deep, p_shallow)
        self.assertGreater(p_deep, 1.0)


class TestAbsoluteHurdle(unittest.TestCase):
    def test_positive_passes(self):
        ok, _ = passes_absolute_hurdle(0.5, -2.0, min_ret=0.0, outperform_buffer_pp=0.25)
        self.assertTrue(ok)

    def test_relative_passes_in_crash(self):
        ok, reason = passes_absolute_hurdle(-0.5, -2.0, min_ret=0.0, outperform_buffer_pp=0.25)
        self.assertTrue(ok)
        self.assertEqual(reason, "relative_outperform")

    def test_fail_both(self):
        ok, _ = passes_absolute_hurdle(-2.0, -1.0, min_ret=0.0, outperform_buffer_pp=0.25)
        self.assertFalse(ok)


class TestCompositeV2(unittest.TestCase):
    def test_hurdle_fail_not_champion_eligible(self):
        cfg = market_deathmatch_params(load_deathmatch_config({}), "KR")
        good = ArmScorecard(
            arm_id="g",
            label="Good",
            group_key="G",
            n_valid=10,
            mean_ret=1.0,
            win_rate_pct=55.0,
            profit_factor=1.4,
            expectancy=0.8,
            mdd_pct=-8.0,
            meta_mult=1.0,
        )
        bad = ArmScorecard(
            arm_id="b",
            label="Bad",
            group_key="B",
            n_valid=10,
            mean_ret=-0.5,
            win_rate_pct=48.0,
            profit_factor=1.2,
            expectancy=-0.2,
            mdd_pct=-20.0,
            meta_mult=1.0,
        )
        compute_composite_v2([good, bad], cfg, market_benchmark=0.0)
        self.assertTrue(good.hurdle_passed)
        self.assertTrue(good.champion_eligible)
        self.assertFalse(bad.hurdle_passed)
        self.assertFalse(bad.champion_eligible)
        self.assertGreater(good.composite_score, bad.composite_score)

    def test_deep_mdd_hurts_more_than_linear(self):
        cfg = market_deathmatch_params(load_deathmatch_config({}), "KR")
        a = ArmScorecard(
            arm_id="a",
            label="A",
            group_key="A",
            n_valid=10,
            mean_ret=2.0,
            win_rate_pct=55.0,
            profit_factor=1.5,
            mdd_pct=-16.0,
            meta_mult=1.0,
        )
        b = ArmScorecard(
            arm_id="b",
            label="B",
            group_key="B",
            n_valid=10,
            mean_ret=2.0,
            win_rate_pct=55.0,
            profit_factor=1.5,
            mdd_pct=-35.0,
            meta_mult=1.0,
        )
        compute_composite_v2([a, b], cfg, market_benchmark=0.0)
        self.assertGreater(a.composite_score, b.composite_score)


if __name__ == "__main__":
    unittest.main()
