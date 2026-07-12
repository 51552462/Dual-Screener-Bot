"""Re-Evolution Z-Score EV Verification (2번) — 섀도우 분포 통계 정합성 테스트."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from re_evolution_ev_rampup import (
    ev_rampup_config,
    process_warm_start_live_closure,
    should_trigger_kill_switch,
)
from re_evolution_redemption_gate import compute_shadow_stats
from re_evolution_warm_start import WARM_START_PHASE, apply_warm_start_meta_on_redemption
from re_evolution_zscore_ev import (
    DEFAULT_KILL_Z_FLOOR,
    DEFAULT_RAMP_Z_FLOOR,
    compute_shadow_return_distribution,
    compute_z_score,
    enrich_ev_ramp_config_with_zscore,
    evaluate_combined_live_ev_verification,
    evaluate_zscore_ramp_alignment,
    should_trigger_zscore_kill,
    zscore_ev_config,
)


def _zscore_cfg() -> dict:
    base = ev_rampup_config({})
    return enrich_ev_ramp_config_with_zscore(base, sys_config={})


def _warm_meta(
    *,
    shadow_ev: float = 2.0,
    shadow_std: float = 1.0,
) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "META_RE_EVOLUTION_WARM_START": {
            "KR|RANK_A": {
                "strategy_id": "strat:abc",
                "market": "KR",
                "group_key": "RANK_A",
                "phase": WARM_START_PHASE,
                "kelly_mult": 0.4,
                "shadow_ev_avg_ret_pct": shadow_ev,
                "shadow_ev_std_ret_pct": shadow_std,
                "shadow_ev_n_closed": 12,
                "redeemed_at": now_iso,
                "live_closure_rets": [],
                "live_closures": 0,
            }
        },
        "META_RE_EVOLUTION_KELLY_OVERLAY": {"RANK_A": 0.4},
        "META_DEATHMATCH_KELLY_OVERLAY": {},
        "META_STRATEGY_HEALTH": {},
        "META_RE_EVOLUTION_SHADOW_GROUPS": [],
        "META_RE_EVOLUTION_DEMOTED": [],
    }


class TestZScoreConfig(unittest.TestCase):
    def test_defaults(self):
        cfg = zscore_ev_config({})
        self.assertTrue(cfg["enabled"])
        self.assertEqual(cfg["ramp_z_floor"], DEFAULT_RAMP_Z_FLOOR)
        self.assertEqual(cfg["kill_z_floor"], DEFAULT_KILL_Z_FLOOR)


class TestShadowDistribution(unittest.TestCase):
    def test_multi_sample_std(self):
        rets = [3.0, 2.0, -1.0, 4.0, 1.5]
        dist = compute_shadow_return_distribution(rets)
        self.assertEqual(dist["n"], 5)
        self.assertAlmostEqual(dist["mean_pct"], 1.9, places=2)
        self.assertGreater(dist["std_pct"], 0.0)
        self.assertGreaterEqual(dist["effective_std_pct"], 0.35)

    def test_single_sample_floor_std(self):
        dist = compute_shadow_return_distribution([2.4])
        self.assertEqual(dist["n"], 1)
        self.assertEqual(dist["mean_pct"], 2.4)
        self.assertGreaterEqual(dist["effective_std_pct"], 0.35)

    def test_compute_shadow_stats_includes_std(self):
        rows = [{"final_ret": r} for r in [3.0, 2.0, -1.0, 4.0, 1.5]]
        stats = compute_shadow_stats(rows)
        self.assertIn("std_ret_pct", stats)
        self.assertIn("effective_std_ret_pct", stats)
        self.assertGreater(stats["effective_std_ret_pct"], 0.0)


class TestZScoreMath(unittest.TestCase):
    def test_at_mean_is_zero(self):
        z = compute_z_score(2.0, 2.0, 1.0)
        self.assertEqual(z, 0.0)

    def test_minus_one_point_five_sigma(self):
        # mean=2, std=1 → -1.5σ = 0.5%
        z = compute_z_score(0.5, 2.0, 1.0)
        self.assertAlmostEqual(z, -1.5, places=4)

    def test_minus_two_sigma(self):
        z = compute_z_score(0.0, 2.0, 1.0)
        self.assertAlmostEqual(z, -2.0, places=4)


class TestZScoreKill(unittest.TestCase):
    def test_kill_at_minus_two_sigma(self):
        cfg = _zscore_cfg()
        kill, reason, detail = should_trigger_zscore_kill(0.0, 2.0, 1.0, cfg)
        self.assertTrue(kill)
        self.assertIn("z_score", reason)
        self.assertAlmostEqual(detail["z_score"], -2.0, places=2)

    def test_no_kill_within_band(self):
        cfg = _zscore_cfg()
        kill, _, _ = should_trigger_zscore_kill(0.6, 2.0, 1.0, cfg)
        self.assertFalse(kill)

    def test_kill_switch_integration(self):
        cfg = _zscore_cfg()
        ok, reason = should_trigger_kill_switch(
            0.0, 2.0, cfg, shadow_std_pct=1.0
        )
        self.assertTrue(ok)
        self.assertIn("z_score", reason)


class TestZScoreRamp(unittest.TestCase):
    def test_single_trade_at_ramp_floor(self):
        cfg = _zscore_cfg()
        ok, detail = evaluate_zscore_ramp_alignment([0.5], 2.0, 1.0, cfg)
        self.assertTrue(ok)
        self.assertEqual(detail["reason"], "single_trade_within_z_band")
        self.assertAlmostEqual(detail["per_trade_z"][0], -1.5, places=2)

    def test_second_trade_rescues_after_first_miss(self):
        cfg = _zscore_cfg()
        # 1차: z=-2.5 fail, 2차: z=-1.0 pass → 단일 청산 경로로 ramp
        ok, detail = evaluate_zscore_ramp_alignment([-3.0, 1.0], 4.0, 2.0, cfg)
        self.assertTrue(ok)
        self.assertEqual(detail["reason"], "single_trade_within_z_band")
        self.assertTrue(detail["per_trade_z_ok"][1])

    def test_mismatch_beyond_ramp_band(self):
        cfg = _zscore_cfg()
        ok, detail = evaluate_zscore_ramp_alignment([-1.5], 2.0, 1.0, cfg)
        self.assertFalse(ok)
        self.assertEqual(detail["reason"], "z_score_mismatch")


class TestCombinedVerification(unittest.TestCase):
    def test_tolerance_pass_without_z(self):
        cfg = _zscore_cfg()
        cfg["slippage_tolerance_pct"] = 2.5
        ok, detail = evaluate_combined_live_ev_verification(
            [2.1], 2.0, 0.3, cfg
        )
        self.assertTrue(ok)
        self.assertIn(
            detail["reason"],
            ("single_trade_within_slippage", "tolerance_and_zscore"),
        )

    def test_zscore_pass_when_tolerance_fails(self):
        cfg = _zscore_cfg()
        cfg["slippage_tolerance_pct"] = 2.5
        # |2.0 - 5.0| = 3.0 > 2.5 tolerance fail, z = -1.5 pass
        ok, detail = evaluate_combined_live_ev_verification(
            [2.0], 5.0, 2.0, cfg
        )
        self.assertTrue(ok)
        self.assertEqual(detail["reason"], "single_trade_within_z_band")

    def test_both_fail(self):
        cfg = _zscore_cfg()
        cfg["slippage_tolerance_pct"] = 1.0
        ok, detail = evaluate_combined_live_ev_verification(
            [-3.0], 2.0, 0.5, cfg
        )
        self.assertFalse(ok)
        self.assertEqual(detail["reason"], "ev_and_zscore_mismatch")


class TestKrUsScenarios(unittest.TestCase):
    """KR/US 수학 시나리오 — ATR tolerance vs Z-Score 상호작용."""

    def test_kr_sideways_z_rescues_narrow_atr(self):
        """KR SIDEWAYS: ATR tolerance 1.2% 좁음 → Z-Score로 ramp."""
        cfg = _zscore_cfg()
        cfg["slippage_tolerance_pct"] = 1.2
        # mean=2.5%, live=1.0% → |Δ|=1.5% > tol, z=-1.5σ 통과
        ok, detail = evaluate_combined_live_ev_verification(
            [1.0], 2.5, 1.0, cfg
        )
        self.assertTrue(ok)
        self.assertEqual(detail["zscore_gate"]["reason"], "single_trade_within_z_band")
        self.assertFalse(detail["tolerance_gate"]["match"])

    def test_us_high_vol_tolerance_covers_z_miss(self):
        """US HIGH_VOL: ATR tolerance 3% 넓음 → tolerance만으로 ramp."""
        cfg = _zscore_cfg()
        cfg["slippage_tolerance_pct"] = 3.0
        ok, detail = evaluate_combined_live_ev_verification(
            [4.5], 2.0, 0.4, cfg
        )
        self.assertTrue(ok)
        self.assertEqual(detail["tolerance_gate"]["reason"], "single_trade_within_slippage")

    def test_us_kill_zscore_before_divergence_floor(self):
        """US: z=-2.2σ 킬 — divergence floor(-5.5%)보다 먼저 발동."""
        cfg = _zscore_cfg()
        ok, reason = should_trigger_kill_switch(
            0.9, 2.0, cfg, shadow_std_pct=0.5
        )
        self.assertTrue(ok)
        self.assertIn("z_score", reason)


class TestWarmStartSnapshot(unittest.TestCase):
    def test_redemption_stores_shadow_std(self):
        meta: dict = {}
        stats = compute_shadow_stats(
            [{"final_ret": r} for r in [2.0, 3.0, 1.0, -0.5, 2.5]]
        )
        rec = apply_warm_start_meta_on_redemption(
            meta,
            market="KR",
            group_key="RANK_A",
            strategy_id="strat:abc",
            shadow_stats=stats,
        )
        self.assertIsNotNone(rec.get("shadow_ev_std_ret_pct"))
        self.assertGreater(float(rec["shadow_ev_std_ret_pct"]), 0.0)
        self.assertEqual(rec["shadow_ev_avg_ret_pct"], stats["avg_ret_pct"])


class TestPromotionEngineBridge(unittest.TestCase):
    @patch("re_evolution_dynamic_tolerance.compute_dynamic_ev_tolerance_pct")
    def test_evaluate_live_ev_performance_verification(self, mock_dyn):
        from strategy_promotion_engine import evaluate_live_ev_performance_verification

        mock_dyn.return_value = {
            "tolerance_pct": 2.5,
            "regime_key": "SIDEWAYS",
            "source": "test",
        }
        warm = {
            "shadow_ev_avg_ret_pct": 2.0,
            "shadow_ev_std_ret_pct": 1.0,
            "shadow_ev_n_closed": 10,
        }
        out = evaluate_live_ev_performance_verification(
            [0.5], warm, market="KR", meta={}
        )
        self.assertTrue(out["match"])
        self.assertIn(
            out["detail"]["reason"],
            ("single_trade_within_z_band", "tolerance_and_zscore"),
        )


class TestProcessClosureZScore(unittest.TestCase):
    def _patch_dynamic_tol(self, tolerance_pct: float = 1.0):
        return patch(
            "re_evolution_dynamic_tolerance.compute_dynamic_ev_tolerance_pct",
            return_value={
                "tolerance_pct": tolerance_pct,
                "regime_key": "SIDEWAYS",
                "regime_weight": 0.8,
                "atr_pct": 1.25,
                "source": "atr_x_regime",
            },
        )

    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_zscore_ramp_when_tolerance_too_narrow(
        self, mock_load, _save, _inv
    ):
        mock_load.return_value = _warm_meta(shadow_ev=5.0, shadow_std=2.0)
        with self._patch_dynamic_tol(1.0):
            out = process_warm_start_live_closure(
                market="KR",
                sig_type="[STANDARD] RANK_A",
                final_ret_pct=2.0,
                sim_kelly_invest=40_000.0,
            )
        self.assertEqual(out["action"], "full_ramp")
        self.assertEqual(
            out["match_detail"]["zscore_gate"]["reason"],
            "single_trade_within_z_band",
        )

    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_zscore_kill_recall(self, mock_load, _save, _inv):
        mock_load.return_value = _warm_meta(shadow_ev=2.0, shadow_std=0.5)
        with self._patch_dynamic_tol(2.5):
            out = process_warm_start_live_closure(
                market="KR",
                sig_type="[STANDARD] RANK_A",
                final_ret_pct=0.8,
                sim_kelly_invest=40_000.0,
            )
        self.assertEqual(out["action"], "shadow_recall")
        self.assertIn("z_score", out.get("reason", ""))


if __name__ == "__main__":
    unittest.main()
