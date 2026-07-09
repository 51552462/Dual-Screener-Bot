"""Re-Evolution Phase 3 — Redemption & Promotion 테스트."""
from __future__ import annotations

import copy
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from re_evolution_redemption_gate import (
    apply_redemption_meta_updates,
    compute_shadow_stats,
    is_re_evolution_observing_row,
    passes_redemption_gate,
    re_evolution_redemption_config,
    resolve_dynamic_shadow_verification_window,
    restore_redemption_capital_overlay,
    try_promote_re_evolution_redemption,
)
from strategy_lifecycle_config import (
    apply_regime_time_dilation,
    compute_dynamic_shadow_base_window,
    compute_dynamic_shadow_verification_window,
    regime_time_dilation_factor,
    resolve_effective_regime_key,
)


def _cfg(**overrides: object) -> dict:
    base = re_evolution_redemption_config(
        {
            "RE_EVOLUTION_REDEMPTION_MIN_TRADES": 5,
            "RE_EVOLUTION_REDEMPTION_MIN_WR": 0.52,
            "RE_EVOLUTION_REDEMPTION_MIN_PF": 1.25,
            "RE_EVOLUTION_REDEMPTION_MIN_ALPHA_PCT": 1.0,
        }
    )
    base.update(overrides)
    if "min_samples_regime" in overrides:
        redeem_min = int(base.get("min_trades") or 5)
        regime_min = int(overrides["min_samples_regime"])
        base["effective_min_trades"] = max(redeem_min, regime_min)
    return base


def _passing_stats(n: int = 16) -> dict:
    return {
        "n_closed": n,
        "win_rate": 0.67,
        "profit_factor": 1.8,
        "avg_ret_pct": 2.5,
    }


class TestDynamicShadowBaseWindow(unittest.TestCase):
    """1번 — alpha_half_life 연동 Base Window."""

    def test_kr_fresh_demotion_70pct(self):
        w = compute_dynamic_shadow_base_window(
            "KR",
            shadow_days_elapsed=0,
        )
        self.assertEqual(w["alpha_half_life_days"], 10)
        self.assertEqual(w["base_window_days"], 7)
        self.assertAlmostEqual(w["window_ratio"], 0.70, places=2)

    def test_kr_full_tenure_100pct(self):
        w = compute_dynamic_shadow_base_window(
            "KR",
            shadow_days_elapsed=10,
        )
        self.assertEqual(w["base_window_days"], 10)
        self.assertAlmostEqual(w["window_ratio"], 1.0, places=2)

    def test_us_fresh_demotion_70pct(self):
        w = compute_dynamic_shadow_base_window(
            "US",
            shadow_days_elapsed=0,
        )
        self.assertEqual(w["alpha_half_life_days"], 30)
        self.assertEqual(w["base_window_days"], 21)
        self.assertAlmostEqual(w["window_ratio"], 0.70, places=2)

    def test_us_full_tenure_100pct(self):
        w = compute_dynamic_shadow_base_window(
            "US",
            shadow_days_elapsed=30,
        )
        self.assertEqual(w["base_window_days"], 30)
        self.assertAlmostEqual(w["window_ratio"], 1.0, places=2)

    def test_mid_tenure_interpolates(self):
        w = compute_dynamic_shadow_base_window(
            "KR",
            shadow_days_elapsed=5,
        )
        # 5/10 tenure -> ratio 0.85 -> 10*0.85 = 8.5 -> round 8
        self.assertEqual(w["base_window_days"], 8)
        self.assertAlmostEqual(w["window_ratio"], 0.85, places=2)

    def test_resolve_from_registry_demoted_at(self):
        row = {
            "market": "KR",
            "group_key": "RANK_A",
            "last_demoted_at": "2026-07-01T00:00:00+00:00",
        }
        w = resolve_dynamic_shadow_verification_window(
            "KR",
            row=row,
            now=datetime.fromisoformat("2026-07-06T00:00:00+00:00"),
        )
        self.assertEqual(w["shadow_days_elapsed"], 5)
        self.assertEqual(w["base_window_days"], 8)


class TestRegimeTimeDilation(unittest.TestCase):
    """2번 — 국면별 시간 압축 (Regime Time Dilation)."""

    def test_high_vol_compress_half(self):
        dil = regime_time_dilation_factor("HIGH_VOL")
        self.assertEqual(dil["dilation_factor"], 0.5)
        self.assertEqual(dil["dilation_mode"], "compress_high_density")

    def test_bear_panic_raw_key_compress(self):
        dil = regime_time_dilation_factor("BEAR_PANIC", raw_regime_key="BEAR_PANIC")
        self.assertEqual(dil["dilation_factor"], 0.5)

    def test_bear_panic_from_satellite_meta(self):
        meta = {
            "META_REGIME_KEY": "BEAR",
            "META_SATELLITE_INTEL": {"bear_phase": "BEAR_PANIC"},
        }
        dil = regime_time_dilation_factor("BEAR", meta=meta)
        self.assertEqual(dil["dilation_factor"], 0.5)
        self.assertEqual(dil["bear_subphase"], "BEAR_PANIC")

    def test_sideways_neutral(self):
        dil = regime_time_dilation_factor("SIDEWAYS")
        self.assertEqual(dil["dilation_factor"], 1.0)
        self.assertEqual(dil["dilation_mode"], "neutral_sideways")

    def test_chop_maps_sideways_neutral(self):
        rk, raw = resolve_effective_regime_key({"META_REGIME_KEY": "CHOP"})
        self.assertEqual(rk, "SIDEWAYS")
        dil = regime_time_dilation_factor(rk, raw_regime_key=raw)
        self.assertEqual(dil["dilation_factor"], 1.0)

    def test_kr_high_vol_window_compressed(self):
        w = compute_dynamic_shadow_verification_window(
            "KR",
            shadow_days_elapsed=0,
            meta={"META_REGIME_KEY": "HIGH_VOL"},
        )
        self.assertEqual(w["base_window_days"], 7)
        self.assertEqual(w["final_window_days"], 4)
        self.assertEqual(w["verification_window_days"], 4)
        self.assertEqual(w["dilation_mode"], "compress_high_density")

    def test_kr_sideways_window_unchanged(self):
        w = compute_dynamic_shadow_verification_window(
            "KR",
            shadow_days_elapsed=0,
            meta={"META_REGIME_KEY": "SIDEWAYS"},
        )
        self.assertEqual(w["base_window_days"], 7)
        self.assertEqual(w["final_window_days"], 7)

    def test_us_high_vol_mature_compressed(self):
        w = compute_dynamic_shadow_verification_window(
            "US",
            shadow_days_elapsed=30,
            meta={"META_REGIME_KEY": "HIGH_VOL"},
        )
        self.assertEqual(w["base_window_days"], 30)
        self.assertEqual(w["final_window_days"], 15)

    def test_resolve_regime_fallback_treasury_asymmetric(self):
        meta = {
            "META_TREASURY_ASYMMETRIC_WINDOW": {
                "regime_key": "HIGH_VOL",
                "lookback_days": 18,
            }
        }
        rk, raw = resolve_effective_regime_key(meta)
        self.assertEqual(rk, "HIGH_VOL")
        self.assertEqual(raw, "HIGH_VOL")

    def test_redemption_gate_uses_dilated_window(self):
        row = {
            "market": "KR",
            "group_key": "RANK_A",
            "last_demoted_at": "2026-07-01T00:00:00+00:00",
        }
        w = resolve_dynamic_shadow_verification_window(
            "KR",
            row=row,
            meta={"META_REGIME_KEY": "HIGH_VOL"},
            now=datetime.fromisoformat("2026-07-06T00:00:00+00:00"),
        )
        self.assertEqual(w["base_window_days"], 8)
        self.assertEqual(w["final_window_days"], 4)


class TestObservingRowDetection(unittest.TestCase):
    def test_strike_demoted(self):
        row = {
            "state": "OBSERVING",
            "demote_reason": "re_evolution_3_strike(x3)",
        }
        self.assertTrue(is_re_evolution_observing_row(row))

    def test_health_discovery_skip(self):
        row = {
            "state": "OBSERVING",
            "source": "health_discovery",
        }
        self.assertFalse(is_re_evolution_observing_row(row))


class TestShadowStats(unittest.TestCase):
    def test_wr_pf(self):
        rows = [
            {"final_ret": 3.0},
            {"final_ret": 2.0},
            {"final_ret": -1.0},
            {"final_ret": 4.0},
            {"final_ret": 1.5},
        ]
        s = compute_shadow_stats(rows)
        self.assertEqual(s["n_closed"], 5)
        self.assertEqual(s["win_rate"], 0.8)
        self.assertGreater(s["profit_factor"], 1.25)


class TestRedemptionGate(unittest.TestCase):
    def test_pass(self):
        ok, detail = passes_redemption_gate(_passing_stats(), 0.5, _cfg())
        self.assertTrue(ok)
        self.assertTrue(detail.get("pass"))
        self.assertIn("regime_min_samples", detail.get("gates_passed", []))

    def test_fail_alpha(self):
        stats = {**_passing_stats(), "avg_ret_pct": 1.2}
        ok, detail = passes_redemption_gate(stats, 0.5, _cfg())
        self.assertFalse(ok)
        self.assertEqual(detail.get("fail"), "alpha_excess")

    def test_fail_regime_min_samples_even_if_quality_ok(self):
        stats = {
            "n_closed": 10,
            "win_rate": 0.90,
            "profit_factor": 3.0,
            "avg_ret_pct": 5.0,
        }
        ok, detail = passes_redemption_gate(
            stats,
            0.0,
            _cfg(),
            verification_window={"verification_window_days": 30, "regime_key": "SIDEWAYS"},
        )
        self.assertFalse(ok)
        self.assertEqual(detail.get("fail"), "regime_min_samples")
        self.assertEqual(detail.get("min_samples_regime"), 15)
        self.assertEqual(detail.get("effective_min_trades"), 15)

    def test_fail_trades_below_regime_floor(self):
        stats = {
            "n_closed": 2,
            "win_rate": 1.0,
            "profit_factor": 3.0,
            "avg_ret_pct": 5.0,
        }
        ok, detail = passes_redemption_gate(stats, 0.0, _cfg())
        self.assertFalse(ok)
        self.assertEqual(detail.get("fail"), "regime_min_samples")

    def test_fail_benchmark_missing(self):
        ok, detail = passes_redemption_gate(_passing_stats(), None, _cfg())
        self.assertFalse(ok)
        self.assertEqual(detail.get("fail"), "benchmark_unavailable")


class TestRegimeMinSamplesIntegration(unittest.TestCase):
    """3번 — crossmatrix MIN_SAMPLES 통계적 유의성 교차 검증."""

    def test_config_resolves_default_min_samples(self):
        cfg = re_evolution_redemption_config({})
        self.assertEqual(cfg["min_samples_regime"], 15)
        self.assertEqual(cfg["effective_min_trades"], 15)

    def test_config_override_regime_xrank_min_samples(self):
        cfg = re_evolution_redemption_config({"REGIME_XRANK_MIN_SAMPLES": 20})
        self.assertEqual(cfg["min_samples_regime"], 20)
        self.assertEqual(cfg["effective_min_trades"], 20)

    def test_six_trades_blocked_despite_soft_min_five(self):
        stats = {
            "n_closed": 6,
            "win_rate": 0.67,
            "profit_factor": 1.8,
            "avg_ret_pct": 2.5,
        }
        ok, detail = passes_redemption_gate(stats, 0.5, _cfg())
        self.assertFalse(ok)
        self.assertEqual(detail.get("fail"), "regime_min_samples")

    @patch("re_evolution_redemption_gate.fetch_benchmark_return_pct")
    @patch("re_evolution_redemption_gate.fetch_shadow_closed_rows")
    def test_evaluate_blocks_low_sample_in_long_window(
        self, mock_rows, mock_bench
    ):
        from re_evolution_redemption_gate import evaluate_shadow_redemption

        mock_rows.return_value = [{"final_ret": 3.0}] * 8
        mock_bench.return_value = 0.5
        out = evaluate_shadow_redemption(
            market="US",
            group_key="RANK_A",
            sys_config={},
            meta={"META_REGIME_KEY": "SIDEWAYS"},
        )
        self.assertFalse(out.get("passes"))
        self.assertEqual(out["gate_detail"].get("fail"), "regime_min_samples")
        self.assertGreaterEqual(
            int(out["verification_window"]["verification_window_days"]),
            21,
        )

    @patch("re_evolution_redemption_gate.fetch_benchmark_return_pct")
    @patch("re_evolution_redemption_gate.fetch_shadow_closed_rows")
    def test_evaluate_promotes_only_when_samples_and_alpha_ok(
        self, mock_rows, mock_bench
    ):
        from re_evolution_redemption_gate import evaluate_shadow_redemption

        mock_rows.return_value = [{"final_ret": 3.0}] * 16
        mock_bench.return_value = 0.5
        out = evaluate_shadow_redemption(
            market="KR",
            group_key="RANK_A",
            sys_config={},
            meta={"META_REGIME_KEY": "SIDEWAYS"},
        )
        self.assertTrue(out.get("passes"))
        self.assertTrue(out["gate_detail"].get("pass"))


class TestMetaRestore(unittest.TestCase):
    def test_shadow_removed_and_kelly_restored(self):
        meta = {
            "META_RE_EVOLUTION_SHADOW_GROUPS": ["RANK_A", "RANK_B"],
            "META_RE_EVOLUTION_KELLY_OVERLAY": {"RANK_A": 0.0},
            "META_DEATHMATCH_KELLY_OVERLAY": {"RANK_A": 0.0},
            "META_RE_EVOLUTION_STRIKES": {
                "KR|RANK_A": {"demoted": True, "consecutive_strikes": 3},
            },
            "META_RE_EVOLUTION_DEMOTED": [
                {"market": "KR", "group_key": "RANK_A", "mutation_done": True},
            ],
            "META_STRATEGY_HEALTH": {},
        }
        apply_redemption_meta_updates(
            meta,
            market="KR",
            group_key="RANK_A",
            strategy_id="strat:abc",
            gate_detail={"pass": True, "alpha_excess_pct": 2.0},
        )
        self.assertNotIn("RANK_A", meta["META_RE_EVOLUTION_SHADOW_GROUPS"])
        self.assertNotIn("RANK_A", meta["META_RE_EVOLUTION_KELLY_OVERLAY"])
        self.assertFalse(meta["META_RE_EVOLUTION_STRIKES"]["KR|RANK_A"]["demoted"])
        self.assertIn("redeemed_at", meta["META_RE_EVOLUTION_DEMOTED"][0])

    @patch("evolution.deathmatch_allocation.merge_group_kelly_from_overlay")
    @patch("evolution.deathmatch_allocation.health_to_group_mult")
    def test_restore_overlay_calls_merge(self, mock_health, mock_merge):
        mock_health.return_value = {"RANK_A": 1.0}
        mock_merge.return_value = {"RANK_A": 1.0}
        meta = {
            "META_RE_EVOLUTION_KELLY_OVERLAY": {"RANK_A": 0.0},
            "META_DEATHMATCH_KELLY_OVERLAY": {"RANK_A": 0.0},
            "META_STRATEGY_HEALTH": {},
        }
        restore_redemption_capital_overlay(meta, "RANK_A")
        mock_merge.assert_called_once()


class TestTryPromote(unittest.TestCase):
    @patch("re_evolution_redemption_gate.evaluate_shadow_redemption")
    def test_promotes_row_and_meta(self, mock_eval):
        mock_eval.return_value = {
            "passes": True,
            "gate_detail": {
                "pass": True,
                "n_closed": 7,
                "win_rate": 0.6,
                "alpha_excess_pct": 2.0,
            },
        }
        row = {
            "strategy_id": "strat:abc",
            "market": "KR",
            "group_key": "RANK_A",
            "state": "OBSERVING",
            "demote_reason": "re_evolution_3_strike(x3)",
            "capital_mult": 0.0,
        }
        meta = {
            "META_RE_EVOLUTION_SHADOW_GROUPS": ["RANK_A"],
            "META_RE_EVOLUTION_KELLY_OVERLAY": {"RANK_A": 0.0},
            "META_DEATHMATCH_KELLY_OVERLAY": {"RANK_A": 0.0},
            "META_RE_EVOLUTION_STRIKES": {},
            "META_RE_EVOLUTION_DEMOTED": [],
            "META_STRATEGY_HEALTH": {},
        }
        ok, _ = try_promote_re_evolution_redemption(
            row, meta=meta, sys_config={}, forward_db_path=None
        )
        self.assertTrue(ok)
        self.assertEqual(row["state"], "LIVE")
        self.assertEqual(row["capital_mult"], 1.0)
        self.assertEqual(row["promote_reason"], "re_evolution_redemption")
        self.assertNotIn("RANK_A", meta["META_RE_EVOLUTION_SHADOW_GROUPS"])

    def test_skips_non_re_evolution(self):
        row = {
            "state": "OBSERVING",
            "source": "health_discovery",
            "group_key": "RANK_X",
        }
        ok, out = try_promote_re_evolution_redemption(row)
        self.assertFalse(ok)
        self.assertEqual(out.get("reason"), "not_re_evolution_observing")


class TestLifecycleIntegration(unittest.TestCase):
    @patch("re_evolution_redemption_gate.try_promote_re_evolution_redemption")
    @patch("strategy_registry_store.upsert_registry_rows")
    @patch("strategy_registry_store.load_registry_rows")
    def test_registry_lifecycle_calls_redemption(
        self, mock_load, _upsert, mock_try
    ):
        from strategy_promotion_engine import run_registry_lifecycle

        mock_load.return_value = []
        mock_try.return_value = (False, {"passes": False})

        prior = [
            {
                "strategy_id": "strat:abc",
                "market": "KR",
                "group_key": "RANK_A",
                "state": "OBSERVING",
                "demote_reason": "re_evolution_3_strike(x3)",
                "capital_mult": 0.0,
            }
        ]
        run_registry_lifecycle(
            prior_registry=prior,
            health={},
            meta_working={},
        )
        mock_try.assert_called()


if __name__ == "__main__":
    unittest.main()
