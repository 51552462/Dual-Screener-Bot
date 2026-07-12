"""Re-Evolution Warm-Start (1번) — Base Confidence 40% 초기 배분 테스트."""
from __future__ import annotations

import unittest

from re_evolution_warm_start import (
    DEFAULT_BASE_CONFIDENCE,
    apply_warm_start_kelly_scaler,
    apply_warm_start_meta_on_redemption,
    apply_warm_start_registry_row,
    is_warm_start_live_group,
    resolve_warm_start_kelly_mult,
    warm_start_config,
)


class TestWarmStartConfig(unittest.TestCase):
    def test_default_base_confidence(self):
        cfg = warm_start_config({})
        self.assertEqual(cfg["base_confidence"], DEFAULT_BASE_CONFIDENCE)
        self.assertEqual(DEFAULT_BASE_CONFIDENCE, 0.40)


class TestRegistryRow(unittest.TestCase):
    def test_capital_mult_40pct(self):
        row = {
            "state": "OBSERVING",
            "capital_mult": 0.0,
        }
        apply_warm_start_registry_row(
            row,
            shadow_stats={"avg_ret_pct": 2.1, "n_closed": 16},
        )
        self.assertEqual(row["state"], "LIVE")
        self.assertEqual(row["capital_mult"], 0.4)
        self.assertEqual(row["warm_start_mult"], 0.4)
        self.assertTrue(row["re_evolution_warm_start"])
        self.assertEqual(row["promote_reason"], "re_evolution_redemption_warm_start")


class TestMetaWarmStart(unittest.TestCase):
    def test_meta_overlay_and_record(self):
        meta = {
            "META_RE_EVOLUTION_KELLY_OVERLAY": {"RANK_A": 0.0},
            "META_DEATHMATCH_KELLY_OVERLAY": {"RANK_A": 0.0},
        }
        apply_warm_start_meta_on_redemption(
            meta,
            market="KR",
            group_key="RANK_A",
            strategy_id="strat:abc",
            shadow_stats={"avg_ret_pct": 1.8, "n_closed": 15, "win_rate": 0.6},
            gate_detail={"pass": True},
        )
        self.assertEqual(meta["META_RE_EVOLUTION_KELLY_OVERLAY"]["RANK_A"], 0.4)
        rec = meta["META_RE_EVOLUTION_WARM_START"]["KR|RANK_A"]
        self.assertEqual(rec["phase"], "warm_start")
        self.assertEqual(rec["kelly_mult"], 0.4)
        self.assertEqual(rec["shadow_ev_avg_ret_pct"], 1.8)
        self.assertTrue(is_warm_start_live_group(meta, "KR", "RANK_A"))


class TestKellyScaler(unittest.TestCase):
    def test_scales_to_40pct(self):
        meta = {
            "META_RE_EVOLUTION_WARM_START": {
                "KR|RANK_A": {
                    "market": "KR",
                    "group_key": "RANK_A",
                    "phase": "warm_start",
                    "kelly_mult": 0.4,
                }
            }
        }
        out = apply_warm_start_kelly_scaler(0.02, meta, market="KR", group_key="RANK_A")
        self.assertAlmostEqual(out, 0.008, places=6)
        self.assertEqual(resolve_warm_start_kelly_mult(meta, "KR", "RANK_A"), 0.4)

    def test_no_record_unchanged(self):
        self.assertEqual(
            apply_warm_start_kelly_scaler(0.02, {}, market="KR", group_key="RANK_X"),
            0.02,
        )


if __name__ == "__main__":
    unittest.main()
