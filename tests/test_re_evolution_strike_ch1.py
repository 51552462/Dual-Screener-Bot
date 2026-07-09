"""Re-Evolution Phase 1 — 3-Strike 강등·섀도우 전환 테스트."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from re_evolution_strike_guard import (
    apply_shadow_entry_zero_notional,
    apply_three_strike_demotion,
    extract_core_group_name,
    format_re_evolution_shadow_sig_type,
    is_live_capital_closure,
    is_re_evolution_shadow_group,
    process_live_closure_strike,
    re_evolution_strike_thresholds,
)


def _meta_live(gk: str = "RANK_A") -> dict:
    return {
        "META_STRATEGY_REGISTRY": [
            {
                "strategy_id": "strat:abc",
                "market": "KR",
                "group_key": gk,
                "state": "LIVE",
                "capital_mult": 1.0,
            }
        ],
        "META_STRATEGY_HEALTH": {},
        "META_DEATHMATCH_KELLY_OVERLAY": {},
        "META_RE_EVOLUTION_STRIKES": {},
        "META_RE_EVOLUTION_SHADOW_GROUPS": [],
    }


class TestStrikeThresholds(unittest.TestCase):
    def test_defaults(self):
        th = re_evolution_strike_thresholds({})
        self.assertEqual(th["loss_threshold_pct"], -5.0)
        self.assertEqual(th["strike_need"], 3.0)


class TestLiveCapitalDetection(unittest.TestCase):
    def test_live_trade(self):
        self.assertTrue(
            is_live_capital_closure(
                "[STANDARD] RANK_A",
                sim_kelly_invest=100_000.0,
            )
        )

    def test_observe_skip(self):
        self.assertFalse(
            is_live_capital_closure(
                "[OBSERVE_ONLY][x] RANK_A",
                sim_kelly_invest=0.0,
            )
        )


class TestShadowSigType(unittest.TestCase):
    def test_tag_format(self):
        s = format_re_evolution_shadow_sig_type("strat:abc", "RANK_A [🔥]")
        self.assertIn("OBSERVE_ONLY", s)
        self.assertIn("RE_EVOL_SHADOW", s)
        self.assertIn("strat:abc", s)


class TestShadowGroupDetection(unittest.TestCase):
    def test_shadow_set(self):
        meta = {
            "META_RE_EVOLUTION_SHADOW_GROUPS": ["RANK_A"],
            "META_STRATEGY_REGISTRY": [],
        }
        self.assertTrue(is_re_evolution_shadow_group(meta, "KR", "RANK_A"))

    def test_registry_observing_demoted(self):
        meta = {
            "META_RE_EVOLUTION_SHADOW_GROUPS": [],
            "META_STRATEGY_REGISTRY": [
                {
                    "market": "KR",
                    "group_key": "RANK_B",
                    "state": "OBSERVING",
                    "demote_reason": "re_evolution_3_strike(x3)",
                }
            ],
        }
        self.assertTrue(is_re_evolution_shadow_group(meta, "KR", "RANK_B"))


class TestProcessClosureStrike(unittest.TestCase):
    @patch("re_evolution_strike_guard.apply_three_strike_demotion")
    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_third_strike_demotes(
        self,
        mock_load,
        _inv,
        _save,
        mock_demote,
    ):
        meta = _meta_live()
        meta["META_RE_EVOLUTION_STRIKES"] = {
            "KR|RANK_A": {"consecutive_strikes": 2, "demoted": False},
        }
        mock_load.return_value = meta
        mock_demote.return_value = {
            "demoted": True,
            "group_key": "RANK_A",
            "state": "OBSERVING",
        }

        out = process_live_closure_strike(
            market="KR",
            sig_type="[STANDARD] RANK_A [🔥]",
            final_ret_pct=-6.0,
            sim_kelly_invest=50_000.0,
            sys_config={"RE_EVOLUTION_STRIKE_NEED": 3},
        )
        self.assertEqual(out["action"], "demoted_observing")
        mock_demote.assert_called_once()

    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_win_resets_streak(self, mock_load, _inv, _save):
        meta = _meta_live()
        meta["META_RE_EVOLUTION_STRIKES"] = {
            "KR|RANK_A": {"consecutive_strikes": 2},
        }
        mock_load.return_value = meta

        out = process_live_closure_strike(
            market="KR",
            sig_type="[STANDARD] RANK_A",
            final_ret_pct=3.5,
            sim_kelly_invest=40_000.0,
        )
        self.assertEqual(out["action"], "strike_reset_win")

    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_skip_non_live_registry(self, mock_load):
        meta = {
            "META_STRATEGY_REGISTRY": [
                {
                    "market": "KR",
                    "group_key": "RANK_A",
                    "state": "CANDIDATE",
                }
            ],
        }
        mock_load.return_value = meta
        out = process_live_closure_strike(
            market="KR",
            sig_type="[STANDARD] RANK_A",
            final_ret_pct=-8.0,
            sim_kelly_invest=10_000.0,
        )
        self.assertEqual(out["action"], "skip_not_registry_live")


class TestShadowEntryZero(unittest.TestCase):
    def test_zero_notional(self):
        sig, sh, inv, sk = apply_shadow_entry_zero_notional(
            "RANK_A [test]", strategy_id="strat:x"
        )
        self.assertEqual(sh, 0)
        self.assertEqual(inv, 0.0)
        self.assertEqual(sk, 0.0)
        self.assertIn("OBSERVE_ONLY", sig)


class TestExtractGroup(unittest.TestCase):
    def test_from_sig(self):
        self.assertEqual(
            extract_core_group_name("[STANDARD] RANK_A [🔥]"),
            "RANK_A",
        )


if __name__ == "__main__":
    unittest.main()
