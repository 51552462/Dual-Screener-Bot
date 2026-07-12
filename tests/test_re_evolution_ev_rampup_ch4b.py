"""Re-Evolution EV Ramp-up (2번) — 실측 매칭 가속·킬스위치 테스트."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from datetime import datetime, timezone

from re_evolution_ev_rampup import (
    apply_fake_resurrection_recall,
    apply_full_ramp_promotion,
    evaluate_ev_alignment,
    ev_rampup_config,
    process_warm_start_live_closure,
    should_trigger_kill_switch,
)
from re_evolution_warm_start import WARM_START_PHASE


def _warm_meta(shadow_ev: float = 2.0) -> dict:
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


class TestEvRampConfig(unittest.TestCase):
    def test_defaults(self):
        cfg = ev_rampup_config({})
        self.assertEqual(cfg["max_eval_closures"], 3)
        self.assertEqual(cfg["slippage_tolerance_pct"], 2.5)
        self.assertEqual(cfg["kill_single_loss_pct"], -8.0)


class TestKillSwitch(unittest.TestCase):
    def test_big_loss_triggers(self):
        cfg = ev_rampup_config({})
        ok, reason = should_trigger_kill_switch(-9.0, 2.0, cfg)
        self.assertTrue(ok)
        self.assertIn("single_loss", reason)

    def test_ev_divergence_triggers(self):
        cfg = ev_rampup_config({})
        # shadow +2%, live -5% → floor = 2 - 2.5 - 5 = -5.5; -6 < -5.5
        ok, reason = should_trigger_kill_switch(-6.0, 2.0, cfg)
        self.assertTrue(ok)
        self.assertIn("ev_divergence", reason)

    def test_within_slippage_no_kill(self):
        cfg = ev_rampup_config({})
        ok, _ = should_trigger_kill_switch(1.5, 2.0, cfg)
        self.assertFalse(ok)


class TestEvAlignment(unittest.TestCase):
    def test_first_trade_match(self):
        cfg = ev_rampup_config({})
        ok, detail = evaluate_ev_alignment([2.1], 2.0, cfg)
        self.assertTrue(ok)
        self.assertEqual(detail["reason"], "single_trade_within_slippage")

    def test_running_avg_match(self):
        cfg = ev_rampup_config({})
        # 개별 거래는 불일치, 평균만 섀도우 EV와 일치
        ok, detail = evaluate_ev_alignment([-0.6, 4.6], 2.0, cfg)
        self.assertTrue(ok)
        self.assertEqual(detail["reason"], "running_avg_within_slippage")

    def test_mismatch(self):
        cfg = ev_rampup_config({})
        ok, detail = evaluate_ev_alignment([-1.0, -2.0], 2.0, cfg)
        self.assertFalse(ok)
        self.assertEqual(detail["reason"], "ev_mismatch")


class TestFullRampMeta(unittest.TestCase):
    @patch("strategy_registry_store.upsert_registry_rows")
    def test_full_ramp_sets_100pct(self, mock_upsert):
        meta = _warm_meta()
        out = apply_full_ramp_promotion(
            meta,
            market="KR",
            group_key="RANK_A",
            strategy_id="strat:abc",
            ramp_detail={"match": True},
        )
        self.assertEqual(out["action"], "full_ramp")
        rec = meta["META_RE_EVOLUTION_WARM_START"]["KR|RANK_A"]
        self.assertEqual(rec["phase"], "full_ramp")
        self.assertEqual(rec["kelly_mult"], 1.0)
        self.assertNotIn("RANK_A", meta.get("META_RE_EVOLUTION_KELLY_OVERLAY", {}))
        mock_upsert.assert_called_once()


class TestShadowRecall(unittest.TestCase):
    @patch("strategy_registry_store.upsert_registry_rows")
    def test_recall_to_observing(self, mock_upsert):
        meta = _warm_meta()
        out = apply_fake_resurrection_recall(
            meta,
            market="KR",
            group_key="RANK_A",
            strategy_id="strat:abc",
            kill_reason="single_loss",
            live_ret_pct=-9.0,
        )
        self.assertEqual(out["action"], "shadow_recall")
        self.assertIn("RANK_A", meta["META_RE_EVOLUTION_SHADOW_GROUPS"])
        self.assertEqual(
            meta["META_RE_EVOLUTION_WARM_START"]["KR|RANK_A"]["phase"],
            "shadow_recall",
        )
        self.assertEqual(meta["META_RE_EVOLUTION_KELLY_OVERLAY"]["RANK_A"], 0.0)
        mock_upsert.assert_called_once()


class TestProcessClosure(unittest.TestCase):
    def _patch_dynamic_tol(self, tolerance_pct: float = 2.5):
        return patch(
            "re_evolution_dynamic_tolerance.compute_dynamic_ev_tolerance_pct",
            return_value={
                "tolerance_pct": tolerance_pct,
                "regime_key": "SIDEWAYS",
                "regime_weight": 0.8,
                "atr_pct": 2.0,
                "source": "atr_x_regime",
            },
        )

    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_first_match_full_ramp(
        self, mock_load, _save, _inv
    ):
        mock_load.return_value = _warm_meta(shadow_ev=2.5)
        with self._patch_dynamic_tol(2.5):
            out = process_warm_start_live_closure(
                market="KR",
                sig_type="[STANDARD] RANK_A [🔥]",
                final_ret_pct=2.8,
                sim_kelly_invest=50_000.0,
            )
        self.assertEqual(out["action"], "full_ramp")
        self.assertEqual(out["group_key"], "RANK_A")

    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_kill_on_big_loss(self, mock_load, _save, _inv):
        mock_load.return_value = _warm_meta(shadow_ev=2.0)
        with self._patch_dynamic_tol(2.5):
            out = process_warm_start_live_closure(
                market="KR",
                sig_type="[STANDARD] RANK_A",
                final_ret_pct=-10.0,
                sim_kelly_invest=40_000.0,
            )
        self.assertEqual(out["action"], "shadow_recall")

    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_pending_on_mismatch(self, mock_load, _save, _inv):
        mock_load.return_value = _warm_meta(shadow_ev=3.0)
        with self._patch_dynamic_tol(2.5):
            out = process_warm_start_live_closure(
                market="KR",
                sig_type="[STANDARD] RANK_A",
                final_ret_pct=-2.0,
                sim_kelly_invest=30_000.0,
            )
        self.assertEqual(out["action"], "ev_pending")
        self.assertEqual(out["live_closures"], 1)


if __name__ == "__main__":
    unittest.main()
