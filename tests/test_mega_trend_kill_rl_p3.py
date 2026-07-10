"""P3 — Internal vs Climax dual-lane Kill RL 테스트."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from mega_trend_climax import climax_config
from mega_trend_kill_rl import (
    KILL_LANE_EXTERNAL,
    KILL_LANE_INTERNAL,
    KILL_TYPE_CLIMAX,
    KILL_TYPE_INTERNAL_MOMENTUM,
    KILL_TYPE_TOXIC,
    MEGA_TREND_KILL_RL_STATE_KEY,
    apply_kill_rl_climax_adjustments,
    classify_kill_lane,
    compute_kill_feedback_rates,
    evolve_mega_trend_kill_sensitivity,
    record_mega_trend_kill_event,
    update_climax_kill_sensitivity_rl,
    update_internal_kill_sensitivity_rl,
)


class TestKillLaneClassification(unittest.TestCase):
    def test_internal_types(self):
        self.assertEqual(classify_kill_lane(KILL_TYPE_INTERNAL_MOMENTUM), KILL_LANE_INTERNAL)
        self.assertEqual(classify_kill_lane(KILL_TYPE_TOXIC), KILL_LANE_INTERNAL)
        self.assertEqual(classify_kill_lane(""), KILL_LANE_INTERNAL)

    def test_external_type(self):
        self.assertEqual(classify_kill_lane(KILL_TYPE_CLIMAX), KILL_LANE_EXTERNAL)


class TestLaneSeparatedFeedback(unittest.TestCase):
    def _events(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        return [
            {
                "kill_at": recent,
                "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                "outcome": "defense_success",
            },
            {
                "kill_at": recent,
                "kill_type": KILL_TYPE_CLIMAX,
                "outcome": "opportunity_cost",
            },
            {
                "kill_at": recent,
                "kill_type": KILL_TYPE_TOXIC,
                "outcome": "defense_success",
            },
        ]

    def test_lane_filter_splits_counts(self):
        events = self._events()
        all_rates = compute_kill_feedback_rates(events)
        int_rates = compute_kill_feedback_rates(events, kill_lane=KILL_LANE_INTERNAL)
        clx_rates = compute_kill_feedback_rates(events, kill_lane=KILL_LANE_EXTERNAL)

        self.assertEqual(all_rates["n"], 3)
        self.assertEqual(int_rates["n"], 2)
        self.assertEqual(clx_rates["n"], 1)
        self.assertEqual(int_rates["defense_success_rate"], 1.0)
        self.assertEqual(clx_rates["opportunity_cost_rate"], 1.0)


class TestDualLaneRLUpdate(unittest.TestCase):
    def test_internal_does_not_touch_climax_deltas(self):
        st = update_internal_kill_sensitivity_rl(
            {"flow_reversal_z_delta": 0.1, "climax_vol_shrink_delta": 0.05},
            opportunity_cost_rate=0.2,
            defense_success_rate=0.6,
            eta=0.05,
        )
        self.assertLess(float(st["win_rate_min_delta"]), 0.0)
        self.assertAlmostEqual(float(st["flow_reversal_z_delta"]), 0.1, places=4)
        self.assertAlmostEqual(float(st["climax_vol_shrink_delta"]), 0.05, places=4)

    def test_climax_does_not_touch_internal_deltas(self):
        st = update_climax_kill_sensitivity_rl(
            {"win_rate_min_delta": -0.05, "consecutive_loss_delta": -1},
            opportunity_cost_rate=0.7,
            defense_success_rate=0.1,
            eta=0.05,
        )
        self.assertAlmostEqual(float(st["win_rate_min_delta"]), -0.05, places=4)
        self.assertEqual(int(st["consecutive_loss_delta"]), -1)
        self.assertLess(float(st["flow_reversal_z_delta"]), 0.0)

    def test_climax_defense_tightens_triggers(self):
        st = update_climax_kill_sensitivity_rl(
            {},
            opportunity_cost_rate=0.1,
            defense_success_rate=0.8,
            eta=0.05,
        )
        self.assertGreater(float(st["flow_reversal_z_delta"]), 0.0)
        self.assertGreater(float(st["flow_z_drop_min_delta"]), 0.0)
        self.assertGreater(float(st["climax_vol_shrink_delta"]), 0.0)


class TestClimaxConfigRLApplication(unittest.TestCase):
    def test_apply_climax_adjustments(self):
        base = {
            "flow_reversal_z": 0.0,
            "flow_z_drop_min": 1.5,
            "climax_vol_shrink": 0.85,
            "scale_out_fraction": 0.75,
        }
        rl = {
            "flow_reversal_z_delta": 0.2,
            "flow_z_drop_min_delta": 0.3,
            "climax_vol_shrink_delta": 0.04,
            "scale_out_fraction_delta": 0.05,
        }
        out = apply_kill_rl_climax_adjustments(base, rl_state=rl)
        self.assertAlmostEqual(out["flow_reversal_z"], 0.2, places=4)
        self.assertAlmostEqual(out["flow_z_drop_min"], 1.2, places=4)
        self.assertAlmostEqual(out["climax_vol_shrink"], 0.89, places=4)
        self.assertTrue(out.get("_kill_rl_applied"))

    def test_climax_config_loads_rl(self):
        rl_block = {
            "flow_reversal_z_delta": 0.15,
            "flow_z_drop_min_delta": 0.1,
        }
        with patch(
            "mega_trend_kill_rl.load_kill_rl_state",
            return_value=rl_block,
        ):
            cfg = climax_config()
        self.assertAlmostEqual(cfg["flow_reversal_z"], 0.15, places=4)
        self.assertAlmostEqual(cfg["flow_z_drop_min"], 1.4, places=4)


class TestEvolveDualLane(unittest.TestCase):
    def test_evolve_updates_only_qualifying_lanes(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        recent2 = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        cfg = {
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "kill_events": [
                    {
                        "sector": "반도체/IT",
                        "kill_at": recent,
                        "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                        "outcome": "defense_success",
                    },
                    {
                        "sector": "반도체/IT",
                        "kill_at": recent2,
                        "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                        "outcome": "defense_success",
                    },
                ]
            }
        }
        with patch(
            "mega_trend_kill_rl.evaluate_pending_kill_events",
            side_effect=lambda ev, conn, now=None: list(ev),
        ):
            with patch("mega_trend_kill_rl._persist_kill_rl_state"):
                out = evolve_mega_trend_kill_sensitivity(cfg, db_path=None, persist=False)

        self.assertTrue(out.get("updated"))
        self.assertIn("internal", out.get("lanes_updated") or [])
        self.assertNotIn("climax", out.get("lanes_updated") or [])
        st = out["state"]
        self.assertNotEqual(float(st.get("win_rate_min_delta") or 0.0), 0.0)
        self.assertEqual(float(st.get("flow_reversal_z_delta") or 0.0), 0.0)

    def test_evolve_climax_lane_only(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        recent2 = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        cfg = {
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "kill_events": [
                    {
                        "sector": "반도체/IT",
                        "kill_at": recent,
                        "kill_type": KILL_TYPE_CLIMAX,
                        "outcome": "defense_success",
                    },
                    {
                        "sector": "반도체/IT",
                        "kill_at": recent2,
                        "kill_type": KILL_TYPE_CLIMAX,
                        "outcome": "defense_success",
                    },
                ]
            }
        }
        with patch(
            "mega_trend_kill_rl.evaluate_pending_kill_events",
            side_effect=lambda ev, conn, now=None: list(ev),
        ):
            with patch("mega_trend_kill_rl._persist_kill_rl_state"):
                out = evolve_mega_trend_kill_sensitivity(cfg, db_path=None, persist=False)

        self.assertTrue(out.get("updated"))
        self.assertIn("climax", out.get("lanes_updated") or [])
        st = out["state"]
        self.assertGreater(float(st.get("flow_reversal_z_delta") or 0.0), 0.0)
        self.assertEqual(float(st.get("win_rate_min_delta") or 0.0), 0.0)


class TestKillEventLanePersistence(unittest.TestCase):
    def test_record_stores_kill_lane(self):
        cfg: dict = {}
        record_mega_trend_kill_event(
            cfg,
            sector="반도체/IT",
            kill_type=KILL_TYPE_CLIMAX,
            kill_at="2026-02-07 10:00:00",
            ignited_at="2026-02-01",
        )
        ev = cfg[MEGA_TREND_KILL_RL_STATE_KEY]["kill_events"][0]
        self.assertEqual(ev["kill_lane"], KILL_LANE_EXTERNAL)
        self.assertEqual(ev["kill_type"], KILL_TYPE_CLIMAX)


if __name__ == "__main__":
    unittest.main()
