"""P5 — Per-sector Kill RL overlay 테스트."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from exit_dynamics import mega_trend_internal_thresholds
from mega_trend_climax import climax_config
from mega_trend_kill_rl import (
    KILL_LANE_EXTERNAL,
    KILL_LANE_INTERNAL,
    KILL_TYPE_CLIMAX,
    KILL_TYPE_INTERNAL_MOMENTUM,
    MEGA_TREND_KILL_RL_STATE_KEY,
    MEGA_TREND_SECTOR_DELTAS_KEY,
    apply_kill_rl_threshold_adjustments,
    compute_kill_feedback_rates,
    evolve_mega_trend_kill_sensitivity,
    resolve_effective_kill_rl_state,
    update_internal_kill_sensitivity_rl,
)
from mega_trend_toxic_kill import toxic_kill_config


def _eligible_overlay_block(**extra: object) -> dict:
    base = {
        "internal_n": 2,
        "climax_n": 1,
        "avg_purity": 0.9,
        "bound_ignited_at": "2026-02-01",
    }
    base.update(extra)
    return base


def _patch_apply_context(rl_state: dict, sector: str):
    return {
        "rl_state": rl_state,
        "ignited_at": "2026-02-01",
        "active_sectors": (sector,),
    }


class TestResolveEffectiveKillRL(unittest.TestCase):
    def test_global_only_without_sector(self):
        rl = {"win_rate_min_delta": -0.02, "flow_reversal_z_delta": 0.03}
        eff = resolve_effective_kill_rl_state(rl)
        self.assertAlmostEqual(eff["win_rate_min_delta"], -0.02)
        self.assertFalse(eff.get("_kill_rl_overlay"))

    def test_global_plus_sector_overlay_additive(self):
        sector = "반도체/IT"
        rl = {
            "win_rate_min_delta": -0.02,
            "flow_reversal_z_delta": 0.03,
            MEGA_TREND_SECTOR_DELTAS_KEY: {
                sector: _eligible_overlay_block(
                    win_rate_min_delta=-0.01,
                    flow_reversal_z_delta=0.02,
                )
            },
        }
        eff = resolve_effective_kill_rl_state(
            rl,
            sector=sector,
            ignited_at="2026-02-01",
            active_sectors=[sector],
        )
        self.assertTrue(eff.get("_kill_rl_overlay"))
        self.assertAlmostEqual(eff["win_rate_min_delta"], -0.03)
        self.assertAlmostEqual(eff["flow_reversal_z_delta"], 0.05)


class TestSectorFilteredFeedback(unittest.TestCase):
    def _events(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        return [
            {
                "kill_at": recent,
                "sector": "반도체/IT",
                "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                "outcome": "defense_success",
            },
            {
                "kill_at": recent,
                "sector": "2차전지",
                "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                "outcome": "opportunity_cost",
            },
            {
                "kill_at": recent,
                "sector": "반도체/IT",
                "kill_type": KILL_TYPE_CLIMAX,
                "outcome": "defense_success",
            },
        ]

    def test_sector_filter_splits_counts(self):
        events = self._events()
        sec_rates = compute_kill_feedback_rates(
            events, kill_lane=KILL_LANE_INTERNAL, sector="반도체/IT"
        )
        self.assertEqual(sec_rates["n"], 1)
        self.assertEqual(sec_rates["defense_success_rate"], 1.0)

        other = compute_kill_feedback_rates(
            events, kill_lane=KILL_LANE_INTERNAL, sector="2차전지"
        )
        self.assertEqual(other["n"], 1)
        self.assertEqual(other["opportunity_cost_rate"], 1.0)


class TestSectorOverlayEvolve(unittest.TestCase):
    def test_sector_overlay_updated_on_weekend_evolve(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        sector = "반도체/IT"
        cfg = {
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "kill_events": [
                    {
                        "sector": sector,
                        "kill_at": recent,
                        "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                        "outcome": "defense_success",
                    },
                    {
                        "sector": sector,
                        "kill_at": recent,
                        "kill_type": KILL_TYPE_CLIMAX,
                        "outcome": "opportunity_cost",
                    },
                ],
                MEGA_TREND_SECTOR_DELTAS_KEY: {},
            }
        }
        with patch(
            "mega_trend_kill_rl.evaluate_pending_kill_events",
            side_effect=lambda ev, conn, now=None: list(ev),
        ):
            with patch("mega_trend_kill_rl._persist_kill_rl_state"):
                out = evolve_mega_trend_kill_sensitivity(cfg, db_path=None, persist=False)

        self.assertIn(sector, out.get("sectors_updated") or [])
        overlays = (out["state"].get(MEGA_TREND_SECTOR_DELTAS_KEY) or {}).get(sector) or {}
        self.assertGreater(int(overlays.get("internal_n") or 0), 0)
        self.assertGreater(int(overlays.get("climax_n") or 0), 0)
        summary = out["state"].get("last_evolve_summary") or {}
        self.assertIn(sector, summary.get("sectors_updated") or [])


class TestSectorSpecificThresholdApplication(unittest.TestCase):
    def test_internal_thresholds_differ_by_sector(self):
        sector = "반도체/IT"
        rl = {
            "win_rate_min_delta": -0.02,
            MEGA_TREND_SECTOR_DELTAS_KEY: {
                sector: _eligible_overlay_block(win_rate_min_delta=-0.03)
            },
        }
        base = {"win_rate_min": 0.40, "mfe_reach_min": 0.35, "bounce_stop_max_rate": 0.45, "pnl_accel_drop_min": 0.15}
        ctx = _patch_apply_context(rl, sector)
        with patch("mega_trend_kill_rl._kill_rl_apply_context", return_value=ctx):
            global_out = apply_kill_rl_threshold_adjustments(base, rl_state=rl)
            sector_out = apply_kill_rl_threshold_adjustments(base, rl_state=rl, sector=sector)
        self.assertLess(sector_out["win_rate_min"], global_out["win_rate_min"])
        self.assertTrue(sector_out.get("_kill_rl_overlay"))

    def test_mega_trend_internal_thresholds_sector_param(self):
        sector = "반도체/IT"
        rl_patch = {
            MEGA_TREND_SECTOR_DELTAS_KEY: {
                sector: _eligible_overlay_block(win_rate_min_delta=-0.04)
            },
            "win_rate_min_delta": 0.0,
        }
        ctx = _patch_apply_context(rl_patch, sector)
        with patch("mega_trend_kill_rl.load_kill_rl_state", return_value=rl_patch):
            with patch("mega_trend_kill_rl._kill_rl_apply_context", return_value=ctx):
                global_thr = mega_trend_internal_thresholds()
                sector_thr = mega_trend_internal_thresholds(sector=sector)
        self.assertLess(sector_thr["win_rate_min"], global_thr["win_rate_min"])

    def test_toxic_and_climax_configs_use_sector_overlay(self):
        sector = "반도체/IT"
        rl_patch = {
            "consecutive_loss_delta": 0,
            "flow_reversal_z_delta": 0.0,
            MEGA_TREND_SECTOR_DELTAS_KEY: {
                sector: _eligible_overlay_block(
                    consecutive_loss_delta=-1,
                    flow_reversal_z_delta=0.05,
                )
            },
        }
        ctx = _patch_apply_context(rl_patch, sector)
        with patch("mega_trend_kill_rl.load_kill_rl_state", return_value=rl_patch):
            with patch("mega_trend_kill_rl._kill_rl_apply_context", return_value=ctx):
                global_toxic = toxic_kill_config()
                sector_toxic = toxic_kill_config(sector=sector)
                global_clx = climax_config()
                sector_clx = climax_config(sector=sector)
        self.assertLess(sector_toxic["consecutive_loss_min"], global_toxic["consecutive_loss_min"])
        self.assertGreater(sector_clx["flow_reversal_z"], global_clx["flow_reversal_z"])


class TestSectorOverlayStepIsolation(unittest.TestCase):
    def test_sector_overlay_uses_tighter_clamps_than_global(self):
        from mega_trend_kill_rl import _update_sector_overlay_block

        st = update_internal_kill_sensitivity_rl(
            {},
            opportunity_cost_rate=0.9,
            defense_success_rate=0.0,
            eta=0.5,
        )
        global_wr = float(st["win_rate_min_delta"])

        st2 = _update_sector_overlay_block(
            {MEGA_TREND_SECTOR_DELTAS_KEY: {}},
            "반도체/IT",
            lane=KILL_LANE_INTERNAL,
            opportunity_cost_rate=0.9,
            defense_success_rate=0.0,
            eta=0.5,
            sample_n=1,
        )
        overlay_wr = float(
            st2[MEGA_TREND_SECTOR_DELTAS_KEY]["반도체/IT"]["win_rate_min_delta"]
        )
        self.assertLess(abs(overlay_wr), abs(global_wr))


if __name__ == "__main__":
    unittest.main()
