"""P4 — Mega-Trend Kill 리포트·Telegram 노출 테스트."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from mega_trend_ignition import MEGA_TREND_CONFIG_KEY
from mega_trend_kill_rl import (
    KILL_TYPE_CLIMAX,
    KILL_TYPE_INTERNAL_MOMENTUM,
    MEGA_TREND_KILL_RL_STATE_KEY,
    evolve_mega_trend_kill_sensitivity,
)
from reports.mega_trend_kill_report_section import (
    append_mega_trend_daily_to_satellite,
    build_mega_trend_kill_report_block,
    build_mega_trend_kill_weekly_appendix,
    format_mega_trend_kill_daily_html,
    format_mega_trend_kill_rl_evolution_telegram,
    format_mega_trend_kill_weekly_html,
)


class TestMegaTrendKillReportBlock(unittest.TestCase):
    def test_build_block_from_active_state(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
                "internal_diagnostics": {
                    "any_momentum_lost": False,
                    "momentum_lost_sectors": [],
                },
            },
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "win_rate_min_delta": -0.03,
                "flow_reversal_z_delta": 0.05,
                "kill_events": [
                    {
                        "kill_at": "2026-02-07 10:00:00",
                        "sector": "반도체/IT",
                        "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                        "kill_lane": "internal",
                        "outcome": "defense_success",
                        "post_kill": {"avg_ret_pct": -3.5, "n_trades": 2},
                    }
                ],
                "events_pending": 0,
                "events_evaluated": 1,
            },
        }
        block = build_mega_trend_kill_report_block(cfg)
        self.assertTrue(block.enabled)
        self.assertTrue(block.active)
        self.assertEqual(block.primary_sector, "반도체/IT")
        self.assertAlmostEqual(block.win_rate_min_delta, -0.03)
        self.assertEqual(len(block.recent_events), 1)

    def test_daily_html_includes_status_and_rl(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": False,
                "toxic_kill_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "toxic_kill_reason": "toxic_graveyard: test",
            },
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "win_rate_min_delta": -0.02,
                "consecutive_loss_delta": -1,
            },
        }
        html_out = format_mega_trend_kill_daily_html(
            build_mega_trend_kill_report_block(cfg)
        )
        self.assertIn("Mega-Trend Kill-Switch", html_out)
        self.assertIn("킬쿨다운", html_out)
        self.assertIn("WRΔ", html_out)
        self.assertIn("Toxic", html_out)

    def test_weekly_html_shows_sector_overlay(self):
        sector = "반도체/IT"
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": sector,
                "sectors": [sector],
            },
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "win_rate_min_delta": -0.01,
                "flow_reversal_z_delta": 0.02,
                "sector_deltas": {
                    sector: {
                        "win_rate_min_delta": -0.02,
                        "flow_reversal_z_delta": 0.03,
                        "internal_n": 2,
                        "climax_n": 1,
                    }
                },
            },
        }
        html_out = format_mega_trend_kill_daily_html(build_mega_trend_kill_report_block(cfg))
        self.assertIn("Sector RL overlay", html_out)
        self.assertIn("primary RL", html_out)
        self.assertIn(sector, html_out)

    def test_weekly_html_from_last_evolve_summary(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {"active": False},
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "last_evolve_summary": {
                    "updated": True,
                    "lanes_updated": ["internal", "climax"],
                    "rates_internal": {
                        "n": 3,
                        "opportunity_cost_rate": 0.33,
                        "defense_success_rate": 0.67,
                    },
                    "rates_climax": {
                        "n": 2,
                        "opportunity_cost_rate": 0.0,
                        "defense_success_rate": 1.0,
                    },
                    "sectors_updated": ["반도체/IT"],
                },
                "flow_reversal_z_delta": 0.04,
            },
        }
        html_out = build_mega_trend_kill_weekly_appendix(cfg)
        self.assertIn("주간 감사", html_out)
        self.assertIn("internal", html_out)
        self.assertIn("Climax", html_out)
        self.assertIn("flowZ", html_out)
        self.assertIn("섹터 RL 갱신", html_out)

    def test_append_to_satellite(self):
        cfg = {MEGA_TREND_CONFIG_KEY: {"active": True, "primary_sector": "반도체/IT"}}
        out = append_mega_trend_daily_to_satellite(cfg, "BASE\n")
        self.assertTrue(out.startswith("BASE"))
        self.assertIn("Mega-Trend", out)


class TestEvolveAuditPersistence(unittest.TestCase):
    def test_last_evolve_summary_persisted(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
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
                        "kill_at": recent,
                        "kill_type": KILL_TYPE_CLIMAX,
                        "outcome": "opportunity_cost",
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

        st = out["state"]
        summary = st.get("last_evolve_summary") or {}
        self.assertIn("evaluated_at", summary)
        self.assertIn("rates_internal", summary)
        self.assertIn("rates_climax", summary)


class TestEvolutionTelegramBrief(unittest.TestCase):
    def test_updated_evolution_brief(self):
        msg = format_mega_trend_kill_rl_evolution_telegram(
            {
                "updated": True,
                "lanes_updated": ["internal"],
                "rates_internal": {"n": 2, "opportunity_cost_rate": 0.5},
                "state": {"win_rate_min_delta": -0.02},
            }
        )
        self.assertIn("Mega-Trend Kill RL", msg)

    def test_weekly_block_shows_evaluated_event(self):
        block = build_mega_trend_kill_report_block(
            {
                MEGA_TREND_CONFIG_KEY: {"active": True, "sectors": ["반도체/IT"]},
                MEGA_TREND_KILL_RL_STATE_KEY: {
                    "kill_events": [
                        {
                            "kill_at": "2026-02-07",
                            "sector": "반도체/IT",
                            "kill_type": KILL_TYPE_CLIMAX,
                            "kill_lane": "external",
                            "outcome": "defense_success",
                            "post_kill": {"avg_ret_pct": -4.0, "n_trades": 1},
                        }
                    ]
                },
            }
        )
        html_out = format_mega_trend_kill_weekly_html(block)
        self.assertIn("Climax", html_out)
        self.assertIn("방어성공", html_out)


if __name__ == "__main__":
    unittest.main()
