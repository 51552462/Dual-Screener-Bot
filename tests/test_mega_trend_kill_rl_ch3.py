"""Mega-Trend Kill-Switch RL Sensitivity (내부 3번) 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from exit_dynamics import mega_trend_internal_thresholds
from mega_trend_kill_rl import (
    MEGA_TREND_KILL_RL_STATE_KEY,
    apply_kill_rl_threshold_adjustments,
    apply_kill_rl_toxic_adjustments,
    classify_kill_outcome,
    compute_kill_feedback_rates,
    evaluate_pending_kill_events,
    evolve_mega_trend_kill_sensitivity,
    measure_post_kill_sector_outcome,
    record_mega_trend_kill_event,
    update_kill_sensitivity_rl,
)
from mega_trend_toxic_kill import toxic_kill_config


class TestKillOutcomeClassification(unittest.TestCase):
    def test_opportunity_cost(self):
        self.assertEqual(classify_kill_outcome(3.5), "opportunity_cost")

    def test_defense_success(self):
        self.assertEqual(classify_kill_outcome(-4.0), "defense_success")

    def test_neutral(self):
        self.assertEqual(classify_kill_outcome(0.5), "neutral")


class TestKillRLUpdate(unittest.TestCase):
    def test_defense_success_tightens(self):
        st = update_kill_sensitivity_rl(
            {},
            opportunity_cost_rate=0.1,
            defense_success_rate=0.7,
            eta=0.05,
        )
        self.assertLess(float(st["win_rate_min_delta"]), 0.0)
        self.assertLessEqual(int(st["consecutive_loss_delta"]), 0)

    def test_opportunity_cost_loosens(self):
        st = update_kill_sensitivity_rl(
            {},
            opportunity_cost_rate=0.8,
            defense_success_rate=0.1,
            eta=0.05,
        )
        self.assertGreater(float(st["win_rate_min_delta"]), 0.0)
        self.assertGreaterEqual(int(st["consecutive_loss_delta"]), 0)


class TestThresholdApplication(unittest.TestCase):
    def test_internal_thresholds_reflect_rl(self):
        cfg = {
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "win_rate_min_delta": -0.05,
                "mfe_reach_min_delta": -0.03,
                "bounce_stop_max_delta": -0.04,
                "consecutive_loss_delta": -1,
                "defensive_scale_delta": 0.02,
            }
        }
        with patch("mega_trend_kill_rl.load_kill_rl_state", return_value=cfg[MEGA_TREND_KILL_RL_STATE_KEY]):
            thr = mega_trend_internal_thresholds()
        self.assertAlmostEqual(thr["win_rate_min"], 0.35, places=2)
        self.assertTrue(thr.get("_kill_rl_applied"))

    def test_toxic_config_reflects_rl(self):
        rl = {"consecutive_loss_delta": -1, "defensive_scale_delta": 0.03}
        with patch("mega_trend_kill_rl.load_kill_rl_state", return_value=rl):
            tcfg = toxic_kill_config()
        self.assertEqual(tcfg["consecutive_loss_min"], 2)
        self.assertGreaterEqual(tcfg["defensive_scale_out_min"], 0.85)


class TestPostKillMeasurement(unittest.TestCase):
    def _mk_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE forward_trades (
                id INTEGER PRIMARY KEY,
                market TEXT, code TEXT, sector TEXT, sig_type TEXT,
                sim_stat_ret REAL, final_ret REAL, status TEXT,
                entry_date TEXT, exit_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO forward_trades VALUES
            (1,'KR','005930','반도체/IT','#MegaTrend언락',4.5,4.5,'CLOSED_WIN','2026-02-08','2026-02-09'),
            (2,'KR','000660','반도체/IT','#MegaTrend자본합산',3.8,3.8,'CLOSED_WIN','2026-02-09','2026-02-10')
            """
        )
        conn.commit()
        return conn

    def test_measure_rally_after_kill(self):
        conn = self._mk_conn()
        out = measure_post_kill_sector_outcome(
            conn, "반도체/IT", "2026-02-07", eval_days=5, ignited_at="2026-02-01"
        )
        self.assertGreater(out["avg_ret_pct"], 2.5)
        self.assertEqual(out["n_trades"], 2)
        self.assertEqual(out["reason"], "computed_megatrend")


class TestKillEventPipeline(unittest.TestCase):
    def test_record_and_evaluate_events(self):
        cfg: dict = {}
        record_mega_trend_kill_event(
            cfg,
            sector="반도체/IT",
            kill_type="toxic_graveyard",
            reason="test_kill",
            kill_at="2026-01-01 10:00:00",
            ignited_at="2025-12-20",
        )
        events = cfg[MEGA_TREND_KILL_RL_STATE_KEY]["kill_events"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["outcome"], "pending")

        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE forward_trades (
                id INTEGER PRIMARY KEY,
                market TEXT, code TEXT, sector TEXT, sig_type TEXT,
                sim_stat_ret REAL, final_ret REAL, status TEXT,
                entry_date TEXT, exit_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO forward_trades VALUES
            (1,'KR','005930','반도체/IT','#MegaTrend언락',-4.0,-4.0,'CLOSED_LOSS','2026-01-02','2026-01-03')
            """
        )
        conn.commit()

        now = datetime(2026, 1, 10)
        evaluated = evaluate_pending_kill_events(events, conn, now=now)
        self.assertEqual(evaluated[0]["outcome"], "defense_success")

        rates = compute_kill_feedback_rates(evaluated, now=now)
        self.assertEqual(rates["n"], 1)
        self.assertEqual(rates["defense_success_rate"], 1.0)

    def test_evolve_weekend_cycle(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        recent2 = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        cfg = {
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "kill_events": [
                    {
                        "sector": "반도체/IT",
                        "kill_at": recent,
                        "outcome": "defense_success",
                    },
                    {
                        "sector": "반도체/IT",
                        "kill_at": recent2,
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
        self.assertTrue(out.get("updated"))
        st = out["state"]
        self.assertIn("win_rate_min_delta", st)


if __name__ == "__main__":
    unittest.main()
