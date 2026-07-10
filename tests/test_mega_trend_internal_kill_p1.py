"""P1 — Internal Momentum Kill-Switch (내부 1번 실행) 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime
from unittest.mock import patch

from mega_trend_ignition import MEGA_TREND_CONFIG_KEY
from mega_trend_internal_kill import (
    evaluate_mega_trend_internal_momentum_kill,
    refresh_mega_trend_internal_momentum_kill,
)
from mega_trend_internal_monitor import refresh_mega_trend_internal_diagnostics
from mega_trend_toxic_kill import (
    FORGIVENESS_REVOKED_KEY,
    evaluate_mega_trend_toxic_kill,
)


def _loss_row(st: str, ed: str, fr: float, et: str = "STAT_MAE") -> tuple:
    return (st, ed, fr, et)


class TestEvaluateInternalMomentumKill(unittest.TestCase):
    def test_kill_when_diagnostics_show_momentum_lost(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "internal_diagnostics": {
                    "any_momentum_lost": True,
                    "momentum_lost_sectors": ["반도체/IT"],
                    "sectors": {
                        "반도체/IT": {
                            "momentum_lost": True,
                            "reason": "internal_momentum_lost: win_rate_collapse",
                            "triggers": ["win_rate_collapse_0.20<=0.40"],
                        }
                    },
                },
            }
        }
        out = evaluate_mega_trend_internal_momentum_kill(cfg)
        self.assertTrue(out.get("kill"))
        self.assertEqual(out.get("exit_mode"), "defensive_exit")
        self.assertEqual(out.get("sector"), "반도체/IT")

    def test_no_kill_when_momentum_ok(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "internal_diagnostics": {"any_momentum_lost": False},
            }
        }
        out = evaluate_mega_trend_internal_momentum_kill(cfg)
        self.assertFalse(out.get("kill"))

    def test_no_kill_when_inactive(self):
        cfg = {MEGA_TREND_CONFIG_KEY: {"active": False}}
        out = evaluate_mega_trend_internal_momentum_kill(cfg)
        self.assertEqual(out.get("reason"), "mega_trend_inactive")


class TestInternalMomentumKillIntegration(unittest.TestCase):
    def _mk_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE forward_trades (
                id INTEGER PRIMARY KEY,
                market TEXT, code TEXT, sector TEXT, sig_type TEXT,
                status TEXT, entry_date TEXT, exit_date TEXT,
                final_ret REAL, mfe REAL, max_high REAL, entry_price REAL,
                sim_stat_ret REAL, exit_type TEXT, exit_reason TEXT,
                invest_amount REAL, sim_kelly_invest REAL,
                scaled_out_frac REAL, free_runner INTEGER, realized_partial_ret REAL
            )
            """
        )
        losses = [
            _loss_row("CLOSED_LOSS", "2026-02-01", -2.0),
            _loss_row("CLOSED_LOSS", "2026-02-02", 0.3),
            _loss_row("CLOSED_LOSS", "2026-02-03", -1.5),
            _loss_row("CLOSED_WIN", "2026-02-04", 0.8),
            _loss_row("CLOSED_LOSS", "2026-02-05", -2.5),
        ]
        for st, ed, fr, et in losses:
            conn.execute(
                """
                INSERT INTO forward_trades (
                    market, code, sector, sig_type, status, entry_date, exit_date,
                    final_ret, mfe, max_high, entry_price, sim_stat_ret,
                    exit_type, invest_amount, sim_kelly_invest, scaled_out_frac
                ) VALUES (
                    'KR','005930','반도체/IT','#MegaTrend언락',?, '2026-02-01', ?,
                    ?, 6.0, 70000, 65000, ?, ?, 1e6, 1e6, 0.0
                )
                """,
                (st, ed, fr, fr, et),
            )
        conn.execute(
            """
            INSERT INTO forward_trades (
                market, code, sector, sig_type, status, entry_date,
                sim_stat_ret, invest_amount, sim_kelly_invest, scaled_out_frac
            ) VALUES (
                'KR','000660','반도체/IT','#MegaTrend언락','OPEN','2026-02-06',
                1.2, 1e6, 1e6, 0.0
            )
            """
        )
        conn.commit()
        return conn

    def test_full_pipeline_diagnose_then_kill(self):
        conn = self._mk_conn()
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
            }
        }
        saved: dict = {}

        def _save(c):
            saved.clear()
            saved.update(c)
            return True

        diag_out = refresh_mega_trend_internal_diagnostics(
            cfg, save_config_fn=_save, conn=conn
        )
        self.assertTrue(diag_out.get("any_momentum_lost"))

        kill_out = refresh_mega_trend_internal_momentum_kill(
            cfg, save_config_fn=_save, conn=conn
        )
        self.assertTrue(kill_out.get("kill"))

        block = saved[MEGA_TREND_CONFIG_KEY]
        self.assertFalse(block["active"])
        self.assertTrue(block[FORGIVENESS_REVOKED_KEY])
        self.assertFalse(block["rotation_advantage_active"])
        self.assertIn("internal_momentum_kill_at", block)
        self.assertGreaterEqual(kill_out.get("liquidation", {}).get("scaled", 0), 1)

    def test_toxic_kill_skipped_after_internal_kill(self):
        conn = self._mk_conn()
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
                "internal_diagnostics": {
                    "any_momentum_lost": True,
                    "momentum_lost_sectors": ["반도체/IT"],
                    "sectors": {
                        "반도체/IT": {
                            "momentum_lost": True,
                            "reason": "internal_momentum_lost",
                        }
                    },
                },
            },
            "ANTI_PATTERNS": {},
        }

        refresh_mega_trend_internal_momentum_kill(cfg, conn=conn)
        toxic = evaluate_mega_trend_toxic_kill(cfg, conn)
        self.assertFalse(toxic.get("kill"))
        self.assertEqual(toxic.get("reason"), "mega_trend_inactive")

    def test_rl_event_recorded_on_kill(self):
        conn = self._mk_conn()
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
            }
        }
        refresh_mega_trend_internal_diagnostics(cfg, conn=conn)
        refresh_mega_trend_internal_momentum_kill(cfg, conn=conn)
        rl = cfg.get("MEGA_TREND_KILL_RL_STATE") or {}
        events = rl.get("kill_events") or []
        self.assertGreaterEqual(len(events), 1)
        self.assertEqual(events[-1].get("kill_type"), "internal_momentum")


class TestInternalKillIgnitionCooldown(unittest.TestCase):
    def test_internal_kill_triggers_p0_cooldown(self):
        from mega_trend_ignition import assess_toxic_kill_cooldown, refresh_mega_trend_ignition

        today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prev = {"internal_momentum_kill_at": today}
        cooldown = assess_toxic_kill_cooldown(prev)
        self.assertTrue(cooldown.get("active"))

        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": False,
                "primary_sector": "반도체/IT",
                "correlation_forgiveness_revoked": True,
                "internal_momentum_kill_at": today,
                "internal_diagnostics": {"any_momentum_lost": True},
            }
        }
        detection = {
            "ignited": True,
            "sectors": ["반도체/IT"],
            "primary_sector": "반도체/IT",
            "primary_detail": {"turnover_share_pct": 35.0, "flow_z": 2.5},
            "candidates_checked": 1,
            "turnover_snapshot": {},
        }
        with patch(
            "mega_trend_ignition.detect_mega_trend_sectors",
            return_value=detection,
        ):
            state = refresh_mega_trend_ignition(cfg)
        self.assertFalse(state["active"])
        self.assertIn("ignition_blocked_reason", state)


if __name__ == "__main__":
    unittest.main()
