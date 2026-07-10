"""Mega-Trend Toxic Graveyard Kill-Switch (내부 2번) 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from mega_trend_ignition import MEGA_TREND_CONFIG_KEY
from mega_trend_toxic_kill import (
    FORGIVENESS_REVOKED_KEY,
    detect_consecutive_loss_streak,
    evaluate_mega_trend_toxic_kill,
    is_mega_trend_forgiveness_revoked,
    refresh_mega_trend_toxic_graveyard_kill,
    register_mega_trend_sector_toxic_bbox,
    resolve_defensive_exit_fraction,
    revoke_mega_trend_correlation_forgiveness,
    scan_sector_toxic_bbox_signals,
)
from portfolio_risk_overlay import (
    apply_mega_trend_correlation_forgiveness,
    evaluate_mega_trend_forgiveness_eligibility,
)


def _loss_trade(ret: float = -2.0, **kw) -> dict:
    base = {
        "status": "CLOSED_LOSS",
        "final_ret": ret,
        "sim_stat_ret": ret,
        "dyn_cpv": 0.55,
        "dyn_tb": 12.0,
        "v_energy": 18.0,
    }
    base.update(kw)
    return base


class TestConsecutiveLossStreak(unittest.TestCase):
    def test_three_losses_trigger(self):
        trades = [
            {"status": "CLOSED_WIN", "final_ret": 5.0},
            _loss_trade(-1),
            _loss_trade(-2),
            _loss_trade(-3),
        ]
        out = detect_consecutive_loss_streak(trades, min_streak=3)
        self.assertTrue(out["triggered"])
        self.assertEqual(out["streak"], 3)

    def test_no_trigger_on_win_break(self):
        trades = [
            _loss_trade(-1),
            {"status": "CLOSED_WIN", "final_ret": 3.0},
        ]
        out = detect_consecutive_loss_streak(trades, min_streak=3)
        self.assertFalse(out["triggered"])


class TestToxicBBoxScan(unittest.TestCase):
    def test_detects_new_sector_toxic_rule(self):
        cfg = {
            "ANTI_PATTERNS": {
                "TOXIC_PATTERN_01": {
                    "source": "TOXIC_ML",
                    "sector_match": "반도체/IT",
                    "dyn_cpv_min": 0.1,
                    "dyn_cpv_max": 0.9,
                    "created_at": "2026-02-05",
                }
            }
        }
        scan = scan_sector_toxic_bbox_signals(
            cfg, "반도체/IT", ignited_at="2026-02-01", known_keys=[]
        )
        self.assertTrue(scan["toxic_signal"])
        self.assertIn("TOXIC_PATTERN_01", scan["new_rule_keys"])

    def test_ignores_known_keys(self):
        cfg = {
            "ANTI_PATTERNS": {
                "TOXIC_PATTERN_01": {
                    "source": "TOXIC_ML",
                    "sector_match": "반도체/IT",
                    "dyn_cpv_min": 0.1,
                    "dyn_cpv_max": 0.9,
                    "created_at": "2026-02-05",
                }
            }
        }
        scan = scan_sector_toxic_bbox_signals(
            cfg,
            "반도체/IT",
            ignited_at="2026-02-01",
            known_keys=["TOXIC_PATTERN_01"],
        )
        self.assertFalse(scan["toxic_signal"])

    def test_toxic_fade_target_hit(self):
        cfg = {
            "ANTI_PATTERNS": {},
            "TOXIC_FADE_TARGETS": {
                "RANK_A": {
                    "sector": "에너지/화학",
                    "win_rate": 0.2,
                    "n": 10,
                }
            },
        }
        scan = scan_sector_toxic_bbox_signals(cfg, "에너지/화학")
        self.assertTrue(scan["toxic_signal"])
        self.assertTrue(scan["toxic_fade_hit"])


class TestForgivenessRevocation(unittest.TestCase):
    def _mega_cfg(self, *, revoked: bool = False):
        return {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "sectors": ["반도체/IT"],
                "primary_sector": "반도체/IT",
                "rotation_advantage_active": not revoked,
                FORGIVENESS_REVOKED_KEY: revoked,
            }
        }

    def test_revoke_state(self):
        state = {"active": True, "rotation_advantage_active": True}
        out = revoke_mega_trend_correlation_forgiveness(state, reason="toxic_test")
        self.assertTrue(out[FORGIVENESS_REVOKED_KEY])
        self.assertFalse(out["rotation_advantage_active"])

    def test_forgiveness_blocked_when_revoked(self):
        cfg = self._mega_cfg(revoked=True)
        self.assertTrue(is_mega_trend_forgiveness_revoked(cfg))
        self.assertFalse(
            evaluate_mega_trend_forgiveness_eligibility(cfg, "반도체/IT")
        )
        sizing = apply_mega_trend_correlation_forgiveness(
            {"action": "penalty", "kelly_mult": 0.5},
            sys_config=cfg,
            candidate_sector="반도체/IT",
        )
        self.assertEqual(sizing["action"], "penalty")

    def test_defensive_exit_fraction_high(self):
        frac = resolve_defensive_exit_fraction()
        self.assertGreaterEqual(frac, 0.75)


    def test_rotation_advantage_revoked(self):
        from mega_trend_ignition import is_mega_trend_rotation_advantage

        cfg = self._mega_cfg(revoked=True)
        self.assertFalse(is_mega_trend_rotation_advantage("반도체/IT", cfg))


class TestToxicKillEvaluate(unittest.TestCase):
    def test_kill_on_graveyard_bbox_scan_only(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
                "toxic_watch": {"known_rule_keys": []},
            },
            "ANTI_PATTERNS": {
                "TOXIC_PATTERN_99": {
                    "source": "TOXIC_ML",
                    "sector_match": "반도체/IT",
                    "dyn_cpv_min": 0.1,
                    "dyn_cpv_max": 0.9,
                    "created_at": "2026-02-06",
                }
            },
        }
        out = evaluate_mega_trend_toxic_kill(cfg, conn=None, auto_register_bbox=False)
        self.assertTrue(out.get("kill"))
        self.assertEqual(out.get("exit_mode"), "defensive_exit")

    def test_no_kill_without_signal(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
                "toxic_watch": {"known_rule_keys": ["TOXIC_PATTERN_99"]},
            },
            "ANTI_PATTERNS": {
                "TOXIC_PATTERN_99": {
                    "source": "TOXIC_ML",
                    "sector_match": "반도체/IT",
                    "dyn_cpv_min": 0.1,
                    "dyn_cpv_max": 0.9,
                    "created_at": "2026-02-06",
                }
            },
        }
        out = evaluate_mega_trend_toxic_kill(cfg, conn=None, auto_register_bbox=False)
        self.assertFalse(out.get("kill"))


class TestToxicKillIntegration(unittest.TestCase):
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
                dyn_cpv REAL, dyn_tb REAL, v_energy REAL, dyn_rs REAL,
                sim_stat_status TEXT, sim_tech_status TEXT, sim_breadth_status TEXT,
                scaled_out_frac REAL, free_runner INTEGER, realized_partial_ret REAL
            )
            """
        )
        losses = [
            ("CLOSED_LOSS", "2026-02-03", -2.0, "STAT_MAE"),
            ("CLOSED_LOSS", "2026-02-04", -1.5, "STAT_MAE"),
            ("CLOSED_LOSS", "2026-02-05", -3.0, "STAT_MAE"),
        ]
        for i, (st, ed, fr, et) in enumerate(losses, 1):
            conn.execute(
                """
                INSERT INTO forward_trades (
                    market, code, sector, sig_type, status, entry_date, exit_date,
                    final_ret, mfe, max_high, entry_price, sim_stat_ret,
                    exit_type, invest_amount, sim_kelly_invest,
                    dyn_cpv, dyn_tb, v_energy
                ) VALUES (
                    'KR','005930','반도체/IT','#MegaTrend언락',?,
                    '2026-02-01',?, ?, 4.0, 70000, 65000, ?,
                    ?, 1e6, 1e6, 0.55, 12.0, 18.0
                )
                """,
                (st, ed, fr, fr, et),
            )
        conn.execute(
            """
            INSERT INTO forward_trades (
                market, code, sector, sig_type, status, entry_date,
                sim_stat_ret, invest_amount, sim_kelly_invest,
                dyn_cpv, dyn_tb, v_energy, scaled_out_frac
            ) VALUES (
                'KR','000660','반도체/IT','#MegaTrend언락','OPEN','2026-02-06',
                1.5, 1e6, 1e6, 0.52, 11.0, 17.0, 0.0
            )
            """
        )
        conn.commit()
        return conn

    def test_register_mega_trend_toxic_bbox(self):
        cfg: dict = {"ANTI_PATTERNS": {}}
        trade = _loss_trade(-2.5)
        with patch(
            "clustered_immune_vaccine.register_failed_template",
            return_value={"registered": True, "prune": {"pruned": False}},
        ):
            reg = register_mega_trend_sector_toxic_bbox(
                cfg, "반도체/IT", trade, streak=3
            )
        self.assertTrue(reg.get("registered"))
        ap = cfg["ANTI_PATTERNS"]
        self.assertTrue(isinstance(ap, dict))
        has_sector = any(
            isinstance(v, dict) and v.get("mega_trend_sector") == "반도체/IT"
            for v in ap.values()
        )
        self.assertTrue(has_sector)

    def test_refresh_toxic_kill_revokes_and_scales(self):
        conn = self._mk_conn()
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
                "rotation_advantage_active": True,
                "toxic_watch": {"known_rule_keys": []},
            }
        }
        saved = {}

        def _save(c):
            saved.update(c)
            return True

        def _fake_register(cfg, **kwargs):
            ap = cfg.setdefault("ANTI_PATTERNS", {})
            name = str(kwargs.get("name") or "MEGA_TREND_TEST")
            ap[f"IMMUNE_{name}"] = {
                "label": name,
                "source": "DEEP_EVOLVED_FAIL",
                "dyn_cpv_min": 0.1,
                "dyn_cpv_max": 0.9,
            }
            return {"registered": True, "prune": {}}

        with patch(
            "clustered_immune_vaccine.register_failed_template",
            side_effect=_fake_register,
        ):
            out = refresh_mega_trend_toxic_graveyard_kill(
                cfg, save_config_fn=_save, conn=conn
            )
        self.assertTrue(out.get("kill"))
        block = saved[MEGA_TREND_CONFIG_KEY]
        self.assertFalse(block["active"])
        self.assertTrue(block[FORGIVENESS_REVOKED_KEY])
        self.assertFalse(block["rotation_advantage_active"])
        self.assertGreaterEqual(out.get("liquidation", {}).get("scaled", 0), 1)
        rl_block = saved.get("MEGA_TREND_KILL_RL_STATE") or cfg.get("MEGA_TREND_KILL_RL_STATE")
        if rl_block:
            events = rl_block.get("kill_events") or []
            self.assertGreaterEqual(len(events), 1)
            self.assertEqual(events[-1].get("kill_type"), "toxic_graveyard")


if __name__ == "__main__":
    unittest.main()
