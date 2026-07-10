"""Mega-Trend Climax Kill-Switch (3번) — 재검증 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

import pandas as pd

from mega_trend_climax import (
    MEGA_TREND_CLIMAX_EXIT_TAG,
    detect_climax_trap_from_bars,
    detect_sector_climax_trap,
    detect_sector_flow_reversal,
    evaluate_mega_trend_climax,
    liquidate_mega_trend_sector_positions,
    refresh_mega_trend_climax_kill,
)
from mega_trend_ignition import MEGA_TREND_CONFIG_KEY


def _ledger_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY,
            market TEXT, code TEXT, sector TEXT, sig_type TEXT,
            status TEXT, scaled_out_frac REAL, sim_stat_ret REAL,
            free_runner INTEGER, exit_date TEXT, exit_reason TEXT, final_ret REAL,
            realized_partial_ret REAL, sim_kelly_invest REAL, invest_amount REAL,
            sim_stat_status TEXT, sim_tech_status TEXT, sim_breadth_status TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO forward_trades (
            market, code, sector, sig_type, status,
            scaled_out_frac, sim_stat_ret, free_runner,
            sim_kelly_invest, invest_amount
        ) VALUES ('KR', '005930', '반도체/IT', 'TEST', 'OPEN', 0, 5.0, 0, 1e6, 1e6)
        """
    )
    conn.commit()
    return conn


class TestFlowReversal(unittest.TestCase):
    def test_reversal_on_z_drop(self):
        with patch(
            "kr_flow_factor.compute_sector_flow_zscore",
            return_value={"z_score": 0.5, "window_krw": 1e8, "reason": "computed"},
        ):
            with patch(
                "mega_trend_climax._detect_recent_flow_flip",
                return_value={"flip": False, "reason": ""},
            ):
                out = detect_sector_flow_reversal("반도체/IT", ignition_flow_z=3.0)
        self.assertTrue(out["reversal"])

    def test_reversal_on_net_outflow(self):
        with patch(
            "kr_flow_factor.compute_sector_flow_zscore",
            return_value={"z_score": 1.5, "window_krw": -5e9, "reason": "computed"},
        ):
            with patch(
                "mega_trend_climax._detect_recent_flow_flip",
                return_value={"flip": False, "reason": ""},
            ):
                out = detect_sector_flow_reversal("반도체/IT", ignition_flow_z=3.0)
        self.assertTrue(out["reversal"])
        self.assertIn("net_outflow", out["reason"])

    def test_reversal_on_flow_flip(self):
        with patch(
            "kr_flow_factor.compute_sector_flow_zscore",
            return_value={"z_score": 2.0, "window_krw": 1e9, "reason": "computed"},
        ):
            with patch(
                "mega_trend_climax._detect_recent_flow_flip",
                return_value={"flip": True, "reason": "smart_money_2d_outflow_flip"},
            ):
                out = detect_sector_flow_reversal("반도체/IT")
        self.assertTrue(out["reversal"])

    def test_no_reversal_when_still_strong(self):
        with patch(
            "kr_flow_factor.compute_sector_flow_zscore",
            return_value={"z_score": 2.2, "window_krw": 2e9, "reason": "computed"},
        ):
            with patch(
                "mega_trend_climax._detect_recent_flow_flip",
                return_value={"flip": False, "reason": ""},
            ):
                out = detect_sector_flow_reversal("반도체/IT", ignition_flow_z=2.8)
        self.assertFalse(out["reversal"])


class TestClimaxTrap(unittest.TestCase):
    def _trap_bars(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "open": [100.0, 102.0, 103.0],
                "high": [101.0, 103.0, 103.5],
                "low": [99.0, 100.5, 98.0],
                "close": [100.5, 101.0, 99.0],
                "volume": [1000.0, 1200.0, 900.0],
            }
        )

    def test_engulfing_trap_from_bars(self):
        out = detect_climax_trap_from_bars(self._trap_bars())
        self.assertTrue(out["climax_trap"])

    def test_sector_trap_with_forensics_path(self):
        with patch(
            "mega_trend_climax._sector_proxy_codes",
            return_value=["005930", "000660"],
        ):
            with patch(
                "mega_trend_climax._aggregate_sector_proxy_bars",
                return_value=self._trap_bars(),
            ):
                with patch(
                    "mega_trend_climax._recent_trade_ymd",
                    return_value="20260201",
                ):
                    out = detect_sector_climax_trap("반도체/IT", use_forensics=True)
        self.assertTrue(out["climax_trap"])
        self.assertEqual(out["source"], "forensics")


class TestMultiSectorEvaluate(unittest.TestCase):
    def test_kill_any_active_sector(self):
        state = {
            "active": True,
            "primary_sector": "반도체/IT",
            "sectors": ["반도체/IT", "에너지/화학"],
            "flow_z": 3.0,
        }
        ok_v = {"kill": False, "reason": "no", "sector": "반도체/IT", "exit_mode": "scale_out"}
        kill_v = {
            "kill": True,
            "reason": "climax_trap",
            "sector": "에너지/화학",
            "exit_mode": "full",
            "flow_reversal": {},
            "climax_trap": {"climax_trap": True},
        }
        with patch(
            "mega_trend_climax.evaluate_sector_climax",
            side_effect=[ok_v, kill_v],
        ):
            v = evaluate_mega_trend_climax(state)
        self.assertTrue(v["kill"])
        self.assertEqual(v["sector"], "에너지/화학")
        self.assertEqual(v["exit_mode"], "full")
        self.assertIn("에너지/화학", v["sectors"])


class TestClimaxKill(unittest.TestCase):
    def test_evaluate_kill_on_reversal(self):
        state = {
            "active": True,
            "primary_sector": "반도체/IT",
            "sectors": ["반도체/IT"],
            "flow_z": 3.0,
        }
        with patch(
            "mega_trend_climax.evaluate_sector_climax",
            return_value={
                "kill": True,
                "exit_mode": "scale_out",
                "sector": "반도체/IT",
                "reason": "flow_z_drop",
                "flow_reversal": {"reversal": True},
                "climax_trap": {"climax_trap": False},
            },
        ):
            v = evaluate_mega_trend_climax(state)
        self.assertTrue(v["kill"])
        self.assertEqual(v["exit_mode"], "scale_out")

    def test_liquidate_full_on_climax_trap(self):
        conn = _ledger_conn()
        liq = liquidate_mega_trend_sector_positions(
            conn,
            ["반도체/IT"],
            exit_mode="full",
            exit_reason="test_kill",
        )
        self.assertEqual(liq["liquidated"], 1)
        row = conn.execute(
            "SELECT status, exit_reason, sim_stat_status FROM forward_trades WHERE id=1"
        ).fetchone()
        self.assertEqual(row[0], "CLOSED_WIN")
        self.assertIn("test_kill", row[1])
        self.assertEqual(row[2], "CLOSED_WIN")

    def test_liquidate_scale_out_reduces_capital(self):
        conn = _ledger_conn()
        with patch(
            "mega_trend_climax._resolve_exit_fraction",
            return_value=0.75,
        ):
            liq = liquidate_mega_trend_sector_positions(
                conn,
                ["반도체/IT"],
                exit_mode="scale_out",
                exit_reason=f"{MEGA_TREND_CLIMAX_EXIT_TAG}: test_scale",
            )
        self.assertEqual(liq["scaled"], 1)
        row = conn.execute(
            """
            SELECT status, scaled_out_frac, sim_kelly_invest, free_runner, exit_reason
            FROM forward_trades WHERE id=1
            """
        ).fetchone()
        self.assertEqual(row[0], "OPEN")
        self.assertAlmostEqual(row[1], 0.75, places=2)
        self.assertAlmostEqual(row[2], 250000.0, places=0)
        self.assertEqual(row[3], 1)
        self.assertIn(MEGA_TREND_CLIMAX_EXIT_TAG, row[4])

    def test_refresh_deactivates_rotation_and_liquidates(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "flow_z": 3.0,
                "rotation_advantage_active": True,
            }
        }
        saved = {}

        def _save(c):
            saved.update(c)
            return True

        verdict = {
            "kill": True,
            "exit_mode": "full",
            "sector": "반도체/IT",
            "sectors": ["반도체/IT"],
            "reason": "climax_trap",
        }
        conn = _ledger_conn()
        with patch(
            "mega_trend_climax.evaluate_mega_trend_climax",
            return_value=verdict,
        ):
            out = refresh_mega_trend_climax_kill(
                cfg, save_config_fn=_save, conn=conn
            )
        self.assertTrue(out["kill"])
        block = saved[MEGA_TREND_CONFIG_KEY]
        self.assertFalse(block["active"])
        self.assertFalse(block["rotation_advantage_active"])
        self.assertIn("climax_kill_at", block)
        self.assertEqual(out["liquidation"]["liquidated"], 1)
        rl = saved.get("MEGA_TREND_KILL_RL_STATE") or cfg.get("MEGA_TREND_KILL_RL_STATE")
        events = (rl or {}).get("kill_events") or []
        self.assertGreaterEqual(len(events), 1)
        self.assertEqual(events[-1].get("kill_type"), "climax_external")
        self.assertEqual(events[-1].get("kill_lane"), "external")


if __name__ == "__main__":
    unittest.main()
