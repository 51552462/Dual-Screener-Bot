"""Ch.4 — Kelly 탄력성 오버레이 테스트."""
from __future__ import annotations

import sqlite3
import unittest

from kelly_elasticity_overlay import (
    apply_elasticity_to_effective_kelly,
    combine_elasticity_mults,
    detect_kelly_inelastic_anomaly,
    evaluate_kelly_elasticity_overlay,
    nav_drawdown_kelly_mult,
    persist_day_clutch_state,
    resolve_day_clutch_mult,
)


class TestNavDrawdownMult(unittest.TestCase):
    def test_below_start_neutral(self):
        ev = nav_drawdown_kelly_mult(1_000_000, 1_020_000, sys_config={})
        self.assertFalse(ev["active"])
        self.assertEqual(ev["kelly_mult"], 1.0)

    def test_dd_265_pct_scales(self):
        # nav 97.35% of hwm → ~2.65% dd
        ev = nav_drawdown_kelly_mult(
            973_500,
            1_000_000,
            sys_config={
                "KELLY_NAV_DD_START_PCT": 2.0,
                "KELLY_NAV_DD_FULL_PCT": 8.0,
                "KELLY_NAV_DD_MIN_MULT": 0.25,
            },
        )
        self.assertTrue(ev["active"])
        self.assertLess(ev["kelly_mult"], 0.95)
        self.assertGreater(ev["kelly_mult"], 0.25)

    def test_full_dd_hits_min(self):
        ev = nav_drawdown_kelly_mult(
            900_000,
            1_000_000,
            sys_config={
                "KELLY_NAV_DD_START_PCT": 2.0,
                "KELLY_NAV_DD_FULL_PCT": 8.0,
                "KELLY_NAV_DD_MIN_MULT": 0.25,
            },
        )
        self.assertAlmostEqual(ev["kelly_mult"], 0.25, places=2)


class TestCombineElasticity(unittest.TestCase):
    def test_multiplicative(self):
        m = combine_elasticity_mults(0.5, 0.8)
        self.assertAlmostEqual(m, 0.4)

    def test_apply_to_effective(self):
        ov = {"elasticity_mult": 0.4, "active": True, "day_mult": 0.5, "nav_mult": 0.8}
        out, det = apply_elasticity_to_effective_kelly(0.01, ov)
        self.assertAlmostEqual(out, 0.004)
        self.assertAlmostEqual(det["effective_pre_overlay"], 0.01)


class TestDayClutchState(unittest.TestCase):
    def test_persist_and_fallback(self):
        cfg: dict = {}
        persist_day_clutch_state(
            {
                "as_of": "2026-07-08",
                "active": True,
                "kelly_mult": 0.15,
                "elasticity_mult": 0.12,
                "block_entry": False,
                "reason": "test",
            },
            sys_config=cfg,
        )
        self.assertIn("KELLY_DAY_CLUTCH_STATE", cfg)
        empty = sqlite3.connect(":memory:")
        empty.execute(
            """
            CREATE TABLE forward_trades (
                market TEXT, status TEXT, exit_date TEXT, final_ret REAL,
                sim_kelly_invest REAL
            )
            """
        )
        out = resolve_day_clutch_mult(
            conn=empty,
            sys_config=cfg,
            today_str="2026-07-08",
            markets=("KR",),
        )
        self.assertTrue(out["active"])
        self.assertAlmostEqual(out["kelly_mult"], 0.15)
        empty.close()


class TestDayClutchFromDb(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            """
            CREATE TABLE forward_trades (
                market TEXT, status TEXT, exit_date TEXT, final_ret REAL,
                sim_kelly_invest REAL
            )
            """
        )
        today = "2026-07-08"
        for i in range(6):
            self.conn.execute(
                """
                INSERT INTO forward_trades VALUES ('KR', 'CLOSED_LOSS', ?, -2.0, 1000000)
                """,
                (today,),
            )
        self.conn.commit()

    def test_worst_mult_across_markets(self):
        out = resolve_day_clutch_mult(
            conn=self.conn,
            sys_config={"ENABLE_CATASTROPHIC_DAY_CLUTCH": True},
            today_str="2026-07-08",
            markets=("KR",),
        )
        self.assertTrue(out["active"])
        self.assertLess(out["kelly_mult"], 0.5)


class TestInelasticAnomaly(unittest.TestCase):
    def test_detects_stuck_kelly(self):
        an = detect_kelly_inelastic_anomaly(
            effective_pre=0.01,
            effective_post=0.01,
            overlay={"elasticity_mult": 0.15, "active": True},
            catastrophic_clutch_active=True,
        )
        self.assertIsNotNone(an)
        self.assertEqual(an["code"], "KELLY_INELASTIC")

    def test_ok_when_scaled(self):
        an = detect_kelly_inelastic_anomaly(
            effective_pre=0.01,
            effective_post=0.0015,
            overlay={"elasticity_mult": 0.15, "active": True},
        )
        self.assertIsNone(an)


class TestEvaluateOverlay(unittest.TestCase):
    def test_disabled_returns_neutral(self):
        ov = evaluate_kelly_elasticity_overlay(
            sys_config={
                "ENABLE_CATASTROPHIC_DAY_CLUTCH": False,
                "ENABLE_KELLY_NAV_DD_OVERLAY": False,
            },
            nav=1_000_000,
            hwm=1_000_000,
        )
        self.assertAlmostEqual(ov["elasticity_mult"], 1.0)


if __name__ == "__main__":
    unittest.main()
