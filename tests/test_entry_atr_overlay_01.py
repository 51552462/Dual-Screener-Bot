"""ENTRY-ATR-OVERLAY-01 Phase 1 — observe-only; live Kelly unchanged."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from entry_atr_overlay import (
    MIN_US_ATR_HIST,
    US_WIPEOUT_START,
    empirical_percentile,
    identity_ok,
    in_us_wipeout_window,
    observe_entry_atr_kelly,
    plan_multipliers,
    would_be_from_plans,
)


class TestEntryAtrOverlay01(unittest.TestCase):
    def test_kr_never_shrinks(self) -> None:
        hist = [1.0] * MIN_US_ATR_HIST + [9.0]
        stamp = observe_entry_atr_kelly(
            market="KR",
            entry_atr=9.0,
            entry_date="2026-09-26",
            hist=hist,
        )
        self.assertEqual(stamp["reason"], "kr_excluded")
        self.assertEqual(stamp["would_be_kelly_mult"], 1.0)
        self.assertEqual(stamp["actual_kelly_mult"], 1.0)
        self.assertEqual(stamp["entry_atr_wipeout_window"], 0)

    def test_us_high_atr_would_be_shrinks_actual_stays_one(self) -> None:
        hist = [1.0] * MIN_US_ATR_HIST
        stamp = observe_entry_atr_kelly(
            market="US",
            entry_atr=100.0,
            entry_date="2026-09-01",
            hist=hist,
        )
        self.assertEqual(stamp["reason"], "observe_us")
        self.assertLess(stamp["would_be_kelly_mult"], 1.0)
        self.assertAlmostEqual(stamp["would_be_kelly_mult"], 0.50)
        self.assertEqual(stamp["actual_kelly_mult"], 1.0)
        self.assertEqual(stamp["applied_kelly_mult"], 1.0)
        self.assertEqual(stamp["entry_atr_wipeout_window"], 1)
        plans = json.loads(stamp["entry_atr_plan_json"])
        self.assertAlmostEqual(plans["A"], 0.50)
        self.assertAlmostEqual(plans["B"], 0.70)
        self.assertAlmostEqual(plans["C"], 0.85)

    def test_us_low_atr_no_shrink(self) -> None:
        hist = list(range(1, MIN_US_ATR_HIST + 1))
        stamp = observe_entry_atr_kelly(
            market="US",
            entry_atr=1.0,
            entry_date="2026-08-01",
            hist=hist,
        )
        self.assertEqual(stamp["would_be_kelly_mult"], 1.0)
        self.assertEqual(stamp["actual_kelly_mult"], 1.0)
        self.assertEqual(stamp["entry_atr_wipeout_window"], 0)

    def test_insufficient_hist_neutral(self) -> None:
        stamp = observe_entry_atr_kelly(
            market="US",
            entry_atr=99.0,
            entry_date="2026-09-26",
            hist=[1.0, 2.0],
        )
        self.assertEqual(stamp["reason"], "insufficient_us_atr_hist")
        self.assertEqual(stamp["would_be_kelly_mult"], 1.0)

    def test_wipeout_window_flag(self) -> None:
        self.assertTrue(in_us_wipeout_window("US", US_WIPEOUT_START.isoformat()))
        self.assertFalse(in_us_wipeout_window("US", "2026-08-23"))
        self.assertFalse(in_us_wipeout_window("KR", "2026-09-26"))

    def test_identity_ok_requires_unchanged_kelly(self) -> None:
        stamp = {
            "actual_kelly_mult": 1.0,
            "applied_kelly_mult": 1.0,
            "would_be_kelly_mult": 0.5,
        }
        self.assertTrue(identity_ok(stamp, 0.02, 0.02))
        self.assertFalse(identity_ok(stamp, 0.02, 0.01))

    def test_plan_ladder(self) -> None:
        self.assertAlmostEqual(would_be_from_plans(plan_multipliers(0.95)), 0.50)
        self.assertAlmostEqual(would_be_from_plans(plan_multipliers(0.85)), 0.70)
        self.assertAlmostEqual(would_be_from_plans(plan_multipliers(0.75)), 0.85)
        self.assertAlmostEqual(would_be_from_plans(plan_multipliers(0.50)), 1.0)

    def test_percentile_rank(self) -> None:
        hist = [1.0] * MIN_US_ATR_HIST
        self.assertEqual(empirical_percentile(1.0, hist), 1.0)
        self.assertIsNone(empirical_percentile(1.0, [1.0]))

    def test_fetch_hist_skips_observe_rows(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        import os

        os.close(fd)
        try:
            conn = sqlite3.connect(path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE forward_trades (market TEXT, entry_atr REAL, sig_type TEXT)"
            )
            for i in range(MIN_US_ATR_HIST):
                cur.execute(
                    "INSERT INTO forward_trades VALUES ('US', ?, 'LIVE')",
                    (1.0 + i * 0.01,),
                )
            cur.execute(
                "INSERT INTO forward_trades VALUES ('US', 999.0, 'OBSERVE_ONLY')"
            )
            cur.execute(
                "INSERT INTO forward_trades VALUES ('KR', 50.0, 'LIVE')"
            )
            conn.commit()
            from entry_atr_overlay import fetch_us_entry_atr_hist

            hist = fetch_us_entry_atr_hist(cur)
            self.assertEqual(len(hist), MIN_US_ATR_HIST)
            self.assertNotIn(999.0, hist)
            conn.close()
        finally:
            os.remove(path)

    def test_shared_does_not_multiply_kelly_by_would_be(self) -> None:
        src = Path("forward/shared.py").read_text(encoding="utf-8")
        self.assertIn("observe_entry_atr_kelly", src)
        self.assertIn("kelly_risk_pct = _kelly_before_atr_obs", src)
        self.assertNotIn("kelly_risk_pct *= float(_atr_obs", src)
        self.assertNotIn("kelly_risk_pct * would_be", src)

    def test_insert_cols_include_overlay(self) -> None:
        from forward.shared import _FORWARD_TRADE_INSERT_COLS

        for col in (
            "would_be_kelly_mult",
            "actual_kelly_mult",
            "entry_atr_pct",
            "entry_atr_wipeout_window",
            "entry_atr_plan_json",
        ):
            self.assertIn(col, _FORWARD_TRADE_INSERT_COLS)

    def test_init_forward_db_adds_overlay_columns(self) -> None:
        import os

        from forward.shared import init_forward_db

        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            init_forward_db(path)
            conn = sqlite3.connect(path)
            names = {
                row[1]
                for row in conn.execute("PRAGMA table_info(forward_trades)").fetchall()
            }
            conn.close()
            self.assertIn("would_be_kelly_mult", names)
            self.assertIn("actual_kelly_mult", names)
            self.assertIn("entry_atr_wipeout_window", names)
        finally:
            os.remove(path)


if __name__ == "__main__":
    unittest.main()
