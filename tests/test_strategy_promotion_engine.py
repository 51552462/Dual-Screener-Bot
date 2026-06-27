"""strategy_promotion_engine — LIVE Hard Gate · Whipsaw 정의 단위 검증."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone

from strategy_lifecycle_config import load_strategy_lifecycle_config, market_params
from strategy_promotion_engine import (
    is_below_live_threshold,
    passes_candidate_gate,
    passes_live_hard_gate,
    run_registry_lifecycle,
    stable_strategy_id,
)
from strategy_registry_store import (
    consecutive_below_live_days,
    ensure_strategy_registry_schema,
    record_quality_daily,
)


class TestPromotionGates(unittest.TestCase):
    def test_live_hard_gate_kr_wr50(self):
        mp = market_params(load_strategy_lifecycle_config(), "KR")
        hv = {"n": 20, "rolling_wr": 0.52, "rolling_pf": 1.40, "mult": 1.0}
        self.assertTrue(passes_live_hard_gate(hv, mp))

    def test_live_hard_gate_kr_mid_band_pf(self):
        mp = market_params(load_strategy_lifecycle_config(), "KR")
        hv = {"n": 20, "rolling_wr": 0.47, "rolling_pf": 1.55, "mult": 1.0}
        self.assertTrue(passes_live_hard_gate(hv, mp))

    def test_live_hard_gate_kr_mid_band_fail_pf(self):
        mp = market_params(load_strategy_lifecycle_config(), "KR")
        hv = {"n": 20, "rolling_wr": 0.47, "rolling_pf": 1.40, "mult": 1.0}
        self.assertFalse(passes_live_hard_gate(hv, mp))

    def test_candidate_gate_vs_live(self):
        mp = market_params(load_strategy_lifecycle_config(), "KR")
        hv = {"n": 16, "rolling_wr": 0.46, "rolling_pf": 1.25, "mdd_pct": -10, "mult": 1.0}
        self.assertTrue(passes_candidate_gate(hv, mp))
        self.assertFalse(passes_live_hard_gate(hv, mp))

    def test_below_live_threshold(self):
        mp = market_params(load_strategy_lifecycle_config(), "KR")
        self.assertFalse(is_below_live_threshold({"rolling_wr": 0.52, "rolling_pf": 1.40}, mp))
        self.assertTrue(is_below_live_threshold({"rolling_wr": 0.40, "rolling_pf": 1.60}, mp))


class TestWhipsawStreak(unittest.TestCase):
    def test_consecutive_below_days_db(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "m.sqlite")
            sqlite3.connect(db).close()
            ensure_strategy_registry_schema(db)
            sid = stable_strategy_id("KR", "TEST_LOGIC")
            record_quality_daily(
                sid,
                "KR",
                rolling_wr=0.55,
                rolling_pf=1.4,
                below_live_threshold=False,
                trade_date="2026-05-16",
                db_path=db,
            )
            for d in ("2026-05-17", "2026-05-18"):
                record_quality_daily(
                    sid,
                    "KR",
                    rolling_wr=0.4,
                    rolling_pf=1.0,
                    below_live_threshold=True,
                    trade_date=d,
                    db_path=db,
                )
            self.assertEqual(consecutive_below_live_days(sid, db_path=db), 2)


class TestDiscovery(unittest.TestCase):
    def test_discovery_from_health(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "m.sqlite")
            open(db, "a").close()
            health = {
                "__meta__": {"window_days_kst": 90},
                "KR|SUPERNOVA_S4": {
                    "n": 18,
                    "rolling_wr": 0.48,
                    "rolling_pf": 1.30,
                    "mdd_pct": -12,
                    "mult": 1.0,
                },
            }
            reg, stats = run_registry_lifecycle(
                prior_registry=[],
                health=health,
                forward_db_path=db,
                now=datetime(2026, 5, 18, tzinfo=timezone.utc),
            )
            self.assertGreaterEqual(stats["discovery_new"], 1)
            self.assertTrue(any(r.get("state") in ("OBSERVING", "CANDIDATE") for r in reg))


if __name__ == "__main__":
    unittest.main()
