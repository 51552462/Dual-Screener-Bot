"""P6 — Cross-sector contamination guard 테스트."""
from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from mega_trend_kill_rl import (
    CONTAMINATION_FLAG_HIGH,
    CONTAMINATION_FLAG_OK,
    KILL_TYPE_INTERNAL_MOMENTUM,
    MEGA_TREND_KILL_RL_STATE_KEY,
    MEGA_TREND_SECTOR_DELTAS_KEY,
    MEGA_TREND_SECTOR_QUARANTINE_KEY,
    assess_sector_overlay_eligibility,
    compute_kill_event_sector_purity,
    compute_kill_feedback_rates,
    evolve_mega_trend_kill_sensitivity,
    measure_post_kill_sector_outcome,
    record_mega_trend_kill_event,
    resolve_effective_kill_rl_state,
    sanitize_sector_deltas,
)


class TestSectorPurity(unittest.TestCase):
    def test_high_purity_when_tagged_dominant(self):
        purity = compute_kill_event_sector_purity(
            {
                "n_trades": 4,
                "n_trades_sector_all": 4,
                "n_trades_tagged": 4,
            }
        )
        self.assertGreaterEqual(purity, 0.9)

    def test_low_purity_when_mostly_untagged(self):
        purity = compute_kill_event_sector_purity(
            {
                "n_trades": 1,
                "n_trades_sector_all": 10,
                "n_trades_tagged": 0,
            }
        )
        self.assertLess(purity, 0.55)


class TestSanitizeSectorDeltas(unittest.TestCase):
    def test_rekeys_alias_to_standard(self):
        merged, notes = sanitize_sector_deltas(
            {
                "반도체": {
                    "win_rate_min_delta": -0.02,
                    "internal_n": 1,
                }
            }
        )
        self.assertIn("반도체/IT", merged)
        self.assertAlmostEqual(merged["반도체/IT"]["win_rate_min_delta"], -0.02)
        self.assertTrue(any("rekey" in n for n in notes))


class TestOverlayEligibilityGuard(unittest.TestCase):
    def _block(self):
        return {
            "win_rate_min_delta": -0.02,
            "internal_n": 2,
            "climax_n": 1,
            "avg_purity": 0.8,
            "bound_ignited_at": "2026-02-01",
        }

    def test_blocks_quarantined_sector(self):
        ok, reason = assess_sector_overlay_eligibility(
            self._block(),
            "반도체/IT",
            rl_state={MEGA_TREND_SECTOR_QUARANTINE_KEY: {"반도체/IT": {"count": 1}}},
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "quarantined")

    def test_blocks_ignition_mismatch(self):
        ok, reason = assess_sector_overlay_eligibility(
            self._block(),
            "반도체/IT",
            ignited_at="2026-03-01",
        )
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("ignition_mismatch"))

    def test_blocks_inactive_sector(self):
        ok, reason = assess_sector_overlay_eligibility(
            self._block(),
            "반도체/IT",
            active_sectors=["2차전지/배터리"],
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "inactive_sector")


class TestResolveEffectiveWithGuard(unittest.TestCase):
    def test_overlay_blocked_when_quarantined(self):
        sector = "반도체/IT"
        rl = {
            "win_rate_min_delta": -0.01,
            MEGA_TREND_SECTOR_DELTAS_KEY: {
                sector: {"win_rate_min_delta": -0.03, "internal_n": 2, "avg_purity": 0.9}
            },
            MEGA_TREND_SECTOR_QUARANTINE_KEY: {sector: {"count": 1}},
        }
        eff = resolve_effective_kill_rl_state(
            rl,
            sector=sector,
            ignited_at="2026-02-01",
            active_sectors=[sector],
        )
        self.assertFalse(eff.get("_kill_rl_overlay"))
        self.assertEqual(eff.get("_kill_rl_overlay_block_reason"), "quarantined")
        self.assertAlmostEqual(eff["win_rate_min_delta"], -0.01)


class TestRecordEventSectorSSOT(unittest.TestCase):
    def test_record_normalizes_sector_std(self):
        cfg = {MEGA_TREND_KILL_RL_STATE_KEY: {"kill_events": []}}
        ev = record_mega_trend_kill_event(
            cfg,
            sector="반도체",
            kill_type=KILL_TYPE_INTERNAL_MOMENTUM,
            snapshot={"sectors": ["반도체", "2차전지"]},
        )
        self.assertEqual(ev["sector_std"], "반도체/IT")
        self.assertEqual(ev["sector_raw"], "반도체")
        self.assertIn("반도체/IT", ev["affected_sectors"])


class TestFeedbackExcludesContaminated(unittest.TestCase):
    def test_high_contamination_excluded_from_sector_rates(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        sector = "반도체/IT"
        events = [
            {
                "kill_at": recent,
                "sector": sector,
                "sector_std": sector,
                "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                "outcome": "defense_success",
                "contamination_flag": CONTAMINATION_FLAG_HIGH,
            },
            {
                "kill_at": recent,
                "sector": sector,
                "sector_std": sector,
                "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                "outcome": "opportunity_cost",
                "contamination_flag": CONTAMINATION_FLAG_OK,
            },
        ]
        rates = compute_kill_feedback_rates(events, sector=sector)
        self.assertEqual(rates["n"], 1)


class TestMeasureUsesNormalizedSector(unittest.TestCase):
    def test_alias_sector_matches_standard_trades(self):
        import sqlite3

        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE forward_trades (
                market TEXT, code TEXT, sector TEXT, sig_type TEXT,
                sim_stat_ret REAL, final_ret REAL, status TEXT,
                entry_date TEXT, exit_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO forward_trades VALUES
            ('KR','005930','반도체/IT','MegaTrend_Unlock',3.0,3.0,'CLOSED_WIN','2026-02-08','2026-02-09')
            """
        )
        out = measure_post_kill_sector_outcome(
            conn,
            "반도체",
            "2026-02-07",
            eval_days=5,
            mega_trend_only=False,
        )
        self.assertEqual(out.get("n_trades"), 1)
        self.assertEqual(out.get("sector_std"), "반도체/IT")


class TestEvolveQuarantinePath(unittest.TestCase):
    def test_low_purity_event_triggers_quarantine_on_evolve(self):
        recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
        sector = "반도체/IT"
        cfg = {
            MEGA_TREND_KILL_RL_STATE_KEY: {
                "kill_events": [
                    {
                        "sector": sector,
                        "sector_std": sector,
                        "kill_at": recent,
                        "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                        "outcome": "defense_success",
                        "contamination_flag": CONTAMINATION_FLAG_HIGH,
                        "sector_purity": 0.2,
                    }
                ],
                MEGA_TREND_SECTOR_DELTAS_KEY: {},
            }
        }
        with patch(
            "mega_trend_kill_rl.evaluate_pending_kill_events",
            side_effect=lambda ev, conn, now=None: list(ev),
        ):
            with patch("mega_trend_kill_rl._persist_kill_rl_state"):
                with patch.dict(os.environ, {"MEGA_TREND_KILL_RL_QUARANTINE_STRIKES": "1"}):
                    out = evolve_mega_trend_kill_sensitivity(cfg, db_path=None, persist=False)

        quarantine = out["state"].get(MEGA_TREND_SECTOR_QUARANTINE_KEY) or {}
        self.assertIn(sector, quarantine)
        summary = out["state"].get("last_evolve_summary") or {}
        self.assertTrue(summary.get("contamination_guard"))


if __name__ == "__main__":
    unittest.main()
