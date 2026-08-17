"""OBS-HOLD panel + Cursor/Claude paste blocks on North Star digest."""
from __future__ import annotations

import unittest

import dual_north_star_ledger as ledger
from dual_north_star_telegram import (
    build_obs_hold_claude_prompt,
    build_obs_hold_cursor_prompt,
    format_north_star_digest_html,
    format_obs_hold_section_html,
)


def _snap(*, cadence: str = "daily", daily_n: int = 8, composite: float = 4.09) -> dict:
    snap = {
        "cadence": cadence,
        "date_kst": "2026-08-17",
        "tracks": {
            "A": {
                "label": "주식 KR+US",
                "phase_label": "운영",
                "mdd_cap_pct": 10,
                "cagr_target_lo": 40,
                "cagr_target_hi": 70,
                "aggregate": {
                    "max_mdd_pct": 2.0,
                    "avg_return_pct": 1.0,
                    "return_pace_score": 5.0,
                    "mdd_safety_score": 80.0,
                    "composite_score": composite,
                },
            },
            "B": {
                "label": "Bitget",
                "phase_label": "B0",
                "mdd_cap_pct": 5,
                "cagr_target_lo": 12,
                "cagr_target_hi": 25,
                "aggregate": {
                    "max_mdd_pct": 1.0,
                    "avg_return_pct": 0.0,
                    "return_pace_score": 0.0,
                    "mdd_safety_score": 80.0,
                    "composite_score": 40.0,
                    "measure_only": True,
                },
            },
        },
        "comparison": {"leader_mode": "side_by_side", "leader_reason": "B0"},
        "ledger": {"A": {"gate": "G0", "gate_label": "측정·구조"}, "B": {"gate": "G0"}},
        "meta": {},
        "period_returns": {"A": {}, "B": {}},
    }
    ledger.enrich_obs_hold_meta(snap, daily_n=daily_n)
    return snap


class TestObsHoldTelegram(unittest.TestCase):
    def test_action_observe_at_n8(self) -> None:
        self.assertEqual(ledger.resolve_obs_hold_action(cadence="daily", daily_n=8), "OBSERVE_HOLD")
        snap = _snap(daily_n=8)
        self.assertEqual(snap["meta"]["cursor_action"], "OBSERVE_HOLD")
        self.assertEqual(snap["meta"]["obs_hold_remaining"], 12)
        self.assertTrue(snap["meta"]["obs_hold_active"])
        html = format_obs_hold_section_html(snap)
        self.assertIn("[OBS_HOLD]", html)
        self.assertIn("8", html)
        self.assertIn("/20", html)
        self.assertIn("---CURSOR---", html)
        self.assertIn("---CLAUDE---", html)
        self.assertIn("OBSERVE_HOLD", html)
        self.assertIn("Alpha Handoff", build_obs_hold_cursor_prompt(snap))
        self.assertIn("신규 Handoff 없음", build_obs_hold_claude_prompt(snap))

    def test_action_recall_at_n20(self) -> None:
        self.assertEqual(ledger.resolve_obs_hold_action(cadence="daily", daily_n=20), "RECALL_FORK")
        snap = _snap(daily_n=20)
        self.assertEqual(snap["meta"]["cursor_action"], "RECALL_FORK")
        self.assertEqual(snap["meta"]["obs_hold_remaining"], 0)
        self.assertFalse(snap["meta"]["obs_hold_active"])
        html = format_obs_hold_section_html(snap)
        self.assertIn("RECALL_FORK", html)
        self.assertIn("재소집", html)
        self.assertIn("갈림길 3택", build_obs_hold_claude_prompt(snap))

    def test_weekly_omits_obs_section(self) -> None:
        self.assertEqual(ledger.resolve_obs_hold_action(cadence="weekly", daily_n=8), "NONE")
        snap = _snap(cadence="weekly", daily_n=8)
        self.assertEqual(snap["meta"]["cursor_action"], "NONE")
        self.assertEqual(format_obs_hold_section_html(snap), "")
        full = format_north_star_digest_html(snap)
        self.assertNotIn("[OBS_HOLD]", full)
        self.assertNotIn("---CURSOR---", full)

    def test_daily_digest_includes_obs(self) -> None:
        snap = _snap(daily_n=8)
        full = format_north_star_digest_html(snap)
        self.assertIn("[OBS_HOLD]", full)
        self.assertIn("---CURSOR---", full)
        self.assertIn("---CLAUDE---", full)


if __name__ == "__main__":
    unittest.main()
