"""P0 — Toxic 킬 후 점화 재활성 차단·상태 보존 테스트."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from mega_trend_ignition import (
    MEGA_TREND_CONFIG_KEY,
    assess_toxic_kill_cooldown,
    merge_preserved_mega_trend_state,
    refresh_mega_trend_ignition,
)


def _ignited_detection(sector: str = "반도체/IT") -> dict:
    return {
        "ignited": True,
        "sectors": [sector],
        "primary_sector": sector,
        "primary_detail": {
            "sector": sector,
            "turnover_share_pct": 35.0,
            "flow_z": 2.5,
        },
        "candidates_checked": 1,
        "turnover_snapshot": {},
        "ignition_details": [],
    }


class TestToxicKillCooldown(unittest.TestCase):
    def test_cooldown_active_within_window(self):
        today = datetime.now().strftime("%Y-%m-%d")
        for prev in (
            {"toxic_kill_at": today},
            {"internal_momentum_kill_at": today},
            {"climax_kill_at": today},
        ):
            out = assess_toxic_kill_cooldown(prev)
            self.assertTrue(out["active"], msg=str(prev))
            self.assertGreater(out.get("days_remaining", 0), 0)

    def test_cooldown_expired(self):
        old = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        prev = {"toxic_kill_at": old}
        out = assess_toxic_kill_cooldown(prev)
        self.assertFalse(out["active"])
        self.assertEqual(out["reason"], "cooldown_expired")


class TestIgnitionKillGuard(unittest.TestCase):
    def test_blocks_reignite_after_toxic_kill_same_day(self):
        """P0: 외부 점화 조건 충족이어도 Toxic 킬 직후 재언락 금지."""
        today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prev_block = {
            "active": False,
            "primary_sector": "반도체/IT",
            "correlation_forgiveness_revoked": True,
            "toxic_kill_at": today,
            "toxic_kill_reason": "toxic_graveyard: test",
            "internal_diagnostics": {"any_momentum_lost": True},
            "toxic_watch": {"known_rule_keys": ["TOXIC_01"]},
            "deactivated_at": today,
            "ignited_at": "2026-02-01",
        }
        cfg = {MEGA_TREND_CONFIG_KEY: dict(prev_block)}
        saved = {}

        with patch(
            "mega_trend_ignition.detect_mega_trend_sectors",
            return_value=_ignited_detection(),
        ):
            state = refresh_mega_trend_ignition(cfg, save_config_fn=saved.update)

        self.assertFalse(state["active"])
        self.assertFalse(state.get("rotation_advantage_active"))
        self.assertTrue(state.get("correlation_forgiveness_revoked"))
        self.assertEqual(state.get("toxic_kill_at"), today)
        self.assertIn("internal_diagnostics", state)
        self.assertIn("toxic_watch", state)
        self.assertIn("ignition_blocked_reason", state)

    def test_preserves_diagnostics_when_not_ignited(self):
        prev_block = {
            "active": False,
            "internal_diagnostics": {"updated_at": "2026-02-05", "any_momentum_lost": False},
            "toxic_watch": {"known_rule_keys": []},
        }
        cfg = {MEGA_TREND_CONFIG_KEY: dict(prev_block)}

        with patch(
            "mega_trend_ignition.detect_mega_trend_sectors",
            return_value={"ignited": False, "sectors": [], "primary_sector": None},
        ):
            state = refresh_mega_trend_ignition(cfg)

        self.assertEqual(
            state["internal_diagnostics"]["updated_at"],
            "2026-02-05",
        )

    def test_fresh_reignite_after_cooldown_clears_revocation(self):
        old_kill = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
        prev_block = {
            "active": False,
            "primary_sector": "반도체/IT",
            "correlation_forgiveness_revoked": True,
            "toxic_kill_at": old_kill,
            "toxic_kill_reason": "old kill",
        }
        cfg = {MEGA_TREND_CONFIG_KEY: dict(prev_block)}

        with patch(
            "mega_trend_ignition.detect_mega_trend_sectors",
            return_value=_ignited_detection(),
        ):
            state = refresh_mega_trend_ignition(cfg)

        self.assertTrue(state["active"])
        self.assertTrue(state.get("rotation_advantage_active"))
        self.assertNotIn("correlation_forgiveness_revoked", state)
        self.assertIn("post_kill_reignite_at", state)

    def test_merge_does_not_copy_kill_keys_on_fresh_reignite(self):
        old_kill = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        prev = {
            "toxic_kill_at": old_kill,
            "correlation_forgiveness_revoked": True,
            "internal_diagnostics": {"n": 1},
        }
        state = {
            "active": True,
            "rotation_advantage_active": True,
        }
        cooldown = assess_toxic_kill_cooldown(prev)
        out = merge_preserved_mega_trend_state(prev, state, cooldown=cooldown)
        self.assertNotIn("correlation_forgiveness_revoked", out)
        self.assertEqual(out["internal_diagnostics"], {"n": 1})


if __name__ == "__main__":
    unittest.main()
