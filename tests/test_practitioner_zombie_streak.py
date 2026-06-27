"""PIL P1.5 — ZOMBIE 연속일 · RETIRED."""
from __future__ import annotations

import unittest

from practitioner_zombie_streak import (
    update_zombie_streaks,
    zombie_retire_days_for_market,
)


class TestZombieStreak(unittest.TestCase):
    def test_retire_days_config(self):
        self.assertEqual(zombie_retire_days_for_market("KR", {"PIL_ZOMBIE_RETIRE_DAYS": {"KR": 3}}), 3)
        self.assertEqual(zombie_retire_days_for_market("US", {}), 7)

    def test_streak_increments(self):
        prior = {
            "KR|G1": {
                "market": "KR",
                "group_key": "G1",
                "streak_days": 4,
                "last_zombie_date": __import__("practitioner_zombie_streak")._yesterday_kst(),
            }
        }
        streaks, force = update_zombie_streaks(
            [{"market": "KR", "group_key": "G1", "is_zombie": True, "vitality_score": 0.2}],
            prior,
            sys_config={"PIL_ZOMBIE_RETIRE_DAYS": {"KR": 5}},
        )
        self.assertEqual(streaks["KR|G1"]["streak_days"], 5)
        self.assertEqual(len(force), 1)

    def test_non_zombie_clears(self):
        prior = {"KR|G1": {"streak_days": 2, "last_zombie_date": "2020-01-01"}}
        streaks, force = update_zombie_streaks(
            [{"market": "KR", "group_key": "G1", "is_zombie": False}],
            prior,
        )
        self.assertNotIn("KR|G1", streaks)
        self.assertEqual(force, [])


if __name__ == "__main__":
    unittest.main()
