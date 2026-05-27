"""deathmatch_battle_royale — 통합 스모크."""
from __future__ import annotations

import unittest

import pandas as pd

from evolution.deathmatch_battle_royale import run_battle_royal


class TestRunBattleRoyal(unittest.TestCase):
    def test_run_basic(self):
        rows = []
        for i in range(10):
            rows.append(
                {
                    "sig_type": f"STANDARD_TEST_{i % 2}",
                    "final_ret": 1.5 if i % 2 == 0 else -0.3,
                    "exit_date": "2026-05-10",
                    "market": "KR",
                    "sim_kelly_invest": 400000,
                }
            )
        df = pd.DataFrame(rows)
        br = run_battle_royal(df, {}, market="KR", persist=False)
        self.assertEqual(br.market, "KR")
        self.assertTrue(len(br.arms) >= 1)


if __name__ == "__main__":
    unittest.main()
