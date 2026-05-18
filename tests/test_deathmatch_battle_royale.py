"""deathmatch_battle_royale — MDD 패널티 · 상대평가 면제 · composite."""
from __future__ import annotations

import math
import os
import sqlite3
import tempfile
import unittest

import pandas as pd

from deathmatch_battle_royale import (
    RegistryArmRow,
    _assign_ranks_and_elimination,
    _compute_composite_scores,
    mdd_pct_from_returns,
    run_battle_royal,
)
from deathmatch_config import market_deathmatch_params, load_deathmatch_config


class TestMddPenalty(unittest.TestCase):
    def test_mdd_penalizes_high_drawdown_arm(self):
        cfg = market_deathmatch_params(load_deathmatch_config({}), "KR")
        good = RegistryArmRow(
            arm_id="a1",
            label="Good",
            group_key="G1",
            registry_state="LIVE",
            n_valid=10,
            mean_ret=2.0,
            win_rate_pct=55.0,
            profit_factor=1.5,
            mdd_pct=-5.0,
            vol_pct=1.0,
        )
        bad = RegistryArmRow(
            arm_id="a2",
            label="Bad",
            group_key="G2",
            registry_state="LIVE",
            n_valid=10,
            mean_ret=2.0,
            win_rate_pct=55.0,
            profit_factor=1.5,
            mdd_pct=-35.0,
            vol_pct=2.0,
        )
        arms = [good, bad]
        _compute_composite_scores(arms, cfg)
        self.assertGreater(good.composite_score, bad.composite_score)


class TestRelativeDefense(unittest.TestCase):
    def test_crash_exempts_relative_outperformer(self):
        cfg = market_deathmatch_params(load_deathmatch_config({}), "KR")
        cfg["bottom_pct"] = 1.0
        cfg["crash_market_mean_pct"] = -0.5
        cfg["relative_outperform_buffer_pp"] = 0.1
        a1 = RegistryArmRow(
            arm_id="x1",
            label="Weak",
            group_key="W",
            registry_state="LIVE",
            n_valid=10,
            mean_ret=-2.0,
            composite_score=-2.0,
        )
        a2 = RegistryArmRow(
            arm_id="x2",
            label="RelOK",
            group_key="R",
            registry_state="LIVE",
            n_valid=10,
            mean_ret=0.0,
            composite_score=-1.0,
        )
        arms = [a1, a2]
        crash, _ = _assign_ranks_and_elimination(
            arms,
            n_min=5,
            dmcfg=cfg,
            market_benchmark=-1.5,
        )
        self.assertTrue(crash)
        rel = [a for a in arms if a.relative_exempt]
        self.assertTrue(any(a.label == "RelOK" for a in rel))


class TestMddCalc(unittest.TestCase):
    def test_mdd_negative(self):
        mdd = mdd_pct_from_returns([5.0, -3.0, -4.0, 2.0])
        self.assertLess(mdd, 0)


class TestRunBattleRoyal(unittest.TestCase):
    def test_run_basic(self):
        rows = []
        for i in range(8):
            rows.append(
                {
                    "sig_type": f"STANDARD_TEST_{i % 2}",
                    "final_ret": 1.5 if i % 2 == 0 else -0.5,
                    "exit_date": "2026-05-10",
                    "market": "KR",
                }
            )
        df = pd.DataFrame(rows)
        br = run_battle_royal(df, {}, market="KR", persist=False)
        self.assertEqual(br.market, "KR")
        self.assertTrue(len(br.arms) >= 1)


if __name__ == "__main__":
    unittest.main()
