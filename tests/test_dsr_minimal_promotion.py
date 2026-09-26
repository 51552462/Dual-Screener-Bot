"""DSR-MINIMAL-01 — OBSERVE/WARN 관측만. LIVE 승격 경로 무변경."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from strategy_promotion_engine import (
    PROMOTION_DSR_GATE_OBSERVE,
    PROMOTION_DSR_GATE_WARN,
    apply_registry_meta_dsr_observe,
    promotion_dsr_gate,
    run_registry_lifecycle,
)
from validation.walk_forward import evaluate_ledger_deflated_sharpe, deflated_sharpe_from_trials


def _mk_db(groups: dict[str, list[float]]) -> str:
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE strategy_registry (
            strategy_id TEXT PRIMARY KEY, market TEXT, group_key TEXT,
            state TEXT, display_name TEXT, capital_mult REAL
        );
        CREATE TABLE strategy_quality_daily (
            strategy_id TEXT, trade_date TEXT, market TEXT,
            rolling_wr REAL, rolling_pf REAL, below_live_threshold INTEGER,
            recorded_at TEXT, PRIMARY KEY (strategy_id, trade_date)
        );
        CREATE TABLE forward_trades (
            sig_type TEXT, market TEXT, status TEXT, final_ret REAL, exit_date TEXT
        );
        """
    )
    day = 1
    for gk, rets in groups.items():
        sig = f"[LIVE] {gk}"
        for r in rets:
            conn.execute(
                """
                INSERT INTO forward_trades (sig_type, market, status, final_ret, exit_date)
                VALUES (?, 'KR', 'CLOSED', ?, ?)
                """,
                (sig, r, f"2026-02-{day:02d}"),
            )
            day += 1
            if day > 28:
                day = 1
    conn.commit()
    conn.close()
    return path


class TestDsrGateUnit(unittest.TestCase):
    def test_observe_when_insufficient_trials(self):
        self.assertEqual(
            promotion_dsr_gate(n_trials=1, dsr=0.1, target_missing=False),
            PROMOTION_DSR_GATE_OBSERVE,
        )

    def test_warn_when_low_dsr_many_trials(self):
        self.assertEqual(
            promotion_dsr_gate(n_trials=8, dsr=0.4, target_missing=False),
            PROMOTION_DSR_GATE_WARN,
        )


class TestDsrSyntheticLedger(unittest.TestCase):
    def test_observe_warn_branches_one_scenario(self):
        rng = np.random.default_rng(7)
        noise = {f"NOISE_{i}": rng.normal(0.0, 1.5, size=12).tolist() for i in range(12)}
        lucky = {"LUCKY": rng.normal(6.0, 0.4, size=12).tolist()}
        groups = {**noise, **lucky}
        path = _mk_db(groups)
        try:
            obs_row = {"market": "KR", "group_key": "NOISE_0", "state": "CANDIDATE"}
            apply_registry_meta_dsr_observe(obs_row, forward_db_path=path)
            warn_row = {"market": "KR", "group_key": "LUCKY", "state": "CANDIDATE"}
            apply_registry_meta_dsr_observe(warn_row, forward_db_path=path)

            self.assertGreaterEqual(int(warn_row["meta"]["dsr_n_trials"]), 2)
            self.assertEqual(
                warn_row["meta"]["dsr_n_trials_ssot"],
                "closed_forward_group_key_min10",
            )
            with patch(
                "strategy_promotion_engine.promotion_dsr_warn_min",
                return_value=1.01,
            ):
                apply_registry_meta_dsr_observe(warn_row, forward_db_path=path)
            self.assertEqual(warn_row["meta"]["dsr_gate"], PROMOTION_DSR_GATE_WARN)
            self.assertTrue(warn_row["meta"]["dsr_warn"])

            one = _mk_db({"ONLY": [1.0] * 12})
            try:
                solo = {"market": "KR", "group_key": "ONLY", "state": "CANDIDATE"}
                apply_registry_meta_dsr_observe(solo, forward_db_path=one)
                self.assertEqual(solo["meta"]["dsr_gate"], PROMOTION_DSR_GATE_OBSERVE)
                self.assertFalse(solo["meta"]["dsr_warn"])
                self.assertLess(int(solo["meta"]["dsr_n_trials"]), 2)
            finally:
                os.unlink(one)
        finally:
            os.unlink(path)

    def test_lifecycle_still_promotes_with_dsr_warn(self):
        rng = np.random.default_rng(3)
        groups = {
            "GRP_D": [2.0] * 24 + [-0.5] * 6,
            **{f"Z_{i}": (rng.normal(0.0, 2.0, size=12)).tolist() for i in range(8)},
        }
        path = _mk_db(groups)
        try:
            health = {
                "KR|GRP_D": {
                    "rolling_wr": 0.55,
                    "rolling_pf": 1.5,
                    "n": 30,
                    "mult": 1.0,
                    "mdd_pct": -5.0,
                }
            }
            prior = [
                {
                    "strategy_id": "strat:testdsr",
                    "market": "KR",
                    "group_key": "GRP_D",
                    "state": "CANDIDATE",
                    "capital_mult": 0.0,
                    "display_name": "GRP_D",
                }
            ]
            with patch(
                "strategy_promotion_engine.stable_strategy_id",
                return_value="strat:testdsr",
            ), patch.dict(os.environ, {"PROMOTION_DSR_WARN_MIN": "0.999"}):
                out, stats = run_registry_lifecycle(
                    prior_registry=prior,
                    health=health,
                    forward_db_path=path,
                )
            row = out[0]
            self.assertEqual(str(row.get("state")).upper(), "LIVE")
            self.assertIn(row.get("meta", {}).get("dsr_gate"), (
                PROMOTION_DSR_GATE_OBSERVE,
                PROMOTION_DSR_GATE_WARN,
            ))
            self.assertGreaterEqual(int(stats.get("dsr_n_trials") or 0), 2)
        finally:
            os.unlink(path)

    def test_n_trials_counts_group_keys_not_feature_screen(self):
        df = pd.DataFrame(
            {
                "group_key": ["A"] * 10 + ["B"] * 10,
                "final_ret": [1.0] * 10 + [0.2] * 10,
            }
        )
        ev = evaluate_ledger_deflated_sharpe(
            df,
            strategy_col="group_key",
            derive_strategy=False,
            min_trades_per_strategy=10,
            target_key="A",
        )
        self.assertEqual(int(ev["n_trials"]), 2)
        self.assertFalse(ev["target_missing"])


class TestDeflatedTrialsHelper(unittest.TestCase):
    def test_from_trials_records_n(self):
        a = [0.01] * 20
        b = [-0.01, 0.02] * 10
        res = deflated_sharpe_from_trials([a, b], target_index=0)
        self.assertEqual(int(res["n_trials"]), 2)


if __name__ == "__main__":
    unittest.main()
