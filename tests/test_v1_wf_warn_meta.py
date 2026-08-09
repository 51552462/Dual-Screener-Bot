"""V-1 — promotion meta.wf_warn (WARN only, no LIVE block)."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from strategy_promotion_engine import (
    apply_registry_meta_wf_warn,
    run_registry_lifecycle,
    stamp_registry_wf_warn_meta,
    wf_warn_tag_enabled,
)


def _mk_forward_db(group_key: str, rets: list[float]) -> str:
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
    sig = f"[LIVE] {group_key}"
    for i, r in enumerate(rets):
        conn.execute(
            """
            INSERT INTO forward_trades (sig_type, market, status, final_ret, exit_date)
            VALUES (?, 'KR', 'CLOSED', ?, ?)
            """,
            (sig, r, f"2026-01-{i+1:02d}"),
        )
    conn.commit()
    conn.close()
    return path


class TestWfWarnMeta(unittest.TestCase):
    def test_insufficient_data_no_warn(self):
        path = _mk_forward_db("GRP_A", [1.0] * 10)
        try:
            row = {"market": "KR", "group_key": "GRP_A", "state": "CANDIDATE"}
            warn = apply_registry_meta_wf_warn(row, forward_db_path=path)
            self.assertFalse(warn)
            self.assertFalse(row["meta"]["wf_warn"])
        finally:
            os.unlink(path)

    def test_oos_fail_sets_meta_warn(self):
        # 30 trades: train positive, last fold negative → oos_fail
        rets = [2.0] * 24 + [-3.0] * 6
        path = _mk_forward_db("GRP_B", rets)
        try:
            row = {"market": "KR", "group_key": "GRP_B", "state": "CANDIDATE"}
            warn = apply_registry_meta_wf_warn(row, forward_db_path=path)
            self.assertTrue(warn)
            self.assertTrue(row["meta"]["wf_warn"])
        finally:
            os.unlink(path)

    def test_stamp_disabled_clears_warn(self):
        path = _mk_forward_db("GRP_C", [-1.0] * 30)
        try:
            row = {"market": "KR", "group_key": "GRP_C", "state": "LIVE"}
            with patch("strategy_promotion_engine.wf_warn_tag_enabled", return_value=False):
                warned = stamp_registry_wf_warn_meta([row], forward_db_path=path)
            self.assertEqual(warned, [])
            self.assertFalse(row["meta"]["wf_warn"])
        finally:
            os.unlink(path)

    def test_lifecycle_promotion_unchanged_with_warn(self):
        rets = [2.0] * 24 + [-3.0] * 6
        path = _mk_forward_db("GRP_D", rets)
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
                    "strategy_id": "strat:testgrp_d",
                    "market": "KR",
                    "group_key": "GRP_D",
                    "state": "CANDIDATE",
                    "capital_mult": 0.0,
                    "display_name": "GRP_D",
                }
            ]
            with patch(
                "strategy_promotion_engine.stable_strategy_id",
                return_value="strat:testgrp_d",
            ):
                out, stats = run_registry_lifecycle(
                    prior_registry=prior,
                    health=health,
                    forward_db_path=path,
                )
            row = out[0]
            self.assertEqual(str(row.get("state")).upper(), "LIVE")
            self.assertTrue(row.get("meta", {}).get("wf_warn"))
            self.assertGreaterEqual(int(stats.get("wf_warn_count") or 0), 1)
        finally:
            os.unlink(path)

    def test_tag_enabled_default(self):
        self.assertTrue(wf_warn_tag_enabled())


if __name__ == "__main__":
    unittest.main()
