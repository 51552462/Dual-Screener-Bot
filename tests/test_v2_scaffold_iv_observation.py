"""V-2 scaffold + IV observation report."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from iv_observation_report import (
    assess_v2_readiness,
    build_cursor_prompt,
    format_iv_observation_telegram,
    run_iv_observation_report,
)
from strategy_promotion_engine import (
    run_registry_lifecycle,
    stable_strategy_id,
    walk_forward_promotion_block_enabled,
)


class TestV2Readiness(unittest.TestCase):
    def test_not_ready_before_28_days(self):
        self.assertEqual(
            assess_v2_readiness(
                days_elapsed=7,
                false_positive_rate=0.1,
                reality_status="PASS",
                wf_warn_count=5,
            ),
            "NOT_READY",
        )

    def test_ready_after_28_low_fp(self):
        with patch(
            "strategy_promotion_engine.walk_forward_promotion_block_enabled",
            return_value=False,
        ):
            self.assertEqual(
                assess_v2_readiness(
                    days_elapsed=28,
                    false_positive_rate=0.1,
                    reality_status="PASS",
                    wf_warn_count=5,
                ),
                "READY",
            )


class TestIvObservationReport(unittest.TestCase):
    def test_cursor_prompt_contains_key_fields(self):
        report = {
            "observation": {"days_elapsed": 7, "min_days": 28, "v1_started_at": "2026-08-09"},
            "krus_wf": {"wf_warn_count": 2},
            "false_positive": {"false_positive_rate": None},
            "reality_audit": {"status": "PASS"},
            "bitget_shadow": {"oos_fail_rate": 0.2},
            "v2": {"block_enabled": False, "readiness": "NOT_READY"},
        }
        report["cursor_prompt"] = build_cursor_prompt(report)
        self.assertIn("readiness: NOT_READY", report["cursor_prompt"])
        self.assertIn("---CURSOR---", format_iv_observation_telegram(report))

    def test_run_persists_json(self):
        fd, db = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE forward_trades (
                sig_type TEXT, market TEXT, status TEXT, final_ret REAL, exit_date TEXT
            );
            CREATE TABLE strategy_registry (
                strategy_id TEXT PRIMARY KEY, market TEXT, group_key TEXT,
                state TEXT, display_name TEXT, capital_mult REAL
            );
            """
        )
        conn.commit()
        conn.close()

        tmp_dir = tempfile.mkdtemp()
        try:
            with patch("iv_observation_report.factory_data_dir", return_value=tmp_dir):
                with patch("deploy_watch.send_deploy_watch_telegram", return_value=True):
                    report = run_iv_observation_report(
                        db_path=db,
                        send_telegram=True,
                        force_telegram=True,
                    )
            self.assertEqual(report["schema"], "iv_observation_report.v1")
            self.assertTrue(os.path.isfile(os.path.join(tmp_dir, "iv_observation_latest.json")))
            self.assertIn("cursor_prompt", report)
        finally:
            os.unlink(db)
            latest = os.path.join(tmp_dir, "iv_observation_latest.json")
            if os.path.isfile(latest):
                os.unlink(latest)
            state = os.path.join(tmp_dir, "iv_observation_state.json")
            if os.path.isfile(state):
                os.unlink(state)
            os.rmdir(tmp_dir)


class TestV2BlockScaffold(unittest.TestCase):
    def test_block_off_promotes_despite_warn(self):
        rets = [2.0] * 24 + [-3.0] * 6
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
        sig = "[LIVE] GRP_E"
        for i, r in enumerate(rets):
            conn.execute(
                "INSERT INTO forward_trades VALUES (?, 'KR', 'CLOSED', ?, ?)",
                (sig, r, f"2026-01-{i+1:02d}"),
            )
        conn.commit()
        conn.close()

        sid = stable_strategy_id("KR", "GRP_E")
        health = {
            "KR|GRP_E": {
                "rolling_wr": 0.55,
                "rolling_pf": 1.5,
                "n": 30,
                "mult": 1.0,
                "mdd_pct": -5.0,
            }
        }
        prior = [
            {
                "strategy_id": sid,
                "market": "KR",
                "group_key": "GRP_E",
                "state": "CANDIDATE",
                "capital_mult": 0.0,
                "display_name": "GRP_E",
            }
        ]
        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "0"}, clear=False):
                out, stats = run_registry_lifecycle(
                    prior_registry=prior,
                    health=health,
                    forward_db_path=path,
                )
            self.assertEqual(str(out[0].get("state")).upper(), "LIVE")
            self.assertEqual(int(stats.get("wf_promotion_blocked") or 0), 0)
            self.assertTrue((out[0].get("meta") or {}).get("wf_would_block"))
        finally:
            os.unlink(path)

    def test_block_on_skips_live(self):
        rets = [2.0] * 24 + [-3.0] * 6
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
        sig = "[LIVE] GRP_F"
        for i, r in enumerate(rets):
            conn.execute(
                "INSERT INTO forward_trades VALUES (?, 'KR', 'CLOSED', ?, ?)",
                (sig, r, f"2026-01-{i+1:02d}"),
            )
        conn.commit()
        conn.close()

        sid = stable_strategy_id("KR", "GRP_F")
        health = {
            "KR|GRP_F": {
                "rolling_wr": 0.55,
                "rolling_pf": 1.5,
                "n": 30,
                "mult": 1.0,
                "mdd_pct": -5.0,
            }
        }
        prior = [
            {
                "strategy_id": sid,
                "market": "KR",
                "group_key": "GRP_F",
                "state": "CANDIDATE",
                "capital_mult": 0.0,
                "display_name": "GRP_F",
            }
        ]
        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "1"}, clear=False):
                out, stats = run_registry_lifecycle(
                    prior_registry=prior,
                    health=health,
                    forward_db_path=path,
                )
            self.assertEqual(str(out[0].get("state")).upper(), "CANDIDATE")
            self.assertGreaterEqual(int(stats.get("wf_promotion_blocked") or 0), 1)
        finally:
            os.unlink(path)

    def test_block_default_off(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("WALK_FORWARD_PROMOTION_BLOCK_ENABLED", None)
            self.assertFalse(walk_forward_promotion_block_enabled())


if __name__ == "__main__":
    unittest.main()
