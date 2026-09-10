"""V-2 scaffold + IV observation report."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
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

_REPO = Path(__file__).resolve().parents[1]
_WARN_RETS = [2.0] * 24 + [-3.0] * 6
_PASS_RETS = [1.5] * 30


def _mk_lifecycle_db(group_key: str, rets: list[float]) -> str:
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
            "INSERT INTO forward_trades VALUES (?, 'KR', 'CLOSED', ?, ?)",
            (sig, r, f"2026-01-{i + 1:02d}"),
        )
    conn.commit()
    conn.close()
    return path


def _health(group_key: str, *, wr: float = 0.55, pf: float = 1.5, n: int = 30) -> dict:
    return {
        f"KR|{group_key}": {
            "rolling_wr": wr,
            "rolling_pf": pf,
            "n": n,
            "mult": 1.0,
            "mdd_pct": -5.0,
        }
    }


def _prior(group_key: str, state: str, **extra) -> list:
    sid = stable_strategy_id("KR", group_key)
    row = {
        "strategy_id": sid,
        "market": "KR",
        "group_key": group_key,
        "state": state,
        "capital_mult": 0.0,
        "display_name": group_key,
    }
    row.update(extra)
    return [row]


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

    def test_block_already_on(self):
        with patch(
            "strategy_promotion_engine.walk_forward_promotion_block_enabled",
            return_value=True,
        ):
            self.assertEqual(
                assess_v2_readiness(
                    days_elapsed=28,
                    false_positive_rate=0.1,
                    reality_status="PASS",
                    wf_warn_count=5,
                ),
                "BLOCK_ALREADY_ON",
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
        on_report = dict(report)
        on_report["v2"] = {"block_enabled": True, "readiness": "BLOCK_ALREADY_ON"}
        on_report["cursor_prompt"] = build_cursor_prompt(on_report)
        tg = format_iv_observation_telegram(on_report)
        self.assertIn("V-2 심판", tg)
        self.assertIn("ON(작동 중)", tg)
        self.assertIn("ON(작동 중)", on_report["cursor_prompt"])

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
    def test_factory_entrypoints_default_env_on(self):
        needle = (
            'WALK_FORWARD_PROMOTION_BLOCK_ENABLED='
            '"${WALK_FORWARD_PROMOTION_BLOCK_ENABLED:-1}"'
        )
        for rel in (
            "factory.sh",
            "deploy/entrypoints/run_factory_daemon.sh",
            "deploy/entrypoints/run_main_service.sh",
        ):
            text = (_REPO / rel).read_text(encoding="utf-8")
            self.assertIn(needle, text, rel)

    def test_block_off_promotes_despite_warn(self):
        path = _mk_lifecycle_db("GRP_E", _WARN_RETS)
        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "0"}, clear=False):
                out, stats = run_registry_lifecycle(
                    prior_registry=_prior("GRP_E", "CANDIDATE"),
                    health=_health("GRP_E"),
                    forward_db_path=path,
                )
            self.assertEqual(str(out[0].get("state")).upper(), "LIVE")
            self.assertEqual(int(stats.get("wf_promotion_blocked") or 0), 0)
            self.assertTrue((out[0].get("meta") or {}).get("wf_would_block"))
        finally:
            os.unlink(path)

    def test_block_on_skips_live_hard_gate(self):
        path = _mk_lifecycle_db("GRP_F", _WARN_RETS)
        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "1"}, clear=False):
                out, stats = run_registry_lifecycle(
                    prior_registry=_prior("GRP_F", "CANDIDATE"),
                    health=_health("GRP_F"),
                    forward_db_path=path,
                )
            self.assertEqual(str(out[0].get("state")).upper(), "CANDIDATE")
            self.assertGreaterEqual(int(stats.get("wf_promotion_blocked") or 0), 1)
            self.assertTrue((out[0].get("meta") or {}).get("wf_promotion_skipped"))
        finally:
            os.unlink(path)

    def test_block_on_skips_fast_track_live(self):
        gk = "INCUBATOR_WF_FT"
        path = _mk_lifecycle_db(gk, _WARN_RETS)
        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "1"}, clear=False):
                out, stats = run_registry_lifecycle(
                    prior_registry=_prior(gk, "CANDIDATE"),
                    health=_health(gk, wr=0.40, pf=2.2, n=20),
                    forward_db_path=path,
                )
            self.assertNotEqual(str(out[0].get("state")).upper(), "LIVE")
            self.assertGreaterEqual(int(stats.get("wf_promotion_blocked") or 0), 1)
            self.assertEqual(int(stats.get("fast_track_promoted") or 0), 0)
        finally:
            os.unlink(path)

    def test_block_on_skips_re_evolution_live(self):
        gk = "GRP_REEV"
        path = _mk_lifecycle_db(gk, _WARN_RETS)

        def _fake_redeem(row, **_kwargs):
            row["state"] = "LIVE"
            row["capital_mult"] = 1.0
            return True, {"warm_start_applied": False}

        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "1"}, clear=False):
                with patch(
                    "re_evolution_redemption_gate.try_promote_re_evolution_redemption",
                    side_effect=_fake_redeem,
                ):
                    out, stats = run_registry_lifecycle(
                        prior_registry=_prior(
                            gk,
                            "OBSERVING",
                            meta={"wf_warn": True},
                            demote_reason="re_evolution_3_strike(x3)",
                            source="re_evolution_strike",
                        ),
                        health=_health(gk),
                        forward_db_path=path,
                    )
            self.assertEqual(str(out[0].get("state")).upper(), "OBSERVING")
            self.assertEqual(float(out[0].get("capital_mult") or 0), 0.0)
            self.assertGreaterEqual(int(stats.get("wf_promotion_blocked") or 0), 1)
            self.assertEqual(int(stats.get("re_evolution_redemption_promoted") or 0), 0)
        finally:
            os.unlink(path)

    def test_block_on_skips_cooled_recovery(self):
        gk = "GRP_COOL"
        path = _mk_lifecycle_db(gk, _WARN_RETS)
        demoted = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "1"}, clear=False):
                out, stats = run_registry_lifecycle(
                    prior_registry=_prior(gk, "COOLED", last_demoted_at=demoted),
                    health=_health(gk),
                    forward_db_path=path,
                )
            self.assertEqual(str(out[0].get("state")).upper(), "COOLED")
            self.assertGreaterEqual(int(stats.get("wf_promotion_blocked") or 0), 1)
        finally:
            os.unlink(path)

    def test_block_on_allows_clean_live_hard_gate(self):
        gk = "GRP_CLEAN"
        path = _mk_lifecycle_db(gk, _PASS_RETS)
        try:
            with patch.dict(os.environ, {"WALK_FORWARD_PROMOTION_BLOCK_ENABLED": "1"}, clear=False):
                out, stats = run_registry_lifecycle(
                    prior_registry=_prior(gk, "CANDIDATE"),
                    health=_health(gk),
                    forward_db_path=path,
                )
            self.assertEqual(str(out[0].get("state")).upper(), "LIVE")
            self.assertEqual(int(stats.get("wf_promotion_blocked") or 0), 0)
            self.assertFalse((out[0].get("meta") or {}).get("wf_warn"))
            self.assertEqual(str(out[0].get("promote_reason")), "live_hard_gate")
        finally:
            os.unlink(path)

    def test_block_default_off(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("WALK_FORWARD_PROMOTION_BLOCK_ENABLED", None)
            self.assertFalse(walk_forward_promotion_block_enabled())


if __name__ == "__main__":
    unittest.main()
