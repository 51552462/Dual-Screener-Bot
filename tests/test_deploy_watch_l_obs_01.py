"""L-OBS-01 — deploy_watch 자동 판정."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from deploy_watch import (
    STATUS_BREAK,
    STATUS_PASS,
    STATUS_SKIP,
    STATUS_WARN,
    PHASE_POST_BEAR_UNDERDOG_01,
    check_c_bear_underdog_01,
    check_c_funnel_02,
    check_f_gate_01,
    check_f_retire_02,
    build_deploy_watch_cursor_prompt,
    reality_audit_check,
    resolve_cursor_action,
    run_deploy_watch,
)


def _mk_market_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE strategy_registry (
            strategy_id TEXT PRIMARY KEY, market TEXT, group_key TEXT,
            state TEXT, display_name TEXT
        );
        CREATE TABLE scan_funnel_snapshot (ts TEXT, market TEXT);
        CREATE TABLE scan_funnel_drop_event (id INTEGER PRIMARY KEY);
        CREATE TABLE forward_trades (
            sig_type TEXT, market TEXT, status TEXT,
            bars_held INTEGER, final_ret REAL, exit_reason TEXT,
            exit_type TEXT, entry_regime TEXT, exit_date TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return path


class TestCheckFGate01(unittest.TestCase):
    def test_zero_cooled_pass(self):
        path = _mk_market_db()
        try:
            r = check_f_gate_01(db_path=path)
            self.assertEqual(r["status"], STATUS_PASS)
        finally:
            os.unlink(path)

    def test_cooled_still_pass_informational(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                "INSERT INTO strategy_registry VALUES ('a','KR','G','COOLED','G')"
            )
            conn.commit()
            conn.close()
            r = check_f_gate_01(db_path=path)
            self.assertEqual(r["status"], STATUS_PASS)
            self.assertEqual(r["metrics"]["cooled_retired"], 1)
        finally:
            os.unlink(path)


class TestCheckCFunnel02(unittest.TestCase):
    def test_old_max_ts_warn(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                "INSERT INTO scan_funnel_snapshot VALUES ('2026-07-01 10:00','KR')"
            )
            conn.commit()
            conn.close()
            r = check_c_funnel_02(db_path=path, baseline_ts="2026-07-02")
            self.assertEqual(r["status"], STATUS_WARN)
        finally:
            os.unlink(path)

    def test_new_max_ts_pass(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                "INSERT INTO scan_funnel_snapshot VALUES ('2026-08-09 10:00','KR')"
            )
            conn.commit()
            conn.close()
            r = check_c_funnel_02(db_path=path, baseline_ts="2026-07-02")
            self.assertEqual(r["status"], STATUS_PASS)
        finally:
            os.unlink(path)


class TestCheckFRetire02(unittest.TestCase):
    def test_zero_tags_pass(self):
        path = _mk_market_db()
        try:
            r = check_f_retire_02(db_path=path)
            self.assertEqual(r["status"], STATUS_PASS)
        finally:
            os.unlink(path)


class TestCheckCBearUnderdog01(unittest.TestCase):
    def test_zero_shadow_pass(self):
        path = _mk_market_db()
        try:
            r = check_c_bear_underdog_01(db_path=path, phase=PHASE_POST_BEAR_UNDERDOG_01)
            self.assertEqual(r["status"], STATUS_PASS)
            self.assertEqual(r["metrics"]["shadow_tag_rows"], 0)
        finally:
            os.unlink(path)

    def test_untagged_bear_incubator_warn_in_phase(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                """
                INSERT INTO forward_trades (
                    sig_type, market, status, bars_held, final_ret,
                    exit_reason, exit_type, entry_regime, exit_date
                ) VALUES (
                    '[INCUBATOR_KR_UNDERDOG_50점]', 'KR', 'OPEN', NULL, NULL,
                    '', '', 'BEAR', ''
                )
                """
            )
            conn.commit()
            conn.close()
            r = check_c_bear_underdog_01(
                db_path=path, phase=PHASE_POST_BEAR_UNDERDOG_01
            )
            self.assertEqual(r["status"], STATUS_WARN)
            self.assertIn("tag_miss", r["detail"])
        finally:
            os.unlink(path)

    def test_shadow_tagged_not_untagged(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                """
                INSERT INTO forward_trades (
                    sig_type, market, status, bars_held, final_ret,
                    exit_reason, exit_type, entry_regime, exit_date
                ) VALUES (
                    '[INCUBATOR_KR_UNDERDOG_50점]_BEAR_UNDERDOG_SHADOW',
                    'KR', 'OPEN', NULL, NULL, '', '', 'BEAR', ''
                )
                """
            )
            conn.commit()
            conn.close()
            r = check_c_bear_underdog_01(
                db_path=path, phase=PHASE_POST_BEAR_UNDERDOG_01
            )
            self.assertEqual(r["status"], STATUS_PASS)
            self.assertEqual(r["metrics"]["shadow_tag_rows"], 1)
            self.assertEqual(r["metrics"]["untagged_kr_bear_incubator"], 0)
        finally:
            os.unlink(path)

    def test_pain_cluster_warn_on_shadow_cohort(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            for i in range(5):
                conn.execute(
                    """
                    INSERT INTO forward_trades (
                        sig_type, market, status, bars_held, final_ret,
                        exit_reason, exit_type, entry_regime, exit_date
                    ) VALUES (?, 'KR', 'CLOSED', 2, -3.0, 'mae', 'STAT_MAE', 'BEAR', '2026-08-01')
                    """,
                    (f"[INCUBATOR_KR_UNDERDOG_50점]_BEAR_UNDERDOG_SHADOW_{i}",),
                )
            conn.commit()
            conn.close()
            r = check_c_bear_underdog_01(db_path=path, phase="post_f_gate_01")
            self.assertEqual(r["status"], STATUS_WARN)
            self.assertIn("pain_cluster", r["detail"])
        finally:
            os.unlink(path)


class TestRealityAuditCheck(unittest.TestCase):
    def test_no_closed_pass(self):
        path = _mk_market_db()
        try:
            r = reality_audit_check(db_path=path)
            self.assertEqual(r["status"], STATUS_PASS)
            self.assertEqual(r["detail"], "no_closed_rows")
        finally:
            os.unlink(path)

    def test_clean_closed_pass(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                """
                INSERT INTO forward_trades (
                    sig_type, market, status, bars_held, final_ret,
                    exit_reason, exit_type, entry_regime, exit_date
                ) VALUES (
                    'TEST [LIVE]', 'KR', 'CLOSED', 5, 2.5,
                    'test', 'HYBRID_TIME', 'BULL', '2026-08-01'
                )
                """
            )
            conn.commit()
            conn.close()
            r = reality_audit_check(db_path=path)
            self.assertEqual(r["status"], STATUS_PASS)
        finally:
            os.unlink(path)

    def test_bad_exit_type_warn(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            for i in range(10):
                conn.execute(
                    """
                    INSERT INTO forward_trades (
                        sig_type, market, status, bars_held, final_ret,
                        exit_reason, exit_type, entry_regime, exit_date
                    ) VALUES (?, 'KR', 'CLOSED', 5, 1.0, 'x', ?, 'BULL', '2026-08-01')
                    """,
                    (f"G{i}", "UNKNOWN" if i < 2 else "HYBRID_TIME"),
                )
            conn.commit()
            conn.close()
            r = reality_audit_check(db_path=path)
            self.assertEqual(r["status"], STATUS_WARN)
        finally:
            os.unlink(path)

    def test_null_ret_break(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            for i in range(5):
                conn.execute(
                    """
                    INSERT INTO forward_trades (
                        sig_type, market, status, bars_held, final_ret,
                        exit_reason, exit_type, entry_regime, exit_date
                    ) VALUES (?, 'US', 'CLOSED', NULL, NULL, 'x', 'HYBRID_TIME', 'BULL', '2026-08-01')
                    """,
                    (f"G{i}",),
                )
            conn.commit()
            conn.close()
            r = reality_audit_check(db_path=path)
            self.assertEqual(r["status"], STATUS_BREAK)
        finally:
            os.unlink(path)


class TestCursorAction(unittest.TestCase):
    def test_warn_reports_to_claude(self):
        action = resolve_cursor_action(
            [{"id": "c_funnel_02", "status": STATUS_WARN}],
            phase="post_f_gate_01",
        )
        self.assertEqual(action, "REPORT_TO_CLAUDE")

    def test_factory_break_blocks_retire_deploy(self):
        action = resolve_cursor_action(
            [{"id": "factory_health", "status": STATUS_BREAK}],
            phase="post_f_gate_01",
        )
        self.assertEqual(action, "BLOCK_F_RETIRE_02_DEPLOY")

    def test_bear_pain_cluster_observes_l2(self):
        action = resolve_cursor_action(
            [
                {
                    "id": "c_bear_underdog_01",
                    "status": STATUS_WARN,
                    "observation": {"pain_cluster_reproducing": True},
                }
            ],
            phase="post_bear_underdog_01",
        )
        self.assertEqual(action, "OBSERVE_BEAR_UNDERDOG_L2")

    def test_bear_tag_miss_investigates(self):
        action = resolve_cursor_action(
            [
                {
                    "id": "c_bear_underdog_01",
                    "status": STATUS_WARN,
                    "detail": "tag_miss:untagged=1",
                    "observation": {"pain_cluster_reproducing": False},
                }
            ],
            phase="post_bear_underdog_01",
        )
        self.assertEqual(action, "INVESTIGATE_BEAR_UNDERDOG_TAG")

    def test_cursor_prompt_includes_bear_underdog(self):
        report = {
            "phase": "post_bear_underdog_01",
            "overall": STATUS_WARN,
            "cursor_action": "OBSERVE_BEAR_UNDERDOG_L2",
            "checks": [
                {
                    "id": "c_bear_underdog_01",
                    "status": STATUS_WARN,
                    "metrics": {
                        "shadow_tag_rows": 3,
                        "untagged_kr_bear_incubator": 0,
                        "shadow_closed_mae": 2,
                        "shadow_closed": 4,
                    },
                    "observation": {"pain_cluster_reproducing": True},
                }
            ],
        }
        prompt = build_deploy_watch_cursor_prompt(report)
        self.assertIn("BEAR_UNDERDOG", prompt)
        self.assertIn("OBSERVE", prompt)


class TestRunDeployWatch(unittest.TestCase):
    def test_coin_only_host_skips_entire_run(self):
        with patch("deploy_watch.is_coin_only_deploy_host", return_value=True):
            report = run_deploy_watch(
                send_telegram=True,
                persist=False,
                record_ops=False,
            )
        self.assertEqual(report.get("overall"), STATUS_SKIP)
        self.assertEqual(report.get("skipped_reason"), "coin_only_host")
        self.assertFalse(report.get("telegram_sent"))

    def test_dry_run_no_telegram(self):
        path = _mk_market_db()
        try:
            with patch("deploy_watch.check_factory_health") as mock_health:
                mock_health.return_value = {
                    "id": "factory_health",
                    "status": STATUS_SKIP,
                    "detail": "test",
                }
                report = run_deploy_watch(
                    db_path=path,
                    send_telegram=False,
                    persist=False,
                    record_ops=False,
                )
            self.assertIn("overall", report)
            self.assertIn("cursor_prompt", report)
            self.assertFalse(report.get("telegram_sent"))
        finally:
            os.unlink(path)

    def test_warn_triggers_telegram_call(self):
        path = _mk_market_db()
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                "INSERT INTO scan_funnel_snapshot VALUES ('2026-06-01','KR')"
            )
            conn.commit()
            conn.close()
            with patch("deploy_watch.check_factory_health") as mock_health:
                mock_health.return_value = {
                    "id": "factory_health",
                    "status": STATUS_SKIP,
                    "detail": "test",
                }
                with patch("deploy_watch.send_deploy_watch_telegram") as mock_tg:
                    mock_tg.return_value = True
                    report = run_deploy_watch(
                        db_path=path,
                        persist=False,
                        record_ops=False,
                    )
            self.assertEqual(report["overall"], STATUS_WARN)
            mock_tg.assert_called_once()
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
