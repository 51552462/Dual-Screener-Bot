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
    check_c_funnel_02,
    check_f_gate_01,
    check_f_retire_02,
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
        CREATE TABLE forward_trades (sig_type TEXT);
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


class TestRunDeployWatch(unittest.TestCase):
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
