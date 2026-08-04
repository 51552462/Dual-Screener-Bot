"""M-R0 — overseer_quality tests."""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

from overseer_quality import (
    begin_pipeline_run,
    fetch_quality_stats,
    format_overseer_quality_footer_html,
    record_overseer_anomaly,
    record_overseer_deterministic_fallback,
    record_overseer_llm_call,
    record_pipeline_critical_failure,
    resolve_overseer_audit_mode,
)


class TestOverseerQualityMr0(unittest.TestCase):
    def test_degraded_on_meta_flag(self):
        meta = {"META_GOVERNOR_LAST_RUN_AT": "2000-01-01T00:00:00+00:00"}
        with mock.patch("meta_state_store.is_meta_state_degraded", return_value=True):
            mode, reason = resolve_overseer_audit_mode({}, meta)
        self.assertEqual(mode, "degraded_rules_only")
        self.assertEqual(reason, "meta_degraded")

    def test_degraded_on_pipeline_critical(self):
        begin_pipeline_run("daily_audit_kr")
        record_pipeline_critical_failure("meta_governor_sync")
        with mock.patch("meta_state_store.is_meta_state_degraded", return_value=False):
            mode, reason = resolve_overseer_audit_mode({}, {})
        self.assertEqual(mode, "degraded_rules_only")
        self.assertIn("pipeline_critical", reason)

    def test_full_mode_default(self):
        begin_pipeline_run("daily_audit_kr")
        with mock.patch("meta_state_store.is_meta_state_degraded", return_value=False):
            mode, _ = resolve_overseer_audit_mode({}, {})
        self.assertEqual(mode, "full")

    def test_footer_sanitize_passes(self):
        html_out = format_overseer_quality_footer_html({})
        self.assertIn("Overseer", html_out)

    def test_quality_stats_from_ops_db(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "ops_events.sqlite")
            import sqlite3

            conn = sqlite3.connect(db)
            conn.execute(
                """
                CREATE TABLE ops_events (
                    id INTEGER PRIMARY KEY,
                    ts_utc TEXT,
                    component TEXT,
                    severity TEXT,
                    event TEXT,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO ops_events VALUES (1, datetime('now'), 'overseer_quality', 'INFO', "
                "'overseer_llm_call', '{}')"
            )
            conn.execute(
                "INSERT INTO ops_events VALUES (2, datetime('now'), 'overseer_quality', 'INFO', "
                "'overseer_deterministic_fallback', '{}')"
            )
            conn.commit()
            conn.close()
            with mock.patch("ops_logger.OPS_EVENTS_DB_PATH", db):
                stats = fetch_quality_stats(window_days=7)
            self.assertEqual(stats["llm"], 1)
            self.assertEqual(stats["deterministic"], 1)

    @mock.patch("overseer_quality._insert_quality_event")
    def test_record_events_separate_from_cio(self, mock_insert):
        record_overseer_llm_call()
        record_overseer_deterministic_fallback(reason="test")
        record_overseer_anomaly(kind="degraded_audit")
        events = [c.args[0] for c in mock_insert.call_args_list if c.args]
        self.assertIn("overseer_llm_call", events)
        self.assertIn("overseer_deterministic_fallback", events)
        self.assertIn("overseer_anomaly", events)


if __name__ == "__main__":
    unittest.main()
