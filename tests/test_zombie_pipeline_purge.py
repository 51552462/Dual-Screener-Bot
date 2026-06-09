"""Zombie pipeline guard — critical abort, SQL date, overseer Kelly failsafe."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from unittest import mock

from factory_runtime import StepSpec, dispatch_factory_mode
from overseer_audit_binder import (
    _resolve_overseer_config_regime,
    _resolve_overseer_kelly_display,
    _sql_date_normalized,
)


class TestSqlDateNormalized(unittest.TestCase):
    def test_date_parses_mixed_formats(self):
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db = f.name
        try:
            conn = sqlite3.connect(db)
            conn.execute(
                "CREATE TABLE forward_trades (id INT, exit_date TEXT, entry_date TEXT)"
            )
            rows = [
                (1, "2026-06-09T16:30:00+09:00", "2026-06-09"),
                (2, "2026-06-09 15:00:00", "2026/06/09"),
                (3, "2026-06-08", "2026-06-08"),
            ]
            conn.executemany(
                "INSERT INTO forward_trades VALUES (?,?,?)", rows
            )
            exit_d = _sql_date_normalized("exit_date")
            cur = conn.execute(
                f"SELECT id FROM forward_trades WHERE {exit_d} = date(?)",
                ("2026-06-09",),
            )
            ids = {r[0] for r in cur.fetchall()}
            self.assertEqual(ids, {1, 2})
            conn.close()
        finally:
            import os

            os.unlink(db)


class TestOverseerFailsafe(unittest.TestCase):
    def test_config_unknown_uses_meta_regime(self):
        meta = {"META_REGIME_KEY": "BULL"}
        cfg = {"CURRENT_REGIME_KEY": "UNKNOWN", "DYNAMIC_KELLY_RISK": 0.01}
        self.assertEqual(_resolve_overseer_config_regime(meta, cfg), "BULL")

    def test_kelly_lift_on_meta_bull_config_unknown(self):
        meta = {
            "META_REGIME_KEY": "BULL",
            "META_GLOBAL_KELLY_MULT": 1.0,
            "META_REGIME_ACTION": {"kelly_cap": 0.028},
        }
        cfg = {"CURRENT_REGIME_KEY": "UNKNOWN", "DYNAMIC_KELLY_RISK": 0.01}
        with mock.patch(
            "meta_state_store.resolve_config_regime_key", return_value="UNKNOWN"
        ):
            eff = _resolve_overseer_kelly_display(meta, cfg, 0.01)
        self.assertGreater(eff, 0.02)


class TestPipelineCriticalAbort(unittest.TestCase):
    def test_critical_failure_skips_downstream(self):
        calls: list[str] = []

        def boom() -> None:
            calls.append("critical")
            raise RuntimeError("sync failed")

        def overseer() -> None:
            calls.append("overseer")

        pipeline = [
            StepSpec("meta_governor_sync", boom, critical=True),
            StepSpec("ai_overseer", overseer, critical=False),
        ]
        report = dispatch_factory_mode(
            "daily_audit_kr",
            pipeline,
            skip_telegram=True,
        )
        self.assertEqual(calls, ["critical"])
        self.assertFalse(report.all_critical_ok)
        skipped = [s for s in report.steps if s.name == "ai_overseer"]
        self.assertEqual(len(skipped), 1)
        self.assertFalse(skipped[0].ok)
        self.assertIn("skipped", skipped[0].error or "")


if __name__ == "__main__":
    unittest.main()
