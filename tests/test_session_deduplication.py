"""SessionDeduplicationGuard — stale session 재스캔 차단."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from session_deduplication_guard import SessionDeduplicationGuard, allow_session_rescan


class TestSessionDeduplicationGuard(unittest.TestCase):
    def _make_db(self, entry_date: str, market: str = "US") -> str:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                """
                CREATE TABLE forward_trades (
                    market TEXT, entry_date TEXT, ticker TEXT,
                    dyn_cpv REAL, dyn_tb REAL, v_energy REAL
                )
                """
            )
            conn.execute(
                "INSERT INTO forward_trades VALUES (?, ?, 'TEST', 0.7, 10.0, 20.0)",
                (market, entry_date),
            )
            conn.commit()
        finally:
            conn.close()
        return path

    def test_abort_when_session_matches_last_entry(self):
        db = self._make_db("2026-06-09", "US")
        self.addCleanup(lambda: os.path.exists(db) and os.remove(db))
        guard = SessionDeduplicationGuard({})
        with patch.object(
            guard,
            "resolve_session_date",
            return_value=type(
                "R",
                (),
                {
                    "session_date": "2026-06-09",
                    "mode": "carry_over",
                },
            )(),
        ):
            d = guard.evaluate("US", db_path=db)
        self.assertTrue(d.abort_scan)
        self.assertEqual(d.session_date, "2026-06-09")
        self.assertIn("stale_session_dedup", d.reason)

    def test_allow_when_session_advances(self):
        db = self._make_db("2026-06-08", "KR")
        self.addCleanup(lambda: os.path.exists(db) and os.remove(db))
        guard = SessionDeduplicationGuard({})
        with patch.object(
            guard,
            "resolve_session_date",
            return_value=type(
                "R",
                (),
                {
                    "session_date": "2026-06-09",
                    "mode": "live",
                },
            )(),
        ):
            d = guard.evaluate("KR", db_path=db)
        self.assertFalse(d.abort_scan)

    def test_force_rescan_bypass(self):
        with patch.dict(os.environ, {"FACTORY_ALLOW_SESSION_RESCAN": "1"}):
            self.assertTrue(allow_session_rescan())
        db = self._make_db("2026-06-09", "US")
        self.addCleanup(lambda: os.path.exists(db) and os.remove(db))
        guard = SessionDeduplicationGuard({})
        with patch.dict(os.environ, {"FACTORY_ALLOW_SESSION_RESCAN": "1"}):
            with patch.object(
                guard,
                "resolve_session_date",
                return_value=type(
                    "R",
                    (),
                    {"session_date": "2026-06-09", "mode": "carry_over"},
                )(),
            ):
                d = guard.evaluate("US", db_path=db)
        self.assertFalse(d.abort_scan)


class TestMarketSessionGateDedup(unittest.TestCase):
    def test_evaluate_session_deduplication_reexport(self):
        from market_session_gate import evaluate_session_deduplication

        with patch(
            "session_deduplication_guard.SessionDeduplicationGuard.evaluate",
            return_value=type(
                "D",
                (),
                {
                    "abort_scan": True,
                    "reason": "test_block",
                    "as_dict": lambda self: {},
                },
            )(),
        ):
            ok, msg = evaluate_session_deduplication("US")
        self.assertFalse(ok)
        self.assertEqual(msg, "test_block")


if __name__ == "__main__":
    unittest.main()
