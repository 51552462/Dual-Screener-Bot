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
                    market TEXT, entry_date TEXT, ticker TEXT, status TEXT,
                    dyn_cpv REAL, dyn_tb REAL, v_energy REAL,
                    sim_kelly_invest REAL, shares REAL, sig_type TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO forward_trades VALUES
                (?, ?, 'TEST', 'OPEN', 0.7, 10.0, 20.0, 500000.0, 10.0, 'RANK_C')
                """,
                (market, entry_date),
            )
            conn.commit()
        finally:
            conn.close()
        return path

    def test_abort_when_session_matches_last_entry(self):
        db = self._make_db("2026-06-09", "US")
        self.addCleanup(lambda: os.path.exists(db) and os.remove(db))
        conn = sqlite3.connect(db)
        conn.execute("UPDATE forward_trades SET status='OPEN'")
        conn.commit()
        conn.close()
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
        self.assertIn("valid_open_dedup", d.reason)

    def test_allow_when_closed_only_entry_same_session(self):
        """CLOSED-only entry_date — OPEN=0 이면 재스캔 허용 (drought 복구)."""
        db = self._make_db("2026-06-09", "KR")
        self.addCleanup(lambda: os.path.exists(db) and os.remove(db))
        conn = sqlite3.connect(db)
        conn.execute(
            "UPDATE forward_trades SET status='CLOSED', entry_date='2026-06-09'"
        )
        conn.commit()
        conn.close()
        guard = SessionDeduplicationGuard({})
        with patch.object(
            guard,
            "resolve_session_date",
            return_value=type(
                "R",
                (),
                {"session_date": "2026-06-09", "mode": "live"},
            )(),
        ):
            d = guard.evaluate("KR", db_path=db)
        self.assertFalse(d.abort_scan)
        self.assertIn(
            d.reason,
            ("no_prior_entry_anchor", "session_anchor_no_live_book", "rescan_allowed"),
        )

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

    def test_funnel_only_does_not_abort(self):
        """funnel 스냅샷만 있고 유효 OPEN=0 이면 재스캔 허용."""
        fd, db = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(db) and os.remove(db))
        conn = sqlite3.connect(db)
        try:
            conn.execute(
                """
                CREATE TABLE scan_funnel_snapshot (
                    ts TEXT, market TEXT, universe_size INTEGER,
                    survivors INTEGER, pass_rate_pct REAL
                )
                """
            )
            conn.execute(
                "INSERT INTO scan_funnel_snapshot VALUES ('2026-06-23 10:00', 'US', 500, 3, 0.6)"
            )
            conn.commit()
        finally:
            conn.close()
        guard = SessionDeduplicationGuard({})
        with patch.object(
            guard,
            "resolve_session_date",
            return_value=type(
                "R",
                (),
                {"session_date": "2026-06-23", "mode": "live"},
            )(),
        ):
            d = guard.evaluate("US", db_path=db)
        self.assertFalse(d.abort_scan)
        self.assertGreater(d.funnel_slots_session, 0)

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
