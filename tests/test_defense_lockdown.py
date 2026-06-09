"""Defense lockdown — session gate, scanned JSON cache, entry_date SQL."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from unittest import mock

import pytz

from daily_dispatch_cache import mark_scanned_today, was_scanned_today
from market_session_gate import is_market_open


class TestMarketSessionGate(unittest.TestCase):
    def test_kr_open_midday(self):
        kst = pytz.timezone("Asia/Seoul")
        ref = kst.localize(datetime(2026, 6, 10, 11, 0, 0))
        with mock.patch("market_session_gate.datetime") as mdt:
            mdt.now.side_effect = lambda tz=None: ref if tz else ref
            ok, _ = is_market_open("KR")
        self.assertTrue(ok)

    def test_kr_closed_evening(self):
        kst = pytz.timezone("Asia/Seoul")
        ref = kst.localize(datetime(2026, 6, 10, 16, 0, 0))
        with mock.patch("market_session_gate.datetime") as mdt:
            mdt.now.side_effect = lambda tz=None: ref if tz else ref
            ok, msg = is_market_open("KR")
        self.assertFalse(ok)
        self.assertIn("장외", msg)

    def test_us_open_ny_regular(self):
        et = pytz.timezone("America/New_York")
        ref = et.localize(datetime(2026, 6, 10, 11, 0, 0))
        with mock.patch("market_session_gate.datetime") as mdt:
            mdt.now.side_effect = lambda tz=None: ref if tz else ref
            ok, _ = is_market_open("US")
        self.assertTrue(ok)


class TestScannedTodayJson(unittest.TestCase):
    def test_mark_and_was_persist(self):
        with mock.patch("daily_dispatch_cache._scanned_cache_path") as p:
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
                path = f.name
            p.return_value = path
            try:
                mark_scanned_today("KR", "005930")
                self.assertTrue(was_scanned_today("KR", "005930"))
                self.assertFalse(was_scanned_today("KR", "000660"))
            finally:
                import os

                os.unlink(path)


class TestEntryDateSql(unittest.TestCase):
    def test_legacy_timestamp_blocks_reentry(self):
        from forward.shared import _sql_entry_date_normalized

        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db = f.name
        try:
            conn = sqlite3.connect(db)
            entry_d = _sql_entry_date_normalized("entry_date")
            conn.execute(
                """
                CREATE TABLE forward_trades (
                    id INTEGER PRIMARY KEY, entry_date TEXT, market TEXT,
                    code TEXT, status TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO forward_trades VALUES (1, ?, 'KR', '005930', 'CLOSED_WIN')",
                ("2026-06-09T16:30:00+09:00",),
            )
            cur = conn.execute(
                f"SELECT id FROM forward_trades WHERE market='KR' AND code='005930' "
                f"AND {entry_d} = date(?)",
                ("2026-06-09",),
            )
            self.assertIsNotNone(cur.fetchone())
            conn.close()
        finally:
            import os

            os.unlink(db)


if __name__ == "__main__":
    unittest.main()
