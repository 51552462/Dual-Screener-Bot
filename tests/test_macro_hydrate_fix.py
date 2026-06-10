"""macro empty_row — lookback fallback must not crash pipeline."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from unittest import mock

from report_pipeline_hydrate import (
    _load_macro_row_lookback,
    _row_has_usable_macro_data,
    ensure_report_pipeline_data,
    refresh_macro_daily,
)


class TestMacroLookback(unittest.TestCase):
    def test_usable_row_detection(self):
        self.assertTrue(_row_has_usable_macro_data({"date": "2026-06-09", "vix_index": 18.5}))
        self.assertFalse(_row_has_usable_macro_data({"date": "2026-06-09"}))

    def test_lookback_finds_yesterday_row(self):
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db = f.name
        try:
            conn = sqlite3.connect(db)
            conn.execute(
                """
                CREATE TABLE macro_daily (
                    date TEXT PRIMARY KEY, usd_krw REAL, us_10y_yield REAL, vix_index REAL
                )
                """
            )
            conn.execute(
                "INSERT INTO macro_daily VALUES (?, ?, ?, ?)",
                ("2026-06-08", 1380.0, 4.2, 17.5),
            )
            conn.commit()
            conn.close()
            with mock.patch("factory_data_paths.alt_data_db_path", return_value=db):
                row = _load_macro_row_lookback(max_rows=10)
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(str(row["date"])[:10], "2026-06-08")
            self.assertAlmostEqual(float(row["vix_index"]), 17.5)
        finally:
            import os

            os.unlink(db)

    def test_empty_row_uses_lookback_not_crash(self):
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db = f.name
        try:
            conn = sqlite3.connect(db)
            conn.execute(
                """
                CREATE TABLE macro_daily (
                    date TEXT PRIMARY KEY, usd_krw REAL, us_10y_yield REAL, vix_index REAL
                )
                """
            )
            conn.execute(
                "INSERT INTO macro_daily VALUES (?, ?, ?, ?)",
                ("2026-06-09T00:00:00", 1390.0, 4.1, 16.0),
            )
            conn.commit()
            conn.close()

            with mock.patch("factory_data_paths.alt_data_db_path", return_value=db):
                with mock.patch(
                    "legacy_archive.alt_data_miner.run_once", return_value={}
                ):
                    out = refresh_macro_daily()
            self.assertTrue(out.get("ok"))
            self.assertEqual(out.get("source"), "lookback")
            self.assertEqual(out.get("fallback_reason"), "empty_row")
        finally:
            import os

            os.unlink(db)

    def test_no_db_degraded_pipeline_continues(self):
        with mock.patch(
            "legacy_archive.alt_data_miner.run_once", return_value={}
        ), mock.patch(
            "report_pipeline_hydrate._load_macro_row_lookback", return_value=None
        ), mock.patch(
            "report_pipeline_hydrate._load_macro_row_lookback_via_miner",
            return_value=None,
        ), mock.patch(
            "report_pipeline_hydrate.refresh_kr_benchmarks",
            return_value={"ok": True},
        ), mock.patch(
            "cross_market_ssot.hydrate_kr_runtime_from_ssot",
            return_value={"ok": True},
        ):
            out = ensure_report_pipeline_data(
                market="KR",
                refresh_ohlcv=True,
            )
        self.assertTrue(out["macro"].get("degraded"))
        self.assertNotIn("macro:empty_row", str(out))


if __name__ == "__main__":
    unittest.main()
