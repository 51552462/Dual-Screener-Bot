"""US listing cache-first — FDR 중복 호출 방지."""
from __future__ import annotations

import os
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


class TestUsListSurvivalCache(unittest.TestCase):
    def test_fresh_csv_skips_fdr(self) -> None:
        import us_list_survival as uls

        uls._SESSION_CACHE.clear()
        with tempfile.TemporaryDirectory() as td:
            cache = os.path.join(td, "us_list_cache.csv")
            rows = pd.DataFrame(
                {
                    "Code": [f"T{i}" for i in range(500)],
                    "Name": [f"T{i}" for i in range(500)],
                    "Market": ["US"] * 500,
                }
            )
            rows.to_csv(cache, index=False)
            os.utime(cache, (time.time(), time.time()))

            mock_fdr = MagicMock()
            mock_fdr.StockListing.side_effect = AssertionError("FDR must not be called")

            df, src = uls.collect_us_list_survival(
                db_path=os.path.join(td, "market_data.sqlite"),
                primary_cache_csv=cache,
                min_live_rows=400,
                fdr_module=mock_fdr,
            )
            self.assertEqual(src, "cache")
            self.assertGreaterEqual(len(df), 400)

    def test_session_cache_second_call_no_fdr(self) -> None:
        import us_list_survival as uls

        uls._SESSION_CACHE.clear()
        with tempfile.TemporaryDirectory() as td:
            cache = os.path.join(td, "us_list_cache.csv")
            rows = pd.DataFrame(
                {
                    "Code": [f"X{i}" for i in range(500)],
                    "Name": [f"X{i}" for i in range(500)],
                    "Market": ["US"] * 500,
                }
            )
            rows.to_csv(cache, index=False)
            os.utime(cache, (time.time(), time.time()))
            db = os.path.join(td, "market_data.sqlite")

            mock_fdr = MagicMock()
            mock_fdr.StockListing.side_effect = AssertionError("FDR must not be called")

            uls.collect_us_list_survival(
                db_path=db, primary_cache_csv=cache, min_live_rows=400, fdr_module=mock_fdr
            )
            df2, src2 = uls.collect_us_list_survival(
                db_path=db, primary_cache_csv=cache, min_live_rows=400, fdr_module=mock_fdr
            )
            self.assertEqual(src2, "cache")
            self.assertGreaterEqual(len(df2), 400)


if __name__ == "__main__":
    unittest.main()
