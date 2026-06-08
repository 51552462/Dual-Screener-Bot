"""Phase 7 validation unit tests."""
from __future__ import annotations

import os
import tempfile
import unittest


class TestSignalParity(unittest.TestCase):
    def test_compare_empty_union_passes(self):
        from bitget.validation import signal_parity

        r = signal_parity.compare_signal_parity(
            baseline={"hit_keys": [], "recorded_at_utc": "x"},
            current_keys=set(),
        )
        self.assertTrue(r["passed"])

    def test_compare_detects_diff(self):
        from bitget.validation import signal_parity

        base = {"hit_keys": ["A:1H:ENG1", "B:4H:ENG2"], "recorded_at_utc": "x"}
        cur = {"A:1H:ENG1", "C:1H:ENG3"}
        r = signal_parity.compare_signal_parity(baseline=base, current_keys=cur, max_diff_pct=1.0)
        self.assertFalse(r["passed"])
        self.assertGreater(r["diff_pct"], 1.0)


class TestPnlParity(unittest.TestCase):
    def test_fingerprint_stable(self):
        from bitget.validation.pnl_parity import _fingerprint_db

        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "t.sqlite")
            import sqlite3

            conn = sqlite3.connect(path)
            conn.execute(
                """
                CREATE TABLE bitget_forward_trades (
                    id INTEGER PRIMARY KEY, symbol TEXT, timeframe TEXT,
                    market_type TEXT, position_side TEXT, sig_type TEXT,
                    entry_price REAL, margin_used REAL, sim_kelly_invest REAL,
                    status TEXT, final_ret REAL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                INSERT INTO bitget_forward_trades
                (symbol,timeframe,market_type,position_side,sig_type,entry_price,
                 margin_used,sim_kelly_invest,status)
                VALUES ('BTC_USDT','1H','futures','LONG','S1',1,10,100,'OPEN')
                """
            )
            conn.commit()
            conn.close()
            a = _fingerprint_db(path)
            b = _fingerprint_db(path)
            self.assertEqual(a["open_fingerprint"], b["open_fingerprint"])


class TestLoadTest(unittest.TestCase):
    def test_load_test_runs_on_real_db_if_present(self):
        from bitget.validation.load_test import run_load_test
        from bitget.infra.data_paths import market_data_db_path

        if not os.path.isfile(market_data_db_path()):
            self.skipTest("no market db")
        r = run_load_test(min_symbols=1, min_timeframes=1, max_elapsed_sec=600)
        self.assertTrue(r.get("ok"))


class TestCutover(unittest.TestCase):
    def test_cutover_without_ssot_not_passed(self):
        from bitget.validation.cutover import check_cutover_readiness

        old = os.environ.pop("BITGET_PIPELINE_SSOT", None)
        try:
            os.environ["BITGET_PIPELINE_SSOT"] = "0"
            r = check_cutover_readiness()
            self.assertFalse(r["passed"])
        finally:
            if old is None:
                os.environ.pop("BITGET_PIPELINE_SSOT", None)
            else:
                os.environ["BITGET_PIPELINE_SSOT"] = old


if __name__ == "__main__":
    unittest.main()
