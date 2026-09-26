"""CAT-H-REPAIR-01 — REGIME_INDEX · OOS L604 · JSON pipeline_ok."""
from __future__ import annotations

import ast
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from mutant_oos_validator import _oos_forward_returns_at_signals, _stamp_pipeline_fields
from reports.director_watchdog import load_cath_h_watch_line
from synthetic_data_generator import (
    REGIME_INDEX,
    REGIMES,
    simulate_regime_switching_ohlcv,
)


class TestCathHRepair01(unittest.TestCase):
    def test_regime_index_covers_defined_regimes(self) -> None:
        self.assertEqual(REGIME_INDEX["BLACK_SWAN"], 3)
        self.assertNotIn("HIGH_VOL", REGIME_INDEX)
        for i, r in enumerate(REGIMES):
            self.assertEqual(REGIME_INDEX[r.name], i)

    def test_init_db_adds_regime_column(self) -> None:
        import sqlite3
        import tempfile
        from synthetic_data_generator import _init_db

        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                "CREATE TABLE synthetic_ohlcv (ticker TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, PRIMARY KEY (ticker, date))"
            )
            conn.commit()
            _init_db(conn)
            cols = [r[1] for r in conn.execute("PRAGMA table_info(synthetic_ohlcv)")]
            conn.close()
            self.assertIn("regime", cols)
        finally:
            os.remove(path)
        df = simulate_regime_switching_ohlcv(n_days=40, initial_price=10000.0, seed=7)
        self.assertGreaterEqual(len(df), 40)
        self.assertTrue((df["high"] >= df["low"]).all())

    def test_oos_l604_indent_compiles_and_eval(self) -> None:
        src = Path("mutant_oos_validator.py").read_text(encoding="utf-8")
        ast.parse(src)
        ev = pd.DataFrame(
            {
                "close": [10.0, 11.0, 12.0, 9.0],
                "high": [11.0, 12.0, 13.0, 10.0],
                "low": [9.0, 10.0, 11.0, 8.0],
            }
        )
        rv = _oos_forward_returns_at_signals("close > 10", ev)
        self.assertIsNotNone(rv)

    def test_stamp_ok_zero_vs_error(self) -> None:
        z = _stamp_pipeline_fields({"promoted": []}, ok=True, error=None, prev={})
        self.assertTrue(z["pipeline_ok"])
        self.assertEqual(z["gate_result"], "ok_zero")
        e = _stamp_pipeline_fields(
            {"promoted": []}, ok=False, error="IndentationError", prev={"last_promoted_at": "2026-07-31"}
        )
        self.assertFalse(e["pipeline_ok"])
        self.assertEqual(e["gate_result"], "error")
        self.assertEqual(e["last_promoted_at"], "2026-07-31")

    def test_watch_line_ok_zero_and_stale(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, "v.json")
            Path(p).write_text(
                '{"pipeline_ok": true, "gate_result": "ok_zero", "promoted": [], "last_promoted_at": null}',
                encoding="utf-8",
            )
            line = load_cath_h_watch_line(json_path=p)
            self.assertEqual(line["light"], "🟢")
            self.assertIn("OK(0)", line["text"])
            os.utime(p, (0, 0))
            stale = load_cath_h_watch_line(json_path=p)
            self.assertEqual(stale["light"], "🔴")
            self.assertIn("STALE", stale["text"])


if __name__ == "__main__":
    unittest.main()
