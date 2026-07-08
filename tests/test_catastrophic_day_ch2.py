"""Ch.2 — 당일 승률 붕괴 클러치 + regime_tag ledger 추론."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime

import pandas as pd

from catastrophic_day_guard import (
    evaluate_catastrophic_day_clutch,
    query_today_closed_stats,
)
from evolution.regime_logic_crossmatrix import (
    classify_regime_tag_from_wr_table,
    count_regime_mismatch_trades,
    infer_regime_tag_from_ledger,
    resolve_regime_tag_for_entry,
)


def _mem_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY,
            market TEXT,
            code TEXT,
            sig_type TEXT,
            status TEXT,
            entry_date TEXT,
            exit_date TEXT,
            entry_regime TEXT,
            final_ret REAL,
            sim_kelly_invest REAL
        )
        """
    )
    return conn


class TestCatastrophicDayGuard(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = _mem_db()
        self.today = "2026-07-08"

    def _seed_closed(self, rets: list[float]) -> None:
        for i, r in enumerate(rets):
            self.conn.execute(
                """
                INSERT INTO forward_trades
                (market, code, sig_type, status, entry_date, exit_date, final_ret, sim_kelly_invest)
                VALUES ('KR', ?, 'RANK_A', 'CLOSED', ?, ?, ?, 1000000)
                """,
                (f"{i:06d}", self.today, self.today, r),
            )
        self.conn.commit()

    def test_insufficient_sample_neutral(self):
        self._seed_closed([-1.0, -2.0])
        out = evaluate_catastrophic_day_clutch(
            self.conn, "KR", self.today, sys_config={}
        )
        self.assertFalse(out["active"])
        self.assertEqual(out["kelly_mult"], 1.0)

    def test_zero_wr_activates_clutch(self):
        self._seed_closed([-1.0, -2.0, -3.0, -1.5, -0.5, -2.2])
        out = evaluate_catastrophic_day_clutch(
            self.conn, "KR", self.today, sys_config={}
        )
        self.assertTrue(out["active"])
        self.assertLess(out["kelly_mult"], 0.5)
        self.assertGreaterEqual(out["kelly_mult"], 0.15)

    def test_block_at_zero_wr_many_closed(self):
        self._seed_closed([-1.0] * 10)
        out = evaluate_catastrophic_day_clutch(
            self.conn,
            "KR",
            self.today,
            sys_config={"ENABLE_CATASTROPHIC_DAY_BLOCK_ENTRIES": True},
        )
        self.assertTrue(out["block_entry"])

    def test_query_stats(self):
        self._seed_closed([2.0, -1.0, 3.0, -2.0, 1.0])
        st = query_today_closed_stats(self.conn, "KR", self.today)
        self.assertEqual(st["n_closed"], 5)
        self.assertAlmostEqual(st["win_rate_pct"], 60.0)


class TestRegimeTagLedgerInfer(unittest.TestCase):
    def test_classify_bull_only(self):
        wr = {
            "BULL": (12, 62.0),
            "BEAR": (8, 12.0),
        }
        self.assertEqual(
            classify_regime_tag_from_wr_table(wr),
            "BULL_ONLY",
        )

    def test_classify_bear_only(self):
        wr = {
            "BULL": (10, 15.0),
            "BEAR": (11, 58.0),
        }
        self.assertEqual(
            classify_regime_tag_from_wr_table(wr),
            "BEAR_ONLY",
        )

    def test_infer_from_ledger(self):
        conn = _mem_db()
        for i in range(8):
            conn.execute(
                """
                INSERT INTO forward_trades
                (market, code, sig_type, status, entry_date, exit_date, entry_regime, final_ret)
                VALUES ('KR', ?, 'RANK_A [test]', 'CLOSED', '2026-06-01', '2026-06-02', 'BULL', 5.0)
                """,
                (f"A{i:03d}",),
            )
        for i in range(6):
            conn.execute(
                """
                INSERT INTO forward_trades
                (market, code, sig_type, status, entry_date, exit_date, entry_regime, final_ret)
                VALUES ('KR', ?, 'RANK_A [test]', 'CLOSED', '2026-06-01', '2026-06-02', 'BEAR', -3.0)
                """,
                (f"B{i:03d}",),
            )
        conn.commit()
        tag = infer_regime_tag_from_ledger(conn, "KR", "RANK_A", sys_config={})
        self.assertEqual(tag, "BULL_ONLY")

    def test_resolve_entry_ledger_fallback(self):
        conn = _mem_db()
        for i in range(7):
            conn.execute(
                """
                INSERT INTO forward_trades
                (market, code, sig_type, status, entry_date, exit_date, entry_regime, final_ret)
                VALUES ('KR', ?, 'SUPERNOVA', 'CLOSED', '2026-06-01', '2026-06-02', 'BULL', 4.0)
                """,
                (f"C{i:03d}",),
            )
        for i in range(5):
            conn.execute(
                """
                INSERT INTO forward_trades
                (market, code, sig_type, status, entry_date, exit_date, entry_regime, final_ret)
                VALUES ('KR', ?, 'SUPERNOVA', 'CLOSED', '2026-06-01', '2026-06-02', 'BEAR', -2.0)
                """,
                (f"D{i:03d}",),
            )
        conn.commit()
        tag, src = resolve_regime_tag_for_entry(
            {},
            sig_type="SUPERNOVA [RANK_A]",
            group_key="SUPERNOVA",
            conn=conn,
            market="KR",
        )
        self.assertEqual(tag, "BULL_ONLY")
        self.assertEqual(src, "ledger_infer")

    def test_count_mismatch_bear_day_bull_only(self):
        df = pd.DataFrame(
            [
                {
                    "market": "KR",
                    "sig_type": "SUPERNOVA [RANK_A]",
                }
            ]
        )
        meta = {
            "META_STRATEGY_REGISTRY": [
                {"group_key": "SUPERNOVA", "regime_tag": "BULL_ONLY"},
            ]
        }
        hits = count_regime_mismatch_trades(
            df, "BEAR", sys_config={}, meta_state=meta, conn=None
        )
        self.assertEqual(hits, 1)


if __name__ == "__main__":
    unittest.main()
