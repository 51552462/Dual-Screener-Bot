"""P2 — RL 사후평가 MegaTrend 태그 필터 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime

from mega_trend_kill_rl import (
    evaluate_pending_kill_events,
    measure_post_kill_sector_outcome,
    record_mega_trend_kill_event,
)
from mega_trend_trade_filter import (
    is_mega_trend_sig_type,
    is_mega_trend_unlock_trade,
)


class TestMegaTrendTradeFilter(unittest.TestCase):
    def test_sig_type_markers(self):
        self.assertTrue(is_mega_trend_sig_type("#MegaTrend언락"))
        self.assertTrue(is_mega_trend_sig_type("[MEGA_TREND] test"))
        self.assertFalse(is_mega_trend_sig_type("[STANDARD] GROUP_A"))

    def test_unlock_window_by_ignited_at(self):
        self.assertTrue(
            is_mega_trend_unlock_trade(
                sig_type="plain",
                entry_date="2026-02-05",
                ignited_at="2026-02-01",
            )
        )
        self.assertFalse(
            is_mega_trend_unlock_trade(
                sig_type="plain",
                entry_date="2026-01-15",
                ignited_at="2026-02-01",
            )
        )


class TestPostKillMegaTrendFilter(unittest.TestCase):
    def _mk_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE forward_trades (
                id INTEGER PRIMARY KEY,
                market TEXT, code TEXT, sector TEXT, sig_type TEXT,
                sim_stat_ret REAL, final_ret REAL, status TEXT,
                entry_date TEXT, exit_date TEXT
            )
            """
        )
        rows = [
            # MegaTrend — should count
            (
                1,
                "KR",
                "005930",
                "반도체/IT",
                "#MegaTrend언락",
                4.5,
                4.5,
                "CLOSED_WIN",
                "2026-02-08",
                "2026-02-09",
            ),
            # Same sector, no tag, before ignited — should NOT count
            (
                2,
                "KR",
                "035420",
                "반도체/IT",
                "[STANDARD] A",
                8.0,
                8.0,
                "CLOSED_WIN",
                "2026-01-20",
                "2026-02-09",
            ),
            # Same sector, no tag, after ignited & kill — unlock window assist
            (
                3,
                "KR",
                "000660",
                "반도체/IT",
                "[STANDARD] B",
                -3.0,
                -3.0,
                "CLOSED_LOSS",
                "2026-02-08",
                "2026-02-10",
            ),
        ]
        for row in rows:
            conn.execute(
                """
                INSERT INTO forward_trades (
                    id, market, code, sector, sig_type,
                    sim_stat_ret, final_ret, status, entry_date, exit_date
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                row,
            )
        conn.commit()
        return conn

    def test_filters_out_sector_beta_noise(self):
        conn = self._mk_conn()
        out = measure_post_kill_sector_outcome(
            conn,
            "반도체/IT",
            "2026-02-07",
            eval_days=5,
            ignited_at="2026-02-01",
            mega_trend_only=True,
        )
        self.assertEqual(out["reason"], "computed_megatrend")
        self.assertEqual(out["n_trades"], 2)
        self.assertEqual(out["n_trades_sector_all"], 3)
        self.assertEqual(out["n_trades_tagged"], 1)
        self.assertEqual(out["n_trades_unlock_window"], 1)
        # (4.5 + -3.0) / 2 = 0.75 — not 8% rally from noise trade
        self.assertAlmostEqual(out["avg_ret_pct"], 0.75, places=2)

    def test_without_filter_includes_all_sector(self):
        conn = self._mk_conn()
        out = measure_post_kill_sector_outcome(
            conn,
            "반도체/IT",
            "2026-02-07",
            eval_days=5,
            mega_trend_only=False,
        )
        self.assertEqual(out["n_trades"], 3)
        self.assertGreater(out["avg_ret_pct"], 3.0)

    def test_no_megatrend_sample_returns_neutral_reason(self):
        conn = self._mk_conn()
        out = measure_post_kill_sector_outcome(
            conn,
            "금융/지주",
            "2026-02-07",
            eval_days=5,
            ignited_at="2026-02-01",
            mega_trend_only=True,
        )
        self.assertIsNone(out["avg_ret_pct"])
        self.assertEqual(out["reason"], "no_megatrend_trades_in_window")

    def test_evaluate_event_uses_ignited_at_from_record(self):
        conn = self._mk_conn()
        cfg: dict = {}
        record_mega_trend_kill_event(
            cfg,
            sector="반도체/IT",
            kill_type="internal_momentum",
            kill_at="2026-02-07 10:00:00",
            ignited_at="2026-02-01",
        )
        events = cfg["MEGA_TREND_KILL_RL_STATE"]["kill_events"]
        evaluated = evaluate_pending_kill_events(
            events, conn, now=datetime(2026, 2, 15)
        )
        self.assertEqual(evaluated[0]["outcome"], "neutral")
        post = evaluated[0]["post_kill"]
        self.assertEqual(post["n_trades"], 2)
        self.assertTrue(post.get("mega_trend_filter"))


if __name__ == "__main__":
    unittest.main()
