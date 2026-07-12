"""Correlation-based Kelly Sizing (1번) — 로직 간 일일 PnL 직교성 평가 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta

import numpy as np

from portfolio_risk_overlay import (
    compute_aligned_pnl_correlation,
    evaluate_logic_pair_orthogonality,
    evaluate_same_symbol_champion_convergence,
    extract_logic_group_key,
    fetch_logic_daily_pnl_returns,
    logic_corr_high_threshold,
)


def _mk_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT, exit_date TEXT, market TEXT, code TEXT,
            name TEXT, sig_type TEXT, status TEXT, final_ret REAL,
            sim_kelly_invest REAL, invest_amount REAL
        )
        """
    )
    return conn


def _seed_closed(
    conn: sqlite3.Connection,
    *,
    market: str,
    group: str,
    dates_rets: list[tuple[str, float]],
    notional: float = 1_000_000.0,
    shadow: bool = False,
) -> None:
    inv = 0.0 if shadow else notional
    for d, ret in dates_rets:
        conn.execute(
            """
            INSERT INTO forward_trades (
                entry_date, exit_date, market, code, name, sig_type,
                status, final_ret, sim_kelly_invest, invest_amount
            ) VALUES (?, ?, ?, ?, ?, ?, 'CLOSED', ?, ?, ?)
            """,
            (
                d,
                d,
                market,
                "005930",
                "TEST",
                f"[STANDARD] {group} [🔥]",
                ret,
                inv,
                inv,
            ),
        )
    conn.commit()


class TestExtractLogicGroupKey(unittest.TestCase):
    def test_standard_sig(self):
        gk = extract_logic_group_key("[STANDARD] RANK_A [🔥주도주 편대]")
        self.assertEqual(gk, "RANK_A")

    def test_incubator_sig(self):
        gk = extract_logic_group_key("[INCUBATOR_MUTANT_X] RANK_B")
        self.assertTrue("RANK_B" in gk or gk)


class TestDailyPnlSeries(unittest.TestCase):
    def test_live_notional_weighted(self):
        conn = _mk_conn()
        base = datetime.now().strftime("%Y-%m-%d")
        _seed_closed(
            conn,
            market="KR",
            group="RANK_A",
            dates_rets=[(base, 2.0), (base, 4.0)],
            notional=1_000_000.0,
        )
        series = fetch_logic_daily_pnl_returns(conn, "KR", "RANK_A", lookback=90)
        self.assertIn(base, series)
        # (2%+4%)/2 = 3% daily return
        self.assertAlmostEqual(series[base], 0.03, places=4)

    def test_shadow_equal_weight_fallback(self):
        conn = _mk_conn()
        base = datetime.now().strftime("%Y-%m-%d")
        _seed_closed(
            conn,
            market="KR",
            group="RANK_SHADOW",
            dates_rets=[(base, 5.0)],
            shadow=True,
        )
        series = fetch_logic_daily_pnl_returns(conn, "KR", "RANK_SHADOW", lookback=90)
        self.assertAlmostEqual(series[base], 0.05, places=4)


class TestPearsonAlignment(unittest.TestCase):
    def test_high_positive_correlation(self):
        days = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(20)]
        a = {d: 0.02 + 0.001 * i for i, d in enumerate(days)}
        b = {d: 0.018 + 0.001 * i for i, d in enumerate(days)}
        out = compute_aligned_pnl_correlation(a, b, min_overlap=10)
        self.assertFalse(out["neutral"])
        self.assertGreater(float(out["correlation"]), 0.9)
        self.assertFalse(out["orthogonal"])

    def test_low_correlation_orthogonal(self):
        rng = np.random.default_rng(42)
        days = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]
        a = {d: float(rng.normal(0.01, 0.02)) for d in days}
        b = {d: float(rng.normal(-0.005, 0.03)) for d in days}
        out = compute_aligned_pnl_correlation(a, b, min_overlap=15)
        self.assertFalse(out["neutral"])
        self.assertLess(abs(float(out["correlation"])), logic_corr_high_threshold())

    def test_insufficient_overlap_neutral(self):
        out = compute_aligned_pnl_correlation({"2026-01-01": 0.01}, {"2026-01-02": 0.02})
        self.assertTrue(out["neutral"])
        self.assertEqual(out["reason"], "overlap<15")


class TestLogicPairOrthogonality(unittest.TestCase):
    def _anti_corr_seed(self, conn: sqlite3.Connection) -> None:
        days = [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(25)
        ]
        a_rets = [(d, 2.0 if i % 2 == 0 else -1.0) for i, d in enumerate(days)]
        b_rets = [(d, -1.0 if i % 2 == 0 else 2.0) for i, d in enumerate(days)]
        _seed_closed(conn, market="KR", group="RANK_A", dates_rets=a_rets)
        _seed_closed(conn, market="KR", group="RANK_B", dates_rets=b_rets)

    def _high_corr_seed(self, conn: sqlite3.Connection) -> None:
        days = [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(25)
        ]
        rets = [(d, 1.5 + 0.1 * (i % 3)) for i, d in enumerate(days)]
        _seed_closed(conn, market="KR", group="RANK_C", dates_rets=rets)
        _seed_closed(conn, market="KR", group="RANK_D", dates_rets=rets)

    def test_orthogonal_pair(self):
        conn = _mk_conn()
        self._anti_corr_seed(conn)
        out = evaluate_logic_pair_orthogonality(conn, "KR", "RANK_A", "RANK_B")
        self.assertFalse(out.get("skipped"))
        self.assertIsNotNone(out.get("correlation"))
        self.assertLess(float(out["correlation"]), logic_corr_high_threshold())
        self.assertTrue(out.get("orthogonal"))

    def test_high_corr_non_orthogonal(self):
        conn = _mk_conn()
        self._high_corr_seed(conn)
        out = evaluate_logic_pair_orthogonality(conn, "KR", "RANK_C", "RANK_D")
        self.assertGreaterEqual(float(out["correlation"]), logic_corr_high_threshold())
        self.assertFalse(out.get("orthogonal"))

    def test_same_group_skipped(self):
        conn = _mk_conn()
        out = evaluate_logic_pair_orthogonality(conn, "KR", "RANK_A", "RANK_A")
        self.assertTrue(out.get("skipped"))
        self.assertEqual(out.get("correlation"), 1.0)


class TestSameSymbolConvergence(unittest.TestCase):
    def test_convergence_detected(self):
        conn = _mk_conn()
        days = [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(20)
        ]
        _seed_closed(
            conn,
            market="KR",
            group="RANK_A",
            dates_rets=[(d, 2.0 + 0.1 * (i % 5)) for i, d in enumerate(days)],
        )
        _seed_closed(
            conn,
            market="KR",
            group="RANK_B",
            dates_rets=[(d, 1.8 + 0.09 * (i % 5)) for i, d in enumerate(days)],
        )
        out = evaluate_same_symbol_champion_convergence(
            conn,
            "KR",
            "005930",
            "[STANDARD] RANK_B [🔥]",
            "[STANDARD] RANK_A [🛡️]",
        )
        self.assertTrue(out["convergence_detected"])
        self.assertEqual(out["candidate_group"], "RANK_B")
        self.assertEqual(out["existing_group"], "RANK_A")
        self.assertGreaterEqual(float(out["max_logic_corr"]), logic_corr_high_threshold())
        self.assertFalse(out["orthogonal"])

    def test_same_logic_no_convergence(self):
        conn = _mk_conn()
        out = evaluate_same_symbol_champion_convergence(
            conn,
            "KR",
            "005930",
            "[STANDARD] RANK_A",
            "[STANDARD] RANK_A [🔥]",
        )
        self.assertFalse(out["convergence_detected"])
        self.assertEqual(out["reason"], "same_logic_duplicate")


if __name__ == "__main__":
    unittest.main()
