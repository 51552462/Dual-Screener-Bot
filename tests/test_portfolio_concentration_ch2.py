"""Correlation-based Kelly Sizing (2번) — OPEN Concentration 평가 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from portfolio_risk_overlay import (
    check_portfolio_correlation,
    corr_threshold,
    evaluate_champion_convergence_risk_profile,
    evaluate_portfolio_concentration_risk,
    fetch_open_positions,
)


def _mk_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT, code TEXT, name TEXT, sig_type TEXT,
            sector TEXT, status TEXT, sim_kelly_invest REAL, invest_amount REAL
        )
        """
    )
    return conn


def _open_row(
    conn: sqlite3.Connection,
    *,
    market: str,
    code: str,
    sig: str,
    notional: float = 500_000.0,
) -> None:
    conn.execute(
        """
        INSERT INTO forward_trades (
            market, code, name, sig_type, sector, status,
            sim_kelly_invest, invest_amount
        ) VALUES (?, ?, ?, ?, ?, 'OPEN', ?, ?)
        """,
        (market, code, "TEST", sig, "반도체/IT", notional, notional),
    )
    conn.commit()


def _synthetic_returns(base: float, n: int = 30, step: float = 0.001) -> dict[str, float]:
    return {f"2026-01-{i+1:02d}": base + step * i for i in range(n)}


class TestFetchOpenPositions(unittest.TestCase):
    def test_kr_code_normalized(self):
        conn = _mk_conn()
        _open_row(conn, market="KR", code="5930", sig="[STANDARD] RANK_A [🔥]")
        rows = fetch_open_positions(conn, "KR")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["code"], "005930")
        self.assertEqual(rows[0]["group_key"], "RANK_A")


class TestConcentrationRisk(unittest.TestCase):
    def _patch_returns(self, mapping: dict[str, dict[str, float]]):
        def _fake(conn, market, code, lookback):
            key = str(code).zfill(6) if str(market).upper() == "KR" else str(code)
            return mapping.get(key) or mapping.get(str(code))

        return patch(
            "portfolio_risk_overlay._returns_by_date",
            side_effect=_fake,
        )

    def test_diversified_low_correlation(self):
        conn = _mk_conn()
        rets = {
            "005930": _synthetic_returns(0.01, step=0.001),
            "000660": _synthetic_returns(-0.005, step=-0.002),
        }
        with self._patch_returns(rets):
            out = evaluate_portfolio_concentration_risk(
                conn,
                "KR",
                "005930",
                open_codes=["005930", "000660"],
                exclude_same_code=True,
                min_overlap=10,
            )
        self.assertFalse(out["neutral"])
        self.assertTrue(out["diversified"])
        self.assertFalse(out["concentrated"])
        self.assertLess(float(out["max_corr"]), corr_threshold())

    def test_concentrated_high_correlation(self):
        conn = _mk_conn()
        base = _synthetic_returns(0.012, step=0.0008)
        rets = {
            "005930": base,
            "000660": {d: v * 0.98 + 0.0001 for d, v in base.items()},
        }
        with self._patch_returns(rets):
            out = evaluate_portfolio_concentration_risk(
                conn,
                "KR",
                "005930",
                open_codes=["005930", "000660"],
                min_overlap=10,
            )
        self.assertFalse(out["neutral"])
        self.assertTrue(out["concentrated"])
        self.assertFalse(out["diversified"])
        self.assertGreaterEqual(float(out["max_corr"]), corr_threshold())
        self.assertEqual(out["peer_code"], "000660")

    def test_no_peers_neutral(self):
        conn = _mk_conn()
        out = evaluate_portfolio_concentration_risk(
            conn, "KR", "005930", open_codes=["005930"]
        )
        self.assertTrue(out["neutral"])
        self.assertEqual(out["reason"], "no_open_peers")

    def test_pairwise_detail_populated(self):
        conn = _mk_conn()
        rets = {
            "005930": _synthetic_returns(0.01),
            "000660": _synthetic_returns(0.02),
            "035420": _synthetic_returns(-0.01),
        }
        with self._patch_returns(rets):
            out = evaluate_portfolio_concentration_risk(
                conn,
                "KR",
                "005930",
                open_codes=["000660", "035420"],
                min_overlap=10,
            )
        self.assertEqual(len(out["pairwise"]), 2)
        self.assertTrue(all("peer_code" in p for p in out["pairwise"]))


class TestCheckPortfolioCorrelationWrapper(unittest.TestCase):
    def test_conflict_maps_to_concentrated(self):
        conn = _mk_conn()
        base = _synthetic_returns(0.01)
        rets = {"005930": base, "000660": dict(base)}
        with patch(
            "portfolio_risk_overlay._returns_by_date",
            side_effect=lambda _c, _m, code, _lb: rets.get(str(code).zfill(6)),
        ):
            out = check_portfolio_correlation(
                conn, "KR", "005930", ["005930", "000660"], min_overlap=10
            )
        self.assertTrue(out["conflict"])
        self.assertIn("concentration", out)


class TestConvergenceRiskProfile(unittest.TestCase):
    def test_profile_includes_concentration(self):
        conn = _mk_conn()
        _open_row(conn, market="KR", code="005930", sig="[STANDARD] RANK_A")
        _open_row(conn, market="KR", code="000660", sig="[STANDARD] RANK_C")
        rets = {
            "005930": _synthetic_returns(0.01),
            "000660": _synthetic_returns(-0.008, step=-0.0015),
        }
        with patch(
            "portfolio_risk_overlay._returns_by_date",
            side_effect=lambda _c, _m, code, _lb: rets.get(str(code).zfill(6)),
        ):
            profile = evaluate_champion_convergence_risk_profile(
                conn,
                "KR",
                "005930",
                "[STANDARD] RANK_B [🔥]",
                "[STANDARD] RANK_A [🛡️]",
            )
        self.assertTrue(profile["convergence_detected"])
        self.assertIn("concentration", profile)
        self.assertEqual(profile["open_position_count"], 2)
        self.assertIn("portfolio_diversified", profile)
        self.assertIn("portfolio_max_corr", profile)


if __name__ == "__main__":
    unittest.main()
