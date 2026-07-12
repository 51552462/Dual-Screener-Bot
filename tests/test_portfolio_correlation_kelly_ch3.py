"""Correlation-based Kelly Sizing (3번) — 최종 Kelly 클램핑 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from portfolio_risk_overlay import (
    apply_entry_correlation_kelly_overlay,
    corr_kelly_mult,
    evaluate_convergence_entry_gate,
    resolve_correlation_kelly_sizing,
    resolve_correlation_kelly_from_profile,
)


def _mk_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT, code TEXT, name TEXT, sig_type TEXT,
            sector TEXT, status TEXT, sim_kelly_invest REAL, invest_amount REAL,
            exit_date TEXT, entry_date TEXT, final_ret REAL
        )
        """
    )
    return conn


def _seed_closed_logic(
    conn: sqlite3.Connection,
    group: str,
    rets: list[tuple[str, float]],
) -> None:
    for d, r in rets:
        conn.execute(
            """
            INSERT INTO forward_trades (
                market, code, name, sig_type, sector, status,
                exit_date, entry_date, final_ret, sim_kelly_invest, invest_amount
            ) VALUES ('KR', '005930', 'T', ?, '반도체/IT', 'CLOSED', ?, ?, ?, 1e6, 1e6)
            """,
            (f"[STANDARD] {group} [🔥]", d, d, r),
        )
    conn.commit()


def _open_row(conn: sqlite3.Connection, code: str, sig: str) -> None:
    conn.execute(
        """
        INSERT INTO forward_trades (
            market, code, name, sig_type, sector, status,
            sim_kelly_invest, invest_amount
        ) VALUES ('KR', ?, 'T', ?, '반도체/IT', 'OPEN', 5e5, 5e5)
        """,
        (code, sig),
    )
    conn.commit()


def _synthetic_returns(base: float, n: int = 30, step: float = 0.001) -> dict[str, float]:
    return {f"2026-01-{i+1:02d}": base + step * i for i in range(n)}


class TestResolveKellySizing(unittest.TestCase):
    def test_joint_attack(self):
        out = resolve_correlation_kelly_sizing(
            convergence_detected=True,
            orthogonal=True,
            logic_corr=0.25,
            logic_corr_neutral=False,
            portfolio_diversified=True,
            portfolio_concentrated=False,
            portfolio_neutral=False,
            portfolio_max_corr=0.35,
        )
        self.assertEqual(out["action"], "joint_attack")
        self.assertEqual(out["kelly_mult"], 1.0)
        self.assertTrue(out["allow_convergence_entry"])

    def test_single_tail_penalty(self):
        out = resolve_correlation_kelly_sizing(
            convergence_detected=True,
            orthogonal=False,
            logic_corr=0.85,
            portfolio_diversified=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.3,
        )
        self.assertEqual(out["action"], "penalty")
        self.assertEqual(out["kelly_mult"], corr_kelly_mult())

    def test_dual_tail_reject(self):
        out = resolve_correlation_kelly_sizing(
            convergence_detected=True,
            orthogonal=False,
            logic_corr=0.9,
            portfolio_concentrated=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.88,
        )
        self.assertEqual(out["action"], "reject")
        self.assertTrue(out["reject_entry"])

    def test_standard_concentration_penalty(self):
        out = resolve_correlation_kelly_sizing(
            convergence_detected=False,
            portfolio_concentrated=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.82,
        )
        self.assertEqual(out["action"], "penalty")
        self.assertEqual(out["kelly_mult"], corr_kelly_mult())


class TestConvergenceEntryGate(unittest.TestCase):
    def _patch_returns(self, mapping: dict):
        return patch(
            "portfolio_risk_overlay._returns_by_date",
            side_effect=lambda _c, _m, code, _lb: mapping.get(
                str(code).zfill(6)
            ),
        )

    def test_joint_attack_allows_second_champion(self):
        conn = _mk_conn()
        days = [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(20)
        ]
        anti_a = [(d, 2.0 if i % 2 == 0 else -1.0) for i, d in enumerate(days)]
        anti_b = [(d, -1.0 if i % 2 == 0 else 2.0) for i, d in enumerate(days)]
        _seed_closed_logic(conn, "RANK_A", anti_a)
        _seed_closed_logic(conn, "RANK_B", anti_b)
        _open_row(conn, "005930", "[STANDARD] RANK_A [🔥]")
        rets = {
            "005930": _synthetic_returns(0.01),
            "000660": _synthetic_returns(-0.005, step=-0.002),
        }
        rows = [(1, "[STANDARD] RANK_A [🔥]", "2026-07-01")]
        with self._patch_returns(rets):
            gate = evaluate_convergence_entry_gate(
                conn,
                "KR",
                "005930",
                "[STANDARD] RANK_B [🔥]",
                rows,
            )
        self.assertTrue(gate["allow_entry"])
        self.assertEqual(gate["action"], "joint_attack")
        self.assertEqual(gate["kelly_mult"], 1.0)

    def test_dual_tail_rejects(self):
        conn = _mk_conn()
        days = [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(20)
        ]
        same = [(d, 1.5 + 0.1 * (i % 3)) for i, d in enumerate(days)]
        _seed_closed_logic(conn, "RANK_A", same)
        _seed_closed_logic(conn, "RANK_B", same)
        _open_row(conn, "005930", "[STANDARD] RANK_A [🔥]")
        _open_row(conn, "000660", "[STANDARD] RANK_C [🔥]")
        base = _synthetic_returns(0.01)
        rets = {"005930": base, "000660": dict(base)}
        rows = [(1, "[STANDARD] RANK_A [🔥]", "2026-07-01")]
        with self._patch_returns(rets):
            gate = evaluate_convergence_entry_gate(
                conn,
                "KR",
                "005930",
                "[STANDARD] RANK_B [🔥]",
                rows,
            )
        self.assertFalse(gate["allow_entry"])
        self.assertEqual(gate["action"], "reject")

    def test_same_logic_blocked(self):
        conn = _mk_conn()
        rows = [(1, "[STANDARD] RANK_A [🔥]", "2026-07-01")]
        gate = evaluate_convergence_entry_gate(
            conn, "KR", "005930", "[STANDARD] RANK_A", rows
        )
        self.assertFalse(gate["allow_entry"])
        self.assertTrue(gate["same_logic_block"])


class TestApplyKellyOverlay(unittest.TestCase):
    def test_convergence_penalty_from_facts(self):
        conn = _mk_conn()
        profile = {
            "convergence_detected": True,
            "orthogonal": False,
            "logic_corr": 0.85,
            "portfolio_diversified": True,
            "portfolio_concentrated": False,
            "portfolio_max_corr": 0.2,
            "concentration": {"neutral": False, "max_corr": 0.2},
            "pairwise": [{"neutral": False}],
        }
        facts = {
            "_convergence_profile": profile,
            "_correlation_sizing_action": "penalty",
            "_correlation_kelly_mult": 0.5,
        }
        kelly, sig, detail = apply_entry_correlation_kelly_overlay(
            conn, "KR", "005930", "[STANDARD] RANK_B", 0.04, facts=facts
        )
        self.assertAlmostEqual(kelly, 0.02, places=6)
        self.assertIn("CorrKelly페널티", sig)
        self.assertEqual(detail["action"], "penalty")

    def test_standard_concentration_penalty(self):
        conn = _mk_conn()
        _open_row(conn, "000660", "[STANDARD] RANK_C")
        base = _synthetic_returns(0.01)
        rets = {"005930": base, "000660": dict(base)}
        with patch(
            "portfolio_risk_overlay._returns_by_date",
            side_effect=lambda _c, _m, code, _lb: rets.get(str(code).zfill(6)),
        ):
            kelly, sig, detail = apply_entry_correlation_kelly_overlay(
                conn, "KR", "005930", "[STANDARD] RANK_D", 0.04, facts={}
            )
        self.assertAlmostEqual(kelly, 0.02, places=6)
        self.assertEqual(detail["action"], "penalty")


class TestProfileToSizing(unittest.TestCase):
    def test_from_profile_keys(self):
        profile = {
            "convergence_detected": True,
            "orthogonal": True,
            "logic_corr": 0.2,
            "portfolio_diversified": True,
            "portfolio_concentrated": False,
            "portfolio_max_corr": 0.4,
            "concentration": {"neutral": False, "max_corr": 0.4},
            "pairwise": [{"neutral": False}],
        }
        sz = resolve_correlation_kelly_from_profile(profile)
        self.assertEqual(sz["action"], "joint_attack")


if __name__ == "__main__":
    unittest.main()
