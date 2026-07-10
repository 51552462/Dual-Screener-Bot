"""Mega-Trend Correlation Forgiveness (2번) — CorrKelly Bypass · ROTATION 승인 재검증."""
from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from mega_trend_ignition import (
    MEGA_TREND_CONFIG_KEY,
    is_mega_trend_rotation_advantage,
)
from portfolio_risk_overlay import (
    apply_entry_correlation_kelly_overlay,
    apply_mega_trend_correlation_forgiveness,
    evaluate_convergence_entry_gate,
    evaluate_mega_trend_forgiveness_eligibility,
    resolve_correlation_kelly_sizing,
)


def _mega_cfg(*, rot_adv: bool = True) -> dict:
    return {
        MEGA_TREND_CONFIG_KEY: {
            "active": True,
            "sectors": ["반도체/IT"],
            "primary_sector": "반도체/IT",
            "rotation_advantage_active": rot_adv,
        }
    }


def _mk_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY,
            market TEXT, code TEXT, name TEXT, sig_type TEXT,
            sector TEXT, status TEXT, sim_kelly_invest REAL, invest_amount REAL,
            exit_date TEXT, entry_date TEXT, final_ret REAL
        )
        """
    )
    return conn


def _seed_high_corr_open(conn: sqlite3.Connection, code: str = "005930") -> None:
    conn.execute(
        """
        INSERT INTO forward_trades (
            market, code, name, sig_type, sector, status,
            sim_kelly_invest, invest_amount
        ) VALUES ('KR', ?, 'T', '[STANDARD] GROUP_B [🔥]', '반도체/IT', 'OPEN', 5e5, 5e5)
        """,
        (code,),
    )
    conn.commit()


class TestMegaTrendForgivenessCore(unittest.TestCase):
    def test_bypass_dual_tail_reject(self):
        sizing = resolve_correlation_kelly_sizing(
            convergence_detected=True,
            orthogonal=False,
            logic_corr=0.85,
            logic_corr_neutral=False,
            portfolio_concentrated=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.82,
        )
        self.assertEqual(sizing["action"], "reject")

        forgiven = apply_mega_trend_correlation_forgiveness(
            sizing,
            sys_config=_mega_cfg(),
            candidate_sector="반도체/IT",
            open_sectors=["반도체/IT"],
        )
        self.assertTrue(forgiven["mega_trend_forgiveness"])
        self.assertFalse(forgiven["reject_entry"])
        self.assertEqual(forgiven["kelly_mult"], 1.0)
        self.assertTrue(forgiven["rotation_advantage_approved"])

    def test_bypass_concentration_penalty_only(self):
        sizing = resolve_correlation_kelly_sizing(
            convergence_detected=False,
            portfolio_concentrated=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.85,
        )
        self.assertEqual(sizing["action"], "penalty")
        forgiven = apply_mega_trend_correlation_forgiveness(
            sizing,
            sys_config=_mega_cfg(),
            candidate_sector="반도체/IT",
        )
        self.assertEqual(forgiven["action"], "mega_trend_unlock")
        self.assertEqual(forgiven["kelly_mult"], 1.0)

    def test_no_bypass_outside_mega_sector(self):
        sizing = resolve_correlation_kelly_sizing(
            convergence_detected=False,
            portfolio_concentrated=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.85,
        )
        out = apply_mega_trend_correlation_forgiveness(
            sizing,
            sys_config=_mega_cfg(),
            candidate_sector="금융/지주",
        )
        self.assertEqual(out["action"], "penalty")

    def test_no_bypass_when_open_peer_other_sector(self):
        sizing = resolve_correlation_kelly_sizing(
            convergence_detected=True,
            orthogonal=False,
            logic_corr=0.85,
            portfolio_concentrated=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.82,
        )
        out = apply_mega_trend_correlation_forgiveness(
            sizing,
            sys_config=_mega_cfg(),
            candidate_sector="반도체/IT",
            open_sectors=["금융/지주"],
        )
        self.assertEqual(out["action"], "reject")

    def test_inactive_mega_trend_blocks(self):
        sizing = resolve_correlation_kelly_sizing(
            convergence_detected=False,
            portfolio_concentrated=True,
            portfolio_neutral=False,
            portfolio_max_corr=0.85,
        )
        inactive = {MEGA_TREND_CONFIG_KEY: {"active": False, "sectors": []}}
        self.assertFalse(
            evaluate_mega_trend_forgiveness_eligibility(
                inactive, "반도체/IT"
            )
        )
        out = apply_mega_trend_correlation_forgiveness(
            sizing, sys_config=inactive, candidate_sector="반도체/IT"
        )
        self.assertEqual(out["action"], "penalty")


class TestRotationAdvantageApproval(unittest.TestCase):
    def test_rotation_advantage_flag_on_ignition(self):
        cfg = _mega_cfg(rot_adv=True)
        self.assertTrue(is_mega_trend_rotation_advantage("반도체/IT", cfg))
        self.assertFalse(is_mega_trend_rotation_advantage("금융/지주", cfg))

    def test_rotation_advantage_off_when_deactivated(self):
        cfg = _mega_cfg(rot_adv=False)
        self.assertFalse(is_mega_trend_rotation_advantage("반도체/IT", cfg))


class TestConvergenceEntryGate(unittest.TestCase):
    def test_gate_allows_mega_trend_convergence(self):
        conn = _mk_conn()
        _seed_high_corr_open(conn)
        high_profile = {
            "convergence_detected": True,
            "orthogonal": False,
            "logic_corr": 0.88,
            "portfolio_concentrated": True,
            "portfolio_diversified": False,
            "portfolio_max_corr": 0.81,
            "concentration": {"neutral": False, "concentrated": True, "max_corr": 0.81},
            "pairwise": [{"neutral": False}],
        }
        with patch(
            "portfolio_risk_overlay.evaluate_champion_convergence_risk_profile",
            return_value=high_profile,
        ):
            gate = evaluate_convergence_entry_gate(
                conn,
                "KR",
                "005930",
                "[STANDARD] GROUP_A [🔥]",
                [(1, "[STANDARD] GROUP_B [🔥]", "2026-02-01")],
                sys_config=_mega_cfg(),
                candidate_sector="반도체/IT",
            )
        self.assertTrue(gate["allow_entry"])
        self.assertEqual(gate["action"], "mega_trend_unlock")
        self.assertTrue(gate.get("mega_trend_forgiveness"))
        self.assertTrue(gate.get("rotation_advantage_approved"))


class TestApplyEntryOverlay(unittest.TestCase):
    def test_pre_action_mega_trend_skips_penalty_mult(self):
        conn = _mk_conn()
        facts = {
            "_correlation_sizing_action": "mega_trend_unlock",
            "_correlation_kelly_mult": 1.0,
            "_convergence_profile": {
                "convergence_detected": True,
                "orthogonal": False,
                "logic_corr": 0.9,
                "portfolio_concentrated": True,
                "portfolio_diversified": False,
                "portfolio_max_corr": 0.8,
                "concentration": {"neutral": False},
                "pairwise": [{"neutral": False}],
            },
            "_sys_config": _mega_cfg(),
            "sector": "반도체/IT",
        }
        kelly, sig, detail = apply_entry_correlation_kelly_overlay(
            conn,
            "KR",
            "005930",
            "TEST",
            4.0,
            facts=facts,
            sys_config=_mega_cfg(),
            candidate_sector="반도체/IT",
        )
        self.assertEqual(kelly, 4.0)
        self.assertEqual(detail.get("action"), "mega_trend_unlock")
        self.assertIn("MegaTrend언락", sig)


if __name__ == "__main__":
    unittest.main()
