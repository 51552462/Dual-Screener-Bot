"""A-2 — Tail-risk fund consumption circuit (TAIL_FUND_CONSUMPTION_ENABLED)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from bitget.trading.tail_risk_gate import (
    compute_tail_fund_debit_amount,
    evaluate_tail_fund_gate,
    process_tail_fund_drawdown_on_snap,
    tail_fund_consumption_enabled,
    tail_fund_total_usdt,
    tail_risk_entry_blocked,
)


class TestTailFundDebit(unittest.TestCase):
    def test_debit_formula_uses_a1_block_pct(self):
        debit = compute_tail_fund_debit_amount(
            nav_peak=1000.0,
            dd_pct=0.25,
            block_pct=0.20,
            tail_fund_balance=100.0,
        )
        self.assertAlmostEqual(debit, 50.0)

    def test_debit_capped_by_balance(self):
        debit = compute_tail_fund_debit_amount(
            nav_peak=1000.0,
            dd_pct=0.30,
            block_pct=0.20,
            tail_fund_balance=30.0,
        )
        self.assertAlmostEqual(debit, 30.0)

    def test_no_debit_below_block_threshold(self):
        debit = compute_tail_fund_debit_amount(
            nav_peak=1000.0,
            dd_pct=0.18,
            block_pct=0.20,
            tail_fund_balance=100.0,
        )
        self.assertEqual(debit, 0.0)


class TestTailFundGate(unittest.TestCase):
    def test_escalate_block_only_when_exhausted_and_block_tier(self):
        ok = evaluate_tail_fund_gate(0.0, "BLOCK")
        self.assertTrue(ok["tail_exhausted"])
        self.assertTrue(ok["escalate_block"])

        no_esc = evaluate_tail_fund_gate(10.0, "BLOCK")
        self.assertFalse(no_esc["escalate_block"])

        halt = evaluate_tail_fund_gate(0.0, "HALT")
        self.assertFalse(halt["escalate_block"])


class TestTailFundConsumption(unittest.TestCase):
    def test_snap_cache_delegates_to_portfolio_mdd_gate_only(self):
        cfg = {"TREASURY_SPOT_USDT": 500.0, "TREASURY_FUTURES_USDT": 500.0}
        with patch(
            "bitget.trading.execution_safety.evaluate_portfolio_mdd_gate",
            return_value={"tier": "NORMAL", "dd_pct": 0.0, "nav_peak": 1000.0},
        ) as gate_fn:
            from bitget.trading.execution_safety import get_portfolio_mdd_snap_cached

            snap1 = get_portfolio_mdd_snap_cached(cfg)
            snap2 = get_portfolio_mdd_snap_cached(cfg)
        self.assertIs(snap1, snap2)
        gate_fn.assert_called_once()

    def _block_snap(self) -> dict:
        return {
            "tier": "BLOCK",
            "dd_pct": 0.25,
            "nav_peak": 1000.0,
            "nav_current": 750.0,
            "blocks_entry": True,
        }

    def test_drawdown_event_debits_paper_fund(self):
        cfg = {
            "TAIL_FUND_CONSUMPTION_ENABLED": True,
            "TAIL_RISK_FUND_SPOT": 80.0,
            "TAIL_RISK_FUND_FUTURES": 20.0,
            "PORTFOLIO_MDD_BLOCK_PCT": 0.20,
        }
        with patch(
            "bitget.trading.tail_risk_gate._persist_tail_fund_balances", return_value=True
        ):
            out = process_tail_fund_drawdown_on_snap(cfg, self._block_snap())
        self.assertTrue(out["drawdown_event"])
        self.assertAlmostEqual(out["debited"], 50.0)
        self.assertAlmostEqual(tail_fund_total_usdt(cfg), 50.0)

    def test_escalate_block_without_portfolio_tier_mutation(self):
        cfg = {
            "TAIL_FUND_CONSUMPTION_ENABLED": True,
            "TAIL_RISK_FUND_SPOT": 0.0,
            "TAIL_RISK_FUND_FUTURES": 0.0,
            "PORTFOLIO_MDD_CURRENT_TIER": "BLOCK",
            "PORTFOLIO_MDD_BLOCK_PCT": 0.20,
        }
        snap = self._block_snap()
        with patch(
            "bitget.trading.execution_safety.get_portfolio_mdd_snap_cached",
            return_value=snap,
        ), patch(
            "bitget.trading.tail_risk_gate._persist_tail_fund_balances",
            return_value=True,
        ), patch(
            "bitget.trading.execution_safety._maybe_portfolio_halt_alert"
        ) as halt_alert:
            blocked, meta = tail_risk_entry_blocked(cfg)
        self.assertTrue(blocked)
        self.assertEqual(meta.get("tail_risk_gate"), "escalate_block_exhausted")
        self.assertEqual(cfg.get("PORTFOLIO_MDD_CURRENT_TIER"), "BLOCK")
        halt_alert.assert_not_called()

    def test_consumption_disabled_restores_legacy_path(self):
        cfg = {
            "TAIL_FUND_CONSUMPTION_ENABLED": False,
            "TAIL_RISK_FUND_SPOT": 0.0,
            "TAIL_RISK_FUND_FUTURES": 0.0,
            "TAIL_RISK_EMPTY_BLOCK": True,
            "NAV_DD_REDUCE_PCT": 15,
        }
        self.assertFalse(tail_fund_consumption_enabled(cfg))
        with patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 1000.0, "mdd_pct": 16.0},
        ):
            blocked, meta = tail_risk_entry_blocked(cfg)
        self.assertTrue(blocked)
        self.assertEqual(meta.get("tail_risk_gate"), "block_empty_under_dd")


if __name__ == "__main__":
    unittest.main()
