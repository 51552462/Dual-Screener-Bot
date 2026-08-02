"""A-1 — Portfolio NAV MDD circuit breaker (PORTFOLIO_MDD_* SSOT)."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd


def _synthetic_hist_df(rows: int = 100, price: float = 50_000.0) -> pd.DataFrame:
    close = price + np.linspace(-50, 50, rows)
    return pd.DataFrame(
        {
            "Open": close,
            "High": close + 80,
            "Low": close - 80,
            "Close": close,
            "Volume": [1_000_000.0] * rows,
        }
    )


def _base_try_add_cfg(**overrides) -> dict:
    cfg = {
        "GLOBAL_CIRCUIT_BREAKER": "OFF",
        "BITGET_MAX_OPEN_POSITIONS": 99,
        "PORTFOLIO_MDD_BREAKER_ENABLED": True,
        "PORTFOLIO_NAV_PEAK": 1000.0,
        "PORTFOLIO_MDD_REDUCE_PCT": 0.15,
        "PORTFOLIO_MDD_BLOCK_PCT": 0.20,
        "PORTFOLIO_MDD_HALT_PCT": 0.30,
        "PORTFOLIO_MDD_REDUCE_SIZE_MULT": 0.5,
        "ACCOUNT_SIZE_USDT": 100_000.0,
        "TREASURY_SPOT_USDT": 500.0,
        "TREASURY_FUTURES_USDT": 500.0,
        "DYNAMIC_KELLY_RISK": 0.01,
        "FIXED_RISK_PCT": 0.02,
        "ATR_SL_MULT": 2.0,
        "ANTI_PATTERNS": [],
        "WEIGHT_S1": 1.0,
        "WEIGHT_S4": 1.0,
        "FUTURES_LEVERAGE": 3.0,
        "MAX_LEVERAGE": 5,
    }
    cfg.update(overrides)
    return cfg


class TestPortfolioMddTier(unittest.TestCase):
    def test_tier_transitions(self):
        from bitget.trading.execution_safety import evaluate_portfolio_mdd_tier

        cfg = {
            "PORTFOLIO_MDD_REDUCE_PCT": 0.15,
            "PORTFOLIO_MDD_BLOCK_PCT": 0.20,
            "PORTFOLIO_MDD_HALT_PCT": 0.30,
            "PORTFOLIO_MDD_REDUCE_SIZE_MULT": 0.5,
        }
        peak = 1000.0

        normal = evaluate_portfolio_mdd_tier(900.0, peak, cfg)
        self.assertEqual(normal["tier"], "NORMAL")
        self.assertEqual(normal["size_mult"], 1.0)
        self.assertFalse(normal["blocks_entry"])

        reduce = evaluate_portfolio_mdd_tier(850.0, peak, cfg)
        self.assertEqual(reduce["tier"], "REDUCE")
        self.assertAlmostEqual(reduce["size_mult"], 0.5)
        self.assertFalse(reduce["blocks_entry"])

        block = evaluate_portfolio_mdd_tier(800.0, peak, cfg)
        self.assertEqual(block["tier"], "BLOCK")
        self.assertTrue(block["blocks_entry"])

        halt = evaluate_portfolio_mdd_tier(700.0, peak, cfg)
        self.assertEqual(halt["tier"], "HALT")
        self.assertTrue(halt["blocks_entry"])

    def test_breaker_disabled_bypass(self):
        from bitget.trading.execution_safety import (
            ExecutionGateOutcome,
            evaluate_nav_risk_gate,
            evaluate_portfolio_mdd_gate,
        )

        cfg = {
            "PORTFOLIO_MDD_BREAKER_ENABLED": False,
            "TREASURY_SPOT_USDT": 100.0,
            "TREASURY_FUTURES_USDT": 50.0,
        }
        snap = evaluate_portfolio_mdd_gate(cfg)
        self.assertTrue(snap.get("bypassed"))
        self.assertEqual(snap["tier"], "NORMAL")

        gate = evaluate_nav_risk_gate(cfg)
        self.assertEqual(gate.outcome, ExecutionGateOutcome.APPROVED)
        self.assertEqual(gate.meta.get("nav_size_mult"), 1.0)

    def test_halt_alert_only_on_transition(self):
        from bitget.trading.execution_safety import evaluate_portfolio_mdd_gate

        cfg = {
            "PORTFOLIO_MDD_BREAKER_ENABLED": True,
            "PORTFOLIO_NAV_PEAK": 1000.0,
            "PORTFOLIO_MDD_CURRENT_TIER": "BLOCK",
            "TREASURY_SPOT_USDT": 350.0,
            "TREASURY_FUTURES_USDT": 350.0,
        }
        with patch(
            "bitget.trading.execution_safety._persist_portfolio_mdd_state", return_value=True
        ), patch("bitget.trading.execution_safety._maybe_portfolio_halt_alert") as alert:
            evaluate_portfolio_mdd_gate(cfg)
        alert.assert_called_once()
        self.assertEqual(alert.call_args[0][0], "HALT")
        self.assertEqual(alert.call_args[0][1], "BLOCK")

        cfg["PORTFOLIO_MDD_CURRENT_TIER"] = "HALT"
        with patch(
            "bitget.trading.execution_safety._persist_portfolio_mdd_state", return_value=True
        ), patch("bitget.trading.execution_safety._maybe_portfolio_halt_alert") as alert2:
            evaluate_portfolio_mdd_gate(cfg)
        alert2.assert_not_called()

    def test_halt_alert_skipped_when_persist_fails(self):
        from bitget.trading.execution_safety import evaluate_portfolio_mdd_gate

        cfg = {
            "PORTFOLIO_MDD_BREAKER_ENABLED": True,
            "PORTFOLIO_NAV_PEAK": 1000.0,
            "PORTFOLIO_MDD_CURRENT_TIER": "BLOCK",
            "TREASURY_SPOT_USDT": 350.0,
            "TREASURY_FUTURES_USDT": 350.0,
        }
        with patch(
            "bitget.trading.execution_safety._persist_portfolio_mdd_state", return_value=False
        ), patch("bitget.trading.execution_safety._maybe_portfolio_halt_alert") as alert:
            evaluate_portfolio_mdd_gate(cfg)
        alert.assert_not_called()

    def test_nav_peak_init_from_account_size(self):
        from bitget.trading.execution_safety import resolve_portfolio_nav_peak

        cfg = {"ACCOUNT_SIZE_USDT": 50_000.0}
        peak = resolve_portfolio_nav_peak(cfg, 48_000.0)
        self.assertEqual(peak, 50_000.0)

        cfg2 = {"ACCOUNT_SIZE_USDT": 50_000.0, "PORTFOLIO_NAV_PEAK": 55_000.0}
        peak2 = resolve_portfolio_nav_peak(cfg2, 54_000.0)
        self.assertEqual(peak2, 55_000.0)

    def test_nav_risk_gate_blocks_on_halt_tier(self):
        from bitget.trading.execution_safety import (
            ExecutionGateOutcome,
            evaluate_nav_risk_gate,
        )

        cfg = {
            "PORTFOLIO_MDD_BREAKER_ENABLED": True,
            "PORTFOLIO_NAV_PEAK": 1000.0,
            "TREASURY_SPOT_USDT": 350.0,
            "TREASURY_FUTURES_USDT": 350.0,
        }
        with patch("bitget.trading.execution_safety._persist_portfolio_mdd_state", return_value=True):
            nav = evaluate_nav_risk_gate(cfg)
        self.assertEqual(nav.outcome, ExecutionGateOutcome.NAV_BLOCKED)
        self.assertEqual(nav.meta.get("nav_risk_stage"), "halt")

    def test_try_add_rejects_on_halt_tier(self):
        from bitget.forward import ledger
        from bitget.forward.shared import _init_forward_db_schema

        halt_cfg = _base_try_add_cfg(
            TREASURY_SPOT_USDT=350.0,
            TREASURY_FUTURES_USDT=350.0,
        )

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "forward.sqlite")
            conn = sqlite3.connect(db_path)
            _init_forward_db_schema(conn)
            conn.commit()
            conn.close()

            with patch.object(ledger, "DB_PATH", db_path), patch.object(
                ledger, "init_forward_db"
            ), patch.object(ledger, "load_system_config", return_value=halt_cfg), patch(
                "bitget.trading.execution_safety._persist_portfolio_mdd_state",
                return_value=True,
            ):
                ok, msg = ledger.try_add_virtual_position(
                    "spot",
                    "BTCUSDT",
                    "4H",
                    "[ALPHA]",
                    85,
                    50_000.0,
                    {},
                )

        self.assertFalse(ok)
        self.assertIn("NAV 드로다운", msg)

    def test_try_add_reduce_halves_sim_kelly_invest(self):
        from contextlib import ExitStack

        from bitget.forward import ledger
        from bitget.forward.shared import _init_forward_db_schema

        normal_cfg = _base_try_add_cfg(
            PORTFOLIO_NAV_PEAK=1000.0,
            TREASURY_SPOT_USDT=500.0,
            TREASURY_FUTURES_USDT=500.0,
        )
        reduce_cfg = _base_try_add_cfg(
            PORTFOLIO_NAV_PEAK=1200.0,
            TREASURY_SPOT_USDT=500.0,
            TREASURY_FUTURES_USDT=500.0,
        )
        hist = _synthetic_hist_df()
        facts = {"ml_box_pass": True}

        def _run_try_add(cfg: dict) -> float:
            with tempfile.TemporaryDirectory() as td:
                db_path = os.path.join(td, "forward.sqlite")
                conn = sqlite3.connect(db_path)
                _init_forward_db_schema(conn)
                conn.commit()
                conn.close()

                elastic_mock = MagicMock()
                elastic_mock.apply_pair.side_effect = lambda cos, ml: MagicMock(
                    cos_cutoff=cos, ml_cutoff=ml
                )

                with ExitStack() as stack:
                    stack.enter_context(patch.object(ledger, "DB_PATH", db_path))
                    stack.enter_context(patch.object(ledger, "init_forward_db"))
                    stack.enter_context(
                        patch.object(ledger, "load_system_config", return_value=cfg)
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.execution_safety._persist_portfolio_mdd_state",
                            return_value=True,
                        )
                    )
                    stack.enter_context(patch.object(ledger, "_load_hist", return_value=hist))
                    stack.enter_context(
                        patch.object(ledger, "_calc_market_breadth", return_value=1.0)
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.slippage_guard.check_pre_scan_liquidity",
                            return_value=(True, ""),
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.execution_safety.gross_entry_blocked",
                            return_value=False,
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.tail_risk_gate.tail_risk_entry_blocked",
                            return_value=(False, {}),
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.doomsday_gate.doomsday_long_entry_blocked",
                            return_value=(False, {}),
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.concentration_gate.concentration_entry_blocked",
                            return_value=(False, {}),
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.price_sanity_gate.price_sanity_entry_blocked",
                            return_value=(False, {}),
                        )
                    )
                    stack.enter_context(
                        patch.object(ledger, "fetch_funding_snapshot", return_value=None)
                    )
                    stack.enter_context(
                        patch(
                            "bitget.governance.meta_consumer.load_meta_state_resolved",
                            return_value={},
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.governance.meta_consumer.effective_max_position_pct",
                            return_value=1.0,
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.governance.meta_consumer.apply_meta_kelly_merge",
                            side_effect=lambda k, *a, **kw: k,
                        )
                    )
                    stack.enter_context(
                        patch.object(
                            ledger,
                            "get_exploration_role_scaler",
                            return_value=(1.0, "LIVE"),
                        )
                    )
                    stack.enter_context(
                        patch.object(
                            ledger,
                            "_apply_thompson_kelly_multiplier",
                            side_effect=lambda cfg, tf, sig, k: k,
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.trading.regime_capital_relay.apply_regime_capital_to_kelly",
                            side_effect=lambda k, **kw: (k, {}),
                        )
                    )
                    stack.enter_context(
                        patch(
                            "bitget.evolution.elastic_threshold_bg.BitgetElasticThreshold",
                            return_value=elastic_mock,
                        )
                    )
                    stack.enter_context(
                        patch.object(
                            ledger,
                            "compute_evolved_alpha_bonus_score",
                            return_value=0.0,
                        )
                    )
                    stack.enter_context(
                        patch.object(ledger, "save_system_config")
                    )
                    stack.enter_context(
                        patch(
                            "bitget.evolution.regime_analog_bg.frontrun_gate",
                            return_value=(True, {}),
                        )
                    )
                    ok, msg = ledger.try_add_virtual_position(
                        "spot",
                        "BTCUSDT",
                        "4H",
                        "[ALPHA]",
                        85,
                        50_000.0,
                        facts,
                    )
                self.assertTrue(ok, msg)
                read_conn = sqlite3.connect(db_path)
                try:
                    row = read_conn.execute(
                        "SELECT sim_kelly_invest FROM bitget_forward_trades WHERE status='OPEN'"
                    ).fetchone()
                finally:
                    read_conn.close()
                return float(row[0])

        normal_invest = _run_try_add(normal_cfg)
        reduce_invest = _run_try_add(reduce_cfg)
        self.assertGreater(normal_invest, 0.0)
        self.assertAlmostEqual(reduce_invest / normal_invest, 0.5, places=4)


if __name__ == "__main__":
    unittest.main()
