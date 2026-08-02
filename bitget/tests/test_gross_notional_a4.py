"""A-4 — Gross notional exposure cap (gate 7)."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from bitget.trading.execution_safety import (
    evaluate_gross_notional_gate,
    evaluate_gross_notional_gate_values,
    gross_entry_blocked,
    gross_gate_nav_current,
    portfolio_gross_snapshot,
)


class TestGrossNotionalGateValues(unittest.TestCase):
    def test_pct_spot_fut_pooled(self):
        cfg = {"MAX_GROSS_NOTIONAL_PCT": 80, "GROSS_NOTIONAL_CAP_ENABLED": True}
        out = evaluate_gross_notional_gate_values(1000.0, 850.0, cfg)
        self.assertAlmostEqual(out["gross_notional_pct"], 85.0)
        self.assertTrue(out["blocked"])

        ok = evaluate_gross_notional_gate_values(1000.0, 700.0, cfg)
        self.assertAlmostEqual(ok["gross_notional_pct"], 70.0)
        self.assertFalse(ok["blocked"])

    def test_kill_switch_bypass(self):
        cfg = {"MAX_GROSS_NOTIONAL_PCT": 80, "GROSS_NOTIONAL_CAP_ENABLED": False}
        out = evaluate_gross_notional_gate_values(1000.0, 9999.0, cfg)
        self.assertTrue(out.get("bypassed"))
        self.assertFalse(out["blocked"])

    def test_nav_current_from_mdd_snap_not_treasury_recompute(self):
        cfg = {
            "PORTFOLIO_MDD_BREAKER_ENABLED": True,
            "TREASURY_SPOT_USDT": 999.0,
            "TREASURY_FUTURES_USDT": 999.0,
        }
        mdd_snap = {"nav_current": 500.0, "tier": "NORMAL"}
        with patch(
            "bitget.trading.execution_safety.get_portfolio_mdd_snap_cached",
            return_value=mdd_snap,
        ) as cached:
            nav = gross_gate_nav_current(cfg)
        self.assertEqual(nav, 500.0)
        cached.assert_called_once()


class TestGrossNotionalIntegration(unittest.TestCase):
    def test_open_book_sum_quantity_times_price(self):
        from bitget.forward.shared import _init_forward_db_schema
        from bitget.trading.execution_safety import portfolio_open_gross_usdt

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "md.sqlite")
            conn = sqlite3.connect(db_path)
            _init_forward_db_schema(conn)
            conn.execute(
                """
                INSERT INTO bitget_forward_trades
                (market_type, symbol, timeframe, status, quantity, entry_price, sim_kelly_invest)
                VALUES ('spot', 'BTCUSDT', '4H', 'OPEN', 0.1, 50000.0, 6000.0),
                       ('futures', 'ETHUSDT', '4H', 'OPEN', 2.0, 3000.0, 20000.0)
                """
            )
            conn.commit()
            conn.close()

            with patch("bitget.infra.data_paths.market_data_db_path", return_value=db_path):
                gross = portfolio_open_gross_usdt()
        # spot 0.1*50k=5000 + fut 2*3k=6000 = 11000 (not sim_kelly sum)
        self.assertAlmostEqual(gross, 11000.0)

    def test_try_add_blocked_when_over_cap(self):
        from bitget.forward import ledger
        from bitget.forward.shared import _init_forward_db_schema

        cfg = {
            "GLOBAL_CIRCUIT_BREAKER": "OFF",
            "BITGET_MAX_OPEN_POSITIONS": 99,
            "PORTFOLIO_MDD_BREAKER_ENABLED": False,
            "MAX_GROSS_NOTIONAL_PCT": 80,
            "GROSS_NOTIONAL_CAP_ENABLED": True,
            "TREASURY_SPOT_USDT": 1000.0,
            "TREASURY_FUTURES_USDT": 0.0,
            "ANTI_PATTERNS": [],
        }
        snap = {
            "blocked": True,
            "bypassed": False,
            "gross_notional_pct": 90.0,
            "nav_current": 1000.0,
        }
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "forward.sqlite")
            conn = sqlite3.connect(db_path)
            _init_forward_db_schema(conn)
            conn.commit()
            conn.close()

            with patch.object(ledger, "DB_PATH", db_path), patch.object(
                ledger, "init_forward_db"
            ), patch.object(ledger, "load_system_config", return_value=cfg), patch(
                "bitget.trading.execution_safety.evaluate_nav_risk_gate"
            ) as nav_gate, patch(
                "bitget.trading.execution_safety.evaluate_gross_notional_gate"
            ) as gross_gate:
                from bitget.trading.execution_safety import (
                    ExecutionGateOutcome,
                    GateResult,
                )

                nav_gate.return_value = GateResult(
                    ExecutionGateOutcome.APPROVED, meta={"nav_size_mult": 1.0}
                )
                gross_gate.return_value = GateResult(
                    ExecutionGateOutcome.GROSS_BLOCKED,
                    message="blocked",
                    meta=snap,
                )
                ok, msg = ledger.try_add_virtual_position(
                    "spot", "BTCUSDT", "4H", "[ALPHA]", 85, 50_000.0, {"ml_box_pass": True}
                )
        self.assertFalse(ok)
        self.assertIn("명목노출", msg)

    def test_concentration_gate_independent(self):
        import inspect

        from bitget.trading import concentration_gate

        src = inspect.getsource(concentration_gate.concentration_entry_blocked)
        self.assertIn("CORR_CLUSTER_MAX_PCT", src)
        self.assertNotIn("MAX_GROSS_NOTIONAL_PCT", src)


if __name__ == "__main__":
    unittest.main()
