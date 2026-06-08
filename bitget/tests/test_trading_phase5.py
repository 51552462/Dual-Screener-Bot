"""Phase 5 trading / OMS unit tests (no live exchange)."""
from __future__ import annotations

import unittest


class TestLeverageManager(unittest.TestCase):
    def test_resolve_margin_mode_strategy(self):
        from bitget.trading.leverage_manager import resolve_margin_mode

        cfg = {"MARGIN_MODE_BY_STRATEGY": {"S1": "isolated"}, "DEFAULT_REAL_EXECUTION_MARGIN_MODE": "cross"}
        self.assertEqual(resolve_margin_mode(cfg, strategy_key="S1"), "isolated")
        self.assertEqual(resolve_margin_mode(cfg, strategy_key="UNKNOWN"), "cross")

    def test_resolve_leverage_engine(self):
        from bitget.trading.leverage_manager import resolve_leverage

        cfg = {"LEVERAGE_BY_ENGINE": {"PRACT_01": 5}, "DEFAULT_REAL_EXECUTION_LEVERAGE": 3}
        self.assertEqual(resolve_leverage(cfg, strategy_key="PRACT_01"), 5.0)
        self.assertEqual(resolve_leverage(cfg, strategy_key="X"), 3.0)


class TestSlippageGuard(unittest.TestCase):
    def test_pre_scan_blocks_thin_liquidity(self):
        from bitget.trading.slippage_guard import check_pre_scan_liquidity

        cfg = {"SEED_SLIPPAGE_GUARD_USDT": 50000, "MIN_TRADE_VALUE_24H_SLIP_USDT": 5_000_000}
        ok, reason = check_pre_scan_liquidity(
            100_000,
            {"trade_value_24h": 1_000_000},
            cfg,
            symbol="FOO_USDT",
            market_type="futures",
        )
        self.assertFalse(ok)
        self.assertIn("pre_scan_liquidity", reason)

    def test_pre_scan_passes_small_seed(self):
        from bitget.trading.slippage_guard import check_pre_scan_liquidity

        cfg = {"SEED_SLIPPAGE_GUARD_USDT": 50000, "MIN_TRADE_VALUE_24H_SLIP_USDT": 5_000_000}
        ok, _ = check_pre_scan_liquidity(10_000, {"trade_value_24h": 1000}, cfg)
        self.assertTrue(ok)

    def test_post_trade_audit(self):
        from bitget.trading.slippage_guard import audit_post_trade_slippage

        good = audit_post_trade_slippage(100.0, 100.05, max_bps=50)
        self.assertTrue(good["ok"])
        bad = audit_post_trade_slippage(100.0, 101.0, max_bps=50)
        self.assertTrue(bad["exceeded"])


class TestPositionManager(unittest.TestCase):
    def test_normalize_and_order_side(self):
        from bitget.trading.position_manager import ccxt_order_side, normalize_position_side

        self.assertEqual(normalize_position_side("short"), "SHORT")
        self.assertEqual(ccxt_order_side("LONG", opening=True), "buy")
        self.assertEqual(ccxt_order_side("SHORT", opening=False), "buy")


class TestExecutorDryRun(unittest.TestCase):
    def test_dry_run_spot_long(self):
        from bitget.executor import execute_real_order

        # Uses config defaults: ENABLE_REAL_EXECUTION may be false OR dry_run true
        out = execute_real_order("BTC_USDT", "LONG", 0.01, market_type="spot")
        self.assertIn(out["status"], ("dry_run", "execution_disabled"))

    def test_dry_run_futures_short(self):
        from bitget.executor import execute_real_order

        out = execute_real_order("ETH_USDT", "SHORT", 0.02, leverage=2, market_type="futures")
        self.assertIn(out["status"], ("dry_run", "execution_disabled"))
        if out["status"] == "dry_run":
            self.assertTrue(out.get("client_order_id"))


class TestReconciliationSkip(unittest.TestCase):
    def test_skipped_in_dry_run(self):
        from bitget.trading.reconciliation import run_scheduled_reconciliation

        report = run_scheduled_reconciliation()
        self.assertTrue(report.get("skipped"))
        self.assertIn(report.get("reason"), ("dry_run", "execution_disabled", "meta_kill_switch"))


if __name__ == "__main__":
    unittest.main()
