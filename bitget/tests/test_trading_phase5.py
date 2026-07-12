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

        cfg = {
            "LEVERAGE_BY_ENGINE": {"PRACT_01": 5},
            "DEFAULT_REAL_EXECUTION_LEVERAGE": 3,
            "MAX_LEVERAGE": 5,
        }
        self.assertEqual(resolve_leverage(cfg, strategy_key="PRACT_01"), 5.0)
        self.assertEqual(resolve_leverage(cfg, strategy_key="X"), 3.0)

    def test_resolve_leverage_respects_max_cap(self):
        from bitget.trading.leverage_manager import resolve_leverage

        cfg = {"DEFAULT_REAL_EXECUTION_LEVERAGE": 20, "MAX_LEVERAGE": 5}
        self.assertEqual(resolve_leverage(cfg), 5.0)
        self.assertEqual(resolve_leverage(cfg, leverage_explicit=12), 5.0)
        self.assertEqual(resolve_leverage(cfg, leverage_explicit=2), 2.0)


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


class TestExecutionSafety(unittest.TestCase):
    def test_gate_order_disabled_before_dry_run(self):
        from bitget.trading.execution_safety import evaluate_config_gates, ExecutionGateOutcome

        cfg = {"ENABLE_REAL_EXECUTION": False, "REAL_EXECUTION_DRY_RUN": True}
        r = evaluate_config_gates(cfg)
        self.assertEqual(r.outcome, ExecutionGateOutcome.EXECUTION_DISABLED)

    def test_dry_run_when_enabled(self):
        from bitget.trading.execution_safety import evaluate_config_gates, ExecutionGateOutcome

        cfg = {"ENABLE_REAL_EXECUTION": True, "REAL_EXECUTION_DRY_RUN": True}
        r = evaluate_config_gates(cfg)
        self.assertEqual(r.outcome, ExecutionGateOutcome.DRY_RUN)

    def test_meta_kill_switch_blocks(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import evaluate_config_gates, ExecutionGateOutcome

        cfg = {"ENABLE_REAL_EXECUTION": True, "REAL_EXECUTION_DRY_RUN": False}
        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=True):
            r = evaluate_config_gates(cfg)
        self.assertEqual(r.outcome, ExecutionGateOutcome.META_BLOCKED)

    def test_global_circuit_blocks_live(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import evaluate_config_gates, ExecutionGateOutcome

        cfg = {
            "ENABLE_REAL_EXECUTION": True,
            "REAL_EXECUTION_DRY_RUN": False,
            "GLOBAL_CIRCUIT_BREAKER": "ON",
        }
        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False):
            r = evaluate_config_gates(cfg)
        self.assertEqual(r.outcome, ExecutionGateOutcome.CIRCUIT_BLOCKED)

    def test_nav_reduce_approves_with_size_mult(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import evaluate_config_gates, ExecutionGateOutcome

        cfg = {
            "ENABLE_REAL_EXECUTION": True,
            "REAL_EXECUTION_DRY_RUN": False,
            "GLOBAL_CIRCUIT_BREAKER": "OFF",
            "OMS_ORPHAN_ACTIVE": "OFF",
            "NAV_DD_REDUCE_PCT": 15,
            "NAV_DD_BLOCK_PCT": 20,
            "NAV_DD_HALT_PCT": 30,
            "NAV_DD_REDUCE_SIZE_MULT": 0.5,
            # Funded reserve so empty+DD tail block does not mask NAV reduce stage
            "TAIL_RISK_FUND_SPOT": 50.0,
            "TAIL_RISK_FUND_FUTURES": 50.0,
        }
        snap = {"nav": 850.0, "hwm": 1000.0, "mdd_pct": 15.0}
        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot", return_value=snap
        ):
            r = evaluate_config_gates(cfg)
        self.assertEqual(r.outcome, ExecutionGateOutcome.APPROVED)
        self.assertEqual(r.meta.get("nav_risk_stage"), "reduce")
        self.assertEqual(r.meta.get("nav_size_mult"), 0.5)

    def test_nav_block_and_halt(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import evaluate_config_gates, ExecutionGateOutcome

        cfg = {
            "ENABLE_REAL_EXECUTION": True,
            "REAL_EXECUTION_DRY_RUN": False,
            "GLOBAL_CIRCUIT_BREAKER": "OFF",
            "NAV_DD_REDUCE_PCT": 15,
            "NAV_DD_BLOCK_PCT": 20,
            "NAV_DD_HALT_PCT": 30,
        }
        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 800.0, "hwm": 1000.0, "mdd_pct": 20.0},
        ):
            blocked = evaluate_config_gates(cfg)
        self.assertEqual(blocked.outcome, ExecutionGateOutcome.NAV_BLOCKED)
        self.assertEqual(blocked.meta.get("nav_risk_stage"), "block")

        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 700.0, "hwm": 1000.0, "mdd_pct": 30.0},
        ), patch("bitget.trading.execution_safety._maybe_nav_halt_alert"):
            halted = evaluate_config_gates(cfg)
        self.assertEqual(halted.outcome, ExecutionGateOutcome.NAV_BLOCKED)
        self.assertEqual(halted.meta.get("nav_risk_stage"), "halt")

    def test_oms_defense_circuit_and_nav(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import oms_defense_block_reason

        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False):
            self.assertEqual(
                oms_defense_block_reason({"GLOBAL_CIRCUIT_BREAKER": "ON"}),
                "global_circuit_breaker",
            )
            with patch(
                "bitget.trading.execution_safety.nav_entry_blocked", return_value=True
            ):
                self.assertEqual(
                    oms_defense_block_reason(
                        {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "OFF"}
                    ),
                    "nav_dd_block",
                )
            with patch(
                "bitget.trading.execution_safety.nav_entry_blocked", return_value=False
            ):
                self.assertIsNone(
                    oms_defense_block_reason(
                        {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "OFF"}
                    )
                )
                self.assertEqual(
                    oms_defense_block_reason(
                        {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "ON"}
                    ),
                    "oms_orphan_active",
                )

    def test_orphan_gate_blocks_and_clears(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import evaluate_config_gates, ExecutionGateOutcome
        from bitget.trading.reconciliation import apply_orphan_escalation

        live = {
            "ENABLE_REAL_EXECUTION": True,
            "REAL_EXECUTION_DRY_RUN": False,
            "GLOBAL_CIRCUIT_BREAKER": "OFF",
            "OMS_ORPHAN_ACTIVE": "ON",
            "OMS_ORPHAN_COUNT": 1,
        }
        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False):
            blocked = evaluate_config_gates(live)
        self.assertEqual(blocked.outcome, ExecutionGateOutcome.ORPHAN_BLOCKED)

        patch_on = apply_orphan_escalation(
            {"OMS_ORPHAN_STREAK": 0},
            ["BTC/USDT:USDT LONG ~1"],
            alert=False,
        )
        self.assertEqual(patch_on["OMS_ORPHAN_ACTIVE"], "ON")
        self.assertEqual(patch_on["OMS_ORPHAN_STREAK"], 1)
        self.assertEqual(patch_on["OMS_ORPHAN_KILL_SWITCH_PROPOSED"], "OFF")

        patch_propose = apply_orphan_escalation(
            {"OMS_ORPHAN_STREAK": 1, "OMS_ORPHAN_ACTIVE": "ON"},
            ["BTC/USDT:USDT LONG ~1"],
            alert=False,
        )
        self.assertEqual(patch_propose["OMS_ORPHAN_STREAK"], 2)
        self.assertEqual(patch_propose["OMS_ORPHAN_KILL_SWITCH_PROPOSED"], "ON")

        patch_clear = apply_orphan_escalation(
            {"OMS_ORPHAN_ACTIVE": "ON", "OMS_ORPHAN_STREAK": 2},
            [],
            alert=False,
        )
        self.assertEqual(patch_clear["OMS_ORPHAN_ACTIVE"], "OFF")
        self.assertEqual(patch_clear["OMS_ORPHAN_STREAK"], 0)
        self.assertEqual(patch_clear["OMS_ORPHAN_KILL_SWITCH_PROPOSED"], "OFF")

    def test_gross_notional_gate_blocks(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import (
            evaluate_config_gates,
            evaluate_gross_notional_gate,
            ExecutionGateOutcome,
            GateResult,
            gross_entry_blocked,
            oms_defense_block_reason,
        )

        snap = {"nav": 1000.0, "gross_usdt": 2500.0, "gross_pct": 250.0, "gross_notional_max_pct": 200.0}
        with patch(
            "bitget.trading.execution_safety.portfolio_gross_snapshot", return_value=snap
        ):
            self.assertTrue(gross_entry_blocked({"GROSS_NOTIONAL_MAX_PCT": 200}))
            r = evaluate_gross_notional_gate({"GROSS_NOTIONAL_MAX_PCT": 200})
        self.assertEqual(r.outcome, ExecutionGateOutcome.GROSS_BLOCKED)

        live = {
            "ENABLE_REAL_EXECUTION": True,
            "REAL_EXECUTION_DRY_RUN": False,
            "GLOBAL_CIRCUIT_BREAKER": "OFF",
            "OMS_ORPHAN_ACTIVE": "OFF",
            "GROSS_NOTIONAL_MAX_PCT": 200,
        }
        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.trading.execution_safety.evaluate_nav_risk_gate",
            return_value=GateResult(ExecutionGateOutcome.APPROVED, meta={"nav_size_mult": 1.0}),
        ), patch(
            "bitget.trading.execution_safety.portfolio_gross_snapshot", return_value=snap
        ):
            blocked = evaluate_config_gates(live)
        self.assertEqual(blocked.outcome, ExecutionGateOutcome.GROSS_BLOCKED)

        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.trading.execution_safety.nav_entry_blocked", return_value=False
        ), patch(
            "bitget.trading.execution_safety.gross_entry_blocked", return_value=True
        ):
            self.assertEqual(
                oms_defense_block_reason(
                    {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "OFF"}
                ),
                "gross_notional_cap",
            )

        disabled = evaluate_gross_notional_gate({"GROSS_NOTIONAL_MAX_PCT": 0})
        self.assertEqual(disabled.outcome, ExecutionGateOutcome.APPROVED)

    def test_concentration_gate_blocks_and_soft_passes(self):
        from unittest.mock import patch

        from bitget.trading.concentration_gate import concentration_entry_blocked
        from bitget.trading.execution_safety import (
            ExecutionGateOutcome,
            evaluate_concentration_gate,
            oms_defense_block_reason,
        )

        disabled, dmeta = concentration_entry_blocked(
            {"CORR_CLUSTER_MAX_PCT": 0},
            symbol="ETH_USDT",
            position_side="LONG",
        )
        self.assertFalse(disabled)
        self.assertEqual(dmeta.get("concentration_gate"), "disabled")

        with patch(
            "bitget.trading.concentration_gate.candidate_is_high_beta",
            return_value=(True, 0.85),
        ), patch(
            "bitget.trading.concentration_gate.high_beta_same_side_notional",
            return_value=(1200.0, {"members": ["ETH corr=0.85 n=1200"]}),
        ), patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 1000.0},
        ):
            blocked, meta = concentration_entry_blocked(
                {"CORR_CLUSTER_MAX_PCT": 100, "CORR_BTC_MIN": 0.60},
                symbol="SOL_USDT",
                position_side="LONG",
            )
            self.assertTrue(blocked)
            self.assertEqual(meta.get("concentration_gate"), "block")
            gate = evaluate_concentration_gate(
                {"CORR_CLUSTER_MAX_PCT": 100},
                market_symbol="SOL/USDT:USDT",
                market_type="futures",
                position_side="LONG",
            )
        self.assertEqual(gate.outcome, ExecutionGateOutcome.CONCENTRATION_BLOCKED)

        with patch(
            "bitget.trading.concentration_gate.candidate_is_high_beta",
            return_value=(False, 0.20),
        ):
            low, lmeta = concentration_entry_blocked(
                {"CORR_CLUSTER_MAX_PCT": 50},
                symbol="DOGE_USDT",
                position_side="LONG",
            )
        self.assertFalse(low)
        self.assertEqual(lmeta.get("concentration_gate"), "ok_low_beta")

        with patch(
            "bitget.trading.concentration_gate.candidate_is_high_beta",
            return_value=(None, None),
        ):
            soft, smeta = concentration_entry_blocked(
                {"CORR_CLUSTER_MAX_PCT": 50},
                symbol="UNKNOWN",
                position_side="LONG",
            )
        self.assertFalse(soft)
        self.assertEqual(smeta.get("concentration_gate"), "soft_pass_insufficient_ohlcv")

        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.trading.execution_safety.nav_entry_blocked", return_value=False
        ), patch(
            "bitget.trading.execution_safety.gross_entry_blocked", return_value=False
        ), patch(
            "bitget.trading.concentration_gate.concentration_entry_blocked",
            return_value=(True, {"concentration_gate": "block"}),
        ):
            self.assertEqual(
                oms_defense_block_reason(
                    {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "OFF"},
                    market_symbol="ETH/USDT:USDT",
                    market_type="futures",
                    position_side="LONG",
                ),
                "concentration_cap",
            )

    def test_doomsday_gate_blocks_long_allows_short(self):
        from bitget.trading.doomsday_gate import (
            crypto_contagion_score,
            doomsday_long_entry_blocked,
            floor_score_for_defcon,
        )
        from bitget.trading.execution_safety import (
            ExecutionGateOutcome,
            evaluate_doomsday_gate,
            oms_defense_block_reason,
        )

        raw = crypto_contagion_score(60.0, 0.04, -8.0)
        self.assertGreater(raw, 40.0)
        self.assertGreaterEqual(floor_score_for_defcon(2, raw), 80.0)

        cfg = {"DOOMSDAY_DEFCON": {"level": 2, "scores": {"Global_Contagion_Score": 85.0}}}
        blocked, meta = doomsday_long_entry_blocked(cfg, position_side="LONG")
        self.assertTrue(blocked)
        self.assertEqual(meta.get("doomsday_gate"), "block")

        short_ok, smeta = doomsday_long_entry_blocked(cfg, position_side="SHORT")
        self.assertFalse(short_ok)
        self.assertEqual(smeta.get("doomsday_gate"), "ok_short_allowed")

        soft, soft_meta = doomsday_long_entry_blocked({}, position_side="LONG")
        self.assertFalse(soft)
        self.assertIn("soft_pass", soft_meta.get("doomsday_gate", ""))

        gate = evaluate_doomsday_gate(cfg, position_side="LONG")
        self.assertEqual(gate.outcome, ExecutionGateOutcome.DOOMSDAY_BLOCKED)

        gate_s = evaluate_doomsday_gate(cfg, position_side="SHORT")
        self.assertEqual(gate_s.outcome, ExecutionGateOutcome.APPROVED)
        self.assertLessEqual(float(gate_s.meta.get("doomsday_size_mult") or 1.0), 1.0)

        from unittest.mock import patch

        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.trading.execution_safety.nav_entry_blocked", return_value=False
        ), patch(
            "bitget.trading.execution_safety.gross_entry_blocked", return_value=False
        ):
            self.assertEqual(
                oms_defense_block_reason(
                    {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "OFF", **cfg},
                    position_side="LONG",
                ),
                "doomsday_defcon",
            )
            self.assertIsNone(
                oms_defense_block_reason(
                    {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "OFF", **cfg},
                    position_side="SHORT",
                )
            )


class TestPortfolioNavSnapshot(unittest.TestCase):
    def test_combined_mdd(self):
        from unittest.mock import patch

        from bitget.live_nav_manager import portfolio_nav_snapshot

        state = {
            "spot": {"nav": 400.0, "hwm": 500.0},
            "futures": {"nav": 400.0, "hwm": 500.0},
        }
        with patch("bitget.live_nav_manager.load_treasury_state", return_value=state):
            snap = portfolio_nav_snapshot()
        self.assertEqual(snap["nav"], 800.0)
        self.assertEqual(snap["hwm"], 1000.0)
        self.assertAlmostEqual(snap["mdd_pct"], 20.0)


class TestPaperNavLiveParity(unittest.TestCase):
    def test_ledger_wires_nav_gate_and_leverage_cap(self):
        import inspect

        from bitget.forward import ledger

        src = inspect.getsource(ledger.try_add_virtual_position)
        self.assertIn("evaluate_nav_risk_gate", src)
        self.assertIn("NAV_BLOCKED", src)
        self.assertIn("nav_size_mult", src)
        self.assertIn("max_leverage_cap", src)
        self.assertIn("NAV 드로다운", src)

    def test_nav_reduce_and_block_outcomes(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import (
            ExecutionGateOutcome,
            evaluate_nav_risk_gate,
            max_leverage_cap,
        )

        cfg = {
            "NAV_DD_REDUCE_PCT": 15,
            "NAV_DD_BLOCK_PCT": 20,
            "NAV_DD_HALT_PCT": 30,
            "NAV_DD_REDUCE_SIZE_MULT": 0.5,
            "MAX_LEVERAGE": 5,
            "FUTURES_LEVERAGE": 20,
        }
        with patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 850.0, "hwm": 1000.0, "mdd_pct": 15.0},
        ):
            reduced = evaluate_nav_risk_gate(cfg)
        self.assertEqual(reduced.outcome, ExecutionGateOutcome.APPROVED)
        self.assertAlmostEqual(float(reduced.meta.get("nav_size_mult")), 0.5)

        with patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 750.0, "hwm": 1000.0, "mdd_pct": 25.0},
        ):
            blocked = evaluate_nav_risk_gate(cfg)
        self.assertEqual(blocked.outcome, ExecutionGateOutcome.NAV_BLOCKED)

        # paper path clamps FUTURES_LEVERAGE via the same helper as live
        self.assertEqual(max_leverage_cap(cfg), 5.0)
        self.assertEqual(min(float(cfg["FUTURES_LEVERAGE"]), max_leverage_cap(cfg)), 5.0)


class TestTailRiskReserve(unittest.TestCase):
    def test_accrue_no_twenty_x_mint_and_crisis_one_to_one(self):
        from bitget.trading.tail_risk_gate import accrue_tail_risk_fund

        base = {
            "TREASURY_SPOT_USDT": 10000.0,
            "TREASURY_FUTURES_USDT": 10000.0,
            "TAIL_RISK_FUND_SPOT": 0.0,
            "TAIL_RISK_FUND_FUTURES": 0.0,
            "TAIL_RISK_ACCRUAL_PCT": 1.5,
        }
        accrued = accrue_tail_risk_fund(dict(base))
        # 1.5% of 10k = 150 each
        self.assertAlmostEqual(accrued["TAIL_RISK_FUND_SPOT"], 150.0)
        self.assertAlmostEqual(accrued["TREASURY_SPOT_USDT"], 9850.0)
        self.assertNotIn("20", str(accrued.get("TAIL_RISK_LAST_ACTION")))

        crisis = accrue_tail_risk_fund(
            {
                **accrued,
                "CURRENT_REGIME_KEY": "BEAR",
                "BTC_ATR_PCT": 7.0,
            }
        )
        # 1:1 release — treasury gains fund, not ×20
        self.assertAlmostEqual(crisis["TAIL_RISK_FUND_SPOT"], 0.0)
        self.assertAlmostEqual(crisis["TREASURY_SPOT_USDT"], 10000.0)
        actions = " ".join((crisis.get("TAIL_RISK_LAST_ACTION") or {}).get("actions") or [])
        self.assertIn("release_1to1", actions)

    def test_empty_fund_blocks_under_nav_dd(self):
        from unittest.mock import patch

        from bitget.trading.execution_safety import (
            ExecutionGateOutcome,
            evaluate_tail_risk_gate,
            oms_defense_block_reason,
        )
        from bitget.trading.tail_risk_gate import (
            tail_risk_entry_blocked,
            tail_risk_size_mult,
        )

        cfg = {
            "TAIL_RISK_FUND_SPOT": 0.0,
            "TAIL_RISK_FUND_FUTURES": 0.0,
            "TAIL_RISK_MIN_COVERAGE_PCT": 0.5,
            "TAIL_RISK_UNDERFUND_SIZE_MULT": 0.5,
            "TAIL_RISK_EMPTY_BLOCK": True,
            "NAV_DD_REDUCE_PCT": 15,
        }
        with patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 1000.0, "mdd_pct": 16.0},
        ):
            blocked, meta = tail_risk_entry_blocked(cfg)
            self.assertTrue(blocked)
            self.assertEqual(meta.get("tail_risk_gate"), "block_empty_under_dd")
            gate = evaluate_tail_risk_gate(cfg)
        self.assertEqual(gate.outcome, ExecutionGateOutcome.TAIL_RISK_BLOCKED)

        funded = {**cfg, "TAIL_RISK_FUND_SPOT": 2.0}  # 0.2% < 0.5% → underfund size
        with patch(
            "bitget.live_nav_manager.portfolio_nav_snapshot",
            return_value={"nav": 1000.0, "mdd_pct": 5.0},
        ):
            ok, ometa = tail_risk_entry_blocked(funded)
            self.assertFalse(ok)
            self.assertEqual(ometa.get("tail_risk_gate"), "underfund_size_only")
            self.assertAlmostEqual(tail_risk_size_mult(funded), 0.5)

        with patch("bitget.trading.execution_safety.meta_kill_switch_active", return_value=False), patch(
            "bitget.trading.execution_safety.nav_entry_blocked", return_value=False
        ), patch(
            "bitget.trading.execution_safety.gross_entry_blocked", return_value=False
        ), patch(
            "bitget.trading.tail_risk_gate.tail_risk_entry_blocked",
            return_value=(True, {"tail_risk_gate": "block_empty_under_dd"}),
        ):
            self.assertEqual(
                oms_defense_block_reason(
                    {"GLOBAL_CIRCUIT_BREAKER": "OFF", "OMS_ORPHAN_ACTIVE": "OFF"}
                ),
                "tail_risk_reserve",
            )


class TestPriceSanityGate(unittest.TestCase):
    def test_blocks_entry_gap_and_soft_passes_short_hist(self):
        import pandas as pd

        from bitget.trading.execution_safety import (
            ExecutionGateOutcome,
            evaluate_price_sanity_gate,
        )
        from bitget.trading.price_sanity_gate import analyze_price_sanity

        # Normal series then huge entry gap
        closes = [100.0] * 6 + [101.0]
        df = pd.DataFrame(
            {
                "Open": closes,
                "High": [c * 1.01 for c in closes],
                "Low": [c * 0.99 for c in closes],
                "Close": closes,
                "Volume": [1.0] * len(closes),
            }
        )
        cfg = {
            "BAD_TICK_LOOKBACK_BARS": 5,
            "BAD_TICK_MAX_GAP_PCT": 15.0,
            "BAD_TICK_MAX_VS_MEDIAN_PCT": 20.0,
            "BAD_TICK_MAX_BAR_RANGE_PCT": 40.0,
        }
        blocked, meta = analyze_price_sanity(cfg, entry_price=130.0, hist_df=df)
        self.assertTrue(blocked)
        self.assertEqual(meta.get("price_sanity"), "block_entry_gap")

        ok, ometa = analyze_price_sanity(cfg, entry_price=101.5, hist_df=df)
        self.assertFalse(ok)
        self.assertEqual(ometa.get("price_sanity"), "ok")

        soft, smeta = analyze_price_sanity(cfg, entry_price=100.0, hist_df=None)
        self.assertFalse(soft)
        self.assertIn("soft_pass", smeta.get("price_sanity", ""))

        # high < low integrity
        bad = df.copy()
        bad.loc[bad.index[-1], "High"] = 90.0
        bad.loc[bad.index[-1], "Low"] = 110.0
        b2, m2 = analyze_price_sanity(cfg, entry_price=100.0, hist_df=bad)
        self.assertTrue(b2)
        self.assertEqual(m2.get("price_sanity"), "block_high_lt_low")

        from unittest.mock import patch

        with patch(
            "bitget.trading.price_sanity_gate.price_sanity_entry_blocked",
            return_value=(True, {"price_sanity": "block_entry_gap", "entry_vs_prev_gap_pct": 30.0}),
        ):
            gate = evaluate_price_sanity_gate(
                cfg, market_symbol="ETH/USDT:USDT", market_type="futures", entry_price=130.0
            )
        self.assertEqual(gate.outcome, ExecutionGateOutcome.PRICE_SANITY_BLOCKED)


class TestReconciliationSkip(unittest.TestCase):
    def test_skipped_in_dry_run(self):
        from bitget.trading.reconciliation import run_scheduled_reconciliation

        report = run_scheduled_reconciliation()
        self.assertTrue(report.get("skipped"))
        self.assertIn(report.get("reason"), ("dry_run", "execution_disabled", "meta_kill_switch"))


if __name__ == "__main__":
    unittest.main()
