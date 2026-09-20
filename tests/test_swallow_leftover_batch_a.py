"""SWALLOW-LEFTOVER-BATCH-A-01: CB/좀비/켈리 관측 로그 (로직 무변경)."""
from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

import live_nav_manager as lnav
from forward import ledger as ledger_mod


class TestSwallowLeftoverBatchA(unittest.TestCase):
    def test_cb_helper_emits_distinct_events(self) -> None:
        exc = RuntimeError("cb boom")
        with patch.object(ledger_mod.logger, "error") as err, patch(
            "ops_logger.insert_ops_event"
        ) as ins:
            ledger_mod._observe_cb_swallowed(
                "cb.on_save_swallowed",
                exc,
                market="KR",
                trigger_pct=-5.12,
            )
        err.assert_called_once()
        ins.assert_called_once()
        kwargs = ins.call_args.kwargs
        self.assertEqual(kwargs["event"], "cb.on_save_swallowed")
        self.assertEqual(kwargs["payload"]["market"], "KR")
        self.assertEqual(kwargs["payload"]["trigger_pct"], -5.12)

    def test_cb_load_fail_returns_without_save(self) -> None:
        with patch.object(
            ledger_mod, "load_system_config", side_effect=RuntimeError("cfg")
        ), patch.object(ledger_mod, "save_system_config") as save, patch.object(
            ledger_mod, "_observe_cb_swallowed"
        ) as obs:
            ledger_mod._update_global_circuit_breaker("US", -0.08, -1.0e6, 2.0e7)
        save.assert_not_called()
        obs.assert_called_once()
        self.assertEqual(obs.call_args.args[0], "cb.load_swallowed")

    def test_zombie_helper_includes_days_held(self) -> None:
        exc = RuntimeError("z boom")
        with patch.object(ledger_mod.logger, "error") as err, patch(
            "ops_logger.insert_ops_event"
        ) as ins:
            ledger_mod._observe_zombie_liquidation_swallowed(
                exc,
                market="KR",
                code="316140",
                days_held=41,
            )
        err.assert_called_once()
        kwargs = ins.call_args.kwargs
        self.assertEqual(kwargs["event"], "zombie.liquidation_swallowed")
        self.assertEqual(kwargs["payload"]["code"], "316140")
        self.assertEqual(kwargs["payload"]["days_held"], 41)

    def test_wiring_keeps_trip_and_zombie_constants(self) -> None:
        self.assertEqual(ledger_mod.CB_TRIP_LOSS_RATIO, -0.05)
        cb_src = inspect.getsource(ledger_mod._update_global_circuit_breaker)
        self.assertIn("cb.load_swallowed", cb_src)
        self.assertIn("cb.on_save_swallowed", cb_src)
        self.assertIn("cb.off_save_swallowed", cb_src)
        track_src = inspect.getsource(ledger_mod.track_daily_positions)
        self.assertIn("_observe_zombie_liquidation_swallowed", track_src)
        self.assertIn("final_ret=-15.0", track_src)
        self.assertIn("_zombie_days_held > 30", track_src)

    def test_kelly_config_fallback_still_default(self) -> None:
        with patch(
            "config_manager.load_system_config", side_effect=RuntimeError("cfg")
        ), patch.object(lnav, "_observe_kelly_swallowed") as obs, patch(
            "kelly_elasticity_overlay.evaluate_kelly_elasticity_overlay",
            return_value={"elasticity_mult": 1.0, "active": False},
        ), patch(
            "kelly_elasticity_overlay.apply_elasticity_to_effective_kelly",
            side_effect=lambda eff, ov: (eff, {}),
        ):
            out = lnav.resolve_effective_kelly("KR")
        self.assertEqual(out, lnav.DEFAULT_EFFECTIVE_KELLY)
        obs.assert_called()
        self.assertEqual(obs.call_args.args[0], "kelly.config_load_fallback")

    def test_kelly_overlay_fail_keeps_base_and_logs(self) -> None:
        with patch.object(lnav, "_observe_kelly_swallowed") as obs, patch(
            "kelly_elasticity_overlay.evaluate_kelly_elasticity_overlay",
            side_effect=RuntimeError("ov"),
        ):
            out = lnav.resolve_effective_kelly(
                "US",
                sys_config={
                    "DYNAMIC_KELLY_RISK": 0.02,
                    "META_GLOBAL_KELLY_MULT": 1.0,
                    "ENABLE_KELLY_NAV_DD_OVERLAY": False,
                },
            )
        self.assertEqual(out, 0.02)
        obs.assert_called_once()
        self.assertEqual(obs.call_args.args[0], "kelly.elasticity_overlay_swallowed")

    def test_kelly_observe_payload_has_market_and_code(self) -> None:
        exc = RuntimeError("ov boom")
        with patch.object(lnav.logger, "error") as err, patch(
            "ops_logger.insert_ops_event"
        ) as ins:
            lnav._observe_kelly_swallowed(
                "kelly.elasticity_overlay_swallowed",
                exc,
                market="US",
                code="AAPL",
            )
        err.assert_called_once()
        kwargs = ins.call_args.kwargs
        self.assertEqual(kwargs["event"], "kelly.elasticity_overlay_swallowed")
        self.assertEqual(kwargs["payload"]["market"], "US")
        self.assertEqual(kwargs["payload"]["code"], "AAPL")
