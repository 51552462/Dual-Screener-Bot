"""EOD-FLUID-STOP-FO-01(A): except 관측 로그 (강제청산 로직 무변경)."""
from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from forward import ledger as ledger_mod


class TestEodFluidStopObserveA(unittest.TestCase):
    def test_except_helper_emits_ops_event_with_regime(self) -> None:
        exc = RuntimeError("eod boom")
        with patch.object(ledger_mod.logger, "error") as err, patch(
            "ops_logger.insert_ops_event"
        ) as ins:
            ledger_mod._observe_eod_fluid_except_swallowed(
                exc,
                market="KR",
                code="005930",
                regime="HIGH_VOL",
                trade_id=321,
            )
        err.assert_called_once()
        ins.assert_called_once()
        kwargs = ins.call_args.kwargs
        self.assertEqual(kwargs["event"], "eod_fluid.except_swallowed")
        self.assertEqual(kwargs["payload"]["market"], "KR")
        self.assertEqual(kwargs["payload"]["code"], "005930")
        self.assertEqual(kwargs["payload"]["regime"], "HIGH_VOL")

    def test_track_wires_observe_and_does_not_add_defense(self) -> None:
        src = inspect.getsource(ledger_mod.track_daily_positions)
        self.assertIn("_observe_eod_fluid_except_swallowed", src)
        self.assertIn("_eod_xdyn_none_warned", src)
        self.assertIn("eod fluid xdyn unavailable", src)
        self.assertIn("except Exception as _eod_ex:", src)
        self.assertNotIn("BEAR_GRIND\", \"HIGH_VOL\", \"DEFENSE\"", src)
