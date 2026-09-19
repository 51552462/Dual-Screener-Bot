"""SWALLOW-GATE-FO-01 A: fail-open 관측 로그 (로직 변경 없음)."""
from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

import forward.shared as shared


class TestEntryGateFailOpenObserveA(unittest.TestCase):
    def test_observe_helper_logs_code_and_ops_event(self) -> None:
        exc = RuntimeError("gate boom")
        with patch.object(shared.logger, "error") as err, patch(
            "ops_logger.insert_ops_event"
        ) as ins:
            shared._observe_entry_gate_fail_open(
                "entry_gate.meta_global_fail_open",
                exc,
                market="US",
                code="AAPL",
                name="Apple",
                trade_source="STANDARD",
                sig_type="[TEST] S1",
            )
        err.assert_called_once()
        self.assertIn("AAPL", err.call_args.args + tuple(err.call_args.kwargs.values()))
        ins.assert_called_once()
        kwargs = ins.call_args.kwargs
        self.assertEqual(kwargs["event"], "entry_gate.meta_global_fail_open")
        self.assertEqual(kwargs["payload"]["code"], "AAPL")
        self.assertEqual(kwargs["payload"]["market"], "US")
        self.assertEqual(kwargs["payload"]["exc_type"], "RuntimeError")

    def test_try_add_except_blocks_wire_observe_events(self) -> None:
        src = inspect.getsource(shared.try_add_virtual_position)
        self.assertIn("entry_gate.meta_global_fail_open", src)
        self.assertIn("entry_gate.toxic_fade_fail_open", src)
        self.assertIn("_observe_entry_gate_fail_open", src)
