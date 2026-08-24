"""Regression: try_add_virtual_position must not locally bind load_system_config.

Factory FAIL 20260824T100003 — UnboundLocalError on KR supernova enroll:
local variable 'load_system_config' referenced before assignment.

Cause: nested `from config_manager import load_system_config` later in the
same function made every earlier load_system_config() reference local.
"""
from __future__ import annotations

import ast
import inspect
import unittest
from unittest.mock import patch

import forward.shared as shared


class TestTryAddLoadSystemConfigScope(unittest.TestCase):
    def test_no_local_load_system_config_binding(self) -> None:
        src = inspect.getsource(shared.try_add_virtual_position)
        tree = ast.parse(src)
        fn = tree.body[0]
        self.assertIsInstance(fn, ast.FunctionDef)

        for node in ast.walk(fn):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    self.assertNotEqual(
                        alias.name,
                        "load_system_config",
                        "nested import of load_system_config rebinds "
                        "the name for the whole function",
                    )
                    self.assertNotEqual(alias.asname, "load_system_config")
            if isinstance(node, ast.Name) and node.id == "load_system_config":
                self.assertIsInstance(
                    node.ctx,
                    ast.Load,
                    "load_system_config must not be assigned inside "
                    "try_add_virtual_position",
                )

    def test_early_config_load_does_not_unboundlocal(self) -> None:
        """Hit pre_sys_config = load_system_config() without UnboundLocalError."""
        with patch.object(
            shared,
            "load_system_config",
            return_value={"GLOBAL_CIRCUIT_BREAKER": "ON"},
        ), patch(
            "market_session_gate.is_market_open",
            return_value=(True, "open"),
        ), patch.object(shared, "init_forward_db"):
            ok, msg = shared.try_add_virtual_position(
                market="US",
                code="AAPL",
                name="Apple",
                sig_type="[TEST] scope",
                score=80.0,
                ep=100.0,
                facts={},
                sector="Tech",
                trade_source="SUPERNOVA",
            )

        self.assertFalse(ok)
        self.assertIn("서킷 브레이커", msg)


if __name__ == "__main__":
    unittest.main()
