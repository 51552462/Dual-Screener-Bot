"""A-3 — MAX_LEVERAGE hard cap (resolve_max_leverage SSOT)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from bitget.trading.execution_safety import (
    max_leverage_cap,
    resolve_max_leverage,
)
from bitget.trading.leverage_manager import resolve_leverage


class TestMaxLeverageConflictReport(unittest.TestCase):
    """Handoff gate: operational defaults must not exceed MAX_LEVERAGE=5."""

    def test_operational_defaults_below_cap(self):
        cfg = {"FUTURES_LEVERAGE": 3.0, "DEFAULT_REAL_EXECUTION_LEVERAGE": 3}
        self.assertLessEqual(resolve_leverage(cfg), 5.0)
        self.assertLessEqual(
            resolve_leverage(cfg, leverage_explicit=cfg["FUTURES_LEVERAGE"]),
            5.0,
        )


class TestResolveMaxLeverage(unittest.TestCase):
    def setUp(self):
        self.cfg = {"MAX_LEVERAGE": 5}

    def test_clamp_matrix(self):
        cases = [(3.0, 3.0), (5.0, 5.0), (8.0, 5.0), (20.0, 5.0)]
        for requested, expected in cases:
            with self.subTest(requested=requested):
                self.assertAlmostEqual(
                    resolve_max_leverage(requested, self.cfg),
                    expected,
                )

    def test_default_cap_when_config_missing(self):
        self.assertAlmostEqual(resolve_max_leverage(20.0, {}), 5.0)
        self.assertAlmostEqual(max_leverage_cap({}), 5.0)

    def test_clamp_logs_without_telegram(self):
        with patch("bitget.infra.logging_setup.get_logger") as get_log:
            logger = get_log.return_value
            resolve_max_leverage(20.0, self.cfg)
        logger.info.assert_called_once()
        self.assertIn("MAX_LEVERAGE clamp", str(logger.info.call_args))

    def test_resolve_leverage_delegates_to_resolve_max_leverage(self):
        cfg = {"MAX_LEVERAGE": 5, "DEFAULT_REAL_EXECUTION_LEVERAGE": 20}
        self.assertAlmostEqual(resolve_leverage(cfg), 5.0)
        self.assertAlmostEqual(resolve_leverage(cfg, leverage_explicit=3), 3.0)

    def test_spot_ledger_path_does_not_call_resolve_max_leverage(self):
        import inspect

        from bitget.forward import ledger

        src = inspect.getsource(ledger.try_add_virtual_position)
        self.assertIn("resolve_leverage", src)
        self.assertNotIn("resolve_max_leverage", src)


if __name__ == "__main__":
    unittest.main()
