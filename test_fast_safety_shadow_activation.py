"""Unit tests for fast_safety_shadow_activation (Chapter B0D3A4H)."""

from __future__ import annotations

import importlib
import sys
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import fast_safety_shadow_activation as fssa
from fast_safety_shadow_activation import (
    FAST_SAFETY_SHADOW_ACTIVATION_KEYS,
    resolve_fast_safety_shadow_enabled,
)


class FastSafetyShadowActivationTests(unittest.TestCase):
    def test_kr_bool_true_returns_true(self) -> None:
        def getter(key: str, default: Any = None) -> Any:
            self.assertEqual(key, "FAST_SAFETY_SHADOW_KR")
            self.assertIs(default, False)
            return True

        self.assertTrue(resolve_fast_safety_shadow_enabled("KR", get_value=getter))

    def test_us_bool_true_returns_true(self) -> None:
        def getter(key: str, default: Any = None) -> Any:
            self.assertEqual(key, "FAST_SAFETY_SHADOW_US")
            self.assertIs(default, False)
            return True

        self.assertTrue(resolve_fast_safety_shadow_enabled("US", get_value=getter))

    def test_stored_false_returns_false(self) -> None:
        self.assertFalse(
            resolve_fast_safety_shadow_enabled(
                "KR",
                get_value=lambda _k, _d=None: False,
            )
        )

    def test_absent_key_default_false(self) -> None:
        self.assertFalse(
            resolve_fast_safety_shadow_enabled(
                "KR",
                get_value=lambda _k, default=None: default,
            )
        )

    def test_string_true_returns_false(self) -> None:
        self.assertFalse(
            resolve_fast_safety_shadow_enabled(
                "KR",
                get_value=lambda _k, _d=None: "true",
            )
        )

    def test_integer_one_returns_false(self) -> None:
        self.assertFalse(
            resolve_fast_safety_shadow_enabled(
                "KR",
                get_value=lambda _k, _d=None: 1,
            )
        )

    def test_none_returns_false(self) -> None:
        self.assertFalse(
            resolve_fast_safety_shadow_enabled(
                "KR",
                get_value=lambda _k, _d=None: None,
            )
        )

    def test_unsupported_market_no_get_value_call(self) -> None:
        getter = MagicMock()
        self.assertFalse(resolve_fast_safety_shadow_enabled("JP", get_value=getter))
        getter.assert_not_called()

    def test_market_normalization(self) -> None:
        calls: list[str] = []

        def getter(key: str, default: Any = None) -> Any:
            calls.append(key)
            self.assertIs(default, False)
            return True

        self.assertTrue(resolve_fast_safety_shadow_enabled(" kr ", get_value=getter))
        self.assertTrue(resolve_fast_safety_shadow_enabled("us", get_value=getter))
        self.assertEqual(
            calls,
            [
                FAST_SAFETY_SHADOW_ACTIVATION_KEYS["KR"],
                FAST_SAFETY_SHADOW_ACTIVATION_KEYS["US"],
            ],
        )

    def test_get_value_exception_returns_false(self) -> None:
        def getter(_key: str, _default: Any = None) -> Any:
            raise RuntimeError("read failed")

        self.assertFalse(resolve_fast_safety_shadow_enabled("KR", get_value=getter))

    def test_per_market_key_and_default_false(self) -> None:
        observed: list[tuple[str, Any]] = []

        def getter(key: str, default: Any = None) -> Any:
            observed.append((key, default))
            return False

        resolve_fast_safety_shadow_enabled("KR", get_value=getter)
        resolve_fast_safety_shadow_enabled("US", get_value=getter)

        self.assertEqual(
            observed,
            [
                ("FAST_SAFETY_SHADOW_KR", False),
                ("FAST_SAFETY_SHADOW_US", False),
            ],
        )

    def test_module_import_does_not_read_config(self) -> None:
        config_manager = MagicMock()
        config_manager.get_config_value = MagicMock(
            side_effect=AssertionError("import-time read")
        )
        with patch.dict(sys.modules, {"config_manager": config_manager}):
            reloaded = importlib.reload(fssa)
            self.assertIsNotNone(reloaded.resolve_fast_safety_shadow_enabled)
        config_manager.get_config_value.assert_not_called()


if __name__ == "__main__":
    unittest.main()
