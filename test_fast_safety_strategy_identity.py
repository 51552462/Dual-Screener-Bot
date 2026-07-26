"""Unit tests for fast_safety_strategy_identity (Chapter 3-B0A)."""

from __future__ import annotations

import inspect
import unittest

from fast_safety_strategy_identity import (
    build_strategy_identity,
    resolve_supernova_strategy_identity,
    select_supernova_group_key,
)
from strategy_promotion_engine import stable_strategy_id


class TestFastSafetyStrategyIdentity(unittest.TestCase):
    def test_same_market_group_key_is_deterministic(self) -> None:
        first = build_strategy_identity("KR", "MFE_진화형_황금타점")
        second = build_strategy_identity("KR", "MFE_진화형_황금타점")
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(first.strategy_id, second.strategy_id)

    def test_kr_and_us_ids_differ(self) -> None:
        kr = build_strategy_identity("KR", "MFE_진화형_황금타점")
        us = build_strategy_identity("US", "MFE_진화형_황금타점")
        self.assertIsNotNone(kr)
        self.assertIsNotNone(us)
        assert kr is not None and us is not None
        self.assertNotEqual(kr.strategy_id, us.strategy_id)

    def test_market_and_group_key_whitespace_normalization(self) -> None:
        identity = build_strategy_identity("  kr  ", "  MFE_진화형_황금타점  ")
        self.assertIsNotNone(identity)
        assert identity is not None
        self.assertEqual(identity.market, "KR")
        self.assertEqual(identity.group_key, "MFE_진화형_황금타점")
        expected = stable_strategy_id("KR", "MFE_진화형_황금타점")
        self.assertEqual(identity.strategy_id, expected)

    def test_empty_group_key_rejected(self) -> None:
        self.assertIsNone(build_strategy_identity("KR", ""))
        self.assertIsNone(build_strategy_identity("KR", "   "))

    def test_unknown_none_null_rejected(self) -> None:
        for invalid in ("UNKNOWN", "unknown", "NONE", "none", "NULL", "null"):
            self.assertIsNone(
                build_strategy_identity("KR", invalid),
                msg=f"expected rejection for {invalid!r}",
            )

    def test_cosine_prefers_best_pass_name(self) -> None:
        group_key = select_supernova_group_key(
            "COSINE",
            best_pass_name="PASS_ALPHA",
            best_pattern_name="PATTERN_BETA",
        )
        self.assertEqual(group_key, "PASS_ALPHA")

    def test_cosine_falls_back_to_best_pattern_name(self) -> None:
        group_key = select_supernova_group_key(
            "COSINE",
            best_pass_name="UNKNOWN",
            best_pattern_name="PATTERN_BETA",
        )
        self.assertEqual(group_key, "PATTERN_BETA")

    def test_mlbox_and_underdog_mlbox_use_ml_pattern_name(self) -> None:
        for route in ("MLBOX", "UNDERDOG_MLBOX"):
            group_key = select_supernova_group_key(
                route,
                best_pass_name="PASS_ALPHA",
                best_pattern_name="PATTERN_BETA",
                ml_pattern_name="ML_GAMMA",
            )
            self.assertEqual(group_key, "ML_GAMMA")

    def test_scout_and_unknown_route_return_none(self) -> None:
        self.assertIsNone(
            select_supernova_group_key(
                "SCOUT",
                best_pass_name="PASS_ALPHA",
                best_pattern_name="PATTERN_BETA",
                ml_pattern_name="ML_GAMMA",
            )
        )
        self.assertIsNone(
            select_supernova_group_key(
                "UNDERDOG",
                best_pass_name="PASS_ALPHA",
            )
        )
        self.assertIsNone(
            resolve_supernova_strategy_identity(
                "KR",
                "SCOUT",
                best_pass_name="PASS_ALPHA",
            )
        )

    def test_public_signatures_exclude_final_sig_and_sig_type(self) -> None:
        for func in (
            build_strategy_identity,
            select_supernova_group_key,
            resolve_supernova_strategy_identity,
        ):
            params = inspect.signature(func).parameters
            self.assertNotIn("final_sig", params)
            self.assertNotIn("sig_type", params)


if __name__ == "__main__":
    unittest.main()
