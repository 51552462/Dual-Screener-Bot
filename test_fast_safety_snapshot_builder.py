from __future__ import annotations

import math
import unittest

from fast_safety_snapshot_builder import (
    build_explicit_policy_snapshot,
    build_neutral_live_risk_snapshot,
    build_unbounded_exposure_caps,
)


class ExplicitFastSafetySnapshotBuilderTests(unittest.TestCase):
    def test_valid_explicit_policy_snapshot(self) -> None:
        snapshot = build_explicit_policy_snapshot(
            "US",
            "policy-v1",
            100.0,
            {"s1": 0.10, "s2": 0.05},
            0.10,
        )
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.market, "US")
        self.assertEqual(snapshot.version, "policy-v1")
        self.assertEqual(snapshot.generated_at, 100.0)
        self.assertEqual(dict(snapshot.base_kelly_by_strategy), {"s1": 0.10, "s2": 0.05})
        self.assertEqual(dict(snapshot.alpha_overlay_by_strategy), {"s1": 1.0, "s2": 1.0})
        self.assertEqual(snapshot.max_alpha_overlay, 1.0)
        self.assertEqual(snapshot.absolute_kelly_cap, 0.10)

    def test_market_strip_and_uppercase(self) -> None:
        snapshot = build_explicit_policy_snapshot(
            "  kr  ",
            "policy-v1",
            1.0,
            {"s1": 0.08},
            0.08,
        )
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.market, "KR")

    def test_rejects_non_kr_us_market(self) -> None:
        self.assertIsNone(
            build_explicit_policy_snapshot(
                "JP",
                "policy-v1",
                1.0,
                {"s1": 0.08},
                0.08,
            )
        )

    def test_rejects_empty_version_and_invalid_generated_at(self) -> None:
        base = {"s1": 0.08}
        cap = 0.08
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "", 1.0, base, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "   ", 1.0, base, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", True, base, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", float("nan"), base, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", float("inf"), base, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", -1.0, base, cap)
        )

    def test_rejects_empty_or_non_mapping_base_map(self) -> None:
        cap = 0.08
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", 1.0, {}, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", 1.0, [("s1", 0.08)], cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", 1.0, "s1", cap)
        )

    def test_rejects_invalid_strategy_id(self) -> None:
        cap = 0.08
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", 1.0, {"": 0.08}, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", 1.0, {"   ": 0.08}, cap)
        )
        self.assertIsNone(
            build_explicit_policy_snapshot("US", "policy-v1", 1.0, {123: 0.08}, cap)
        )

    def test_rejects_invalid_base_kelly_values(self) -> None:
        cap = 0.08
        invalid_values = [
            float("nan"),
            float("inf"),
            float("-inf"),
            True,
            False,
            0.0,
            -0.01,
            1.01,
        ]
        for value in invalid_values:
            with self.subTest(value=value):
                self.assertIsNone(
                    build_explicit_policy_snapshot(
                        "US",
                        "policy-v1",
                        1.0,
                        {"s1": value},
                        cap,
                    )
                )

    def test_rejects_invalid_absolute_kelly_cap(self) -> None:
        base = {"s1": 0.08}
        invalid_caps = [
            float("nan"),
            float("inf"),
            0.0,
            -0.01,
            1.01,
            True,
        ]
        for cap in invalid_caps:
            with self.subTest(cap=cap):
                self.assertIsNone(
                    build_explicit_policy_snapshot(
                        "US",
                        "policy-v1",
                        1.0,
                        base,
                        cap,
                    )
                )

    def test_rejects_base_kelly_above_absolute_cap(self) -> None:
        self.assertIsNone(
            build_explicit_policy_snapshot(
                "US",
                "policy-v1",
                1.0,
                {"s1": 0.10},
                0.08,
            )
        )

    def test_neutral_alpha_when_alpha_unspecified(self) -> None:
        snapshot = build_explicit_policy_snapshot(
            "US",
            "policy-v1",
            1.0,
            {"s1": 0.08, "s2": 0.05},
            0.08,
        )
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(
            dict(snapshot.alpha_overlay_by_strategy),
            {"s1": 1.0, "s2": 1.0},
        )
        self.assertEqual(snapshot.max_alpha_overlay, 1.0)

    def test_rejects_non_neutral_alpha_or_key_mismatch(self) -> None:
        base = {"s1": 0.08, "s2": 0.05}
        cap = 0.08
        self.assertIsNone(
            build_explicit_policy_snapshot(
                "US",
                "policy-v1",
                1.0,
                base,
                cap,
                alpha_overlay_by_strategy={"s1": 1.0, "s2": 0.9},
            )
        )
        self.assertIsNone(
            build_explicit_policy_snapshot(
                "US",
                "policy-v1",
                1.0,
                base,
                cap,
                alpha_overlay_by_strategy={"s1": 1.0},
            )
        )
        self.assertIsNone(
            build_explicit_policy_snapshot(
                "US",
                "policy-v1",
                1.0,
                base,
                cap,
                alpha_overlay_by_strategy={"s1": 1.0, "s2": 1.0, "s3": 1.0},
            )
        )
        self.assertIsNone(
            build_explicit_policy_snapshot(
                "US",
                "policy-v1",
                1.0,
                base,
                cap,
                alpha_overlay_by_strategy={"s1": 1.0, "s2": 1.0},
                max_alpha_overlay=1.2,
            )
        )

    def test_neutral_live_risk_and_unbounded_exposure_caps(self) -> None:
        live = build_neutral_live_risk_snapshot("US", "live-v1", 2.0)
        self.assertIsNotNone(live)
        assert live is not None
        self.assertEqual(live.market, "US")
        self.assertEqual(live.version, "live-v1")
        self.assertEqual(live.generated_at, 2.0)
        self.assertEqual(live.hard_gates, ())
        self.assertEqual(live.risk_signals, ())

        caps = build_unbounded_exposure_caps()
        self.assertIsNone(caps.position_cap)
        self.assertIsNone(caps.sector_remaining_cap)
        self.assertIsNone(caps.portfolio_remaining_cap)

        self.assertIsNone(
            build_neutral_live_risk_snapshot("EU", "live-v1", 2.0)
        )

    def test_input_dict_mutation_does_not_change_snapshot(self) -> None:
        base = {"s1": 0.08}
        alpha = {"s1": 1.0}
        snapshot = build_explicit_policy_snapshot(
            "US",
            "policy-v1",
            1.0,
            base,
            0.08,
            alpha_overlay_by_strategy=alpha,
        )
        self.assertIsNotNone(snapshot)
        assert snapshot is not None

        base["s1"] = 0.99
        alpha["s1"] = 0.5

        self.assertEqual(dict(snapshot.base_kelly_by_strategy), {"s1": 0.08})
        self.assertEqual(dict(snapshot.alpha_overlay_by_strategy), {"s1": 1.0})


if __name__ == "__main__":
    unittest.main()
