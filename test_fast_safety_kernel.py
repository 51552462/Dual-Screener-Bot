from __future__ import annotations

import math
import unittest

from fast_safety_kernel import (
    ExposureCaps,
    LiveRiskSnapshot,
    PolicySnapshot,
    RiskSignal,
    build_audit_event,
    compute_kelly_decision,
    try_emit_audit,
)


class _Emitter:
    def __init__(self, result: bool = True, raises: bool = False) -> None:
        self.result = result
        self.raises = raises
        self.events: list[dict[str, object]] = []

    def try_emit(self, event):
        if self.raises:
            raise RuntimeError("queue unavailable")
        self.events.append(dict(event))
        return self.result


class FastSafetyKernelTests(unittest.TestCase):
    def _policy(self, **overrides) -> PolicySnapshot:
        values = {
            "market": "US",
            "version": "policy-v1",
            "generated_at": 1.0,
            "base_kelly_by_strategy": {"s1": 0.10},
            "alpha_overlay_by_strategy": {"s1": 1.40},
            "max_alpha_overlay": 1.20,
            "absolute_kelly_cap": 0.08,
        }
        values.update(overrides)
        return PolicySnapshot(**values)

    def _live(self, **overrides) -> LiveRiskSnapshot:
        values = {
            "market": "US",
            "version": "live-v1",
            "generated_at": 2.0,
            "hard_gates": (),
            "risk_signals": (),
        }
        values.update(overrides)
        return LiveRiskSnapshot(**values)

    def test_full_chain_family_min_top1_and_exposure_cap(self) -> None:
        live = self._live(
            risk_signals=(
                RiskSignal("dd_soft", "drawdown", 0.70),
                RiskSignal("dd_hard", "drawdown", 0.50),
                RiskSignal("thin_book", "liquidity", 0.65),
                RiskSignal("alpha_like_boost_is_clamped", "strategy_health", 1.30),
            )
        )
        decision = compute_kelly_decision(
            self._policy(),
            live,
            "s1",
            ExposureCaps(
                position_cap=0.07,
                sector_remaining_cap=0.04,
                portfolio_remaining_cap=0.06,
            ),
        )
        self.assertAlmostEqual(decision.base_kelly, 0.10)
        self.assertAlmostEqual(decision.alpha_overlay, 1.20)
        self.assertEqual(decision.selected_family, "drawdown")
        self.assertEqual(decision.selected_risk, "dd_hard")
        self.assertAlmostEqual(decision.risk_multiplier, 0.50)
        self.assertAlmostEqual(decision.uncapped_kelly, 0.06)
        self.assertAlmostEqual(decision.effective_cap, 0.04)
        self.assertEqual(decision.cap_source, "sector_remaining")
        self.assertAlmostEqual(decision.final_kelly, 0.04)
        self.assertFalse(decision.blocked)

    def test_hard_gate_blocks_before_allocation(self) -> None:
        live = self._live(
            hard_gates=(
                RiskSignal(
                    "catastrophic_day",
                    "drawdown",
                    1.0,
                    blocked=True,
                    reason="loss_streak",
                ),
            )
        )
        decision = compute_kelly_decision(self._policy(), live, "s1")
        self.assertTrue(decision.blocked)
        self.assertEqual(decision.final_kelly, 0.0)
        self.assertIn("HARD_GATE", decision.reason)
        self.assertEqual(decision.selected_risk, "catastrophic_day")

    def test_market_mismatch_is_fail_closed(self) -> None:
        decision = compute_kelly_decision(
            self._policy(market="KR"), self._live(market="US"), "s1"
        )
        self.assertTrue(decision.blocked)
        self.assertEqual(decision.final_kelly, 0.0)
        self.assertIn("MARKET_MISMATCH", decision.reason)

    def test_missing_strategy_is_fail_closed(self) -> None:
        decision = compute_kelly_decision(self._policy(), self._live(), "unknown")
        self.assertTrue(decision.blocked)
        self.assertIn("INVALID_BASE_KELLY", decision.reason)

    def test_missing_alpha_overlay_is_neutral(self) -> None:
        policy = self._policy(alpha_overlay_by_strategy={})
        decision = compute_kelly_decision(policy, self._live(), "s1")
        self.assertAlmostEqual(decision.alpha_overlay, 1.0)
        self.assertAlmostEqual(decision.final_kelly, 0.08)

    def test_nan_risk_signal_is_fail_closed(self) -> None:
        live = self._live(
            risk_signals=(RiskSignal("bad", "data_integrity", math.nan),)
        )
        decision = compute_kelly_decision(self._policy(), live, "s1")
        self.assertTrue(decision.blocked)
        self.assertEqual(decision.final_kelly, 0.0)
        self.assertIn("INVALID_RISK_SIGNAL", decision.reason)

    def test_negative_exposure_cap_is_fail_closed(self) -> None:
        decision = compute_kelly_decision(
            self._policy(),
            self._live(),
            "s1",
            ExposureCaps(position_cap=-0.01),
        )
        self.assertTrue(decision.blocked)
        self.assertEqual(decision.final_kelly, 0.0)
        self.assertIn("INVALID_EXPOSURE_CAP", decision.reason)

    def test_policy_mapping_is_copied_and_read_only(self) -> None:
        base = {"s1": 0.1}
        policy = self._policy(base_kelly_by_strategy=base)
        base["s1"] = 0.9
        self.assertEqual(policy.base_kelly_by_strategy["s1"], 0.1)
        with self.assertRaises(TypeError):
            policy.base_kelly_by_strategy["s1"] = 0.2  # type: ignore[index]

    def test_audit_payload_and_best_effort_emission(self) -> None:
        decision = compute_kelly_decision(self._policy(), self._live(), "s1")
        event = build_audit_event(decision)
        self.assertEqual(event["event_type"], "fast_safety_kelly_decision")
        emitter = _Emitter(result=True)
        self.assertTrue(try_emit_audit(decision, emitter))
        self.assertEqual(len(emitter.events), 1)
        self.assertFalse(try_emit_audit(decision, _Emitter(raises=True)))
        self.assertFalse(try_emit_audit(decision, None))

    def test_tie_break_is_deterministic(self) -> None:
        live = self._live(
            risk_signals=(
                RiskSignal("z", "beta", 0.5),
                RiskSignal("b", "alpha", 0.5),
                RiskSignal("a", "alpha", 0.5),
            )
        )
        decision = compute_kelly_decision(self._policy(), live, "s1")
        self.assertEqual(decision.selected_family, "alpha")
        self.assertEqual(decision.selected_risk, "a")


if __name__ == "__main__":
    unittest.main(verbosity=2)
