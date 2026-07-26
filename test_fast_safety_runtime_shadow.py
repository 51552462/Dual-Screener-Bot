"""Unit tests for fast_safety_runtime_shadow (Chapter 3-B0D3A1)."""

from __future__ import annotations

import copy
import unittest
from typing import Any
from unittest.mock import patch

from fast_safety_audit_queue import BoundedAuditEmitter
from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    FAST_SAFETY_POLICY_VERSION,
)
from fast_safety_strategy_identity import resolve_supernova_strategy_identity

from fast_safety_runtime_shadow import (
    FastSafetyShadowContext,
    evaluate_supernova_fast_safety_shadow,
    prepare_fast_safety_shadow_context,
)


def _strategy_id_for(market: str, route: str, **kwargs: object) -> str:
    identity = resolve_supernova_strategy_identity(market, route, **kwargs)
    assert identity is not None
    return identity.strategy_id


def _enabled_policy_document(
    *,
    market: str,
    strategy_id: str,
    generated_at: float = 100.0,
    base_kelly: float = 0.08,
    cap: float = 0.10,
) -> dict[str, Any]:
    return {
        "enabled": True,
        "market": market,
        "version": FAST_SAFETY_POLICY_VERSION,
        "generated_at": generated_at,
        "base_kelly_by_strategy": {strategy_id: base_kelly},
        "absolute_kelly_cap": cap,
    }


def _store_from_documents(documents: dict[str, Any]):
    def getter(key: str, default: Any = None) -> Any:
        return documents.get(key, default)

    return getter


class FastSafetyRuntimeShadowTests(unittest.TestCase):
    def test_shadow_disabled(self) -> None:
        with patch(
            "fast_safety_runtime_shadow.load_fast_safety_policy_snapshot"
        ) as load_mock:
            ctx = prepare_fast_safety_shadow_context(
                "KR",
                shadow_enabled=False,
            )

        load_mock.assert_not_called()
        self.assertFalse(ctx.ready)
        self.assertEqual(ctx.reason, "shadow-disabled")
        self.assertIsNone(ctx.policy)
        self.assertIsNone(ctx.live_risk)
        self.assertIsNone(ctx.exposure_caps)

    def test_invalid_market(self) -> None:
        cases = (
            ("empty string", ""),
            ("unsupported market", "JP"),
            ("non-string", 123),
        )
        for label, market in cases:
            with self.subTest(label=label):
                with patch(
                    "fast_safety_runtime_shadow.load_fast_safety_policy_snapshot"
                ) as load_mock:
                    ctx = prepare_fast_safety_shadow_context(
                        market,
                        shadow_enabled=True,
                    )

                load_mock.assert_not_called()
                self.assertFalse(ctx.ready)
                self.assertEqual(ctx.reason, "invalid-market")

    def test_shadow_enabled_invalid_type(self) -> None:
        for value in (1, 0, "true", None):
            with self.subTest(value=value):
                with patch(
                    "fast_safety_runtime_shadow.load_fast_safety_policy_snapshot"
                ) as load_mock:
                    ctx = prepare_fast_safety_shadow_context(
                        "KR",
                        shadow_enabled=value,  # type: ignore[arg-type]
                    )

                load_mock.assert_not_called()
                self.assertFalse(ctx.ready)
                self.assertEqual(ctx.reason, "context-error")

    def test_policy_unavailable(self) -> None:
        kr_key = FAST_SAFETY_POLICY_KEYS["KR"]
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name="POLICY_KR")
        valid = _enabled_policy_document(
            market="KR",
            strategy_id=strategy_id,
        )
        cases = (
            ("absent", {}),
            (
                "disabled document",
                {
                    kr_key: {
                        "enabled": False,
                        "market": "KR",
                        "version": FAST_SAFETY_POLICY_VERSION,
                        "generated_at": 1.0,
                    }
                },
            ),
            (
                "invalid document",
                {kr_key: {"enabled": True, "market": "KR"}},
            ),
        )
        for label, documents in cases:
            with self.subTest(label=label):
                with patch(
                    "fast_safety_runtime_shadow.build_neutral_live_risk_snapshot"
                ) as live_mock, patch(
                    "fast_safety_runtime_shadow.compute_kelly_decision"
                ) as kernel_mock:
                    ctx = prepare_fast_safety_shadow_context(
                        "KR",
                        shadow_enabled=True,
                        get_value=_store_from_documents(documents),
                    )

                self.assertFalse(ctx.ready)
                self.assertEqual(ctx.reason, "policy-unavailable")
                live_mock.assert_not_called()
                kernel_mock.assert_not_called()

        self.assertIsNotNone(valid)

    def test_kr_enabled_context_success(self) -> None:
        group_key = "CTX_KR_PASS"
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name=group_key)
        documents = {
            FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                market="KR",
                strategy_id=strategy_id,
            ),
        }

        ctx = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            get_value=_store_from_documents(documents),
        )

        self.assertTrue(ctx.ready)
        self.assertEqual(ctx.market, "KR")
        self.assertEqual(ctx.reason, "context-ready")
        self.assertIsNotNone(ctx.policy)
        assert ctx.policy is not None
        self.assertEqual(ctx.policy.market, "KR")
        self.assertIsNotNone(ctx.live_risk)
        assert ctx.live_risk is not None
        self.assertEqual(ctx.live_risk.hard_gates, ())
        self.assertEqual(ctx.live_risk.risk_signals, ())
        self.assertIsNotNone(ctx.exposure_caps)
        assert ctx.exposure_caps is not None
        self.assertIsNone(ctx.exposure_caps.position_cap)

    def test_us_enabled_context_success(self) -> None:
        group_key = "CTX_US_PASS"
        strategy_id = _strategy_id_for("US", "COSINE", best_pass_name=group_key)
        documents = {
            FAST_SAFETY_POLICY_KEYS["US"]: _enabled_policy_document(
                market="US",
                strategy_id=strategy_id,
            ),
        }

        ctx = prepare_fast_safety_shadow_context(
            "US",
            shadow_enabled=True,
            get_value=_store_from_documents(documents),
        )

        self.assertTrue(ctx.ready)
        self.assertEqual(ctx.market, "US")
        self.assertIsNotNone(ctx.policy)
        assert ctx.policy is not None
        self.assertEqual(ctx.policy.market, "US")
        self.assertNotIn(FAST_SAFETY_POLICY_KEYS["KR"], documents)

    def test_policy_read_exception_isolated(self) -> None:
        with patch(
            "fast_safety_runtime_shadow.load_fast_safety_policy_snapshot",
            side_effect=RuntimeError("injected policy read failure"),
        ):
            ctx = prepare_fast_safety_shadow_context(
                "KR",
                shadow_enabled=True,
                get_value=_store_from_documents({}),
            )

        self.assertFalse(ctx.ready)
        self.assertEqual(ctx.reason, "context-error")

    def test_live_risk_builder_exception_isolated(self) -> None:
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name="LIVE_ERR")
        documents = {
            FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                market="KR",
                strategy_id=strategy_id,
            ),
        }

        with patch(
            "fast_safety_runtime_shadow.build_neutral_live_risk_snapshot",
            side_effect=RuntimeError("injected live risk failure"),
        ):
            ctx = prepare_fast_safety_shadow_context(
                "KR",
                shadow_enabled=True,
                get_value=_store_from_documents(documents),
            )

        self.assertFalse(ctx.ready)
        self.assertEqual(ctx.reason, "context-error")

        evaluation = evaluate_supernova_fast_safety_shadow(
            ctx,
            route="COSINE",
            best_pass_name="LIVE_ERR",
        )
        self.assertFalse(evaluation.attempted)

    def test_context_snapshot_single_policy_read(self) -> None:
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name="ONCE")
        documents = {
            FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                market="KR",
                strategy_id=strategy_id,
            ),
        }

        with patch(
            "fast_safety_runtime_shadow.load_fast_safety_policy_snapshot",
            wraps=__import__(
                "fast_safety_policy_store",
                fromlist=["load_fast_safety_policy_snapshot"],
            ).load_fast_safety_policy_snapshot,
        ) as load_mock:
            ctx = prepare_fast_safety_shadow_context(
                "KR",
                shadow_enabled=True,
                get_value=_store_from_documents(documents),
            )
            self.assertTrue(ctx.ready)

            for _ in range(3):
                result = evaluate_supernova_fast_safety_shadow(
                    ctx,
                    route="COSINE",
                    best_pass_name="ONCE",
                )
                self.assertTrue(result.evaluated)

        self.assertEqual(load_mock.call_count, 1)

    def test_cosine_identity_and_kernel_evaluation(self) -> None:
        group_key = "COSINE_MAIN"
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name=group_key)
        ctx = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            get_value=_store_from_documents(
                {
                    FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                        market="KR",
                        strategy_id=strategy_id,
                    ),
                }
            ),
        )

        result = evaluate_supernova_fast_safety_shadow(
            ctx,
            route="COSINE",
            best_pass_name=group_key,
        )

        expected_identity = resolve_supernova_strategy_identity(
            "KR",
            "COSINE",
            best_pass_name=group_key,
        )
        self.assertEqual(result.identity, expected_identity)
        self.assertTrue(result.attempted)
        self.assertTrue(result.evaluated)
        self.assertIsNotNone(result.decision)
        assert result.decision is not None
        self.assertEqual(result.decision.strategy_id, strategy_id)

    def test_cosine_fallback_and_mlbox_underdog(self) -> None:
        cases = (
            (
                "COSINE best_pattern fallback",
                "KR",
                "COSINE",
                {"best_pass_name": "UNKNOWN", "best_pattern_name": "PATTERN_FB"},
            ),
            (
                "MLBOX ml_pattern",
                "KR",
                "MLBOX",
                {"ml_pattern_name": "ML_CLUSTER_A"},
            ),
            (
                "UNDERDOG_MLBOX ml_pattern",
                "US",
                "UNDERDOG_MLBOX",
                {"ml_pattern_name": "UD_CLUSTER_B"},
            ),
        )

        for label, market, route, identity_kwargs in cases:
            with self.subTest(label=label):
                expected_identity = resolve_supernova_strategy_identity(
                    market,
                    route,
                    **identity_kwargs,
                )
                assert expected_identity is not None
                documents = {
                    FAST_SAFETY_POLICY_KEYS[market]: _enabled_policy_document(
                        market=market,
                        strategy_id=expected_identity.strategy_id,
                    ),
                }
                ctx = prepare_fast_safety_shadow_context(
                    market,
                    shadow_enabled=True,
                    get_value=_store_from_documents(documents),
                )
                result = evaluate_supernova_fast_safety_shadow(
                    ctx,
                    route=route,
                    **identity_kwargs,
                )

                self.assertEqual(result.identity, expected_identity)
                self.assertTrue(result.evaluated)
                self.assertIsNotNone(result.decision)

    def test_scout_and_unknown_route_skip(self) -> None:
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name="SKIP_BASE")
        ctx = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            get_value=_store_from_documents(
                {
                    FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                        market="KR",
                        strategy_id=strategy_id,
                    ),
                }
            ),
        )

        for route in ("SCOUT", "UNKNOWN"):
            with self.subTest(route=route):
                with patch(
                    "fast_safety_runtime_shadow.compute_kelly_decision"
                ) as kernel_mock, patch(
                    "fast_safety_runtime_shadow.try_emit_audit"
                ) as audit_mock:
                    result = evaluate_supernova_fast_safety_shadow(
                        ctx,
                        route=route,
                        best_pass_name="PASS_ALPHA",
                        best_pattern_name="PATTERN_BETA",
                        ml_pattern_name="ML_GAMMA",
                    )

                kernel_mock.assert_not_called()
                audit_mock.assert_not_called()
                self.assertTrue(result.attempted)
                self.assertFalse(result.evaluated)
                self.assertIsNone(result.identity)
                self.assertEqual(result.reason, "identity-unavailable")

    def test_kernel_exception_isolated(self) -> None:
        group_key = "KERNEL_ERR"
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name=group_key)
        ctx = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            get_value=_store_from_documents(
                {
                    FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                        market="KR",
                        strategy_id=strategy_id,
                    ),
                }
            ),
        )

        with patch(
            "fast_safety_runtime_shadow.compute_kelly_decision",
            side_effect=RuntimeError("injected kernel failure"),
        ), patch(
            "fast_safety_runtime_shadow.try_emit_audit"
        ) as audit_mock:
            result = evaluate_supernova_fast_safety_shadow(
                ctx,
                route="COSINE",
                best_pass_name=group_key,
            )

        audit_mock.assert_not_called()
        self.assertTrue(result.attempted)
        self.assertFalse(result.evaluated)
        self.assertEqual(result.reason, "evaluation-error")
        self.assertIsNone(result.decision)

    def test_audit_success_and_no_emitter(self) -> None:
        group_key = "AUDIT_CASE"
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name=group_key)
        documents = {
            FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                market="KR",
                strategy_id=strategy_id,
            ),
        }

        ctx_with_emitter = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            emitter=BoundedAuditEmitter(maxsize=8),
            get_value=_store_from_documents(documents),
        )
        ctx_without_emitter = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            emitter=None,
            get_value=_store_from_documents(documents),
        )

        with_emitter = evaluate_supernova_fast_safety_shadow(
            ctx_with_emitter,
            route="COSINE",
            best_pass_name=group_key,
        )
        without_emitter = evaluate_supernova_fast_safety_shadow(
            ctx_without_emitter,
            route="COSINE",
            best_pass_name=group_key,
        )

        self.assertTrue(with_emitter.audit_emitted)
        self.assertEqual(with_emitter.reason, "evaluated-and-emitted")
        self.assertFalse(without_emitter.audit_emitted)
        self.assertEqual(without_emitter.reason, "evaluated-not-emitted")
        self.assertIsNotNone(with_emitter.decision)
        self.assertIsNotNone(without_emitter.decision)
        assert with_emitter.decision is not None
        assert without_emitter.decision is not None
        self.assertEqual(
            with_emitter.decision.final_kelly,
            without_emitter.decision.final_kelly,
        )
        self.assertEqual(
            with_emitter.decision.strategy_id,
            without_emitter.decision.strategy_id,
        )

    def test_queue_full_and_emitter_exception_isolated(self) -> None:
        group_key = "AUDIT_ERR"
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name=group_key)
        documents = {
            FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                market="KR",
                strategy_id=strategy_id,
            ),
        }

        full_emitter = BoundedAuditEmitter(maxsize=1)
        self.assertTrue(full_emitter.try_emit({"event": "prefill"}))

        class ExplodingEmitter:
            def try_emit(self, event) -> bool:
                raise RuntimeError("injected emitter failure")

        cases = (
            ("queue full", full_emitter),
            ("emitter exception", ExplodingEmitter()),
        )

        for label, emitter in cases:
            with self.subTest(label=label):
                ctx = prepare_fast_safety_shadow_context(
                    "KR",
                    shadow_enabled=True,
                    emitter=emitter,
                    get_value=_store_from_documents(documents),
                )
                result = evaluate_supernova_fast_safety_shadow(
                    ctx,
                    route="COSINE",
                    best_pass_name=group_key,
                )

                self.assertTrue(result.evaluated)
                self.assertIsNotNone(result.decision)
                self.assertFalse(result.audit_emitted)
                self.assertEqual(result.reason, "evaluated-not-emitted")

    def test_shadow_result_does_not_mutate_trading_state(self) -> None:
        group_key = "STATE_GUARD"
        strategy_id = _strategy_id_for("KR", "COSINE", best_pass_name=group_key)
        ctx = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            get_value=_store_from_documents(
                {
                    FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                        market="KR",
                        strategy_id=strategy_id,
                    ),
                }
            ),
        )

        trading_state = {
            "kelly_risk_pct": 0.025,
            "invest_amount": 1_500_000,
            "shares": 42,
            "order_payload": {"market": "KR", "code": "005930", "qty": 42},
        }
        before = copy.deepcopy(trading_state)

        result = evaluate_supernova_fast_safety_shadow(
            ctx,
            route="COSINE",
            best_pass_name=group_key,
        )

        self.assertTrue(result.evaluated)
        self.assertEqual(trading_state, before)

        missing_strategy_ctx = prepare_fast_safety_shadow_context(
            "KR",
            shadow_enabled=True,
            get_value=_store_from_documents(
                {
                    FAST_SAFETY_POLICY_KEYS["KR"]: _enabled_policy_document(
                        market="KR",
                        strategy_id="strat:missing00000000",
                    ),
                }
            ),
        )
        blocked = evaluate_supernova_fast_safety_shadow(
            missing_strategy_ctx,
            route="COSINE",
            best_pass_name=group_key,
        )

        self.assertTrue(blocked.evaluated)
        self.assertIsNotNone(blocked.decision)
        assert blocked.decision is not None
        self.assertTrue(blocked.decision.blocked)
        self.assertEqual(trading_state, before)


if __name__ == "__main__":
    unittest.main()
