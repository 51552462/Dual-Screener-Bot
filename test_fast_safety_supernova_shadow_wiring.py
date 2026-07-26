"""Unit tests for Supernova Fast Safety Shadow wiring (Chapter 3-B0D3A2)."""

from __future__ import annotations

import ast
import copy
import inspect
import sys
import types
import unittest
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

sys.modules.setdefault(
    "auto_forward_tester",
    types.ModuleType("auto_forward_tester"),
)

from fast_safety_runtime_shadow import FastSafetyShadowContext

import supernova_hunter as snh


@dataclass(frozen=True)
class _FakeShadowContext:
    market: str = "KR"


@dataclass(frozen=True)
class _FakeEvaluation:
    attempted: bool = True
    evaluated: bool = True
    audit_emitted: bool = False
    reason: str = "evaluated-not-emitted"
    identity: object = None
    decision: object = None


class _BlockedDecision:
    blocked = True
    final_kelly = 0.0


class FastSafetySupernovaShadowWiringTests(unittest.TestCase):
    def test_scan_prepare_helper_normal_pass_through(self) -> None:
        emitter = object()
        sentinel_ctx = _FakeShadowContext(market="US")

        with patch.object(
            snh,
            "prepare_fast_safety_shadow_context",
            return_value=sentinel_ctx,
        ) as prepare_mock:
            result = snh._prepare_fast_safety_shadow_for_scan(
                "US",
                shadow_enabled=True,
                emitter=emitter,
            )

        prepare_mock.assert_called_once_with(
            "US",
            shadow_enabled=True,
            emitter=emitter,
        )
        self.assertIs(result, sentinel_ctx)

    def test_scan_prepare_helper_exception_isolation(self) -> None:
        with patch.object(
            snh,
            "prepare_fast_safety_shadow_context",
            side_effect=RuntimeError("policy boom"),
        ):
            result = snh._prepare_fast_safety_shadow_for_scan(
                "KR",
                shadow_enabled=True,
                emitter=None,
            )

        self.assertIsNone(result)

    def test_candidate_evaluation_helper_normal_pass_through(self) -> None:
        ctx = MagicMock(spec=FastSafetyShadowContext)

        with patch.object(
            snh,
            "evaluate_supernova_fast_safety_shadow",
            return_value=_FakeEvaluation(),
        ) as evaluate_mock:
            result = snh._evaluate_fast_safety_shadow_candidate(
                ctx,
                route="MLBOX",
                best_pass_name="INCUBATOR_X",
                best_pattern_name="RANK_A",
                ml_pattern_name="CLUSTER_1",
            )

        evaluate_mock.assert_called_once_with(
            ctx,
            route="MLBOX",
            best_pass_name="INCUBATOR_X",
            best_pattern_name="RANK_A",
            ml_pattern_name="CLUSTER_1",
        )
        self.assertIsNone(result)

    def test_candidate_evaluation_helper_context_none(self) -> None:
        with patch.object(
            snh,
            "evaluate_supernova_fast_safety_shadow",
        ) as evaluate_mock:
            snh._evaluate_fast_safety_shadow_candidate(
                None,
                route="COSINE",
            )

        evaluate_mock.assert_not_called()

    def test_candidate_evaluation_helper_exception_isolation(self) -> None:
        ctx = MagicMock(spec=FastSafetyShadowContext)

        with patch.object(
            snh,
            "evaluate_supernova_fast_safety_shadow",
            side_effect=ValueError("kernel fault"),
        ):
            result = snh._evaluate_fast_safety_shadow_candidate(
                ctx,
                route="SCOUT",
            )

        self.assertIsNone(result)

    def test_candidate_external_state_immutable(self) -> None:
        ctx = MagicMock(spec=FastSafetyShadowContext)
        blocked_eval = _FakeEvaluation(decision=_BlockedDecision())

        trading_state: dict[str, Any] = {
            "kelly_risk_pct": 0.08,
            "invest_amount": 1_000_000.0,
            "shares": 42,
            "order_payload": {"side": "BUY", "qty": 42},
            "candidate": {
                "code": "005930",
                "final_sig": "[SUPERNOVA_MLBOX] 🤖C1",
                "final_score": 88.5,
            },
        }
        before = copy.deepcopy(trading_state)

        with patch.object(
            snh,
            "evaluate_supernova_fast_safety_shadow",
            return_value=blocked_eval,
        ):
            snh._evaluate_fast_safety_shadow_candidate(
                ctx,
                route="MLBOX",
                best_pass_name="UNKNOWN",
                best_pattern_name="RANK_A",
                ml_pattern_name="C1",
            )

        self.assertEqual(trading_state, before)

    def test_execute_function_default_args_safe(self) -> None:
        sig = inspect.signature(snh.execute_supernova_live_scan)
        params = list(sig.parameters.values())

        self.assertEqual(params[0].name, "market")
        self.assertEqual(
            sig.parameters["fast_safety_shadow_enabled"].default,
            False,
        )
        self.assertIs(
            sig.parameters["fast_safety_audit_emitter"].default,
            None,
        )
        self.assertEqual(
            [p.name for p in params],
            [
                "market",
                "fast_safety_shadow_enabled",
                "fast_safety_audit_emitter",
            ],
        )

    def test_scan_context_prepare_once_wiring(self) -> None:
        source = inspect.getsource(snh.execute_supernova_live_scan)
        tree = ast.parse(source)

        prepare_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_prepare_fast_safety_shadow_for_scan"
        ]
        self.assertEqual(len(prepare_calls), 1)

        process_live_names = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "process_live_ticker"
        }
        self.assertIn("process_live_ticker", process_live_names)

        for fn_node in ast.walk(tree):
            if (
                isinstance(fn_node, ast.FunctionDef)
                and fn_node.name == "process_live_ticker"
            ):
                inner_prepare = [
                    node
                    for node in ast.walk(fn_node)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "_prepare_fast_safety_shadow_for_scan"
                ]
                self.assertEqual(inner_prepare, [])

    def test_candidate_shadow_hook_exists(self) -> None:
        source = inspect.getsource(snh.execute_supernova_live_scan)
        tree = ast.parse(source)

        process_live_fn = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "process_live_ticker"
        )
        inner_evaluate = [
            node
            for node in ast.walk(process_live_fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_evaluate_fast_safety_shadow_candidate"
        ]
        self.assertEqual(len(inner_evaluate), 2)

        loop_wrapped = 0
        for call_node in inner_evaluate:
            for parent in ast.walk(process_live_fn):
                if not isinstance(parent, (ast.For, ast.While)):
                    continue
                for child in ast.walk(parent):
                    if child is call_node:
                        loop_wrapped += 1
                        break
        self.assertEqual(loop_wrapped, 0)

        self.assertIn('route="SCOUT"', source)
        self.assertIn("_fast_safety_shadow_route", source)
        self.assertIn('"UNDERDOG_MLBOX"', source)

    def test_shadow_result_not_used_invariant(self) -> None:
        source = inspect.getsource(snh.execute_supernova_live_scan)
        tree = ast.parse(source)

        forbidden_assign_targets = {
            "kelly_risk_pct",
            "invest_amount",
            "shares",
            "final_score",
            "final_sig",
        }

        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            value_src = ast.unparse(node.value) if hasattr(ast, "unparse") else ""
            if "evaluate_supernova_fast_safety_shadow" not in value_src:
                if "final_kelly" in value_src:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            self.fail(
                                f"shadow final_kelly assigned to {target.id}"
                            )
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.assertNotIn(
                        target.id,
                        forbidden_assign_targets,
                        "shadow evaluation must not mutate trading state",
                    )

        for node in ast.walk(tree):
            if not isinstance(node, (ast.If, ast.IfExp)):
                continue
            test_src = ast.unparse(node.test) if hasattr(ast, "unparse") else ""
            if ".blocked" in test_src and "shadow" in test_src.lower():
                self.fail("shadow blocked decision must not gate candidates")

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in {"update", "__setitem__"}
            ):
                continue
            call_src = ast.unparse(node) if hasattr(ast, "unparse") else ""
            if "shadow" in call_src.lower() and "evaluate" in call_src.lower():
                self.fail("shadow evaluation must not be stored on dicts")

    def test_try_add_virtual_position_boundary_invariant(self) -> None:
        source = inspect.getsource(snh.execute_supernova_live_scan)
        tree = ast.parse(source)

        forbidden_tokens = (
            "fast_safety_shadow_context",
            "FastSafetyShadowContext",
            "evaluate_supernova_fast_safety_shadow",
            "_evaluate_fast_safety_shadow_candidate",
            "shadow_evaluation",
            "strategy_id",
        )

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (
                isinstance(func, ast.Attribute)
                and func.attr == "try_add_virtual_position"
            ):
                continue
            for arg in node.args + [kw.value for kw in node.keywords]:
                arg_src = ast.unparse(arg) if hasattr(ast, "unparse") else ""
                for token in forbidden_tokens:
                    self.assertNotIn(
                        token,
                        arg_src,
                        f"try_add_virtual_position must not receive shadow data ({token})",
                    )

    def test_kr_us_common_wiring(self) -> None:
        emitter = MagicMock()
        ctx_kr = MagicMock(spec=FastSafetyShadowContext)
        ctx_us = MagicMock(spec=FastSafetyShadowContext)

        trading_state: dict[str, Any] = {
            "kelly_risk_pct": 0.05,
            "invest_amount": 500_000.0,
            "shares": 10,
            "order_payload": {"side": "BUY", "qty": 10},
            "candidate": {"code": "TEST", "final_score": 70.0},
        }

        cases = (
            ("KR", ctx_kr, "COSINE", "RANK_A", "RANK_A", None),
            ("US", ctx_us, "SCOUT", "UNKNOWN", "US_RANK_A", None),
            ("KR", ctx_kr, "UNDERDOG_MLBOX", "UNKNOWN", "UNKNOWN", "UD_C1"),
        )

        for market, ctx, route, best_pass, best_pattern, ml_pattern in cases:
            with self.subTest(market=market, route=route):
                before = copy.deepcopy(trading_state)

                with patch.object(
                    snh,
                    "prepare_fast_safety_shadow_context",
                    return_value=ctx,
                ) as prepare_mock:
                    prepared = snh._prepare_fast_safety_shadow_for_scan(
                        market,
                        shadow_enabled=True,
                        emitter=emitter,
                    )

                prepare_mock.assert_called_once_with(
                    market,
                    shadow_enabled=True,
                    emitter=emitter,
                )
                self.assertIs(prepared, ctx)

                with patch.object(
                    snh,
                    "evaluate_supernova_fast_safety_shadow",
                    return_value=_FakeEvaluation(
                        decision=_BlockedDecision(),
                    ),
                ) as evaluate_mock:
                    snh._evaluate_fast_safety_shadow_candidate(
                        prepared,
                        route=route,
                        best_pass_name=best_pass,
                        best_pattern_name=best_pattern,
                        ml_pattern_name=ml_pattern,
                    )

                evaluate_mock.assert_called_once_with(
                    prepared,
                    route=route,
                    best_pass_name=best_pass,
                    best_pattern_name=best_pattern,
                    ml_pattern_name=ml_pattern,
                )
                self.assertEqual(trading_state, before)


if __name__ == "__main__":
    unittest.main()
