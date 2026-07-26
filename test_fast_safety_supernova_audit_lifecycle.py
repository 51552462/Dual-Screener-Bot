"""Unit tests for Supernova scan-local Fast Safety Audit lifecycle wrapper (Chapter 3-B0D3A4C)."""

from __future__ import annotations

import ast
import inspect
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

sys.modules.setdefault(
    "auto_forward_tester",
    types.ModuleType("auto_forward_tester"),
)

from fast_safety_audit_runtime import FastSafetyAuditRuntime

import supernova_hunter as snh


def _ready_runtime(*, emitter: object | None = None) -> FastSafetyAuditRuntime:
    if emitter is None:
        emitter = object()
    return FastSafetyAuditRuntime(
        shadow_enabled=True,
        ready=True,
        reason="runtime-ready",
        emitter=emitter,
    )


def _not_ready_runtime() -> FastSafetyAuditRuntime:
    return FastSafetyAuditRuntime(
        shadow_enabled=True,
        ready=False,
        reason="runtime-not-ready",
        emitter=None,
    )


_RETURN_SENTINEL = object()


class FastSafetySupernovaAuditLifecycleTests(unittest.TestCase):
    def test_wrapper_default_signature(self) -> None:
        sig = inspect.signature(snh.execute_supernova_live_scan_with_fast_safety_audit)
        self.assertEqual(
            sig.parameters["fast_safety_shadow_enabled"].default,
            False,
        )
        self.assertIs(
            sig.parameters["fast_safety_audit_sink"].default,
            None,
        )

    def test_shadow_off_no_runtime_or_drain(self) -> None:
        expected = {"status": "shadow-off"}

        with patch.object(snh, "create_fast_safety_audit_runtime") as create_mock, patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ) as drain_mock, patch.object(
            snh, "execute_supernova_live_scan", return_value=expected
        ) as scan_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_audit("KR")

        create_mock.assert_not_called()
        drain_mock.assert_not_called()
        scan_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=False,
            fast_safety_audit_emitter=None,
        )
        self.assertIs(result, expected)

    def test_invalid_shadow_enabled_values(self) -> None:
        invalid_values = (1, 0, "true", None, object())

        for value in invalid_values:
            with self.subTest(value=value):
                expected = {"invalid": value}

                with patch.object(
                    snh, "create_fast_safety_audit_runtime"
                ) as create_mock, patch.object(
                    snh, "drain_fast_safety_audit_runtime"
                ) as drain_mock, patch.object(
                    snh, "execute_supernova_live_scan", return_value=expected
                ) as scan_mock:
                    result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                        "KR",
                        fast_safety_shadow_enabled=value,  # type: ignore[arg-type]
                        fast_safety_audit_sink=lambda _event: True,
                    )

                create_mock.assert_not_called()
                drain_mock.assert_not_called()
                scan_mock.assert_called_once_with(
                    "KR",
                    fast_safety_shadow_enabled=False,
                    fast_safety_audit_emitter=None,
                )
                self.assertIs(result, expected)

    def test_shadow_on_with_callable_sink(self) -> None:
        runtime = _ready_runtime()
        sink = MagicMock(return_value=True)

        with patch.object(
            snh, "create_fast_safety_audit_runtime", return_value=runtime
        ) as create_mock, patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ) as drain_mock, patch.object(
            snh, "execute_supernova_live_scan", return_value={"status": "ok"}
        ) as scan_mock:
            snh.execute_supernova_live_scan_with_fast_safety_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_audit_sink=sink,
            )

        create_mock.assert_called_once_with(shadow_enabled=True)
        scan_mock.assert_called_once()
        drain_mock.assert_called_once_with(runtime, sink)

    def test_runtime_emitter_passed_to_scan(self) -> None:
        runtime = _ready_runtime()
        sink = lambda _event: True

        with patch.object(
            snh, "create_fast_safety_audit_runtime", return_value=runtime
        ), patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ), patch.object(
            snh, "execute_supernova_live_scan"
        ) as scan_mock:
            snh.execute_supernova_live_scan_with_fast_safety_audit(
                "US",
                fast_safety_shadow_enabled=True,
                fast_safety_audit_sink=sink,
            )

        _, kwargs = scan_mock.call_args
        self.assertTrue(kwargs["fast_safety_shadow_enabled"])
        self.assertIs(kwargs["fast_safety_audit_emitter"], runtime.emitter)
        self.assertNotIn("runtime", kwargs)

    def test_shadow_on_sink_none(self) -> None:
        expected = {"status": "no-sink"}

        with patch.object(
            snh, "create_fast_safety_audit_runtime"
        ) as create_mock, patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ) as drain_mock, patch.object(
            snh, "execute_supernova_live_scan", return_value=expected
        ) as scan_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_audit_sink=None,
            )

        create_mock.assert_not_called()
        drain_mock.assert_not_called()
        scan_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=True,
            fast_safety_audit_emitter=None,
        )
        self.assertIs(result, expected)

    def test_shadow_on_sink_non_callable(self) -> None:
        invalid_sinks = (0, False, "not-callable", object())

        for sink in invalid_sinks:
            with self.subTest(sink=sink):
                expected = {"sink": sink}

                with patch.object(
                    snh, "create_fast_safety_audit_runtime"
                ) as create_mock, patch.object(
                    snh, "drain_fast_safety_audit_runtime"
                ) as drain_mock, patch.object(
                    snh, "execute_supernova_live_scan", return_value=expected
                ) as scan_mock:
                    result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                        "KR",
                        fast_safety_shadow_enabled=True,
                        fast_safety_audit_sink=sink,
                    )

                create_mock.assert_not_called()
                drain_mock.assert_not_called()
                scan_mock.assert_called_once_with(
                    "KR",
                    fast_safety_shadow_enabled=True,
                    fast_safety_audit_emitter=None,
                )
                self.assertIs(result, expected)

    def test_return_value_preserved_with_drain(self) -> None:
        runtime = _ready_runtime()
        sink = lambda _event: True
        return_values = (None, {"status": "ok"}, _RETURN_SENTINEL)

        for expected in return_values:
            with self.subTest(expected=expected):
                with patch.object(
                    snh, "create_fast_safety_audit_runtime", return_value=runtime
                ), patch.object(
                    snh, "drain_fast_safety_audit_runtime"
                ) as drain_mock, patch.object(
                    snh, "execute_supernova_live_scan", return_value=expected
                ):
                    result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                        "KR",
                        fast_safety_shadow_enabled=True,
                        fast_safety_audit_sink=sink,
                    )

                if expected is _RETURN_SENTINEL:
                    self.assertIs(result, expected)
                else:
                    self.assertEqual(result, expected)
                drain_mock.assert_called_once()

    def test_early_return_preserved_with_drain(self) -> None:
        runtime = _ready_runtime()
        sink = lambda _event: True
        early_returns = (
            {"status": "MARKET_CLOSED"},
            {"status": "EMPTY_UNIVERSE"},
            None,
        )

        for expected in early_returns:
            with self.subTest(expected=expected):
                with patch.object(
                    snh, "create_fast_safety_audit_runtime", return_value=runtime
                ), patch.object(
                    snh, "drain_fast_safety_audit_runtime"
                ) as drain_mock, patch.object(
                    snh, "execute_supernova_live_scan", return_value=expected
                ):
                    result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                        "KR",
                        fast_safety_shadow_enabled=True,
                        fast_safety_audit_sink=sink,
                    )

                self.assertEqual(result, expected)
                drain_mock.assert_called_once()

    def test_scan_exception_preserved(self) -> None:
        runtime = _ready_runtime()
        sink = lambda _event: True
        scan_exc = RuntimeError("scan failed")

        with patch.object(
            snh, "create_fast_safety_audit_runtime", return_value=runtime
        ), patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ) as drain_mock, patch.object(
            snh, "execute_supernova_live_scan", side_effect=scan_exc
        ):
            with self.assertRaises(RuntimeError) as ctx:
                snh.execute_supernova_live_scan_with_fast_safety_audit(
                    "KR",
                    fast_safety_shadow_enabled=True,
                    fast_safety_audit_sink=sink,
                )

        self.assertIs(ctx.exception, scan_exc)
        self.assertEqual(str(ctx.exception), "scan failed")
        drain_mock.assert_called_once()

    def test_runtime_creation_exception_isolation(self) -> None:
        expected = {"status": "scan-ok"}
        sink = lambda _event: True

        with patch.object(
            snh,
            "create_fast_safety_audit_runtime",
            side_effect=RuntimeError("runtime boom"),
        ), patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ) as drain_mock, patch.object(
            snh, "execute_supernova_live_scan", return_value=expected
        ) as scan_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_audit_sink=sink,
            )

        drain_mock.assert_not_called()
        scan_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=True,
            fast_safety_audit_emitter=None,
        )
        self.assertIs(result, expected)

    def test_drain_exception_isolation_on_normal_return(self) -> None:
        runtime = _ready_runtime()
        sink = lambda _event: True
        expected = {"status": "ok"}

        with patch.object(
            snh, "create_fast_safety_audit_runtime", return_value=runtime
        ), patch.object(
            snh,
            "drain_fast_safety_audit_runtime",
            side_effect=RuntimeError("drain boom"),
        ), patch.object(
            snh, "execute_supernova_live_scan", return_value=expected
        ):
            result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_audit_sink=sink,
            )

        self.assertIs(result, expected)

    def test_drain_exception_does_not_replace_scan_exception(self) -> None:
        runtime = _ready_runtime()
        sink = lambda _event: True
        scan_exc = ValueError("scan boom")

        with patch.object(
            snh, "create_fast_safety_audit_runtime", return_value=runtime
        ), patch.object(
            snh,
            "drain_fast_safety_audit_runtime",
            side_effect=RuntimeError("drain boom"),
        ), patch.object(
            snh, "execute_supernova_live_scan", side_effect=scan_exc
        ):
            with self.assertRaises(ValueError) as ctx:
                snh.execute_supernova_live_scan_with_fast_safety_audit(
                    "KR",
                    fast_safety_shadow_enabled=True,
                    fast_safety_audit_sink=sink,
                )

        self.assertIs(ctx.exception, scan_exc)
        self.assertNotIsInstance(ctx.exception, RuntimeError)

    def test_not_ready_runtime(self) -> None:
        runtime = _not_ready_runtime()
        sink = lambda _event: True
        expected = {"status": "scan-with-not-ready-runtime"}

        with patch.object(
            snh, "create_fast_safety_audit_runtime", return_value=runtime
        ), patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ) as drain_mock, patch.object(
            snh, "execute_supernova_live_scan", return_value=expected
        ) as scan_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_audit_sink=sink,
            )

        scan_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=True,
            fast_safety_audit_emitter=None,
        )
        drain_mock.assert_called_once_with(runtime, sink)
        self.assertIs(result, expected)

    def test_kr_us_scan_local_independence_and_production_callers_unchanged(self) -> None:
        kr_runtime = _ready_runtime()
        us_runtime = _ready_runtime()
        sink = lambda _event: True

        for market, runtime in (("KR", kr_runtime), ("US", us_runtime)):
            with self.subTest(market=market):
                with patch.object(
                    snh,
                    "create_fast_safety_audit_runtime",
                    return_value=runtime,
                ) as create_mock, patch.object(
                    snh, "drain_fast_safety_audit_runtime"
                ) as drain_mock, patch.object(
                    snh,
                    "execute_supernova_live_scan",
                    return_value={"market": market},
                ) as scan_mock:
                    snh.execute_supernova_live_scan_with_fast_safety_audit(
                        market,
                        fast_safety_shadow_enabled=True,
                        fast_safety_audit_sink=sink,
                    )

                create_mock.assert_called_once_with(shadow_enabled=True)
                scan_mock.assert_called_once_with(
                    market,
                    fast_safety_shadow_enabled=True,
                    fast_safety_audit_emitter=runtime.emitter,
                )
                drain_mock.assert_called_once_with(runtime, sink)

        scan_source = inspect.getsource(snh.execute_supernova_live_scan)
        self.assertNotIn("create_fast_safety_audit_runtime(", scan_source)
        self.assertNotIn("drain_fast_safety_audit_runtime(", scan_source)

        wrapper_source = inspect.getsource(
            snh.execute_supernova_live_scan_with_fast_safety_audit
        )
        wrapper_tree = ast.parse(wrapper_source)
        wrapper_calls = [
            node
            for node in ast.walk(wrapper_tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
        ]
        wrapper_call_names = [node.func.id for node in wrapper_calls]
        self.assertEqual(wrapper_call_names.count("execute_supernova_live_scan"), 1)
        self.assertEqual(wrapper_call_names.count("create_fast_safety_audit_runtime"), 1)
        self.assertEqual(wrapper_call_names.count("drain_fast_safety_audit_runtime"), 1)

        import factory_pipelines

        factory_source = inspect.getsource(factory_pipelines)
        factory_tree = ast.parse(factory_source)
        for fn_name in ("_step_supernova_kr", "_step_supernova_us"):
            fn_nodes = [
                node
                for node in ast.walk(factory_tree)
                if isinstance(node, ast.FunctionDef) and node.name == fn_name
            ]
            self.assertEqual(len(fn_nodes), 1, msg=f"missing {fn_name}")
            fn_calls = [
                node
                for node in ast.walk(fn_nodes[0])
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
            ]
            wrapper_calls_in_step = [
                call
                for call in fn_calls
                if call.func.id == "execute_supernova_live_scan_with_fast_safety_audit"
            ]
            self.assertEqual(
                wrapper_calls_in_step,
                [],
                msg=f"{fn_name} must not call lifecycle wrapper",
            )

        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)
        self.assertNotIn(
            "execute_supernova_live_scan_with_fast_safety_audit",
            scheduler_source,
        )


if __name__ == "__main__":
    unittest.main()
