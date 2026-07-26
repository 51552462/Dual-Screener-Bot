"""Unit tests for Supernova Fast Safety Ops Audit wrapper integration (Chapter 3-B0D3A4F)."""

from __future__ import annotations

import ast
import builtins
import inspect
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.modules.setdefault(
    "auto_forward_tester",
    types.ModuleType("auto_forward_tester"),
)

import factory_pipelines
import supernova_hunter as snh

REPO_ROOT = Path(__file__).resolve().parent
OPS_WRAPPER_NAME = "execute_supernova_live_scan_with_fast_safety_ops_audit"
LIFECYCLE_WRAPPER_NAME = "execute_supernova_live_scan_with_fast_safety_audit"
SCAN_NAME = "execute_supernova_live_scan"

FORBIDDEN_ACTIVATION_PATTERNS = (
    "ENABLE_FAST",
    "FAST_SAFETY_SHADOW",
    "FEATURE_FAST",
    "get_config_value",
    "os.getenv",
    "os.environ",
)

TRADE_BOUNDARY_SYMBOLS = (
    "try_add_virtual_position",
    "kelly_risk_pct",
    "invest_amount",
    "shares",
    "decision.blocked",
    "final_kelly",
)

IMMUTABLE_FILES = (
    "forward/shared.py",
    "ops_logger.py",
    "fast_safety_ops_sink.py",
    "fast_safety_audit_runtime.py",
    "fast_safety_audit_queue.py",
    "fast_safety_runtime_shadow.py",
    "fast_safety_kernel.py",
    "fast_safety_policy_store.py",
)

_RETURN_SENTINEL = object()


def _function_def(module_source: str, fn_name: str) -> ast.FunctionDef:
    tree = ast.parse(module_source)
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == fn_name
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected exactly one function {fn_name!r}")
    return matches[0]


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _ops_wrapper_calls(fn_node: ast.FunctionDef) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(fn_node)
        if isinstance(node, ast.Call) and _call_name(node) == OPS_WRAPPER_NAME
    ]


def _lifecycle_wrapper_calls(fn_node: ast.FunctionDef) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(fn_node)
        if isinstance(node, ast.Call) and _call_name(node) == LIFECYCLE_WRAPPER_NAME
    ]


def _direct_scan_calls(fn_node: ast.FunctionDef) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(fn_node)
        if isinstance(node, ast.Call) and _call_name(node) == SCAN_NAME
    ]


def _keyword_bool_false(call: ast.Call, name: str) -> bool:
    for kw in call.keywords:
        if kw.arg != name:
            continue
        if isinstance(kw.value, ast.Constant) and kw.value.value is False:
            return True
    return False


def _market_arg(call: ast.Call) -> str | None:
    if not call.args:
        return None
    first = call.args[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    return None


def _assert_ops_wrapper_explicit_off(call: ast.Call) -> None:
    assert _keyword_bool_false(call, "fast_safety_shadow_enabled"), (
        "fast_safety_shadow_enabled=False must be explicit"
    )
    for kw in call.keywords:
        if kw.arg in ("fast_safety_ops_writer", "fast_safety_audit_sink"):
            raise AssertionError(
                f"production call must not pass {kw.arg!r}"
            )


class FastSafetySupernovaOpsWriterIntegrationTests(unittest.TestCase):
    def test_ops_wrapper_default_signature(self) -> None:
        sig = inspect.signature(
            snh.execute_supernova_live_scan_with_fast_safety_ops_audit
        )
        self.assertEqual(
            sig.parameters["fast_safety_shadow_enabled"].default,
            False,
        )
        self.assertIs(
            sig.parameters["fast_safety_ops_writer"].default,
            None,
        )

    def test_shadow_off_no_ops_related_calls(self) -> None:
        expected = {"status": "shadow-off"}

        real_import = builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "ops_logger" or (
                fromlist and "ops_logger" in fromlist
            ):
                raise AssertionError("ops_logger import must not run on OFF path")
            return real_import(name, globals, locals, fromlist, level)

        with patch.object(
            snh,
            LIFECYCLE_WRAPPER_NAME,
            return_value=expected,
        ) as lifecycle_mock, patch.object(
            snh, "create_fast_safety_ops_sink"
        ) as sink_factory_mock, patch(
            "builtins.__import__", side_effect=guarded_import
        ):
            result = snh.execute_supernova_live_scan_with_fast_safety_ops_audit("KR")

        lifecycle_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=False,
            fast_safety_audit_sink=None,
        )
        sink_factory_mock.assert_not_called()
        self.assertIs(result, expected)

    def test_invalid_shadow_enabled_values(self) -> None:
        invalid_values = (1, 0, "true", None, object())

        for value in invalid_values:
            with self.subTest(value=value):
                expected = {"invalid": value}

                with patch.object(
                    snh,
                    LIFECYCLE_WRAPPER_NAME,
                    return_value=expected,
                ) as lifecycle_mock, patch.object(
                    snh, "create_fast_safety_ops_sink"
                ) as sink_factory_mock:
                    result = snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                        "KR",
                        fast_safety_shadow_enabled=value,  # type: ignore[arg-type]
                        fast_safety_ops_writer=lambda *_a, **_k: True,
                    )

                sink_factory_mock.assert_not_called()
                lifecycle_mock.assert_called_once_with(
                    "KR",
                    fast_safety_shadow_enabled=False,
                    fast_safety_audit_sink=None,
                )
                self.assertIs(result, expected)

    def test_shadow_on_with_injected_callable_writer(self) -> None:
        writer = MagicMock()
        created_sink = MagicMock()

        with patch.object(
            snh, "create_fast_safety_ops_sink", return_value=created_sink
        ) as sink_factory_mock, patch.object(
            snh, LIFECYCLE_WRAPPER_NAME, return_value={"status": "ok"}
        ) as lifecycle_mock:
            snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_ops_writer=writer,
            )

        sink_factory_mock.assert_called_once_with(writer)
        lifecycle_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=True,
            fast_safety_audit_sink=created_sink,
        )

    def test_sink_factory_does_not_call_writer_at_creation(self) -> None:
        writer = MagicMock()
        created_sink = object()

        with patch.object(
            snh, "create_fast_safety_ops_sink", return_value=created_sink
        ), patch.object(
            snh, LIFECYCLE_WRAPPER_NAME, return_value={"status": "ok"}
        ):
            snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_ops_writer=writer,
            )

        writer.assert_not_called()

    def test_shadow_on_writer_none_resolves_ops_writer(self) -> None:
        fake_insert = MagicMock()
        fake_ops_logger = types.ModuleType("ops_logger")
        fake_ops_logger.insert_ops_event = fake_insert
        created_sink = object()

        with patch.dict(sys.modules, {"ops_logger": fake_ops_logger}), patch.object(
            snh, "create_fast_safety_ops_sink", return_value=created_sink
        ) as sink_factory_mock, patch.object(
            snh, LIFECYCLE_WRAPPER_NAME, return_value={"status": "ok"}
        ):
            snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_ops_writer=None,
            )

        sink_factory_mock.assert_called_once_with(fake_insert)
        fake_insert.assert_not_called()

    def test_ops_logger_import_failure_isolated(self) -> None:
        expected = {"status": "import-failed"}

        with patch.dict(sys.modules, {"ops_logger": None}), patch.object(
            snh, "create_fast_safety_ops_sink"
        ) as sink_factory_mock, patch.object(
            snh, LIFECYCLE_WRAPPER_NAME, return_value=expected
        ) as lifecycle_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_ops_writer=None,
            )

        sink_factory_mock.assert_not_called()
        lifecycle_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=True,
            fast_safety_audit_sink=None,
        )
        self.assertIs(result, expected)

    def test_non_callable_injected_writer(self) -> None:
        invalid_writers = (0, False, "writer", object())

        for writer in invalid_writers:
            with self.subTest(writer=writer):
                real_import = builtins.__import__

                def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
                    if name == "ops_logger":
                        raise AssertionError(
                            "lazy ops_logger import must not run for non-callable writer"
                        )
                    return real_import(name, globals, locals, fromlist, level)

                with patch(
                    "builtins.__import__", side_effect=guarded_import
                ), patch.object(
                    snh, "create_fast_safety_ops_sink"
                ) as sink_factory_mock, patch.object(
                    snh, LIFECYCLE_WRAPPER_NAME, return_value={"status": "ok"}
                ) as lifecycle_mock:
                    snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                        "KR",
                        fast_safety_shadow_enabled=True,
                        fast_safety_ops_writer=writer,
                    )

                sink_factory_mock.assert_not_called()
                lifecycle_mock.assert_called_once_with(
                    "KR",
                    fast_safety_shadow_enabled=True,
                    fast_safety_audit_sink=None,
                )

    def test_sink_factory_returns_none(self) -> None:
        expected = {"status": "sink-none"}
        writer = MagicMock()

        with patch.object(
            snh, "create_fast_safety_ops_sink", return_value=None
        ), patch.object(
            snh, LIFECYCLE_WRAPPER_NAME, return_value=expected
        ) as lifecycle_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_ops_writer=writer,
            )

        lifecycle_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=True,
            fast_safety_audit_sink=None,
        )
        self.assertIs(result, expected)

    def test_sink_factory_exception_isolated(self) -> None:
        expected = {"status": "sink-error"}
        writer = MagicMock()

        with patch.object(
            snh,
            "create_fast_safety_ops_sink",
            side_effect=RuntimeError("sink factory failed"),
        ), patch.object(
            snh, LIFECYCLE_WRAPPER_NAME, return_value=expected
        ) as lifecycle_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                "KR",
                fast_safety_shadow_enabled=True,
                fast_safety_ops_writer=writer,
            )

        lifecycle_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=True,
            fast_safety_audit_sink=None,
        )
        self.assertIs(result, expected)

    def test_lifecycle_return_value_preserved(self) -> None:
        return_values = (None, {"status": "ok"}, {"status": "MARKET_CLOSED"}, _RETURN_SENTINEL)

        for expected in return_values:
            with self.subTest(expected=expected):
                with patch.object(
                    snh, LIFECYCLE_WRAPPER_NAME, return_value=expected
                ):
                    result = snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                        "KR",
                        fast_safety_shadow_enabled=False,
                    )
                self.assertIs(result, expected)

    def test_lifecycle_exception_preserved(self) -> None:
        class FixedError(RuntimeError):
            pass

        err = FixedError("lifecycle failed")

        with patch.object(
            snh, LIFECYCLE_WRAPPER_NAME, side_effect=err
        ):
            with self.assertRaises(FixedError) as ctx:
                snh.execute_supernova_live_scan_with_fast_safety_ops_audit(
                    "KR",
                    fast_safety_shadow_enabled=False,
                )

        self.assertIs(ctx.exception, err)

    def test_production_kr_us_use_ops_wrapper_explicit_off(self) -> None:
        factory_source = inspect.getsource(factory_pipelines)
        factory_tree = ast.parse(factory_source)
        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)
        scheduler_tree = ast.parse(scheduler_source)

        production_fns = (
            _function_def(factory_source, "_step_supernova_kr"),
            _function_def(factory_source, "_step_supernova_us"),
            _function_def(scheduler_source, "run_live_sniper_scheduler"),
        )

        all_ops_calls: list[ast.Call] = []
        for fn_node in production_fns:
            all_ops_calls.extend(_ops_wrapper_calls(fn_node))

        self.assertGreaterEqual(len(all_ops_calls), 4)
        for call in all_ops_calls:
            with self.subTest(call=ast.unparse(call)):
                _assert_ops_wrapper_explicit_off(call)

        kr_calls = [
            call
            for call in all_ops_calls
            if _market_arg(call) == "KR"
        ]
        us_calls = [
            call
            for call in all_ops_calls
            if _market_arg(call) == "US"
        ]
        self.assertGreaterEqual(len(kr_calls), 2)
        self.assertGreaterEqual(len(us_calls), 2)

        for fn_node in production_fns:
            self.assertEqual(
                _lifecycle_wrapper_calls(fn_node),
                [],
                msg="production callers must not invoke lifecycle wrapper directly",
            )

    def test_production_off_trade_boundary_unchanged(self) -> None:
        factory_source = inspect.getsource(factory_pipelines)
        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)
        ops_wrapper_source = inspect.getsource(
            snh.execute_supernova_live_scan_with_fast_safety_ops_audit
        )

        for fn_name in ("_step_supernova_kr", "_step_supernova_us"):
            fn_source = inspect.getsource(getattr(factory_pipelines, fn_name))
            self.assertNotIn("ops_logger", fn_source)
            fn_node = _function_def(fn_source, fn_name)
            self.assertEqual(_direct_scan_calls(fn_node), [])
            self.assertEqual(_lifecycle_wrapper_calls(fn_node), [])

        scheduler_fn = _function_def(scheduler_source, "run_live_sniper_scheduler")
        self.assertEqual(_direct_scan_calls(scheduler_fn), [])
        self.assertEqual(_lifecycle_wrapper_calls(scheduler_fn), [])
        for call in _ops_wrapper_calls(scheduler_fn):
            self.assertNotIn("ops_logger", ast.unparse(call))

        ops_wrapper_tree = ast.parse(ops_wrapper_source)
        ops_wrapper_calls = [
            node
            for node in ast.walk(ops_wrapper_tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]
        call_names = [node.func.id for node in ops_wrapper_calls]
        self.assertIn("create_fast_safety_ops_sink", call_names)
        self.assertIn(LIFECYCLE_WRAPPER_NAME, call_names)
        self.assertNotIn(SCAN_NAME, call_names)
        self.assertNotIn("create_fast_safety_audit_runtime", call_names)
        self.assertNotIn("drain_fast_safety_audit_runtime", call_names)

        diff_text = subprocess.run(
            ["git", "diff", "--", "factory_pipelines.py", "supernova_hunter.py"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        ).stdout or ""

        for symbol in TRADE_BOUNDARY_SYMBOLS:
            self.assertNotIn(
                f"+{symbol}",
                diff_text,
                msg=f"trade boundary symbol must not change: {symbol}",
            )

        added_lines = [
            line[1:]
            for line in diff_text.splitlines()
            if line.startswith("+") and not line.startswith("+++")
        ]
        joined_added = "\n".join(added_lines)
        for pattern in FORBIDDEN_ACTIVATION_PATTERNS:
            if pattern == "os.environ":
                if "os.environ" in joined_added and "FACTORY_SCAN_OWNER" not in joined_added:
                    self.fail(f"forbidden activation source in diff: {pattern}")
                continue
            self.assertNotIn(pattern, joined_added, msg=f"forbidden pattern: {pattern}")

        chapter_diff_names = subprocess.run(
            [
                "git",
                "diff",
                "--name-only",
                "--",
                "factory_pipelines.py",
                "supernova_hunter.py",
                "test_fast_safety_supernova_ops_writer_integration.py",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        ).stdout or ""
        chapter_changed = {
            line.strip()
            for line in chapter_diff_names.splitlines()
            if line.strip()
        }

        for immutable in IMMUTABLE_FILES:
            self.assertNotIn(
                immutable,
                chapter_changed,
                msg=f"chapter diff must not touch {immutable}",
            )


if __name__ == "__main__":
    unittest.main()
