"""Production OFF gate tests for Supernova lifecycle wrapper call sites (Chapter 3-B0D3A4D)."""

from __future__ import annotations

import ast
import inspect
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault(
    "auto_forward_tester",
    types.ModuleType("auto_forward_tester"),
)

import factory_pipelines
import supernova_hunter as snh

REPO_ROOT = Path(__file__).resolve().parent
WRAPPER_NAME = "execute_supernova_live_scan_with_fast_safety_audit"
SCAN_NAME = "execute_supernova_live_scan"

FORBIDDEN_ACTIVATION_PATTERNS = (
    "os.getenv",
    "os.environ",
    "get_config_value",
    "ENABLE_FAST",
    "FAST_SAFETY_SHADOW",
    "FEATURE_FAST",
)

TRADE_BOUNDARY_SYMBOLS = (
    "try_add_virtual_position",
    "kelly_risk_pct",
    "invest_amount",
    "shares",
    "_insert_forward_trade_row",
    "decision.blocked",
    "final_kelly",
)

IMMUTABLE_FILES = (
    "forward/shared.py",
    "fast_safety_runtime_shadow.py",
    "fast_safety_kernel.py",
)


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


def _direct_scan_calls(fn_node: ast.FunctionDef) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(fn_node)
        if isinstance(node, ast.Call) and _call_name(node) == SCAN_NAME
    ]


def _wrapper_calls(fn_node: ast.FunctionDef) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(fn_node)
        if isinstance(node, ast.Call) and _call_name(node) == WRAPPER_NAME
    ]


def _keyword_bool_false(call: ast.Call, name: str) -> bool:
    for kw in call.keywords:
        if kw.arg != name:
            continue
        if isinstance(kw.value, ast.Constant) and kw.value.value is False:
            return True
    return False


def _keyword_none(call: ast.Call, name: str) -> bool:
    for kw in call.keywords:
        if kw.arg != name:
            continue
        if isinstance(kw.value, ast.Constant) and kw.value.value is None:
            return True
    return False


def _market_arg(call: ast.Call) -> str | None:
    if not call.args:
        return None
    first = call.args[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    return None


def _assert_wrapper_explicit_off(call: ast.Call) -> None:
    assert _keyword_bool_false(call, "fast_safety_shadow_enabled"), (
        "fast_safety_shadow_enabled=False must be explicit"
    )
    assert _keyword_none(call, "fast_safety_audit_sink"), (
        "fast_safety_audit_sink=None must be explicit"
    )


def _git_diff_added_lines(*paths: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    stdout = result.stdout or ""
    return [
        line[1:]
        for line in stdout.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]


class FastSafetySupernovaProductionOffGateTests(unittest.TestCase):
    def test_kr_factory_uses_wrapper(self) -> None:
        with patch.object(
            factory_pipelines, "_require_market_session_for_scan"
        ), patch.object(
            snh, WRAPPER_NAME
        ) as wrapper_mock:
            factory_pipelines._step_supernova_kr()

        wrapper_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=False,
            fast_safety_audit_sink=None,
        )

    def test_us_factory_uses_wrapper(self) -> None:
        with patch.object(
            factory_pipelines, "_require_market_session_for_scan"
        ), patch.object(
            snh, WRAPPER_NAME
        ) as wrapper_mock:
            factory_pipelines._step_supernova_us()

        wrapper_mock.assert_called_once_with(
            "US",
            fast_safety_shadow_enabled=False,
            fast_safety_audit_sink=None,
        )

    def test_kr_factory_no_direct_scan(self) -> None:
        source = inspect.getsource(factory_pipelines._step_supernova_kr)
        fn_node = _function_def(source, "_step_supernova_kr")
        self.assertEqual(_direct_scan_calls(fn_node), [])

    def test_us_factory_no_direct_scan(self) -> None:
        source = inspect.getsource(factory_pipelines._step_supernova_us)
        fn_node = _function_def(source, "_step_supernova_us")
        self.assertEqual(_direct_scan_calls(fn_node), [])

    def test_daemon_kr_branch_uses_wrapper(self) -> None:
        source = inspect.getsource(snh.run_live_sniper_scheduler)
        fn_node = _function_def(source, "run_live_sniper_scheduler")
        kr_calls = [
            call
            for call in _wrapper_calls(fn_node)
            if _market_arg(call) == "KR"
        ]
        self.assertEqual(len(kr_calls), 1)
        _assert_wrapper_explicit_off(kr_calls[0])

    def test_daemon_us_branch_uses_wrapper(self) -> None:
        source = inspect.getsource(snh.run_live_sniper_scheduler)
        fn_node = _function_def(source, "run_live_sniper_scheduler")
        us_calls = [
            call
            for call in _wrapper_calls(fn_node)
            if _market_arg(call) == "US"
        ]
        self.assertEqual(len(us_calls), 1)
        _assert_wrapper_explicit_off(us_calls[0])

    def test_daemon_no_direct_scan(self) -> None:
        source = inspect.getsource(snh.run_live_sniper_scheduler)
        fn_node = _function_def(source, "run_live_sniper_scheduler")
        self.assertEqual(_direct_scan_calls(fn_node), [])

    def test_all_production_wrapper_calls_explicit_off(self) -> None:
        factory_source = inspect.getsource(factory_pipelines)
        factory_tree = ast.parse(factory_source)
        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)
        scheduler_tree = ast.parse(scheduler_source)

        production_fns = (
            _function_def(factory_source, "_step_supernova_kr"),
            _function_def(factory_source, "_step_supernova_us"),
            _function_def(scheduler_source, "run_live_sniper_scheduler"),
        )

        all_wrapper_calls: list[ast.Call] = []
        for fn_node in production_fns:
            all_wrapper_calls.extend(_wrapper_calls(fn_node))

        self.assertGreaterEqual(len(all_wrapper_calls), 4)
        for call in all_wrapper_calls:
            with self.subTest(call=ast.unparse(call)):
                _assert_wrapper_explicit_off(call)

    def test_no_unapproved_activation_source(self) -> None:
        added_lines = _git_diff_added_lines(
            "factory_pipelines.py",
            "supernova_hunter.py",
            "test_fast_safety_supernova_production_off_gate.py",
        )
        joined = "\n".join(added_lines)
        for pattern in FORBIDDEN_ACTIVATION_PATTERNS:
            if pattern == "os.environ":
                if "os.environ" in joined and "FACTORY_SCAN_OWNER" not in joined:
                    self.fail(f"forbidden activation source in diff: {pattern}")
                continue
            self.assertNotIn(pattern, joined, msg=f"forbidden pattern: {pattern}")

        for fn_name in ("_step_supernova_kr", "_step_supernova_us"):
            fn_source = inspect.getsource(getattr(factory_pipelines, fn_name))
            self.assertNotIn("load_system_config", fn_source)

        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)
        for pattern in ("get_config_value", "load_system_config", "ENABLE_FAST", "FAST_SAFETY_SHADOW"):
            self.assertNotIn(pattern, scheduler_source)

    def test_wrapper_off_execution_no_runtime(self) -> None:
        expected = {"status": "production-off"}

        with patch.object(snh, "create_fast_safety_audit_runtime") as create_mock, patch.object(
            snh, "drain_fast_safety_audit_runtime"
        ) as drain_mock, patch.object(
            snh, SCAN_NAME, return_value=expected
        ) as scan_mock:
            result = snh.execute_supernova_live_scan_with_fast_safety_audit(
                "KR",
                fast_safety_shadow_enabled=False,
                fast_safety_audit_sink=None,
            )

        create_mock.assert_not_called()
        drain_mock.assert_not_called()
        scan_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=False,
            fast_safety_audit_emitter=None,
        )
        self.assertIs(result, expected)

    def test_scan_and_wrapper_body_unchanged(self) -> None:
        scan_source = inspect.getsource(snh.execute_supernova_live_scan)
        for forbidden in (
            "create_fast_safety_audit_runtime",
            "drain_fast_safety_audit_runtime",
            "get_config_value",
            "os.getenv",
            "FAST_SAFETY_SHADOW",
        ):
            self.assertNotIn(forbidden, scan_source)

        wrapper_source = inspect.getsource(
            snh.execute_supernova_live_scan_with_fast_safety_audit
        )
        wrapper_tree = ast.parse(wrapper_source)
        for node in ast.walk(wrapper_tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                self.assertNotIn(
                    node.func.id,
                    {"get_config_value", "getenv"},
                    msg="wrapper must not read config/environment",
                )

        wrapper_calls = [
            node
            for node in ast.walk(wrapper_tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]
        call_names = [node.func.id for node in wrapper_calls]
        self.assertEqual(call_names.count(SCAN_NAME), 1)
        self.assertEqual(call_names.count("create_fast_safety_audit_runtime"), 1)
        self.assertEqual(call_names.count("drain_fast_safety_audit_runtime"), 1)

    def test_production_trade_boundary_unchanged(self) -> None:
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

        chapter_diff_names = subprocess.run(
            [
                "git",
                "diff",
                "--name-only",
                "--",
                "factory_pipelines.py",
                "supernova_hunter.py",
                "test_fast_safety_supernova_production_off_gate.py",
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
        self.assertIn("factory_pipelines.py", chapter_changed)
        self.assertIn("supernova_hunter.py", chapter_changed)

        for immutable in IMMUTABLE_FILES:
            self.assertNotIn(
                immutable,
                chapter_changed,
                msg=f"chapter diff must not touch {immutable}",
            )

        status = subprocess.run(
            [
                "git",
                "status",
                "--short",
                "fast_safety_runtime_shadow.py",
                "fast_safety_kernel.py",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        ).stdout or ""
        self.assertEqual(status.strip(), "")


if __name__ == "__main__":
    unittest.main()
