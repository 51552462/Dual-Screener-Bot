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
OPS_WRAPPER_NAME = "execute_supernova_live_scan_with_fast_safety_ops_audit"
LIFECYCLE_WRAPPER_NAME = "execute_supernova_live_scan_with_fast_safety_audit"
SCAN_NAME = "execute_supernova_live_scan"
READER_NAME = "resolve_fast_safety_shadow_enabled"
ACTIVATION_KEY_STRINGS = (
    "FAST_SAFETY_SHADOW_KR",
    "FAST_SAFETY_SHADOW_US",
)

FORBIDDEN_ACTIVATION_PATTERNS = (
    "os.getenv",
    "os.environ",
    "get_config_value",
    "ENABLE_FAST",
    "FAST_SAFETY_SHADOW_KR",
    "FAST_SAFETY_SHADOW_US",
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
        if isinstance(node, ast.Call) and _call_name(node) == OPS_WRAPPER_NAME
    ]


def _keyword_bool_true(call: ast.Call, name: str) -> bool:
    for kw in call.keywords:
        if kw.arg != name:
            continue
        if isinstance(kw.value, ast.Constant) and kw.value.value is True:
            return True
    return False


def _reader_call_market(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call) or _call_name(node) != READER_NAME:
        return None
    if not node.args:
        return None
    first = node.args[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    return None


def _assert_ops_wrapper_uses_reader(call: ast.Call, *, expected_market: str) -> None:
    assert not _keyword_bool_true(call, "fast_safety_shadow_enabled"), (
        "fast_safety_shadow_enabled=True literal is forbidden at production call sites"
    )
    for kw in call.keywords:
        if kw.arg == "fast_safety_shadow_enabled":
            market = _reader_call_market(kw.value)
            assert market == expected_market, (
                f"expected resolve_fast_safety_shadow_enabled({expected_market!r})"
            )
            return
        if kw.arg in ("fast_safety_ops_writer", "fast_safety_audit_sink"):
            raise AssertionError(
                f"production call must not pass {kw.arg!r}"
            )
    raise AssertionError("fast_safety_shadow_enabled must use activation reader")


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
    _assert_ops_wrapper_uses_reader(
        call,
        expected_market=_market_arg(call) or "",
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


def _activation_kw_text(call: ast.Call) -> str:
    for kw in call.keywords:
        if kw.arg == "fast_safety_shadow_enabled":
            return ast.unparse(kw.value)
    return ""


def _assert_production_call_site_trade_boundary(
    fn_node: ast.FunctionDef,
    *,
    expected_market: str,
    fn_source: str,
    check_fn_source_for_activation_reads: bool,
) -> None:
    ops_calls = [
        call
        for call in _wrapper_calls(fn_node)
        if _market_arg(call) == expected_market
    ]
    if not ops_calls:
        raise AssertionError(
            f"expected at least one {OPS_WRAPPER_NAME} call for {expected_market}"
        )

    for call in ops_calls:
        _assert_ops_wrapper_uses_reader(call, expected_market=expected_market)
        call_text = ast.unparse(call)
        activation_text = _activation_kw_text(call)
        for symbol in TRADE_BOUNDARY_SYMBOLS:
            if symbol in call_text:
                raise AssertionError(
                    f"trade boundary symbol {symbol!r} in ops wrapper call"
                )
            if symbol in activation_text:
                raise AssertionError(
                    f"trade boundary symbol {symbol!r} in activation expression"
                )
        for forbidden in ("get_config_value", "os.getenv", "os.environ"):
            if forbidden in activation_text:
                raise AssertionError(
                    f"forbidden activation read {forbidden!r} in activation expression"
                )

    if check_fn_source_for_activation_reads:
        for forbidden in ("get_config_value", "os.getenv", "os.environ"):
            if forbidden in fn_source:
                raise AssertionError(
                    f"forbidden activation read {forbidden!r} in call-site function"
                )
        for key in ACTIVATION_KEY_STRINGS:
            if key in fn_source:
                raise AssertionError(
                    f"activation key {key!r} must not appear outside reader module"
                )
        if READER_NAME not in fn_source:
            raise AssertionError(
                f"call site must invoke {READER_NAME!r}"
            )


class FastSafetySupernovaProductionOffGateTests(unittest.TestCase):
    def test_kr_factory_uses_ops_wrapper_with_reader(self) -> None:
        with patch.object(
            factory_pipelines, "_require_market_session_for_scan"
        ), patch(
            "fast_safety_shadow_activation.resolve_fast_safety_shadow_enabled",
            return_value=False,
        ) as reader_mock, patch.object(
            snh, OPS_WRAPPER_NAME
        ) as wrapper_mock:
            factory_pipelines._step_supernova_kr()

        reader_mock.assert_called_once_with("KR")
        wrapper_mock.assert_called_once_with(
            "KR",
            fast_safety_shadow_enabled=False,
        )

    def test_us_factory_uses_ops_wrapper_with_reader(self) -> None:
        with patch.object(
            factory_pipelines, "_require_market_session_for_scan"
        ), patch(
            "fast_safety_shadow_activation.resolve_fast_safety_shadow_enabled",
            return_value=False,
        ) as reader_mock, patch.object(
            snh, OPS_WRAPPER_NAME
        ) as wrapper_mock:
            factory_pipelines._step_supernova_us()

        reader_mock.assert_called_once_with("US")
        wrapper_mock.assert_called_once_with(
            "US",
            fast_safety_shadow_enabled=False,
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

    def test_all_production_ops_wrapper_calls_use_reader(self) -> None:
        factory_source = inspect.getsource(factory_pipelines)
        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)

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
            market = _market_arg(call)
            with self.subTest(call=ast.unparse(call), market=market):
                assert market in ("KR", "US")
                _assert_ops_wrapper_uses_reader(call, expected_market=market)

    def test_no_unapproved_activation_source(self) -> None:
        call_site_added = _git_diff_added_lines(
            "factory_pipelines.py",
            "supernova_hunter.py",
        )
        joined_call_sites = "\n".join(call_site_added)
        for pattern in FORBIDDEN_ACTIVATION_PATTERNS:
            if pattern == "os.environ":
                if (
                    "os.environ" in joined_call_sites
                    and "FACTORY_SCAN_OWNER" not in joined_call_sites
                ):
                    self.fail(f"forbidden activation source in diff: {pattern}")
                continue
            self.assertNotIn(
                pattern,
                joined_call_sites,
                msg=f"forbidden pattern in call-site diff: {pattern}",
            )
        self.assertNotIn(
            "fast_safety_shadow_enabled=True",
            joined_call_sites,
            msg="literal production shadow ON forbidden in diff",
        )

        for fn_name in ("_step_supernova_kr", "_step_supernova_us"):
            fn_source = inspect.getsource(getattr(factory_pipelines, fn_name))
            self.assertNotIn("load_system_config", fn_source)
            self.assertNotIn("get_config_value", fn_source)
            for key in ACTIVATION_KEY_STRINGS:
                self.assertNotIn(key, fn_source)
            self.assertIn(READER_NAME, fn_source)

        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)
        for pattern in ("get_config_value", "load_system_config", "ENABLE_FAST"):
            self.assertNotIn(pattern, scheduler_source)
        for key in ACTIVATION_KEY_STRINGS:
            self.assertNotIn(key, scheduler_source)
        self.assertIn(READER_NAME, scheduler_source)

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

        if diff_text.strip():
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
                    "fast_safety_shadow_activation.py",
                    "test_fast_safety_shadow_activation.py",
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
            for immutable in IMMUTABLE_FILES:
                self.assertNotIn(
                    immutable,
                    chapter_changed,
                    msg=f"chapter diff must not touch {immutable}",
                )

        reader_path = REPO_ROOT / "fast_safety_shadow_activation.py"
        self.assertTrue(
            reader_path.is_file(),
            msg="fast_safety_shadow_activation.py must exist",
        )

        for fn_name, market in (
            ("_step_supernova_kr", "KR"),
            ("_step_supernova_us", "US"),
        ):
            fn_source = inspect.getsource(getattr(factory_pipelines, fn_name))
            fn_node = _function_def(fn_source, fn_name)
            self.assertEqual(_direct_scan_calls(fn_node), [])
            _assert_production_call_site_trade_boundary(
                fn_node,
                expected_market=market,
                fn_source=fn_source,
                check_fn_source_for_activation_reads=True,
            )

        scheduler_source = inspect.getsource(snh.run_live_sniper_scheduler)
        scheduler_fn = _function_def(scheduler_source, "run_live_sniper_scheduler")
        self.assertEqual(_direct_scan_calls(scheduler_fn), [])
        self.assertIn(READER_NAME, scheduler_source)
        for market in ("KR", "US"):
            _assert_production_call_site_trade_boundary(
                scheduler_fn,
                expected_market=market,
                fn_source=scheduler_source,
                check_fn_source_for_activation_reads=False,
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
