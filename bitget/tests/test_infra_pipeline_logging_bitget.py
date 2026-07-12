"""Infra / pipeline / operator entrypoints — print→logging SSOT audit."""
from __future__ import annotations

import ast
import inspect
from pathlib import Path


def test_manual_report_trigger_parses_and_no_print():
    path = Path(__file__).resolve().parents[1] / "manual_report_trigger.py"
    src = path.read_text(encoding="utf-8")
    ast.parse(src)
    assert "print(" not in src
    assert "log_exception" in src
    assert "import bitget.auto_pilot as bap" in src
    assert "as bitget_auto_pilot as" not in src


def test_bitget_pipelines_no_print():
    from bitget.pipelines import bitget_pipelines as bp

    src = inspect.getsource(bp)
    assert "print(" not in src
    assert "log_exception" in src
    assert "get_logger" in src


def test_pipelines_runner_no_print():
    from bitget.pipelines import runner as rn

    src = inspect.getsource(rn)
    assert "print(" not in src
    assert "get_logger" in src


def test_config_manager_no_print():
    from bitget.infra import config_manager as cm

    src = inspect.getsource(cm)
    assert "print(" not in src
    assert "log_exception" in src


def test_snapshot_service_no_print():
    from bitget.infra import snapshot_service as ss

    src = inspect.getsource(ss)
    assert "print(" not in src
    assert "log_exception" in src


def test_artifact_guard_no_print():
    from bitget.infra import artifact_guard as ag

    src = inspect.getsource(ag)
    assert "print(" not in src
    assert "get_logger" in src


def test_meta_alerts_no_print():
    from bitget.governance import meta_alerts as ma

    src = inspect.getsource(ma)
    assert "print(" not in src
    assert "log_exception" in src


def test_report_pipeline_hydrate_no_print():
    from bitget import report_pipeline_hydrate as rph

    src = inspect.getsource(rph)
    assert "print(" not in src
    assert "get_logger" in src


def test_async_telegram_daemon_no_print():
    from bitget import async_telegram_daemon as atd

    src = inspect.getsource(atd)
    assert "print(" not in src
    assert "get_logger" in src
