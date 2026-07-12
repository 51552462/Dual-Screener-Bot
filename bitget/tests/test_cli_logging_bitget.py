"""CLI / validation / deploy / ops scripts — print→logging SSOT audit."""
from __future__ import annotations

import inspect
from pathlib import Path


def test_validation_runner_no_print():
    from bitget.validation import runner as vr

    src = inspect.getsource(vr)
    assert "print(" not in src
    assert "get_logger" in src


def test_institutional_db_backup_no_print():
    from bitget.scripts import institutional_db_backup as idb

    src = inspect.getsource(idb)
    assert "print(" not in src
    assert "log_exception" in src
    assert "get_logger" in src


def test_generate_bitget_crontab_no_print():
    path = Path(__file__).resolve().parents[1] / "deploy" / "generate_bitget_crontab.py"
    src = path.read_text(encoding="utf-8")
    assert "print(" not in src
    assert "get_logger" in src


def test_one_shot_scripts_no_print():
    root = Path(__file__).resolve().parents[1] / "scripts"
    for name in (
        "split_forward_core.py",
        "split_forward_physical.py",
        "resolve_merge_conflicts.py",
    ):
        src = (root / name).read_text(encoding="utf-8")
        assert "print(" not in src, name
        assert "get_logger" in src, name
