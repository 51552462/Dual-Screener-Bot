"""bitget.mtf_data_updater — production logging audit (no print, exc_info path)."""
from __future__ import annotations

import inspect

from bitget import mtf_data_updater as mtf


def test_mtf_data_updater_has_no_print():
    src = inspect.getsource(mtf)
    assert "print(" not in src


def test_mtf_data_updater_uses_log_exception():
    src = inspect.getsource(mtf)
    assert "log_exception" in src
    assert "get_logger" in src
    assert "logger.info" in src
    assert "logger.warning" in src


def test_db_lock_retry_logs_without_print(caplog):
    import logging

    calls = {"n": 0}

    def _fail_then_ok():
        calls["n"] += 1
        if calls["n"] < 2:
            raise __import__("sqlite3").OperationalError("database is locked")
        return "ok"

    with caplog.at_level(logging.WARNING):
        out = mtf._run_with_db_retry(_fail_then_ok, "unit_test_op")
    assert out == "ok"
    assert any("DB lock retry" in r.message for r in caplog.records)
