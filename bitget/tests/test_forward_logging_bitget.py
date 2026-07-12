"""forward hot-path — print→logging SSOT audit."""
from __future__ import annotations

import inspect


def test_forward_ledger_no_print():
    from bitget.forward import ledger as lg

    src = inspect.getsource(lg)
    assert "print(" not in src
    assert "log_exception" in src
    assert "get_logger" in src


def test_forward_reports_no_print():
    from bitget.forward import reports as rp

    src = inspect.getsource(rp)
    assert "print(" not in src
    assert "log_exception" in src
    assert "get_logger" in src
