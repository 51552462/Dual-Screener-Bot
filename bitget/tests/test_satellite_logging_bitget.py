"""Satellite modules — print→logging SSOT audit."""
from __future__ import annotations

import inspect


def test_toxic_graveyard_no_print():
    from bitget import toxic_graveyard_analyzer as tga

    src = inspect.getsource(tga)
    assert "print(" not in src
    assert "log_exception" in src


def test_time_machine_no_print():
    from bitget import time_machine_backtester as tmb

    src = inspect.getsource(tmb)
    assert "print(" not in src
    assert "get_logger" in src


def test_synthetic_data_no_print():
    from bitget import synthetic_data_generator as sdg

    src = inspect.getsource(sdg)
    assert "print(" not in src
    assert "get_logger" in src


def test_shadow_performance_tracker_no_print():
    from bitget import shadow_performance_tracker as spt

    src = inspect.getsource(spt)
    assert "print(" not in src
    assert "get_logger" in src
