"""bitget validation + ops panel Clock SSOT + bounded reads."""
from __future__ import annotations

from unittest import mock


def test_synthetic_data_generator_module_uses_clock_ssot():
    import inspect

    from bitget import synthetic_data_generator as sdg

    src = inspect.getsource(sdg)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(" not in src
    assert "utc_datetime_str" in src


def test_dashboard_ops_panel_module_uses_clock_ssot():
    import inspect

    from bitget import dashboard_ops_panel as dop

    src = inspect.getsource(dop)
    assert "datetime.now(" not in src
    assert "utc_now" in src
    assert "parse_utc_iso" in src


def test_weekly_action_plan_module_uses_clock_ssot():
    import inspect

    from bitget import weekly_action_plan as wap

    src = inspect.getsource(wap)
    assert "datetime.now(" not in src
    assert "utc_hm_key" in src


def test_validation_signal_parity_module_uses_clock_ssot():
    import inspect

    from bitget.validation import signal_parity as sp

    src = inspect.getsource(sp)
    assert "datetime.now(" not in src
    assert "utc_datetime_str_tz" in src


def test_validation_pnl_parity_module_uses_clock_ssot():
    import inspect

    from bitget.validation import pnl_parity as pp

    src = inspect.getsource(pp)
    assert "datetime.now(" not in src
    assert "utc_datetime_str_tz" in src


def test_validation_cutover_module_uses_clock_ssot():
    import inspect

    from bitget.validation import cutover as co

    src = inspect.getsource(co)
    assert "datetime.now(" not in src
    assert "utc_now_iso" in src
    assert "parse_utc_iso" in src


def test_toxic_graveyard_analyzer_module_uses_clock_and_bounded_read():
    import inspect

    from bitget import toxic_graveyard_analyzer as tga

    src = inspect.getsource(tga)
    assert "datetime.now()" not in src
    assert "utc_date_str" in src
    assert "forward_toxic_graveyard_closed_sql" in src


def test_practitioner_bitget_adapter_module_uses_clock_ssot():
    import inspect

    from bitget.forward import practitioner_bitget_adapter as pba

    src = inspect.getsource(pba)
    assert "datetime.now(" not in src
    assert "utc_date_str" in src


def test_bitget_schedule_guard_delegates_utc_to_clock():
    import inspect

    from bitget import bitget_schedule_guard as sg

    src = inspect.getsource(sg)
    assert "def utc_now()" not in src
    assert "from bitget.infra.clock import utc_now" in src


def test_cutover_parallel_run_status_elapsed_hours():
    from bitget.validation import cutover as co

    with mock.patch.object(co, "load_parallel_state", return_value={
        "started_at_utc": "2026-07-11T10:00:00+00:00",
        "target_hours": 48,
        "mode": "pipeline",
    }), mock.patch("bitget.validation.cutover.utc_now") as mock_now:
        from datetime import datetime, timezone

        mock_now.return_value = datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
        st = co.parallel_run_status()
    assert st["active"] is True
    assert st["elapsed_hours"] == 4.0
    assert st["ready_for_cutover"] is False
