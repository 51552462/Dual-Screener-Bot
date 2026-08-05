"""SQLite writer flock SSOT — single lock for data_refresh + scan/track."""
from __future__ import annotations

from unittest import mock


def test_job_lock_path_is_unified_for_all_modes():
    from bitget.infra.data_paths import data_refresh_lock_path, job_lock_path, runtime_lock_path

    rt = runtime_lock_path()
    assert job_lock_path("data_refresh") == rt
    assert job_lock_path("scan_spot_dante") == rt
    assert job_lock_path("track_positions") == rt
    assert data_refresh_lock_path() != rt


def test_active_bitget_job_lock_detects_live_runtime_holder(tmp_path):
    from bitget.infra import runtime as rt

    lock = tmp_path / ".bitget_runtime.lock"
    lock.write_text("data_refresh\n2026-08-05T00:00:00+09:00\n4242\n", encoding="utf-8")

    with mock.patch.object(rt, "runtime_lock_path", return_value=str(lock)), mock.patch.object(
        rt, "data_refresh_lock_path", return_value=str(tmp_path / ".bitget_data_refresh.lock")
    ), mock.patch.object(rt, "_pid_is_alive", return_value=True):
        meta = rt.active_bitget_job_lock()

    assert meta is not None
    assert meta.mode == "data_refresh"
    assert meta.pid == 4242


def test_is_bitget_db_writer_active_false_when_no_lock(tmp_path):
    from bitget.infra import runtime as rt

    with mock.patch.object(rt, "runtime_lock_path", return_value=str(tmp_path / "missing.lock")), mock.patch.object(
        rt, "data_refresh_lock_path", return_value=str(tmp_path / "missing2.lock")
    ):
        assert rt.is_bitget_db_writer_active() is False
