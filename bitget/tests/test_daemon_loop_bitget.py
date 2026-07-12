"""Bitget daemon loop runtime — unit tests."""
from __future__ import annotations

from datetime import datetime, timezone

from bitget.infra.daemon_loop import (
    LoopDedup,
    ReusableBuffer,
    UtcTick,
    collect_due_timeframes,
    satellite_flag_once,
)


def _is_close(h: int, m: int, tf: str) -> bool:
    if tf == "1H":
        return m == 0
    if tf == "2H":
        return m == 0 and h % 2 == 0
    if tf == "4H":
        return m == 0 and h % 4 == 0
    if tf == "1D":
        return m == 0 and h == 0
    return False


def test_utc_tick_refresh_truncates_minute():
    from bitget.infra.clock import utc_date_key, utc_hm_key

    tick = UtcTick()
    tick.refresh(truncate_minute=True)
    assert tick.now.second == 0
    assert tick.now.microsecond == 0
    assert tick.hour == tick.now.hour
    assert tick.minute == tick.now.minute
    assert tick.day_key == utc_date_key(anchor=tick.now)
    assert tick.hm_key == utc_hm_key(anchor=tick.now)


def test_utc_tick_refresh_uses_clock_ssot():
    import inspect

    from bitget.infra import daemon_loop

    src = inspect.getsource(daemon_loop.UtcTick.refresh)
    assert "utc_now()" in src
    assert "utc_date_key" in src
    assert "utc_hm_key" in src
    assert "datetime.now(timezone.utc)" not in src


def test_loop_dedup_once_semantics():
    dedup = LoopDedup()
    assert dedup.hm_once("2026-07-11 12:00") is True
    assert dedup.hm_once("2026-07-11 12:00") is False
    assert dedup.hm_once("2026-07-11 12:01") is True
    assert dedup.day_once("2026-07-11") is True
    assert dedup.day_once("2026-07-11") is False


def test_reusable_buffer_collect_due_timeframes():
    buf = ReusableBuffer()
    tfs = ("1D", "4H", "2H", "1H")
    collect_due_timeframes(4, 0, tfs, buf, is_close_fn=_is_close)
    assert buf.items == ["4H", "2H", "1H"]
    assert buf.join("|") == "4H|2H|1H"
    buf.clear()
    assert not buf


def test_satellite_flag_once_reuses_dict():
    flags: dict[str, str] = {}
    assert satellite_flag_once(flags, "sentiment", "2026-07-11 10:10") is True
    assert satellite_flag_once(flags, "sentiment", "2026-07-11 10:10") is False
    assert flags["sentiment"] == "2026-07-11 10:10"


def test_daemon_loop_frame_bundles_state():
    from bitget.infra.daemon_loop import DaemonLoopFrame

    frame = DaemonLoopFrame()
    frame.refresh_utc()
    assert frame.tick.hour == frame.tick.now.hour
    frame.mark_error()
    assert frame.loop_error is True
    frame.mark_ok()
    assert frame.loop_error is False


def test_reusable_dict_payload_clears_extra():
    from bitget.infra.daemon_loop import ReusableDictPayload

    payload = ReusableDictPayload(status="idle")
    payload.fill_with_extra(extra={"task_id": 1}, status="running")
    assert payload.data["task_id"] == 1
    payload.fill_with_extra(status="idle")
    assert "task_id" not in payload.data
    assert payload.data["status"] == "idle"
