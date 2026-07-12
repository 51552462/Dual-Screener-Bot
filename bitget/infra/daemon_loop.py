"""
Bitget 24/7 daemon loop runtime — per-iteration object reuse (stock autopilot parity).

주식 `system_auto_pilot`·`async_telegram_daemon` 패턴:
  - datetime / strftime / dedup key 는 루프 밖 1회 할당 → 매 tick `.refresh()` 만
  - satellite·스케줄 플래그 dict 는 프로세스 수명 동안 유지 (매 tick 재생성 금지)
  - due_tf·heartbeat extra 등 버퍼 list/dict 는 `.clear()` 후 재사용
  - sleep interval 은 모듈 상수 SSOT — 에러 backoff 만 tick 별 분기
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from typing import Any, Callable, Optional, TypeVar

from bitget.infra.clock import utc_date_key, utc_hm_key, utc_now

# ---------------------------------------------------------------------------
# Loop interval SSOT (seconds)
# ---------------------------------------------------------------------------
DAEMON_TICK_SLEEP_SEC: float = 20.0
DAEMON_ERROR_SLEEP_SEC: float = 60.0

DISK_MGR_POLL_SEC: float = 60.0

MTF_SCHEDULER_POLL_SEC: float = 10.0
MTF_SCHEDULER_POST_SCAN_SLEEP_SEC: float = 60.0
MTF_SCHEDULER_ERROR_SEC: float = 30.0

SNIPER_SCHEDULER_POLL_SEC: float = 20.0
SNIPER_SCHEDULER_POST_SCAN_SLEEP_SEC: float = 60.0
SNIPER_SCHEDULER_ERROR_SEC: float = 60.0

OVERSEER_POLL_SEC: float = 30.0
OVERSEER_POST_AUDIT_SLEEP_SEC: float = 65.0
OVERSEER_ERROR_SEC: float = 60.0

QUEUE_WORKER_IDLE_POLL_SEC: float = 5.0
QUEUE_WORKER_ERROR_SLEEP_SEC: float = 60.0


@dataclass
class UtcTick:
    """Single UTC clock snapshot per iteration — avoids duplicate strftime/datetime."""

    now: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    hour: int = -1
    minute: int = -1
    weekday: int = -1
    day_key: str = ""
    hm_key: str = ""

    def refresh(self, *, truncate_minute: bool = False) -> UtcTick:
        self.now = utc_now()
        if truncate_minute:
            self.now = self.now.replace(second=0, microsecond=0)
        self.hour = self.now.hour
        self.minute = self.now.minute
        self.weekday = self.now.weekday()
        self.day_key = utc_date_key(anchor=self.now)
        self.hm_key = utc_hm_key(anchor=self.now)
        return self


@dataclass
class LocalTick:
    """Timezone-aware tick for KST overseer / local cron slots."""

    tz: tzinfo
    now: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    hour: int = -1
    minute: int = -1
    weekday: int = -1
    day_key: str = ""
    hm_key: str = ""

    def refresh(self) -> LocalTick:
        self.now = datetime.now(self.tz)
        self.hour = self.now.hour
        self.minute = self.now.minute
        self.weekday = self.now.weekday()
        self.day_key = self.now.strftime("%Y-%m-%d")
        self.hm_key = self.now.strftime("%Y-%m-%d %H:%M")
        return self


class LoopDedup:
    """Compare-and-set dedup keys without per-tick dict allocation."""

    __slots__ = ("_last_hm", "_last_day", "_last_trigger", "_last_track", "_last_mutant_day")

    def __init__(self) -> None:
        self._last_hm = ""
        self._last_day = ""
        self._last_trigger = ""
        self._last_track = ""
        self._last_mutant_day = ""

    def hm_once(self, hm_key: str) -> bool:
        if hm_key == self._last_hm:
            return False
        self._last_hm = hm_key
        return True

    def day_once(self, day_key: str) -> bool:
        if day_key == self._last_day:
            return False
        self._last_day = day_key
        return True

    def trigger_once(self, trigger_key: str) -> bool:
        if trigger_key == self._last_trigger:
            return False
        self._last_trigger = trigger_key
        return True

    def track_once(self, track_key: str) -> bool:
        if track_key == self._last_track:
            return False
        self._last_track = track_key
        return True

    def mutant_day_once(self, day_key: str) -> bool:
        if day_key == self._last_mutant_day:
            return False
        self._last_mutant_day = day_key
        return True

    def reset_mutant_day(self) -> None:
        self._last_mutant_day = ""


class ReusableBuffer:
    """Clear-and-fill list buffer for due_tfs, market iteration, etc."""

    __slots__ = ("_items",)

    def __init__(self) -> None:
        self._items: list[Any] = []

    def clear(self) -> None:
        self._items.clear()

    @property
    def items(self) -> list[Any]:
        return self._items

    def append(self, item: Any) -> None:
        self._items.append(item)

    def join(self, sep: str = ",") -> str:
        return sep.join(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)


def sleep_or_backoff(
    *,
    normal_sec: float,
    after_error: bool,
    error_sec: float = DAEMON_ERROR_SLEEP_SEC,
) -> None:
    time.sleep(error_sec if after_error else normal_sec)


_T = TypeVar("_T")


def satellite_flag_once(flags: dict[str, str], name: str, hm_key: str) -> bool:
    """Satellite dedup — reuse persistent flags dict, no per-tick allocation."""
    if flags.get(name) == hm_key:
        return False
    flags[name] = hm_key
    return True


def collect_due_timeframes(
    hour: int,
    minute: int,
    timeframes: tuple[str, ...],
    buf: ReusableBuffer,
    *,
    is_close_fn: Callable[[int, int, str], bool],
) -> ReusableBuffer:
    """Fill *buf* with TFs whose candle just closed — no new list per tick."""
    buf.clear()
    for tf in timeframes:
        if is_close_fn(hour, minute, tf):
            buf.append(tf)
    return buf


@dataclass
class DaemonLoopFrame:
    """
    프로세스 수명 동안 재사용하는 데몬 루프 상태 번들.

    주식 factory `system_main_loop` 의 tz/flags/dedup 을 코인 24/7 데몬에 맞게 집약.
    """

    tick: UtcTick = field(default_factory=UtcTick)
    dedup: LoopDedup = field(default_factory=LoopDedup)
    due_buf: ReusableBuffer = field(default_factory=ReusableBuffer)
    loop_error: bool = False

    def refresh_utc(self, *, truncate_minute: bool = False) -> UtcTick:
        return self.tick.refresh(truncate_minute=truncate_minute)

    def mark_ok(self) -> None:
        self.loop_error = False

    def mark_error(self) -> None:
        self.loop_error = True


class ReusableDictPayload:
    """JSON heartbeat / gauge snapshot — 매 tick dict 재생성 방지."""

    __slots__ = ("_data", "_extra_keys")

    def __init__(self, **base: Any) -> None:
        self._data: dict[str, Any] = dict(base)
        self._extra_keys: list[str] = []

    def fill(self, **fields: Any) -> dict[str, Any]:
        self._data.update(fields)
        return self._data

    def fill_with_extra(self, *, extra: Optional[dict[str, Any]] = None, **fields: Any) -> dict[str, Any]:
        for k in self._extra_keys:
            self._data.pop(k, None)
        self._extra_keys.clear()
        self.fill(**fields)
        if extra:
            self._data.update(extra)
            self._extra_keys.extend(extra.keys())
        return self._data

    @property
    def data(self) -> dict[str, Any]:
        return self._data
