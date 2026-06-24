"""
Bitget 장중 스캐너 슬롯 SSOT — cron · bitget.sh · 파이프라인 동일 순서.

SPOT / FUTURES: UTC 01:00부터 50분 간격 (24/7 · 글로벌 flock).
각 슬롯은 단일 스캐너만 실행. 1회차 supernova = full prelude, 2회차 = light.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple

PreludeKind = Literal["full", "light", "none"]
Market = Literal["SPOT", "FUTURES"]

SLOT_INTERVAL_MINUTES = 50
SLOT_START_HOUR = 1
SLOT_START_MINUTE = 0
SCAN_LOCK_WAIT_SEC = float(SLOT_INTERVAL_MINUTES * 60 + 300)

# (mode_suffix, scanner_key, human label)
_SPOT_SCANNER_ROWS: Tuple[Tuple[str, str, str], ...] = (
    ("supernova", "supernova", "슈퍼노바"),
    ("nulrim", "nulrim", "눌림목"),
    ("dante", "dante", "역매공파"),
    ("ema5", "ema5", "5일선"),
    ("master", "master", "마스터"),
    ("shadow", "shadow", "섀도우"),
)

_FUTURES_SCANNER_ROWS: Tuple[Tuple[str, str, str], ...] = (
    ("supernova", "supernova", "슈퍼노바"),
    ("nulrim", "nulrim", "눌림목"),
    ("dante", "dante", "역매공파"),
    ("ema5", "ema5", "5일선"),
    ("shadow", "shadow", "섀도우"),
)

_CYCLE2_SUFFIXES: Tuple[str, ...] = ("supernova", "nulrim", "dante", "ema5")


def _slot_clock(slot_index: int) -> Tuple[int, int]:
    base = SLOT_START_HOUR * 60 + SLOT_START_MINUTE
    total = base + int(slot_index) * SLOT_INTERVAL_MINUTES
    return total // 60, total % 60


@dataclass(frozen=True)
class ScanSlot:
    mode: str
    bitget_flag: str
    market: Market
    scanner_key: str
    label: str
    hour: int
    minute: int
    prelude: PreludeKind
    tail_doomsday: bool
    tail_track: bool
    tail_shadow: bool
    cycle: int


def _mode(market: Market, suffix: str) -> str:
    return f"scan_{market.lower()}_{suffix}"


def _flag(market: Market, suffix: str) -> str:
    return f"--scan-{market.lower()}-{suffix}"


def _build_market_slots(market: Market) -> List[ScanSlot]:
    slots: List[ScanSlot] = []
    rows = _SPOT_SCANNER_ROWS if market == "SPOT" else _FUTURES_SCANNER_ROWS
    row_map = {r[0]: r for r in rows}

    for i, suffix in enumerate(r[0] for r in rows):
        _, step_key, human = row_map[suffix]
        hour, minute = _slot_clock(i)
        slots.append(
            ScanSlot(
                mode=_mode(market, suffix),
                bitget_flag=_flag(market, suffix),
                market=market,
                scanner_key=step_key,
                label=human,
                hour=hour,
                minute=minute,
                prelude="full" if suffix == "supernova" else "none",
                tail_doomsday=(suffix == "shadow"),
                tail_track=(suffix == "shadow"),
                tail_shadow=(suffix == "shadow"),
                cycle=1,
            )
        )

    cycle2_base = len(rows)
    for i, suffix in enumerate(_CYCLE2_SUFFIXES):
        if suffix not in row_map:
            continue
        _, step_key, human = row_map[suffix]
        hour, minute = _slot_clock(cycle2_base + i)
        slots.append(
            ScanSlot(
                mode=_mode(market, suffix) + "_r2",
                bitget_flag=_flag(market, suffix) + "-r2",
                market=market,
                scanner_key=step_key,
                label=f"{human} (2회차)",
                hour=hour,
                minute=minute,
                prelude="light" if suffix == "supernova" else "none",
                tail_doomsday=False,
                tail_track=False,
                tail_shadow=False,
                cycle=2,
            )
        )
    return slots


SPOT_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("SPOT"))
FUTURES_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("FUTURES"))
ALL_SCAN_SLOTS: Tuple[ScanSlot, ...] = SPOT_SCAN_SLOTS + FUTURES_SCAN_SLOTS

STAGGERED_SCAN_MODES: Tuple[str, ...] = tuple(s.mode for s in ALL_SCAN_SLOTS)
LEGACY_SCAN_MODES: Tuple[str, ...] = ("scan_spot", "scan_futures", "scan_all")
ALL_SCAN_MODES: Tuple[str, ...] = LEGACY_SCAN_MODES + STAGGERED_SCAN_MODES

SCHEDULE_MARKET_TZ = {"SPOT": "UTC", "FUTURES": "UTC"}
SCHEDULE_WEEKDAYS = {"SPOT": "*", "FUTURES": "*"}

SCANNER_ENGINE_KEYS: dict[str, Tuple[str, ...]] = {
    "nulrim": ("NULRIM",),
    "dante": ("TV_SHORT_V1", "TV_SHORT_V2"),
    "ema5": ("EMA5",),
    "master": ("MASTER",),
}


def scan_mode_market(mode: str) -> Optional[str]:
    m = str(mode or "").strip().lower()
    if m in ("scan_spot",) or m.startswith("scan_spot_"):
        return "SPOT"
    if m in ("scan_futures", "scan_fut") or m.startswith("scan_futures_"):
        return "FUTURES"
    return None


def is_staggered_scan_mode(mode: str) -> bool:
    return str(mode or "").strip().lower() in STAGGERED_SCAN_MODES


def is_legacy_monolithic_scan_mode(mode: str) -> bool:
    return str(mode or "").strip().lower() in LEGACY_SCAN_MODES


def slot_for_mode(mode: str) -> Optional[ScanSlot]:
    key = str(mode or "").strip().lower()
    for slot in ALL_SCAN_SLOTS:
        if slot.mode == key:
            return slot
    return None


def slots_for_market(market: str) -> Tuple[ScanSlot, ...]:
    mk = str(market or "").strip().upper()
    if mk == "SPOT":
        return SPOT_SCAN_SLOTS
    if mk in ("FUTURES", "FUT"):
        return FUTURES_SCAN_SLOTS
    return ()


def resolve_lock_timeout_sec(mode: str, *, explicit: Optional[float] = None) -> float:
    if explicit is not None and explicit > 0:
        return float(explicit)
    m = str(mode or "").strip().lower()
    if m == "daily_audit":
        return 7200.0
    if is_staggered_scan_mode(m) or m.startswith("scan_"):
        return SCAN_LOCK_WAIT_SEC
    return 120.0
