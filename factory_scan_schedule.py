"""
장중 스캐너 슬롯 SSOT — 크론·factory.sh·파이프라인이 동일 순서를 참조.

KR: KST 10:00–14:30, 30분 간격, 겹침 없음 (글로벌 flock + 슬롯 분리)
US: America/New_York 10:00–14:30, 동일 패턴

각 슬롯은 단일 스캐너만 실행. 1회차 10:00 supernova에만 full prelude,
13:00 2회차 supernova는 light prelude (hydrate/증분만).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple

PreludeKind = Literal["full", "light", "none"]
Market = Literal["KR", "US"]

# (mode_suffix, step_key, human label)
_KR_SCANNER_ROWS: Tuple[Tuple[str, str, str], ...] = (
    ("supernova", "supernova", "슈퍼노바"),
    ("nulrim", "nulrim", "눌림목"),
    ("dante", "dante", "역매공파"),
    ("ema5", "ema5", "5일선"),
    ("master", "master", "마스터"),
    ("bowl", "bowl", "밥그릇"),
)

_US_SCANNER_ROWS: Tuple[Tuple[str, str, str], ...] = (
    ("supernova", "supernova", "슈퍼노바"),
    ("nulrim", "nulrim", "눌림목"),
    ("dante", "dante", "역매공파"),
    ("ema5", "ema5", "5일선"),
    ("bowl", "bowl", "밥그릇"),
)

# 2회차: 장 마감 전 핵심 4종만 재스캔
_CYCLE2_SUFFIXES: Tuple[str, ...] = ("supernova", "nulrim", "dante", "ema5")

_KR_CYCLE1_HOURS: Tuple[int, ...] = (10, 10, 11, 11, 12, 12)
_KR_CYCLE1_MINUTES: Tuple[int, ...] = (0, 30, 0, 30, 0, 30)
_US_CYCLE1_HOURS: Tuple[int, ...] = (10, 10, 11, 11, 12)
_US_CYCLE1_MINUTES: Tuple[int, ...] = (0, 30, 0, 30, 0)
_CYCLE2_HOURS: Tuple[int, ...] = (13, 13, 14, 14)
_CYCLE2_MINUTES: Tuple[int, ...] = (0, 30, 0, 30)


@dataclass(frozen=True)
class ScanSlot:
    mode: str
    factory_flag: str
    market: Market
    scanner_key: str
    label: str
    hour: int
    minute: int
    prelude: PreludeKind
    tail_doomsday: bool
    tail_us_publish: bool
    cycle: int


def _mode(market: Market, suffix: str) -> str:
    return f"scan_{market.lower()}_{suffix}"


def _flag(market: Market, suffix: str) -> str:
    return f"--scan-{market.lower()}-{suffix}"


def _build_market_slots(market: Market) -> List[ScanSlot]:
    slots: List[ScanSlot] = []
    if market == "KR":
        rows = _KR_SCANNER_ROWS
        c1h, c1m = _KR_CYCLE1_HOURS, _KR_CYCLE1_MINUTES
    else:
        rows = _US_SCANNER_ROWS
        c1h, c1m = _US_CYCLE1_HOURS, _US_CYCLE1_MINUTES
    row_map = {r[0]: r for r in rows}

    for i, suffix in enumerate(r[0] for r in rows):
        _, step_key, human = row_map[suffix]
        slots.append(
            ScanSlot(
                mode=_mode(market, suffix),
                factory_flag=_flag(market, suffix),
                market=market,
                scanner_key=step_key,
                label=human,
                hour=c1h[i],
                minute=c1m[i],
                prelude="full" if suffix == "supernova" else "none",
                tail_doomsday=(suffix == "bowl"),
                tail_us_publish=(market == "US" and suffix == "bowl"),
                cycle=1,
            )
        )

    for i, suffix in enumerate(_CYCLE2_SUFFIXES):
        if suffix not in row_map:
            continue
        _, step_key, human = row_map[suffix]
        slots.append(
            ScanSlot(
                mode=_mode(market, suffix) + "_r2",
                factory_flag=_flag(market, suffix) + "-r2",
                market=market,
                scanner_key=step_key,
                label=f"{human} (2회차)",
                hour=_CYCLE2_HOURS[i],
                minute=_CYCLE2_MINUTES[i],
                prelude="light" if suffix == "supernova" else "none",
                tail_doomsday=False,
                tail_us_publish=False,
                cycle=2,
            )
        )
    return slots


KR_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("KR"))
US_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("US"))
ALL_SCAN_SLOTS: Tuple[ScanSlot, ...] = KR_SCAN_SLOTS + US_SCAN_SLOTS

STAGGERED_SCAN_MODES: Tuple[str, ...] = tuple(s.mode for s in ALL_SCAN_SLOTS)
LEGACY_SCAN_MODES: Tuple[str, ...] = ("scan_kr", "scan_us")
ALL_SCAN_MODES: Tuple[str, ...] = LEGACY_SCAN_MODES + STAGGERED_SCAN_MODES

SCHEDULE_MARKET_TZ = {"KR": "Asia/Seoul", "US": "America/New_York"}
SCHEDULE_WEEKDAYS = {"KR": "1-5", "US": "1-5"}  # Mon–Fri in each market TZ


def scan_mode_market(mode: str) -> Optional[str]:
    """scan_kr_* / scan_us_* / legacy scan_kr|scan_us → KR|US."""
    m = str(mode or "").strip().lower()
    if m == "scan_kr" or m.startswith("scan_kr_"):
        return "KR"
    if m == "scan_us" or m.startswith("scan_us_"):
        return "US"
    return None


def is_staggered_scan_mode(mode: str) -> bool:
    return str(mode or "").strip().lower() in STAGGERED_SCAN_MODES


def slot_for_mode(mode: str) -> Optional[ScanSlot]:
    key = str(mode or "").strip().lower()
    for slot in ALL_SCAN_SLOTS:
        if slot.mode == key:
            return slot
    return None


def slots_for_market(market: str) -> Tuple[ScanSlot, ...]:
    mk = str(market or "").strip().upper()
    if mk == "KR":
        return KR_SCAN_SLOTS
    if mk == "US":
        return US_SCAN_SLOTS
    return ()
