"""
장중 스캐너 슬롯 SSOT — 크론·factory.sh·파이프라인이 동일 순서를 참조.

KR: KST 10:00부터 50분 간격 (글로벌 flock — 이전 슬롯 완료까지 대기)
US: America/New_York 동일 패턴

각 슬롯은 단일 스캐너만 실행. 1회차 10:00 supernova에만 full prelude,
2회차 supernova는 light prelude (hydrate/증분만).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple

PreludeKind = Literal["full", "light", "none"]
Market = Literal["KR", "US"]

# 슬롯 간격(분) — 크론·lock-timeout SSOT (30→50: prelude+스캔이 30분 넘을 때 SKIPPED_LOCK 방지)
SLOT_INTERVAL_MINUTES = 50
SLOT_START_HOUR = 10
SLOT_START_MINUTE = 0
# 이전 슬롯이 끝날 때까지 대기 (cron 시각 + 여유 5분)
SCAN_LOCK_WAIT_SEC = float(SLOT_INTERVAL_MINUTES * 60 + 300)

# --- 2회차(cycle-2) "장 마감 전" 편성 SSOT ---
# 1회차는 10:00부터 50분 간격(고정). 2회차는 그 간격을 그대로 이어붙이면 슬롯이 마감
# 이후로 밀려(예: US ema5_r2 16:40 > 16:00) 매일 SKIPPED_SESSION 된다.
# → 2회차는 "정규장 마감 - margin" 을 마지막 슬롯으로 두고 역산/균등 배치하며,
#   마감 전에 들어갈 수 있는 스캐너만 편성한다(못 들어가는 후미는 제외 — 가짜 스킵 제거).
MARKET_CLOSE_LOCAL = {"KR": (15, 30), "US": (16, 0)}  # 정규장 마감(현지시각)
CYCLE2_CLOSE_MARGIN_MINUTES = 5   # 마지막 r2 는 마감 margin 분 전까지는 시작
CYCLE2_MIN_INTERVAL_MINUTES = 20  # r2 슬롯 최소 간격(락-스킵 방지 하한)

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

# 2회차: 장 마감 전 핵심 4종 재스캔(마감 전에 들어가는 것만 — 후미는 제외)
_CYCLE2_SUFFIXES: Tuple[str, ...] = ("supernova", "nulrim", "dante", "ema5")


def _slot_clock(slot_index: int) -> Tuple[int, int]:
    """slot_index=0 → 10:00, 이후 SLOT_INTERVAL_MINUTES 간격."""
    base = SLOT_START_HOUR * 60 + SLOT_START_MINUTE
    total = base + int(slot_index) * SLOT_INTERVAL_MINUTES
    return total // 60, total % 60


def _cycle2_clocks(market: Market, n_cycle1_rows: int) -> List[Tuple[int, int]]:
    """
    2회차 슬롯 시각 — 정규장 마감 전에 모두 시작되도록 역산/균등 배치.
    마감(- margin) 안에 들어가는 스캐너만 반환(후미 초과분은 제외 → 매일 가짜 스킵 제거).
    """
    close_h, close_m = MARKET_CLOSE_LOCAL[market]
    last_start = close_h * 60 + close_m - CYCLE2_CLOSE_MARGIN_MINUTES
    cycle1_last = (
        SLOT_START_HOUR * 60 + SLOT_START_MINUTE
    ) + (int(n_cycle1_rows) - 1) * SLOT_INTERVAL_MINUTES
    earliest = cycle1_last + SLOT_INTERVAL_MINUTES  # 1회차 마지막 이후 한 슬롯 비우고 시작
    n = len(_CYCLE2_SUFFIXES)
    if n <= 0 or last_start < earliest:
        return []
    if n == 1:
        return [(earliest // 60, earliest % 60)]
    raw = (last_start - earliest) / (n - 1)
    spacing = max(float(CYCLE2_MIN_INTERVAL_MINUTES), raw)
    clocks: List[Tuple[int, int]] = []
    for i in range(n):
        t = int(round(earliest + i * spacing))
        if t <= last_start:
            clocks.append((t // 60, t % 60))
    return clocks


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
    rows = _KR_SCANNER_ROWS if market == "KR" else _US_SCANNER_ROWS
    row_map = {r[0]: r for r in rows}

    for i, suffix in enumerate(r[0] for r in rows):
        _, step_key, human = row_map[suffix]
        hour, minute = _slot_clock(i)
        slots.append(
            ScanSlot(
                mode=_mode(market, suffix),
                factory_flag=_flag(market, suffix),
                market=market,
                scanner_key=step_key,
                label=human,
                hour=hour,
                minute=minute,
                prelude="full" if suffix == "supernova" else "none",
                tail_doomsday=(suffix == "bowl"),
                tail_us_publish=(market == "US" and suffix == "bowl"),
                cycle=1,
            )
        )

    cycle2_clocks = _cycle2_clocks(market, len(rows))
    for i, (hour, minute) in enumerate(cycle2_clocks):
        suffix = _CYCLE2_SUFFIXES[i]
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
                hour=hour,
                minute=minute,
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
