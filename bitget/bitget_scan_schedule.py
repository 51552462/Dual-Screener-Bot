"""
Bitget 장중 스캐너 슬롯 SSOT — cron · bitget.sh · 파이프라인 동일 순서.

24/7 분산 스케줄 — 전용 코인 서버(Bot-2) 최적화:
  · 서버 분리(Two-Track air-gap) 완료 → 주식(KR/US) 크론과의 분(minute) 충돌 회피 불필요.
  · SPOT/FUTURES 는 서로 다른 (시,분) 으로 배치해 동시 실행을 금지하고,
    24시간에 걸쳐 ~53분 간격으로 교차(interleave) 배치한다.
  · 3사이클 운영: 1회차 supernova=full prelude, 2·3회차=light.
  · 총 27슬롯 (SPOT 14 + FUTURES 13) — 기존 19슬롯 대비 ~42% 밀도 증가.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple

PreludeKind = Literal["full", "light", "none"]
Market = Literal["SPOT", "FUTURES"]

# lock-timeout 산정용(스캔 1회 최대 대기). 슬롯 간격 자체는 아래 explicit clock 표가 SSOT.
SLOT_INTERVAL_MINUTES = 45
SCAN_LOCK_WAIT_SEC = float(SLOT_INTERVAL_MINUTES * 60 + 300)

# --- 24h explicit clock 표 (UTC) — 전용 서버, ~53분 간격 SPOT/FUTURES 교차 ---
# 인덱스 순서 = 스캐너 행 순서(supernova→…→shadow, 2회차, 3회차). SPOT/FUTURES 교차.
_SPOT_SLOT_CLOCKS: Tuple[Tuple[int, int], ...] = (
    # Cycle 1
    (0, 2),    # supernova  (full prelude)
    (1, 47),   # nulrim
    (3, 33),   # dante
    (5, 20),   # ema5
    (7, 7),    # master
    (8, 52),   # shadow      (tail: doomsday + track)
    # Cycle 2
    (10, 40),  # supernova_r2 (light)
    (12, 27),  # nulrim_r2
    (14, 13),  # dante_r2
    (16, 1),   # ema5_r2
    # Cycle 3
    (17, 47),  # supernova_r3 (light)
    (19, 33),  # nulrim_r3
    (21, 20),  # dante_r3
    (23, 7),   # ema5_r3
)
_FUTURES_SLOT_CLOCKS: Tuple[Tuple[int, int], ...] = (
    # Cycle 1
    (0, 54),   # supernova  (full prelude)
    (2, 40),   # nulrim
    (4, 27),   # dante
    (6, 13),   # ema5
    (8, 1),    # shadow      (tail: doomsday + track)
    # Cycle 2
    (9, 47),   # supernova_r2 (light)
    (11, 33),  # nulrim_r2
    (13, 20),  # dante_r2
    (15, 7),   # ema5_r2
    # Cycle 3
    (16, 52),  # supernova_r3 (light)
    (18, 40),  # nulrim_r3
    (20, 27),  # dante_r3
    (22, 13),  # ema5_r3
)

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
_CYCLE3_SUFFIXES: Tuple[str, ...] = ("supernova", "nulrim", "dante", "ema5")


def _market_clocks(market: Market) -> Tuple[Tuple[int, int], ...]:
    return _SPOT_SLOT_CLOCKS if market == "SPOT" else _FUTURES_SLOT_CLOCKS


def _slot_clock(market: Market, slot_index: int) -> Tuple[int, int]:
    """explicit 24h 표에서 (hour, minute) 조회."""
    return _market_clocks(market)[int(slot_index)]


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
        hour, minute = _slot_clock(market, i)
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
        hour, minute = _slot_clock(market, cycle2_base + i)
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

    cycle3_base = cycle2_base + len(_CYCLE2_SUFFIXES)
    for i, suffix in enumerate(_CYCLE3_SUFFIXES):
        if suffix not in row_map:
            continue
        _, step_key, human = row_map[suffix]
        hour, minute = _slot_clock(market, cycle3_base + i)
        slots.append(
            ScanSlot(
                mode=_mode(market, suffix) + "_r3",
                bitget_flag=_flag(market, suffix) + "-r3",
                market=market,
                scanner_key=step_key,
                label=f"{human} (3회차)",
                hour=hour,
                minute=minute,
                prelude="light" if suffix == "supernova" else "none",
                tail_doomsday=False,
                tail_track=False,
                tail_shadow=False,
                cycle=3,
            )
        )
    return slots


SPOT_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("SPOT"))
FUTURES_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("FUTURES"))
ALL_SCAN_SLOTS: Tuple[ScanSlot, ...] = SPOT_SCAN_SLOTS + FUTURES_SCAN_SLOTS


def _assert_collision_free() -> None:
    """import 시 불변식 검증 — SPOT↔FUTURES 동시각 금지 · :53(reconcile) 회피."""
    seen: dict[Tuple[int, int], str] = {}
    for slot in ALL_SCAN_SLOTS:
        if slot.minute == 53:
            raise AssertionError(
                f"bitget scan {slot.mode} at :{slot.minute:02d} collides with "
                "reconcile cron (:53 every hour); pick another minute."
            )
        key = (slot.hour, slot.minute)
        if key in seen:
            raise AssertionError(
                f"bitget scan time {key[0]:02d}:{key[1]:02d} used by both "
                f"{seen[key]} and {slot.mode} — SPOT/FUTURES must not run simultaneously."
            )
        seen[key] = slot.mode


_assert_collision_free()

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
        # [아키텍트 수술] 일일 감사(daily_audit) 락 타임아웃 대폭 축소
        # 2시간(7200초)의 무한 대기를 허용하면 코인 파이프라인 전체가 마비되는 단일 장애점(SPOF)이 됩니다.
        # 최대 10분(600초) 내에 락을 풀고 다음 스캔으로 넘어가도록 강제 절단합니다.
        return 600.0
    if is_staggered_scan_mode(m) or m.startswith("scan_"):
        return SCAN_LOCK_WAIT_SEC
    return 120.0