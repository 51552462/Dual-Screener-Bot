"""
Bitget 장중 스캐너 슬롯 SSOT — cron · bitget.sh · 파이프라인 동일 순서.

24/7 분산 스케줄 — 주식(KR/US) 크론과 **같은 분(minute)** 에 절대 안 겹치게 설계:
  · KR/US 스캔  : 각 시장 정각 기준 :00/:10/:20/:30/:40/:50 (= UTC 기준에서도 5의 배수 분)
  · KR/US 장후 오딧: 09:45 / 21:45 UTC (5의 배수 분)
  · bitget ops  : track(*/15)·reconcile·data-refresh·watchdog(*/5) — 모두 5의 배수 분
  → bitget 스캔을 **5의 배수가 아닌 분** 에만 두면 위 모든 잡과 동일 분에 겹치지 않는다.

SPOT/FUTURES 는 서로 다른 (시,분) 으로 배치해 동시 실행을 금지하고,
24시간에 걸쳐 ~75분 간격으로 교차(interleave) 배치한다. 1회차 supernova=full
prelude, 2회차=light. 시간 겹침(중복 실행)으로 인한 4GB 서버 과부하는
bitget_schedule_guard.factory_heavy_job_active() 의 yield-to-factory 가드가 추가로 방지.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple

PreludeKind = Literal["full", "light", "none"]
Market = Literal["SPOT", "FUTURES"]

# lock-timeout 산정용(스캔 1회 최대 대기). 슬롯 간격 자체는 아래 explicit clock 표가 SSOT.
SLOT_INTERVAL_MINUTES = 50
SCAN_LOCK_WAIT_SEC = float(SLOT_INTERVAL_MINUTES * 60 + 300)

# --- 24h explicit clock 표 (UTC) — 모든 분은 5의 배수가 아님(주식/ops 충돌 회피) ---
# 인덱스 순서 = 스캐너 행 순서(supernova→…→shadow, 그 뒤 2회차). SPOT/FUTURES 교차.
_SPOT_SLOT_CLOCKS: Tuple[Tuple[int, int], ...] = (
    (0, 7),    # supernova  (full prelude · 장외 idle 창)
    (2, 39),   # nulrim
    (5, 3),    # dante
    (7, 33),   # ema5
    (10, 3),   # master     (KR 장후~US 장전 idle 창)
    (12, 33),  # shadow      (tail: doomsday + track)
    (15, 3),   # supernova_r2 (light)
    (17, 33),  # nulrim_r2
    (20, 3),   # dante_r2
    (22, 33),  # ema5_r2     (US 장후~KR 장전 idle 창)
)
_FUTURES_SLOT_CLOCKS: Tuple[Tuple[int, int], ...] = (
    (1, 23),   # supernova  (full prelude)
    (3, 47),   # nulrim
    (6, 19),   # dante
    (8, 49),   # ema5
    (11, 19),  # shadow      (tail: doomsday + track)
    (13, 49),  # supernova_r2 (light)
    (16, 19),  # nulrim_r2
    (18, 49),  # dante_r2
    (21, 19),  # ema5_r2
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
    return slots


SPOT_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("SPOT"))
FUTURES_SCAN_SLOTS: Tuple[ScanSlot, ...] = tuple(_build_market_slots("FUTURES"))
ALL_SCAN_SLOTS: Tuple[ScanSlot, ...] = SPOT_SCAN_SLOTS + FUTURES_SCAN_SLOTS


def _assert_collision_free() -> None:
    """import 시 불변식 검증 — 주식/ops 와 같은 분 충돌·SPOT↔FUTURES 동시각 금지."""
    seen: dict[Tuple[int, int], str] = {}
    for slot in ALL_SCAN_SLOTS:
        # 5의 배수 분 = KR/US 스캔(:00..:50)·오딧(:45)·bitget ops(*/5) 와 충돌 가능.
        if slot.minute % 5 == 0:
            raise AssertionError(
                f"bitget scan {slot.mode} at :{slot.minute:02d} is a multiple of 5 — "
                "collides with stock/ops cron minutes; pick a non-%5 minute."
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
        return 7200.0
    if is_staggered_scan_mode(m) or m.startswith("scan_"):
        return SCAN_LOCK_WAIT_SEC
    return 120.0
