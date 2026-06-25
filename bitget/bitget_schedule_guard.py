"""
Bitget cron·스캔 가드 — 유지보수 창 스킵 · cron TZ 오설정 힌트 (24/7 코인).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional, Tuple

import pytz

from bitget.bitget_scan_schedule import scan_mode_market, slot_for_mode

_UTC = pytz.UTC


def utc_now() -> datetime:
    return datetime.now(_UTC)


def _parse_hour_ranges(spec: str) -> list[tuple[int, int]]:
    """'2-4,22-23' → [(2,4), (22,23)] inclusive hours."""
    out: list[tuple[int, int]] = []
    for part in str(spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.append((int(a), int(b)))
        else:
            h = int(part)
            out.append((h, h))
    return out


def is_maintenance_window(*, now_utc: Optional[datetime] = None) -> Tuple[bool, str]:
    """
    BITGET_SCAN_MAINTENANCE_UTC_HOURS=2-4,22-23 이면 해당 UTC 시각 스캔 스킵.
    """
    spec = os.environ.get("BITGET_SCAN_MAINTENANCE_UTC_HOURS", "").strip()
    if not spec:
        return False, ""
    now = now_utc or utc_now()
    if now.tzinfo is None:
        now = _UTC.localize(now)
    else:
        now = now.astimezone(_UTC)
    hour = int(now.hour)
    for lo, hi in _parse_hour_ranges(spec):
        if lo <= hour <= hi:
            return True, f"maintenance UTC hour {hour} in {spec}"
    return False, ""


def evaluate_scan_skip(
    mode: str,
    *,
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    스캔 실행 전 게이트. (skip, reason)
    BITGET_FACTORY_SCAN_DISABLED=1 → 전체 스캔 중단.
    """
    m = str(mode or "").strip().lower()
    if not m.startswith("scan_"):
        return False, ""

    if os.environ.get("BITGET_FACTORY_SCAN_DISABLED", "").strip() in ("1", "true", "yes"):
        return True, "BITGET_FACTORY_SCAN_DISABLED"

    if os.environ.get("BITGET_FORCE_SCAN", "").strip() in ("1", "true", "yes"):
        return False, ""

    maint, why = is_maintenance_window(now_utc=now_utc)
    if maint:
        return True, why

    return False, ""


def is_quiet_scan_skip(mode: str, *, detail: str = "") -> bool:
    """예상된 스킵 — 텔레그램 알람 생략."""
    skip, reason = evaluate_scan_skip(mode)
    if skip:
        return True
    if "SKIPPED_SESSION" in detail:
        return False
    # lock 경합(특히 data_refresh OHLCV)은 cron 정상 동작 — 텔레그램 생략
    d = str(detail or "")
    if "bitget lock busy" in d:
        return True
    return False


def cron_misalignment_hint(
    mode: str,
    *,
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    슬롯 모드가 SSOT 시각에서 ±8분 이상 벗어나면 cron drift 힌트.
    """
    slot = slot_for_mode(mode)
    if slot is None:
        return False, ""
    now = now_utc or utc_now()
    if now.tzinfo is None:
        now = _UTC.localize(now)
    else:
        now = now.astimezone(_UTC)
    expected = slot.hour * 60 + slot.minute
    actual = now.hour * 60 + now.minute
    delta = abs(actual - expected)
    delta = min(delta, 24 * 60 - delta)  # circular minute distance (avoid 955m false positives)
    if delta > 8 and delta < (24 * 60 - 8):
        mk = scan_mode_market(mode) or slot.market
        return (
            True,
            f"{mk} {mode} wall_clock drift {delta}m from SSOT "
            f"{slot.hour:02d}:{slot.minute:02d} UTC — regenerate cron: "
            "sudo INSTALL_ROOT=$PWD/bitget/.. bash bitget/deploy/install_bitget_cron.sh",
        )
    return False, ""
