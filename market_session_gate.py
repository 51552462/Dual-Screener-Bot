"""
Runtime regular-session gate — cron/스케줄러와 무관하게 스캔·진입 차단.

KR: 09:00–15:30 KST (월–금)
US: NYSE regular 09:30–16:00 America/New_York (DST 자동, ≈ KST 22:30–05:00 / 23:30–06:00)
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Tuple

import pytz

_KR_TZ = pytz.timezone("Asia/Seoul")
_US_ET = pytz.timezone("America/New_York")


def force_scan_outside_session() -> bool:
    """
    수동 복구·장외 테스트 — FACTORY_FORCE_SCAN_OUTSIDE_SESSION=1 이면 정규장 게이트 우회.
    factory_pipelines._require_market_session_for_scan · supernova_hunter 공통 SSOT.
    """
    return str(os.environ.get("FACTORY_FORCE_SCAN_OUTSIDE_SESSION", "")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _clock_minutes(hour: int, minute: int) -> int:
    return int(hour) * 60 + int(minute)


def is_market_open(market: str) -> Tuple[bool, str]:
    """
    정규장 여부. (ok, reason_code_or_detail)
    """
    if force_scan_outside_session():
        mk = str(market or "").strip().upper()
        return True, f"{mk} FORCE_RECOVERY (FACTORY_FORCE_SCAN_OUTSIDE_SESSION)"
    mk = str(market or "").strip().upper()
    if mk == "KR":
        return _kr_regular_open()
    if mk == "US":
        return _us_regular_open()
    return False, f"unknown market: {mk}"


def _kr_regular_open() -> Tuple[bool, str]:
    now = datetime.now(_KR_TZ)
    if now.weekday() >= 5:
        return False, "KR weekend — 장외 시간 진입 불가"
    cur = _clock_minutes(now.hour, now.minute)
    open_m = _clock_minutes(9, 0)
    close_m = _clock_minutes(15, 30)
    if cur < open_m or cur > close_m:
        return (
            False,
            f"KR 장외 시간 진입 불가 (허용 09:00–15:30 KST, 현재 {now.strftime('%H:%M')} KST)",
        )
    return True, "KR regular session"


def _us_regular_open() -> Tuple[bool, str]:
    now_et = datetime.now(_US_ET)
    if now_et.weekday() >= 5:
        return False, "US weekend (ET) — 장외 시간 진입 불가"
    cur = _clock_minutes(now_et.hour, now_et.minute)
    open_m = _clock_minutes(9, 30)
    close_m = _clock_minutes(16, 0)
    if cur < open_m or cur > close_m:
        now_kst = datetime.now(_KR_TZ)
        return (
            False,
            (
                "US 장외 시간 진입 불가 "
                f"(허용 NY 09:30–16:00 ET ≈ KST 22:30–05:00 DST / 23:30–06:00, "
                f"현재 ET {now_et.strftime('%H:%M')} · KST {now_kst.strftime('%H:%M')})"
            ),
        )
    return True, "US regular session (ET)"


def require_market_open_for_scan(market: str) -> None:
    """스캔 파이프라인 — 장외면 RuntimeError."""
    if force_scan_outside_session():
        return
    ok, detail = is_market_open(market)
    if not ok:
        raise RuntimeError(detail)
