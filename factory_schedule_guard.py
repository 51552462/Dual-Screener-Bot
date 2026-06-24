"""
Cron·세션 정렬 가드 — 장외 예상 스킵(무음) vs cron TZ 오설정(경고).
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

import pytz

from factory_scan_schedule import scan_mode_market

_KR_TZ = pytz.timezone("Asia/Seoul")
_US_ET = pytz.timezone("America/New_York")

# US 정규장 ≈ KST 22:30–05:00(서머타임). 이 구간 밖 주간 스킵 = cron TZ 미적용 징후.
_US_MISALIGN_KST_HOURS = range(8, 23)


def kst_now() -> datetime:
    return datetime.now(_KR_TZ)


def is_quiet_scan_session_skip(
    mode: str,
    *,
    detail: str = "",
    now_kst: Optional[datetime] = None,
) -> bool:
    """
    장외·주말 등 **예상된** SKIPPED_SESSION — 텔레그램 알람 생략(로그만).
    """
    market = scan_mode_market(str(mode or ""))
    if not market:
        return False
    now = now_kst or kst_now()
    mk = market.upper()
    dow = int(now.weekday())
    hour = int(now.hour)

    if mk == "US":
        if dow >= 5:
            return True
        if dow < 5 and hour in _US_MISALIGN_KST_HOURS:
            return True
        return False

    if mk == "KR":
        if dow >= 5:
            return True
        if hour < 9 or hour > 15 or (hour == 15 and now.minute > 30):
            return True
        return False

    return False


def us_cron_misalignment_hint(
    mode: str,
    *,
    now_kst: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    US scan이 KST 주간에 실행됐으면 CRON_TZ 미적용 가능성.
    (bool misaligned, hint_message)
    """
    if scan_mode_market(str(mode or "")) != "US":
        return False, ""
    now = now_kst or kst_now()
    if int(now.weekday()) >= 5:
        return False, ""
    if int(now.hour) not in _US_MISALIGN_KST_HOURS:
        return False, ""
    return (
        True,
        "US scan triggered during KST daytime — reinstall cron "
        "(sudo INSTALL_ROOT=$PWD bash deploy/install_factory_cron.sh) "
        "to enable factory_slot_dispatcher (ET slots without CRON_TZ=NY).",
    )
