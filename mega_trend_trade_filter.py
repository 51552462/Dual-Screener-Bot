"""
Mega-Trend unlock trade filter SSOT.

internal_monitor · kill_rl 이 동일한 언락 체결 정의를 공유한다.
"""
from __future__ import annotations

from typing import Optional

MEGA_TREND_SIG_MARKERS = (
    "MegaTrend",
    "MEGA_TREND",
    "순환매_선취매",
)


def is_mega_trend_sig_type(sig_type: object) -> bool:
    sig_s = str(sig_type or "")
    return any(marker in sig_s for marker in MEGA_TREND_SIG_MARKERS)


def is_mega_trend_unlock_trade(
    *,
    sig_type: object,
    entry_date: object,
    ignited_at: Optional[str] = None,
) -> bool:
    """
    MEGA_TREND 언락 구간 체결 여부 — fetch_mega_trend_sector_trades 와 동일 규칙.

    · sig_type 에 MegaTrend / MEGA_TREND / 순환매_선취매 태그
    · 또는 entry_date >= ignited_at (언락 시점 이후 동일 섹터 체결)
    """
    if is_mega_trend_sig_type(sig_type):
        return True
    since = str(ignited_at or "")[:10]
    ed = str(entry_date or "")[:10]
    if since and ed and ed >= since:
        return True
    return False
