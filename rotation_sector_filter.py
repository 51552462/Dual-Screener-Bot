"""
순환매·스필오버 — 유효 섹터 Hard Block SSOT (Junk 주도 방지).
"""
from __future__ import annotations

from typing import Any, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

JUNK_SECTOR_LABELS = frozenset(
    {
        "기타/혼합",
        "기타",
        "혼합",
        "테마혼합",
        "nan",
        "none",
        "unknown",
        "데이터 없음",
        "분석중",
        "분석 대기",
        "분석대기",
    }
)

JUNK_FRAGMENTS = ("유망", "포착", "분석 대기", "분석대기", "필터 탈락")

US_JUNK_LABELS = frozenset(
    {
        "us/equity",
        "equity",
        "mixed",
        "other",
        "n/a",
    }
)


def is_rotation_eligible_sector(label: Any, *, market: str = "KR") -> bool:
    """순환매 랭킹·주도 섹터 산출에 포함 가능한지."""
    t = str(label or "").strip()
    if not t:
        return False
    low = t.lower()
    if low in ("nan", "none", "null", "undefined"):
        return False
    if t in JUNK_SECTOR_LABELS or low in JUNK_SECTOR_LABELS:
        return False
    if low.startswith("기타") or low.startswith("혼합"):
        return False
    if any(f in t for f in JUNK_FRAGMENTS):
        return False
    mk = str(market or "KR").upper()
    if mk == "US" and low in US_JUNK_LABELS:
        return False
    return len(t) >= 2


def dominant_sector_for_series(sectors: Iterable[Any], *, market: str = "KR") -> Optional[str]:
    """일별 sector 시리즈 → 유효 표준 섹터 1개 (mode)."""
    valid = [str(s) for s in sectors if is_rotation_eligible_sector(s, market=market)]
    if not valid:
        return None
    mode = pd.Series(valid).mode()
    if mode.empty:
        return None
    return str(mode.iloc[0])


def filter_eligible_daily_series(
    series: Sequence[Tuple[str, str]],
    *,
    market: str = "KR",
) -> List[Tuple[str, str]]:
    """(date, sector) 목록에서 Junk 일자·라벨 제거."""
    out: List[Tuple[str, str]] = []
    for day, sec in series:
        if is_rotation_eligible_sector(sec, market=market):
            out.append((str(day), str(sec)))
    return out
