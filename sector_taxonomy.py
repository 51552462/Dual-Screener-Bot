"""
시장별 표준 섹터 taxonomy SSOT — 순환매·스필오버·리포트 공통.

원칙:
  1) 알려진 키워드 → 표준 버킷(16+ KR / 12 US)
  2) 짧은 유효 원시 라벨 → 그대로 보존 (기타/혼합으로 뭉개지 않음)
  3) 정말 매핑 불가 → 미분류(원시) — 리포트에서 원시 라벨 breakdown 별도 노출
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

# 레거시 호환 — rotation filter 에서 junk 처리
LEGACY_CATCHALL_KR = "기타/혼합"
UNMAPPED_KR = "미분류(원시)"
UNMAPPED_US = "Unmapped(raw)"

STANDARD_SECTORS_KR: Tuple[str, ...] = (
    "반도체/IT",
    "2차전지/배터리",
    "에너지/화학",
    "바이오/헬스케어",
    "금융/지주",
    "산업재/기계",
    "조선/방산",
    "자동차/모빌리티",
    "철강/소재",
    "건설/인프라",
    "소비재/엔터",
    "유통/리테일",
    "통신/미디어",
    "게임/콘텐츠",
    "부동산/리츠",
    "우주/항공",
)

STANDARD_SECTORS_US: Tuple[str, ...] = (
    "Technology",
    "Semiconductors",
    "Healthcare",
    "Biotech",
    "Energy",
    "Materials",
    "Financials",
    "Industrials",
    "Consumer Disc.",
    "Consumer Staples",
    "Communication",
    "Real Estate",
)

_KR_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("반도체/IT", ("반도체", "it", "ai", "소프트웨어", "모바일", "테크", "데이터", "클라우드", "saas", "cyber")),
    ("2차전지/배터리", ("2차전지", "배터리", "전지", "양극", "음극", "리튬", "battery", "ev배터리")),
    ("에너지/화학", ("화학", "에너지", "정유", "석유", "가스", "정밀화학", "석유화학", "oil", "refin")),
    ("바이오/헬스케어", ("바이오", "헬스", "의료", "제약", "병원", "진단", "health", "pharma", "biotech")),
    ("금융/지주", ("금융", "은행", "증권", "지주", "보험", "카드", "캐피탈", "bank", "insurance")),
    ("조선/방산", ("조선", "방산", "함정", "군수", "defense", "shipbuild")),
    ("산업재/기계", ("기계", "산업재", "로봇", "전력", "중공업", "plant", "industrial")),
    ("자동차/모빌리티", ("자동차", "완성차", "모빌", "전기차", "ev", "auto", "mobility")),
    ("철강/소재", ("철강", "소재", "비철", "금속", "강판", "steel", "metal", "material")),
    ("건설/인프라", ("건설", "인프라", "플랜트", "토목", "건자재", "시멘트", "construction")),
    ("소비재/엔터", ("소비", "식품", "화장품", "엔터", "미디어", "의류", "패션", "consumer")),
    ("유통/리테일", ("유통", "리테일", "백화", "마트", "이커머", "retail", "commerce")),
    ("통신/미디어", ("통신", "통신사", "네트워크", "telecom", "carrier")),
    ("게임/콘텐츠", ("게임", "콘텐츠", "game", "gaming", "esport")),
    ("부동산/리츠", ("부동산", "리츠", "reit", "real estate")),
    ("우주/항공", ("우주", "항공", "aerospace", "satellite", "space")),
)

_US_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("Semiconductors", ("semiconductor", "chip", "semi", "nvidia", "foundry")),
    ("Technology", ("technology", "software", "cloud", "saas", "cyber", "tech", "it ")),
    ("Biotech", ("biotech", "genomic", "gene")),
    ("Healthcare", ("health", "pharma", "medical", "hospital")),
    ("Energy", ("energy", "oil", "gas", "refin", "solar", "renewable")),
    ("Materials", ("material", "chemical", "mining", "steel", "metal")),
    ("Financials", ("financial", "bank", "insurance", "capital")),
    ("Industrials", ("industrial", "machinery", "aerospace", "defense", "transport")),
    ("Consumer Disc.", ("consumer disc", "retail", "auto", "leisure", "restaurant")),
    ("Consumer Staples", ("staple", "food", "beverage", "grocery")),
    ("Communication", ("communication", "telecom", "media", "entertainment")),
    ("Real Estate", ("real estate", "reit", "property")),
)

_JUNK_FRAGMENTS = ("유망", "포착", "분석 대기", "분석대기", "필터 탈락")
_JUNK_EXACT = frozenset(
    {
        "",
        "nan",
        "none",
        "null",
        "unknown",
        "—",
        "-",
        LEGACY_CATCHALL_KR,
        "기타",
        "혼합",
        "테마혼합",
        UNMAPPED_KR,
        UNMAPPED_US,
        "us/equity",
        "equity",
        "mixed",
        "other",
        "n/a",
    }
)


@dataclass(frozen=True)
class SectorMapping:
    raw: str
    standard: str
    preserved_fine: bool
    matched_rule: Optional[str]


def standard_sectors_for_market(market: str) -> Tuple[str, ...]:
    return STANDARD_SECTORS_US if str(market).upper() == "US" else STANDARD_SECTORS_KR


def _rules_for_market(market: str) -> Tuple[Tuple[str, Tuple[str, ...]], ...]:
    return _US_RULES if str(market).upper() == "US" else _KR_RULES


def _unmapped_label(market: str) -> str:
    return UNMAPPED_US if str(market).upper() == "US" else UNMAPPED_KR


def is_junk_sector_raw(raw: Any) -> bool:
    t = str(raw or "").strip()
    if not t:
        return True
    low = t.lower()
    if low in _JUNK_EXACT:
        return True
    if any(f in t for f in _JUNK_FRAGMENTS):
        return True
    return False


def is_fine_grained_sector_label(raw: Any, *, market: str = "KR") -> bool:
    """짧은 원시 업종명 — taxonomy 버킷 대신 그대로 추적."""
    if is_junk_sector_raw(raw):
        return False
    t = re.sub(r"\s+", "", str(raw).strip())
    if len(t) < 2 or len(t) > 16:
        return False
    standards = set(standard_sectors_for_market(market))
    if t in standards:
        return False
    if _looks_like_sentence(t):
        return False
    return True


def _looks_like_sentence(s: str) -> bool:
    if len(s) > 18:
        return True
    if re.search(r"(업을|영위|하며|하는|등의|및|관련|위해)", s):
        return True
    return s.count("/") >= 2


def map_sector_detailed(raw: Any, *, market: str = "KR") -> SectorMapping:
    mk = str(market).upper()
    raw_s = str(raw or "").strip()
    standards = standard_sectors_for_market(mk)

    if raw_s in standards:
        return SectorMapping(raw=raw_s, standard=raw_s, preserved_fine=False, matched_rule="exact")

    if raw_s == LEGACY_CATCHALL_KR:
        return SectorMapping(
            raw=raw_s,
            standard=_unmapped_label(mk),
            preserved_fine=False,
            matched_rule="legacy_catchall",
        )

    low = raw_s.lower()
    for bucket, keywords in _rules_for_market(mk):
        if bucket in standards and any(k in low for k in keywords):
            return SectorMapping(raw=raw_s, standard=bucket, preserved_fine=False, matched_rule=bucket)

    if is_fine_grained_sector_label(raw_s, market=mk):
        return SectorMapping(raw=raw_s, standard=raw_s, preserved_fine=True, matched_rule="fine_preserve")

    if is_junk_sector_raw(raw_s):
        return SectorMapping(
            raw=raw_s,
            standard=_unmapped_label(mk),
            preserved_fine=False,
            matched_rule=None,
        )

    return SectorMapping(
        raw=raw_s,
        standard=_unmapped_label(mk),
        preserved_fine=False,
        matched_rule=None,
    )


def map_standard_sector(s: Any, market: str = "KR") -> str:
    """SSOT — sector_spillover_refresh 등 전역 import 대상."""
    return map_sector_detailed(s, market=market).standard


def map_standard_sector_legacy(s: Any) -> str:
    """market 미지정 호출 호환 (KR 기본)."""
    return map_standard_sector(s, market="KR")


@dataclass
class SectorEntryStats:
    sector: str
    n_entries: int
    n_days_dominant: int
    avg_streak_days: float
    max_streak_days: int
    is_standard_bucket: bool


def rollup_sector_entries(
    df: pd.DataFrame,
    *,
    market: str,
    sector_col: str = "sector",
    date_col: str = "entry_date",
) -> Tuple[List[SectorEntryStats], Dict[str, int], Dict[str, str]]:
    """
    (섹터별 통계 리스트, 미분류 원시 breakdown, raw→std 샘플)
    """
    mk = str(market).upper()
    standards = set(standard_sectors_for_market(mk))
    unmapped_label = _unmapped_label(mk)

    if df is None or df.empty or sector_col not in df.columns:
        return [], {}, {}

    work = df.copy()
    work["_raw_sector"] = work[sector_col].astype(str).str.strip()
    mappings = work["_raw_sector"].apply(lambda x: map_sector_detailed(x, market=mk))
    work["_sector_std"] = mappings.apply(lambda m: m.standard)

    unmapped_breakdown: Dict[str, int] = {}
    for raw, m in zip(work["_raw_sector"], mappings):
        if m.standard == unmapped_label and raw and not is_junk_sector_raw(raw):
            unmapped_breakdown[raw] = unmapped_breakdown.get(raw, 0) + 1

    entry_counts = work["_sector_std"].value_counts().to_dict()

    dom_days: Dict[str, int] = {}
    streaks: Dict[str, List[int]] = {}
    if date_col in work.columns:
        work["_day"] = work[date_col].astype(str).str[:10]

        def _dominant(group: pd.Series) -> Optional[str]:
            from rotation_sector_filter import dominant_sector_for_series

            return dominant_sector_for_series(group, market=mk)

        daily_dom = work.groupby("_day")["_sector_std"].agg(_dominant).dropna()
        for sec in daily_dom:
            dom_days[str(sec)] = dom_days.get(str(sec), 0) + 1

        current: Optional[str] = None
        streak = 0
        for _d, sec in daily_dom.items():
            sec = str(sec)
            if sec == current:
                streak += 1
            else:
                if current is not None:
                    streaks.setdefault(current, []).append(streak)
                current = sec
                streak = 1
        if current is not None:
            streaks.setdefault(current, []).append(streak)

    stats: List[SectorEntryStats] = []
    all_sectors = set(entry_counts.keys()) | set(streaks.keys())
    for sec in sorted(all_sectors, key=lambda s: (-entry_counts.get(s, 0), s)):
        lengths = streaks.get(sec, [])
        stats.append(
            SectorEntryStats(
                sector=sec,
                n_entries=int(entry_counts.get(sec, 0)),
                n_days_dominant=int(dom_days.get(sec, 0)),
                avg_streak_days=(sum(lengths) / len(lengths)) if lengths else 0.0,
                max_streak_days=max(lengths) if lengths else 0,
                is_standard_bucket=sec in standards,
            )
        )

    raw_to_std: Dict[str, str] = {}
    for r in work["_raw_sector"].unique():
        raw_to_std[str(r)] = map_sector_detailed(r, market=mk).standard
    return stats, unmapped_breakdown, raw_to_std
