"""
US ↔ KR 테마 정밀 매핑 — GICS/영문 섹터 → KR 표준 섹터 + 선취매 타깃.

P0: 규칙·딕셔너리 SSOT (LLM 번역은 P2 옵션, confidence 낮을 때만).
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from sector_spillover_refresh import map_standard_sector

# US 토큰(소문자 부분문자열) → KR 표준 섹터 (sector_spillover_refresh 와 동일 축)
_US_TOKEN_TO_KR_STD: Dict[str, str] = {
    "technology": "반도체/IT",
    "information technology": "반도체/IT",
    "semiconductor": "반도체/IT",
    "software": "반도체/IT",
    "ai": "반도체/IT",
    "cloud": "반도체/IT",
    "internet": "반도체/IT",
    "communication": "반도체/IT",
    "consumer discretionary": "소비재/엔터",
    "consumer cyclical": "소비재/엔터",
    "retail": "소비재/엔터",
    "automobile": "에너지/화학",
    "auto": "에너지/화학",
    "ev": "에너지/화학",
    "electric vehicle": "에너지/화학",
    "battery": "에너지/화학",
    "lithium": "에너지/화학",
    "energy": "에너지/화학",
    "oil": "에너지/화학",
    "materials": "에너지/화학",
    "basic materials": "에너지/화학",
    "industrial": "산업재/기계",
    "industrials": "산업재/기계",
    "aerospace": "산업재/기계",
    "defense": "산업재/기계",
    "machinery": "산업재/기계",
    "health": "바이오/헬스케어",
    "healthcare": "바이오/헬스케어",
    "biotech": "바이오/헬스케어",
    "financial": "금융/지주",
    "financials": "금융/지주",
    "bank": "금융/지주",
    "utilities": "기타/혼합",
    "real estate": "기타/혼합",
    "staples": "소비재/엔터",
    "consumer staples": "소비재/엔터",
}

# US 주도 테마 → KR 선취매 서브테마(복수) — 테슬라 밸류체인 등
_US_STD_TO_KR_PLAYBOOK: Dict[str, List[str]] = {
    "반도체/IT": ["반도체", "AI", "시스템반도체", "HBM", "전장반도체"],
    "에너지/화학": ["2차전지", "배터리", "양극재", "전장", "EV밸류체인"],
    "산업재/기계": ["로봇", "방산", "조선", "기계"],
    "바이오/헬스케어": ["바이오", "제약", "의료기기"],
    "금융/지주": ["은행", "증권", "지주"],
    "소비재/엔터": ["엔터", "게임", "화장품", "유통"],
    "기타/혼합": ["테마혼합"],
}

_INVALID_SECTOR_FRAGMENTS = frozenset(
    {
        "분석",
        "분석중",
        "분석 대기",
        "분석대기",
        "none",
        "unknown",
        "데이터 없음",
        "필터 탈락",
    }
)


def _norm_us_text(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def is_valid_sector_label(s: Any) -> bool:
    t = _norm_us_text(str(s or ""))
    if not t or len(t) < 2:
        return False
    for frag in _INVALID_SECTOR_FRAGMENTS:
        if frag in t:
            return False
    return True


def map_us_sector_to_kr_std(us_sector_raw: str) -> Tuple[str, float]:
    """
    US 원시 섹터 → KR 표준 섹터 + 매핑 신뢰(0~1).
    """
    if not is_valid_sector_label(us_sector_raw):
        return "기타/혼합", 0.0

    us_std = map_standard_sector(us_sector_raw)
    text = _norm_us_text(us_sector_raw)
    best_kr = us_std
    score = 0.55

    for token, kr_std in _US_TOKEN_TO_KR_STD.items():
        if token in text:
            best_kr = kr_std
            score = 0.85
            break

    if us_std == best_kr and score < 0.8:
        score = 0.72

    if best_kr == "기타/혼합":
        score = min(score, 0.45)
    return best_kr, round(min(1.0, max(0.0, score)), 2)


def kr_spillover_play_targets(kr_std: str) -> List[str]:
    return list(_US_STD_TO_KR_PLAYBOOK.get(kr_std) or _US_STD_TO_KR_PLAYBOOK.get("기타/혼합", []))


def build_cross_market_mapping_payload(us_sector_raw: str) -> Dict[str, Any]:
    kr_std, map_conf = map_us_sector_to_kr_std(us_sector_raw)
    return {
        "us_sector_raw": str(us_sector_raw or "").strip(),
        "us_sector_std": map_standard_sector(us_sector_raw) if is_valid_sector_label(us_sector_raw) else "",
        "kr_sector_std": kr_std,
        "kr_play_targets": kr_spillover_play_targets(kr_std),
        "mapping_confidence": map_conf,
    }
