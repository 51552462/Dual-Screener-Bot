"""
forward_trades.sector 적재 전 정규화 — Gemini 장문·사업설명 오염 차단.
"""
from __future__ import annotations

import re
from typing import Any

_SENTENCE_MARKERS = re.compile(
    r"(업을|영위|하며|하는|등의|및|관련|위해|통해|제조|판매|서비스|제공)"
)
_BRACKET_RE = re.compile(r"[\[\]（）(){}<>「」『』]")
_MULTI_SPACE = re.compile(r"\s+")


def _looks_like_sentence(s: str) -> bool:
    if len(s) > 18:
        return True
    if _SENTENCE_MARKERS.search(s):
        return True
    if s.count("/") >= 2:
        return True
    return False


def normalize_sector_for_db(raw: Any, *, market: str = "KR") -> str:
    """
    DB 저장 직전 1~2어절 업종 태그. 실패 시 시장별 폴백.
    """
    raw_s = str(raw or "").strip()
    if len(raw_s) > 18 or _looks_like_sentence(raw_s):
        mk = str(market).upper()
        return "US/EQUITY" if mk == "US" else "기타/혼합"

    try:
        from ace_text_sanitize import sanitize_noun_phrase

        s = sanitize_noun_phrase(raw_s)
    except Exception:
        s = raw_s

    s = _BRACKET_RE.sub("", s)
    s = _SENTENCE_MARKERS.sub("", s)
    s = _MULTI_SPACE.sub(" ", s).strip()
    for sep in ("/", "·", "|", ",", " — ", " - "):
        if sep in s:
            s = s.split(sep)[0].strip()
            break
    s = re.sub(r"\s+", "", s)
    if len(s) > 24:
        s = s[:23] + "…"

    if len(s) < 2 or len(s) > 18 or _looks_like_sentence(s):
        mk = str(market).upper()
        return "US/EQUITY" if mk == "US" else "기타/혼합"
    return s
