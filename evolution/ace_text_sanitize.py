"""
Ace / 테마 텍스트 Sanitize — 텔레그램·LLM 수다쟁이 서술 차단.

- 15자 초과 강제 절삭
- 서술어·접속사·GICS 장황 표현 정규식 제거
- theme_token: 1~2단어 명사만
"""
from __future__ import annotations

import re
from typing import Any, List, Optional

MAX_ACE_TOKEN_CHARS = 15

# 서술·수식어 제거 (한국어 리포트 수다 패턴)
_VERBOSITY_RE = re.compile(
    r"(유망|포착|테마|섹터|관련|중심|기반|대응|확대|강세|약세|"
    r"및|등|위주|측면|관점|전망|예상|추정|가능성|"
    r"산업|업종|그룹|네트워크|플랫폼|솔루션|"
    r"the|and|sector|industry|related|based)",
    re.I,
)
_MULTI_SPACE_RE = re.compile(r"\s+")
_PUNCT_TRIM_RE = re.compile(r"^[\s,.·/|—\-:]+|[\s,.·/|—\-:]+$")


def truncate_ace_token(text: Any, *, max_chars: int = MAX_ACE_TOKEN_CHARS) -> str:
    """15자 초과 시 절삭(말줄임)."""
    s = _PUNCT_TRIM_RE.sub("", str(text or "").strip())
    if not s:
        return ""
    if len(s) <= max_chars:
        return s
    if max_chars <= 1:
        return s[:max_chars]
    return s[: max_chars - 1] + "…"


def strip_ace_verbosity(text: Any) -> str:
    """서술어 제거 후 공백 정리."""
    s = str(text or "").strip()
    if not s:
        return ""
    s = _VERBOSITY_RE.sub("", s)
    s = _MULTI_SPACE_RE.sub(" ", s).strip()
    return _PUNCT_TRIM_RE.sub("", s)


def sanitize_noun_phrase(text: Any, *, max_chars: int = MAX_ACE_TOKEN_CHARS) -> str:
    """
    단답형 1~2단어 명사 — 구분자 첫 토큰만, 서술 제거, 15자 캡.
    """
    s = strip_ace_verbosity(text)
    if not s:
        return ""
    for sep in (",", "·", "/", "|", " — ", " - ", "(", ")", "[", "]"):
        if sep in s:
            s = s.split(sep)[0].strip()
    parts = [p for p in re.split(r"[\s]+", s) if p]
    if len(parts) >= 2:
        s = "/".join(parts[:2])
    elif parts:
        s = parts[0]
    s = re.sub(r"\s+", "", s)
    return truncate_ace_token(s, max_chars=max_chars)


def sanitize_theme_tokens(tokens: Any, *, max_items: int = 3) -> List[str]:
    out: List[str] = []
    if isinstance(tokens, str):
        tokens = re.split(r"[,·/|]", tokens)
    if not isinstance(tokens, list):
        return out
    for t in tokens:
        tok = sanitize_noun_phrase(t)
        if tok and tok not in out:
            out.append(tok)
        if len(out) >= max_items:
            break
    return out


def sanitize_human_insight(text: Any, *, max_chars: int = 48) -> str:
    """인사이트 1문장 압축 — 테마 나열 금지."""
    s = strip_ace_verbosity(text)
    if not s:
        return ""
    for cut in (".", "。", "!", "?"):
        if cut in s:
            s = s.split(cut)[0].strip()
            break
    return truncate_ace_token(s, max_chars=max_chars)


def sanitize_playbook_text_fields(playbook: dict[str, Any]) -> dict[str, Any]:
    """LLM/통계 playbook in-place sanitize."""
    if not isinstance(playbook, dict):
        return playbook
    pb = dict(playbook)
    pb["logic_core"] = sanitize_noun_phrase(pb.get("logic_core"))
    pb["human_insight_ko"] = sanitize_human_insight(pb.get("human_insight_ko"))
    rules = pb.get("rules")
    if isinstance(rules, list):
        cleaned = []
        for r in rules:
            if not isinstance(r, dict):
                continue
            rr = dict(r)
            if str(rr.get("type")) == "theme_token":
                rr["tokens"] = sanitize_theme_tokens(rr.get("tokens"))
                if not rr["tokens"]:
                    continue
            elif str(rr.get("type")) == "logic_match":
                rr["pattern"] = sanitize_noun_phrase(rr.get("pattern"), max_chars=24)
            cleaned.append(rr)
        pb["rules"] = cleaned
    return pb
