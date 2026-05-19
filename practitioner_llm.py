"""
PIL — LLM 1~2줄 실무자 브리핑 (P1 필수, 통계 폴백).
"""
from __future__ import annotations

import html
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_PIL_LLM_SYSTEM = (
    "당신은 퀀트 트레이딩 데스크의 실무자(전략 그룹) 리포트 에디터입니다. "
    "주어진 FACTS만 사용해 한국어로 1~2문장(최대 220자) 요약하세요. "
    "피처 영문 변수명(CPV, dyn_cpv 등)은 한글 라벨로 풀어 쓰거나 괄호로만 병기하세요. "
    "승자 공통점·패자 오답노트·활력(좀비 여부)을 트레이더가 바로 행동할 수 있게 서술하세요. "
    "HTML 태그는 쓰지 마세요."
)


def _stats_fallback(facts: Dict[str, Any]) -> str:
    parts = []
    if facts.get("post_mortem_stats"):
        parts.append(str(facts["post_mortem_stats"]))
    if facts.get("vitality_line"):
        parts.append(str(facts["vitality_line"]))
    if facts.get("sector_line"):
        parts.append(str(facts["sector_line"]))
    if facts.get("toxic_line"):
        parts.append(str(facts["toxic_line"]))
    return " ".join(parts)[:400] if parts else "표본 부족 — 다음 청산 후 재분석합니다."


def build_practitioner_llm_summary(
    facts: Dict[str, Any],
    *,
    force: bool = True,
) -> str:
    """
    Gemini 1~2줄 요약. 키 없거나 실패 시 통계 폴백(항상 비어있지 않은 문자열).
    force=False 이면 환경변수 PRACTITIONER_USE_LLM=0 일 때만 스킵.
    """
    if not force and os.environ.get("PRACTITIONER_USE_LLM", "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return _stats_fallback(facts)

    plain = (
        f"시장={facts.get('market')} 그룹={facts.get('group_key')} 랭크={facts.get('rank_tier')}\n"
        f"윈도우={facts.get('post_mortem_window')}일 활력={facts.get('vitality_lookback')}일\n"
        f"30일승률추세={facts.get('wr_trend_pp')} 활력점수={facts.get('vitality_score')} "
        f"좀비={facts.get('is_zombie')}\n"
        f"DNA={facts.get('post_mortem_stats')}\n"
        f"섹터={facts.get('sector_line')}\n"
        f"오답노트={facts.get('toxic_line')}\n"
        f"롤링성적={facts.get('rolling_wr')} 표본={facts.get('n_closed_window')}\n"
    )
    try:
        from llm_gemini_core import LlmCallSpec, generate_text_sync, load_gemini_api_keys

        if not load_gemini_api_keys():
            return _stats_fallback(facts)
        spec = LlmCallSpec(
            task_id="pil_brief",
            system_prompt=_PIL_LLM_SYSTEM,
            user_payload=f"[FACTS]\n{plain}\n",
            model="gemini-2.0-flash",
            max_attempts=6,
            timeout_sec=20.0,
        )
        res = generate_text_sync(spec, max_wait_sec=2.5)
        text = (res.text or "").strip()
        if not text or not res.ok:
            return _stats_fallback(facts)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()][:2]
        joined = " ".join(lines) if lines else text
        return joined[:400]
    except Exception as ex:
        logger.info("PIL LLM fallback: %s", ex)
        return _stats_fallback(facts)


def format_llm_html_line(summary: str) -> str:
    s = html.escape(str(summary or "").strip(), quote=False)
    if not s:
        return ""
    return f"🤖 <b>실무 브리핑:</b> <i>{s}</i>\n"
