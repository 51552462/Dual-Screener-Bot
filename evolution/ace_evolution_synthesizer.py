"""
AceEvolution LLM 합성 — 장중 미호출, 일일 리포트 후 1회.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Tuple

from evolution.ace_playbook_validator import parse_and_validate_llm_response, stats_only_playbook

logger = logging.getLogger(__name__)

_PROMPT_TEMPLATE = """당신은 퀀트 팩토리의 에이스 DNA 분석가다.
아래 FACT_PACK만 사용하라. 없는 수치를 지어내지 마라.

[출력 형식 — 엄수]
- logic_core: 한국어 명사 1~2단어만 (예: "반도체", "2차전지"). 15자 초과 금지. 문장·서술 금지.
- human_insight_ko: 최대 48자, 동사 1개 포함 가능한 한 줄 행동 지침. 섹터·테마 나열 금지.
- theme_token.tokens: 각 토큰 15자 이하 명사 1~2개만. GICS 전체명·"관련/유망/테마" 금지.
- GICS 섹터 이름만 반복하지 말고, 수치 밴드·짧은 테마 토큰·로직 조합의 교집합을 찾아라.

[FACT_PACK]
{fact_json}

[출력]
반드시 JSON 하나만 출력:
{{
  "logic_core": "명사1~2단어",
  "confidence": 0.0-1.0,
  "min_p_value": 0.0-1.0,
  "human_insight_ko": "48자 이내 한 줄",
  "max_stack_bonus": 0.08,
  "rules": [
    {{"id":"r1","type":"feature_band","column":"dyn_cpv","op":"gte","value":0.0,"bonus":0.03}},
    {{"id":"r2","type":"theme_token","tokens":["반도체"],"match":"any","bonus":0.02}},
    {{"id":"r3","type":"logic_match","pattern":"S4","priority_rank":-1}}
  ]
}}
규칙 최대 5개. value는 FACT_PACK 수치 범위 안에서만.
"""


def synthesize_playbook_from_facts(
    fact_pack: Dict[str, Any],
    *,
    observe_only: bool = True,
    model: str = "gemini-2.5-flash",
) -> Tuple[Dict[str, Any], str]:
    """
    LLM 시도 → 실패 시 통계-only 폴백.
    Returns (playbook, notes).
    """
    if int(fact_pack.get("n_ace") or 0) < 3:
        from evolution.ace_text_sanitize import sanitize_playbook_text_fields

        pb = sanitize_playbook_text_fields(stats_only_playbook(fact_pack, observe_only=True))
        return pb, "n_ace_lt_3_observe"

    prompt = _PROMPT_TEMPLATE.format(
        fact_json=json.dumps(fact_pack, ensure_ascii=False, indent=2, default=str)
    )

    try:
        from llm_gemini_core import LlmCallSpec, generate_text_sync

        spec = LlmCallSpec(
            task_id="ace_evolution",
            user_payload=prompt,
            model=model,
            timeout_sec=40.0,
        )
        res = generate_text_sync(spec, max_wait_sec=60.0)
        text = (res.text or "").strip()
        if not res.ok:
            pb = stats_only_playbook(fact_pack, observe_only=observe_only)
            return pb, "llm_unavailable_stats_fallback"
        if not text or "API" in text[:80] or "실패" in text[:40]:
            pb = stats_only_playbook(fact_pack, observe_only=observe_only)
            return pb, "llm_unavailable_stats_fallback"
        pb, notes = parse_and_validate_llm_response(text, fact_pack, observe_only=observe_only)
        from evolution.ace_text_sanitize import sanitize_playbook_text_fields

        return sanitize_playbook_text_fields(pb), notes
    except Exception as ex:
        logger.warning("AceEvolution LLM failed: %s", ex)
        pb = stats_only_playbook(fact_pack, observe_only=observe_only)
        return pb, f"llm_error:{str(ex)[:80]}"
