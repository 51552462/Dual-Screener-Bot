"""llm_gemini_core — sanitizer, alpha fallback, prompt leak guard."""
from __future__ import annotations

import unittest

from llm_gemini_core import (
    AlphaFormulaFallbackParser,
    LlmCallSpec,
    deterministic_fallback,
    sanitize_user_visible_text,
)


class TestSanitizer(unittest.TestCase):
    def test_blocks_prompt_leak(self):
        raw = "다음은 퀀트 알파 수식이다. 매매 논리를 설명해라 rolling_std div(V)"
        self.assertEqual(sanitize_user_visible_text(raw), "")

    def test_allows_plain_korean(self):
        raw = "거래량 급증 구간에서 단기 모멘텀을 추종하는 보조 신호입니다."
        self.assertTrue(sanitize_user_visible_text(raw))


class TestAlphaParser(unittest.TestCase):
    def test_div_v_formula(self):
        line = AlphaFormulaFallbackParser.explain("motion_score(div(V, rolling_std(C,20)))", 0.06)
        self.assertIn("규칙 기반 요약", line)
        self.assertNotIn("div(", line)
        self.assertNotIn("다음은 퀀트", line)

    def test_deterministic_task(self):
        spec = LlmCallSpec(
            task_id="alpha_explain",
            user_payload="- IC: 0.05\n- 수식: add(ma(C,10), delay(V,1))",
        )
        out = deterministic_fallback(spec)
        self.assertIn("규칙 기반 요약", out)


if __name__ == "__main__":
    unittest.main()
