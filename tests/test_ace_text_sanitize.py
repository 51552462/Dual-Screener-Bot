"""ace_text_sanitize — 15자 절삭·서술어 제거."""
from __future__ import annotations

import unittest

from ace_text_sanitize import (
    sanitize_human_insight,
    sanitize_noun_phrase,
    sanitize_playbook_text_fields,
    sanitize_theme_tokens,
    truncate_ace_token,
)


class TestAceTextSanitize(unittest.TestCase):
    def test_truncate_15(self) -> None:
        s = truncate_ace_token("헬스케어테크놀로지플랫폼관련유망")
        self.assertLessEqual(len(s), 15)

    def test_strip_verbose_sector(self) -> None:
        out = sanitize_noun_phrase("헬스케어 관련 유망 테마 섹터")
        self.assertLessEqual(len(out), 15)
        self.assertNotIn("유망", out)

    def test_theme_tokens_short(self) -> None:
        toks = sanitize_theme_tokens(["반도체 장비 산업 네트워크", "2차전지"])
        self.assertLessEqual(len(toks), 3)
        for t in toks:
            self.assertLessEqual(len(t), 15)

    def test_playbook_sanitize(self) -> None:
        pb = sanitize_playbook_text_fields(
            {
                "logic_core": "슈퍼노바 대장주 모멘텀 추종 전략",
                "human_insight_ko": "내일은 헬스케어와 바이오 섹터 전반을 주시하고 분할 매수한다.",
                "rules": [
                    {"type": "theme_token", "tokens": ["헬스케어테크놀로지"]},
                ],
            }
        )
        self.assertLessEqual(len(pb["logic_core"]), 15)
        self.assertLessEqual(len(pb["human_insight_ko"]), 48)


if __name__ == "__main__":
    unittest.main()
