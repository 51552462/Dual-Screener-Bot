"""M-R0 — llm_provider_core tests."""
from __future__ import annotations

import unittest

from llm_gemini_core import LlmCallSpec, _make_cache_key
from llm_provider_core import (
    DEFAULT_LLM_PROVIDER_MODEL_MAP,
    load_llm_provider_model_map,
    resolve_llm_provider,
)


class TestLlmProviderCore(unittest.TestCase):
    def test_task_alias_ai_overseer(self):
        r = resolve_llm_provider("ai_overseer", {})
        self.assertEqual(r["task_id"], "overseer_audit")
        self.assertEqual(r["provider"], "gemini")

    def test_cache_key_includes_provider_model(self):
        a = LlmCallSpec(
            task_id="overseer_audit",
            user_payload="hello",
            provider="gemini",
            model="gemini-2.0-flash",
        )
        b = LlmCallSpec(
            task_id="overseer_audit",
            user_payload="hello",
            provider="anthropic",
            model="claude-haiku-4-5",
        )
        self.assertNotEqual(_make_cache_key(a), _make_cache_key(b))
        self.assertTrue(_make_cache_key(a).startswith("gemini:gemini-2.0-flash:"))

    def test_anthropic_disabled_falls_back_to_gemini(self):
        cfg = {
            "LLM_PROVIDER_ANTHROPIC_ENABLED": True,
            "LLM_PROVIDER_MODEL_MAP": {
                "overseer_audit": {
                    "provider": "anthropic",
                    "model": "claude-haiku-4-5",
                    "fallback_provider": "gemini",
                    "fallback_model": "gemini-2.0-flash",
                }
            },
        }
        r = resolve_llm_provider("overseer_audit", cfg)
        self.assertEqual(r["provider"], "gemini")

    def test_config_merge(self):
        cfg = {"LLM_PROVIDER_MODEL_MAP": {"weekly_action_plan": {"model": "gemini-2.5-flash"}}}
        m = load_llm_provider_model_map(cfg)
        self.assertEqual(m["weekly_action_plan"]["model"], "gemini-2.5-flash")
        self.assertIn("overseer_audit", m)


if __name__ == "__main__":
    unittest.main()
