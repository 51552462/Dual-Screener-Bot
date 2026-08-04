"""
M-R0 — LLM provider SSOT (gemini|anthropic, task별 model map).

- llm_gemini_core.py 유지 · sanitize/cache/deterministic_fallback 계약 승계
- 비침투: report/narrative 전용 · trading/config write 없음
"""
from __future__ import annotations

import copy
import logging
import os
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

_TASK_ALIASES: Dict[str, str] = {
    "ai_overseer": "overseer_audit",
    "overseer_audit": "overseer_audit",
    "bitget_overseer": "bitget_overseer",
    "weekly_action_plan": "weekly_action_plan",
    "practitioner_llm": "pil_brief",
    "pil_brief": "pil_brief",
    "sentiment_miner": "sentiment_miner",
    "overseer_quality_audit": "overseer_quality_audit",
}

DEFAULT_LLM_PROVIDER_MODEL_MAP: Dict[str, Dict[str, Any]] = {
    "overseer_audit": {
        "provider": "gemini",
        "model": "gemini-2.0-flash",
        "fallback_provider": None,
        "fallback_model": None,
    },
    "weekly_action_plan": {
        "provider": "gemini",
        "model": "gemini-2.0-flash",
        "fallback_provider": None,
        "fallback_model": None,
    },
    "pil_brief": {
        "provider": "gemini",
        "model": "gemini-2.0-flash",
        "fallback_provider": None,
        "fallback_model": None,
    },
    "sentiment_miner": {
        "provider": "gemini",
        "model": "gemini-2.0-flash",
        "fallback_provider": None,
        "fallback_model": None,
    },
    "overseer_quality_audit": {
        "provider": "gemini",
        "model": "gemini-2.0-flash",
        "fallback_provider": None,
        "fallback_model": None,
    },
    "bitget_overseer": {
        "provider": "gemini",
        "model": "gemini-2.0-flash",
        "fallback_provider": None,
        "fallback_model": None,
    },
}


def _canonical_task(task: str) -> str:
    t = (task or "").strip()
    return _TASK_ALIASES.get(t, t)


def _coerce_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _parse_provider_entry(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for k in ("provider", "model", "fallback_provider", "fallback_model"):
        if k in raw:
            v = raw[k]
            out[k] = None if v in ("", "null", "none") else v
    return out


def load_llm_provider_model_map(sys_config: Optional[Mapping[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    merged = copy.deepcopy(DEFAULT_LLM_PROVIDER_MODEL_MAP)
    cfg = sys_config or {}
    raw = cfg.get("LLM_PROVIDER_MODEL_MAP")
    if isinstance(raw, dict):
        for task_key, entry in raw.items():
            canon = _canonical_task(str(task_key))
            base = merged.get(canon, {})
            base.update(_parse_provider_entry(entry))
            merged[canon] = base
    return merged


def is_anthropic_enabled(sys_config: Optional[Mapping[str, Any]] = None) -> bool:
    cfg = sys_config or {}
    if not _coerce_bool(cfg.get("LLM_PROVIDER_ANTHROPIC_ENABLED"), False):
        return False
    return bool((os.environ.get("ANTHROPIC_API_KEY") or "").strip())


def resolve_llm_provider(
    task: str,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    canon = _canonical_task(task)
    pmap = load_llm_provider_model_map(sys_config)
    entry = pmap.get(canon) or pmap.get("overseer_audit") or DEFAULT_LLM_PROVIDER_MODEL_MAP["overseer_audit"]
    provider = str(entry.get("provider") or "gemini").strip().lower()
    model = str(entry.get("model") or "gemini-2.0-flash").strip()
    fb_prov = entry.get("fallback_provider")
    fb_model = entry.get("fallback_model")
    anthropic_on = is_anthropic_enabled(sys_config)

    if provider == "anthropic" and not anthropic_on:
        if fb_prov:
            provider = str(fb_prov).strip().lower()
            model = str(fb_model or model).strip()
        else:
            provider = "gemini"

    return {
        "task_id": canon,
        "provider": provider,
        "model": model,
        "fallback_provider": (str(fb_prov).strip().lower() if fb_prov else None),
        "fallback_model": (str(fb_model).strip() if fb_model else None),
        "anthropic_enabled": anthropic_on,
    }


def _call_anthropic_sync(
    *,
    model: str,
    system_prompt: str,
    user_payload: str,
    timeout_sec: float,
) -> str:
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("anthropic_no_api_key")
    try:
        import anthropic  # type: ignore
    except ImportError as ex:
        raise RuntimeError("anthropic_sdk_missing") from ex

    client = anthropic.Anthropic(api_key=api_key, timeout=timeout_sec)
    kwargs: Dict[str, Any] = {
        "model": model,
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": user_payload}],
    }
    if system_prompt.strip():
        kwargs["system"] = system_prompt.strip()
    msg = client.messages.create(**kwargs)
    parts = getattr(msg, "content", None) or []
    texts = []
    for block in parts:
        t = getattr(block, "text", None)
        if t:
            texts.append(str(t))
    text = "\n".join(texts).strip()
    if not text:
        raise RuntimeError("anthropic_empty_response")
    return text


def generate(
    task: str,
    prompt: str,
    *,
    system_prompt: str = "",
    sys_config: Optional[Mapping[str, Any]] = None,
    timeout_sec: float = 75.0,
    max_wait_sec: float = 180.0,
    max_attempts: int = 2,
) -> Optional[str]:
    from llm_gemini_core import LlmCallSpec, LlmSource, generate_text_sync

    resolved = resolve_llm_provider(task, sys_config)
    providers_to_try = [(resolved["provider"], resolved["model"])]
    fb_p = resolved.get("fallback_provider")
    fb_m = resolved.get("fallback_model")
    if fb_p and fb_m:
        providers_to_try.append((fb_p, fb_m))

    last_text = ""
    for provider, model in providers_to_try:
        if provider == "anthropic":
            try:
                raw = _call_anthropic_sync(
                    model=model,
                    system_prompt=system_prompt,
                    user_payload=prompt,
                    timeout_sec=timeout_sec,
                )
                from llm_gemini_core import sanitize_user_visible_text

                safe = sanitize_user_visible_text(raw, task_id=resolved["task_id"])
                if safe:
                    return safe
            except Exception as ex:
                logger.info("anthropic call failed task=%s: %s", task, ex)
                continue

        spec = LlmCallSpec(
            task_id=resolved["task_id"],
            system_prompt=system_prompt,
            user_payload=prompt,
            model=model,
            provider=provider,
            timeout_sec=timeout_sec,
            max_attempts=max_attempts,
        )
        res = generate_text_sync(spec, max_wait_sec=max_wait_sec)
        last_text = res.text or ""
        if res.ok and res.source in (LlmSource.LLM, LlmSource.CACHED) and last_text:
            return last_text

    return last_text or None


def build_llm_call_spec(
    task: str,
    user_payload: str,
    *,
    system_prompt: str = "",
    sys_config: Optional[Mapping[str, Any]] = None,
    timeout_sec: float = 75.0,
    max_attempts: int = 2,
) -> "LlmCallSpec":
    from llm_gemini_core import LlmCallSpec

    resolved = resolve_llm_provider(task, sys_config)
    return LlmCallSpec(
        task_id=resolved["task_id"],
        system_prompt=system_prompt,
        user_payload=user_payload,
        model=resolved["model"],
        provider=resolved["provider"],
        timeout_sec=timeout_sec,
        max_attempts=max_attempts,
    )
