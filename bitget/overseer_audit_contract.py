"""
M-R0-4 — Bitget overseer parity contract (adapter by interface, no root import).
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


def gather_audit_input(
    *,
    sys_config: Mapping[str, Any],
    meta: Mapping[str, Any],
    facts: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "sys_config": dict(sys_config),
        "meta": dict(meta),
        "facts": dict(facts),
        "track": "BITGET",
    }


def build_audit_narrative(
    audit_input: Mapping[str, Any],
    *,
    llm_text: Optional[str] = None,
    degraded: bool = False,
    degraded_reason: str = "",
) -> Tuple[str, Dict[str, Any]]:
    body = "<b>[Bitget Rules Audit]</b>\n"
    if degraded:
        body += f"⚠️ 규칙 기반 감사 (LLM 비활성 — {degraded_reason})\n"
    if llm_text:
        body += llm_text + "\n"
    record = {
        "log_key": "OVERSEER_QUALITY_LOG",
        "source": "llm" if llm_text and not degraded else "deterministic",
        "degraded": degraded,
    }
    return body, record


def detect_bitget_audit_anomalies(audit_input: Mapping[str, Any]) -> List[Dict[str, Any]]:
    return []
