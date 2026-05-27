"""
AceEvolution 텔레그램 HTML — [에이스 로직 심층 부검] 하단 DNA 블록.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional

from evolution.ace_evolution_clamp import compute_dynamic_multiplier_bounds
from evolution.ace_text_sanitize import sanitize_human_insight, sanitize_noun_phrase


def format_ace_dna_block(playbook: Dict[str, Any]) -> str:
    if not isinstance(playbook, dict) or not playbook.get("logic_core"):
        return ""
    m = str(playbook.get("market") or "KR").upper()
    flag = "🇰🇷" if m == "KR" else "🇺🇸"
    logic = html.escape(sanitize_noun_phrase(playbook.get("logic_core")), quote=False)
    try:
        conf = float(playbook.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0.0
    ttl = int(playbook.get("ttl_days") or (1 if m == "KR" else 5))
    obs = bool(playbook.get("observe_only", True))
    mode = "관측(점수 미반영)" if obs else "활성"
    mult_min, mult_max = compute_dynamic_multiplier_bounds(playbook)
    insight = html.escape(sanitize_human_insight(playbook.get("human_insight_ko")), quote=False)
    try:
        p_val = float(playbook.get("min_p_value", 1.0))
    except (TypeError, ValueError):
        p_val = 1.0

    lines = [
        f"{flag} <b>🧬 [에이스 DNA · {m}]</b> <code>{logic}</code> · 신뢰 <b>{conf:.0%}</b> · TTL <b>{ttl}일</b> · {mode}",
        f"   배수 캡 <b>{mult_min:.2f}~{mult_max:.2f}</b> · p_min={p_val:.3f}",
    ]
    if insight:
        lines.append(f"   └ {insight}")
    return "\n".join(lines) + "\n"
