"""
[Δ] 진화·튜닝 요약 — META_CHANGELOG + config 스냅샷 diff (글로벌 · 1회 송출).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional

from config_manager import CONFIG_SNAPSHOTS_DIR, find_latest_config_snapshot_on_or_before
from tuning_digest_formatter import format_meta_changelog_telegram


def _snapshot_diff_line() -> Optional[str]:
    if not os.path.isdir(CONFIG_SNAPSHOTS_DIR):
        return None
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    p_today = find_latest_config_snapshot_on_or_before(today)
    p_yday = find_latest_config_snapshot_on_or_before(yesterday)
    if not p_today or not p_yday or p_today == p_yday:
        return None
    try:
        with open(p_today, encoding="utf-8") as f:
            cur = json.load(f)
        with open(p_yday, encoding="utf-8") as f:
            prev = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    keys = (
        "DYNAMIC_SUPERNOVA_CUTOFF",
        "DYNAMIC_ML_BOX_CUTOFF",
        "INVERSE_MODE_ACTIVE",
        "DOOMSDAY_DEFCON",
        "META_GLOBAL_KELLY_MULT",
    )
    parts: list[str] = []
    for k in keys:
        if cur.get(k) != prev.get(k):
            parts.append(k)
    if not parts:
        return None
    return f"스냅샷 Δ: {', '.join(parts[:5])}" + ("…" if len(parts) > 5 else "")


def build_global_evolution_digest_messages(meta: dict[str, Any]) -> list[str]:
    """[Δ] 글로벌 블록 — KR/US 루프 밖 1회, 켈리 Δ 많으면 다통."""
    page_groups = format_meta_changelog_telegram(meta, max_entries=5)
    snap = _snapshot_diff_line()
    if not page_groups and not snap:
        return []

    messages: list[str] = []
    for i, lines in enumerate(page_groups):
        header = (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📐 <b>[Δ] 진화·튜닝</b> <i>(글로벌 · MetaGovernor)</i>\n"
        )
        if i > 0:
            header = (
                f"\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📐 <b>[Δ] 진화·튜닝</b> <i>(글로벌 · 계속 {i + 1}/{len(page_groups)})</i>\n"
            )
        body = "\n".join(lines)
        messages.append(f"{header}{body}\n")

    if snap:
        if messages:
            messages[-1] = messages[-1].rstrip() + f"\n{snap}\n"
        else:
            messages.append(
                f"\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📐 <b>[Δ] 진화·튜닝</b> <i>(글로벌 · MetaGovernor)</i>\n"
                f"{snap}\n"
            )
    return messages


def build_global_evolution_digest_html(meta: dict[str, Any]) -> str:
    """[Δ] 글로벌 블록 — 단일 문자열 (레거시·첫 통만)."""
    msgs = build_global_evolution_digest_messages(meta)
    return msgs[0] if msgs else ""


def build_evolution_digest_html(
    meta: dict[str, Any],
    *,
    market: Optional[str] = None,
) -> str:
    """레거시 호환 — 시장 태그 없이 글로벌과 동일 본문."""
    _ = market
    return build_global_evolution_digest_html(meta)
