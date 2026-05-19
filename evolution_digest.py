"""
[Δ] 진화·튜닝 요약 — META_CHANGELOG + config 스냅샷 diff (시장 무관·글로벌).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional

from config_manager import CONFIG_SNAPSHOTS_DIR, find_latest_config_snapshot_on_or_before


def _esc(s: Any) -> str:
    t = str(s)
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _changelog_lines(meta: dict[str, Any], max_lines: int = 3) -> list[str]:
    log = meta.get("META_CHANGELOG") or []
    if not isinstance(log, list) or not log:
        return []
    recent = log[-max_lines:]
    lines: list[str] = []
    for entry in reversed(recent):
        if not isinstance(entry, dict):
            continue
        key = _esc(entry.get("key", "?"))
        reason = _esc(entry.get("reason", ""))
        at = str(entry.get("at") or "")[:19]
        old_v = entry.get("old")
        new_v = entry.get("new")
        if isinstance(old_v, (dict, list)):
            old_v = "…"
        if isinstance(new_v, (dict, list)):
            new_v = "…"
        lines.append(f"• <code>{key}</code> {_esc(old_v)}→{_esc(new_v)} <i>({reason})</i> [{at}]")
    return lines


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


def build_evolution_digest_html(
    meta: dict[str, Any],
    *,
    market: Optional[str] = None,
) -> str:
    """[Δ] 블록 HTML (비어 있으면 빈 문자열)."""
    lines = _changelog_lines(meta, max_lines=3)
    snap = _snapshot_diff_line()
    if snap:
        lines.append(snap)
    if not lines:
        return ""
    mkt_tag = f" ({market})" if market else ""
    body = "\n".join(lines[:4])
    return (
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📐 <b>[Δ] 진화·튜닝{mkt_tag}</b>\n"
        f"{body}\n"
    )
