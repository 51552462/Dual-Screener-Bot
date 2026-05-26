"""
[Δ] 진화·튜닝 — META_CHANGELOG 인간 readable HTML (유니코드 덤프·1.0 숨김).
"""
from __future__ import annotations

import html
import json
from typing import Any, Mapping, Optional


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


def _coerce_mult_map(value: Any) -> dict[str, float]:
    """changelog old/new — dict 또는 레거시 JSON str."""
    if value is None:
        return {}
    if isinstance(value, dict):
        raw = value
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
            raw = parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    else:
        return {}
    out: dict[str, float] = {}
    for k, v in raw.items():
        try:
            out[str(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _fmt_mult(x: float) -> str:
    return f"{x:.2f}"


def format_group_kelly_mult_diff(
    old: Any,
    new: Any,
    *,
    mult_epsilon: float = 0.02,
    max_show: int = 8,
) -> list[str]:
    """실제 변경·1.0 이탈 그룹만 불릿."""
    o_map = _coerce_mult_map(old)
    n_map = _coerce_mult_map(new)
    keys = sorted(set(o_map) | set(n_map))
    changed: list[tuple[str, float, float]] = []
    hidden_neutral = 0
    for k in keys:
        ov = float(o_map.get(k, 1.0))
        nv = float(n_map.get(k, 1.0))
        if abs(ov - nv) < 1e-9:
            if abs(nv - 1.0) <= mult_epsilon:
                hidden_neutral += 1
            continue
        if abs(ov - 1.0) <= mult_epsilon and abs(nv - 1.0) <= mult_epsilon:
            hidden_neutral += 1
            continue
        changed.append((k, ov, nv))
    lines: list[str] = []
    if not changed:
        if o_map != n_map:
            lines.append("<i>• 그룹 켈리 배율: 구조 변경(세부 Δ 없음)</i>")
        return lines
    lines.append(f"<i>• 그룹 켈리 배율 변경 <b>{len(changed)}</b>건</i>")
    for k, ov, nv in changed[:max_show]:
        label = _esc(k[:48])
        lines.append(f"  - {label}: <b>{_fmt_mult(ov)}</b> ➔ <b>{_fmt_mult(nv)}</b>")
    if len(changed) > max_show:
        lines.append(f"  <i>… 외 {len(changed) - max_show}건</i>")
    if hidden_neutral:
        lines.append(f"  <i>(배율 1.0±{mult_epsilon:g} · 무변경 {hidden_neutral}개 그룹 생략)</i>")
    return lines


def format_changelog_entry_html(
    entry: dict[str, Any],
    *,
    mult_epsilon: float = 0.02,
) -> list[str]:
    if not isinstance(entry, dict):
        return []
    key = str(entry.get("key") or "?")
    reason = _esc(entry.get("reason", ""))
    at = str(entry.get("at") or "")[:19]
    old_v = entry.get("old")
    new_v = entry.get("new")

    if key == "META_GROUP_KELLY_MULT":
        body = format_group_kelly_mult_diff(
            old_v, new_v, mult_epsilon=mult_epsilon
        )
        if not body:
            return []
        head = f"<i>• <code>{_esc(key)}</code> ({reason}) [{at}]</i>"
        return [head] + body

    if isinstance(old_v, (dict, list)) or isinstance(new_v, (dict, list)):
        return [
            f"• <code>{_esc(key)}</code> <i>구조 변경</i> ({reason}) [{at}]"
        ]
    return [
        f"• <code>{_esc(key)}</code> {_esc(old_v)} ➔ {_esc(new_v)} "
        f"<i>({reason})</i> [{at}]"
    ]


def format_meta_changelog_telegram(
    meta: Mapping[str, Any],
    *,
    max_entries: int = 5,
    mult_epsilon: float = 0.02,
) -> list[str]:
    log = meta.get("META_CHANGELOG") or []
    if not isinstance(log, list) or not log:
        return []
    recent = log[-max_entries:]
    lines: list[str] = []
    for entry in reversed(recent):
        lines.extend(
            format_changelog_entry_html(
                entry if isinstance(entry, dict) else {},
                mult_epsilon=mult_epsilon,
            )
        )
    return lines

