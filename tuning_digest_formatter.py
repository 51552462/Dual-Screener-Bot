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


def _collect_group_kelly_mult_changes(
    old: Any,
    new: Any,
    *,
    mult_epsilon: float = 0.02,
) -> tuple[list[tuple[str, float, float]], int, bool]:
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
    struct_only = bool(not changed and o_map != n_map)
    return changed, hidden_neutral, struct_only


def format_group_kelly_mult_diff(
    old: Any,
    new: Any,
    *,
    mult_epsilon: float = 0.02,
    max_show: int = 8,
    page: int = 0,
    page_size: int = 15,
    include_summary: bool = True,
    include_footer: bool = True,
) -> list[str]:
    """실제 변경·1.0 이탈 그룹만 불릿. page>0 이면 연속 페이지(요약·푸터 생략 가능)."""
    changed, hidden_neutral, struct_only = _collect_group_kelly_mult_changes(
        old, new, mult_epsilon=mult_epsilon
    )
    lines: list[str] = []
    if struct_only:
        lines.append("<i>• 그룹 켈리 배율: 구조 변경(세부 Δ 없음)</i>")
        return lines
    if not changed:
        return lines

    start = page * page_size
    end = start + page_size
    chunk = changed[start:end]
    if not chunk:
        return lines

    if include_summary and page == 0:
        lines.append(f"<i>• 그룹 켈리 배율 변경 <b>{len(changed)}</b>건</i>")
    elif page > 0:
        lines.append(
            f"<i>• 그룹 켈리 배율 변경 <b>{len(changed)}</b>건 "
            f"(<b>{start + 1}–{min(end, len(changed))}</b>)</i>"
        )

    for k, ov, nv in chunk:
        label = _esc(k[:48])
        lines.append(f"  - {label}: <b>{_fmt_mult(ov)}</b> ➔ <b>{_fmt_mult(nv)}</b>")

    if include_footer and hidden_neutral:
        on_last_page = end >= len(changed)
        if on_last_page:
            lines.append(
                f"  <i>(배율 1.0±{mult_epsilon:g} · 무변경 {hidden_neutral}개 그룹 생략)</i>"
            )
    return lines


def format_group_kelly_mult_diff_pages(
    old: Any,
    new: Any,
    *,
    mult_epsilon: float = 0.02,
    page_size: int = 15,
) -> list[list[str]]:
    """텔레그램 연속 송출용 — 변경 건 전체를 page_size 단위로 분할."""
    changed, hidden_neutral, struct_only = _collect_group_kelly_mult_changes(
        old, new, mult_epsilon=mult_epsilon
    )
    if struct_only:
        return [["<i>• 그룹 켈리 배율: 구조 변경(세부 Δ 없음)</i>"]]
    if not changed:
        return []
    n_pages = (len(changed) + page_size - 1) // page_size
    pages: list[list[str]] = []
    for p in range(n_pages):
        pages.append(
            format_group_kelly_mult_diff(
                old,
                new,
                mult_epsilon=mult_epsilon,
                page=p,
                page_size=page_size,
                include_summary=True,
                include_footer=(p == n_pages - 1),
            )
        )
    return [pg for pg in pages if pg]


def format_changelog_entry_html(
    entry: dict[str, Any],
    *,
    mult_epsilon: float = 0.02,
    kelly_page_size: int = 15,
) -> list[list[str]]:
    """META_GROUP_KELLY_MULT 는 페이지 리스트, 그 외는 단일 페이지."""
    if not isinstance(entry, dict):
        return []
    key = str(entry.get("key") or "?")
    reason = _esc(entry.get("reason", ""))
    at = str(entry.get("at") or "")[:19]
    old_v = entry.get("old")
    new_v = entry.get("new")

    if key == "META_GROUP_KELLY_MULT":
        pages = format_group_kelly_mult_diff_pages(
            old_v, new_v, mult_epsilon=mult_epsilon, page_size=kelly_page_size
        )
        if not pages:
            return []
        head = f"<i>• <code>{_esc(key)}</code> ({reason}) [{at}]</i>"
        out: list[list[str]] = []
        for i, body in enumerate(pages):
            out.append(([head] if i == 0 else []) + body)
        return out

    if isinstance(old_v, (dict, list)) or isinstance(new_v, (dict, list)):
        return [[f"• <code>{_esc(key)}</code> <i>구조 변경</i> ({reason}) [{at}]"]]
    return [[
        f"• <code>{_esc(key)}</code> {_esc(old_v)} ➔ {_esc(new_v)} "
        f"<i>({reason})</i> [{at}]"
    ]]


def format_meta_changelog_telegram(
    meta: Mapping[str, Any],
    *,
    max_entries: int = 5,
    mult_epsilon: float = 0.02,
    kelly_page_size: int = 15,
) -> list[list[str]]:
    """CHANGELOG 항목별 페이지 묶음 — 텔레그램 다통 송출용."""
    log = meta.get("META_CHANGELOG") or []
    if not isinstance(log, list) or not log:
        return []
    recent = log[-max_entries:]
    pages: list[list[str]] = []
    for entry in reversed(recent):
        pages.extend(
            format_changelog_entry_html(
                entry if isinstance(entry, dict) else {},
                mult_epsilon=mult_epsilon,
                kelly_page_size=kelly_page_size,
            )
        )
    return pages


def format_meta_changelog_telegram_flat(
    meta: Mapping[str, Any],
    **kwargs: Any,
) -> list[str]:
    """레거시 단일 리스트 (테스트·덤프용)."""
    flat: list[str] = []
    for page in format_meta_changelog_telegram(meta, **kwargs):
        flat.extend(page)
    return flat

