"""Build minimal context packs for Cursor and Claude adapters."""

from __future__ import annotations

import re
from pathlib import Path

from dev_autonomy.handoff_scope import extract_allowed_paths
from dev_autonomy.paths import resolve_track_ssot
from dev_autonomy.types import ResolvedState, Track


def _read(path: Path, max_chars: int = 12000) -> str:
    if not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) > max_chars:
        return text[:max_chars] + "\n...[truncated]"
    return text


def _extract_handoff_section(handoff_text: str, subphase: str) -> str:
    if not subphase:
        return handoff_text[:8000]
    sid = subphase
    pattern = re.compile(
        rf"(##\s*.*{re.escape(sid)}.*?\n)(.*?)(?=\n## |\Z)",
        re.DOTALL | re.IGNORECASE,
    )
    m = pattern.search(handoff_text)
    if m:
        return m.group(0)[:12000]
    return handoff_text[:8000]


def _infer_test_commands(handoff_section: str, subphase: str) -> list[str]:
    cmds: list[str] = []
    pytest_matches = re.findall(r"pytest\s+[^\n`]+", handoff_section)
    for raw in pytest_matches:
        cmd = raw.strip().rstrip(".")
        if cmd not in cmds:
            cmds.append(cmd)
    if not cmds and subphase:
        slug = subphase.lower().replace("-", "_")
        cmds.append(f"pytest tests/ -k {slug} -q --tb=short")
    return cmds[:3]


def build_cursor_pack(state: ResolvedState) -> dict:
    ssot, _ = resolve_track_ssot(state.track, subphase_id=state.subphase_id)
    handoff_full = _read(ssot["handoff"])
    handoff_section = _extract_handoff_section(handoff_full, state.subphase_id or state.subphase)
    next_action = _read(ssot["next_action"], 4000)

    forbidden = [
        "bitget/** (Track A)",
        "deploy/, systemd, cron",
        ".env, secrets",
        "production risk modules unless Handoff explicitly allows",
        "git push, ssh, VPS operations",
    ]
    if state.track == Track.B:
        forbidden = [
            "root forward/, factory_pipelines, performance_budget_governor",
            "deploy/systemd/dante-* (stock)",
            ".env, secrets",
            "git push without director approval",
        ]

    tests = _infer_test_commands(handoff_section, state.subphase)
    allowed_paths = extract_allowed_paths(handoff_section) or []

    return {
        "track": state.track.value,
        "subphase": state.subphase,
        "status": state.status_canonical,
        "next_action_excerpt": next_action,
        "handoff_section": handoff_section,
        "allowed_paths": allowed_paths,
        "forbidden_paths": forbidden,
        "test_commands": tests,
        "rules": [
            "one sub-phase only",
            "targeted diff only",
            "no scope expansion",
            "no git push / deploy / VPS",
        ],
    }


def build_claude_pack(
    state: ResolvedState,
    cursor_outbox_excerpt: str,
    changed_paths: list[str],
    validation_summary: str,
    test_commands: list[str] | None = None,
    test_exit_code: int | None = None,
    diff_excerpt: str = "",
) -> dict:
    ssot, _ = resolve_track_ssot(state.track, subphase_id=state.subphase_id)
    handoff_full = _read(ssot["handoff"])
    handoff_section = _extract_handoff_section(handoff_full, state.subphase_id or state.subphase)
    outbox = cursor_outbox_excerpt or _read(ssot["outbox"], 6000)

    return {
        "track": state.track.value,
        "subphase": state.subphase,
        "subphase_id": state.subphase_id,
        "handoff_section": handoff_section,
        "cursor_outbox_excerpt": outbox,
        "changed_paths": changed_paths or [],
        "diff_excerpt": diff_excerpt[:4000] if diff_excerpt else "",
        "validation_summary": validation_summary,
        "test_commands": test_commands or [],
        "test_exit_code": test_exit_code,
        "role": "read-only verifier — no production code edits",
        "verdict_options": ["OK", "MODIFY", "REJECT"],
    }
