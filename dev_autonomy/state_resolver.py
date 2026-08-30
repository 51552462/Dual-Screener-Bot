"""Derive autonomous state from existing SSOT files (read-only)."""

from __future__ import annotations

import re
from pathlib import Path

from dev_autonomy.paths import CANONICAL_STATUSES, VPS_DEPLOY_HINTS, resolve_track_ssot
from dev_autonomy.subphase_id import normalize_subphase_id, subphase_ids_match
from dev_autonomy.types import ResolvedState, SourceRef, Track


def _source_ref(path: Path) -> SourceRef | None:
    if not path or not path.is_file():
        return None
    st = path.stat()
    return SourceRef(path=str(path), mtime=st.st_mtime, size=st.st_size)


def _read_text(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _clean_field(val: str) -> str:
    return re.sub(r"\*+", "", val).strip()


def _extract_table_field(text: str, field_name: str) -> str:
    patterns = [
        rf"\|\s*\*\*{re.escape(field_name)}\*\*\s*\|\s*(.+?)\s*\|",
        rf"\|\s*{re.escape(field_name)}\s*\|\s*(.+?)\s*\|",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return _clean_field(m.group(1))
    return ""


def _canonical_status(raw: str) -> str:
    upper = raw.upper()
    for status in CANONICAL_STATUSES:
        if status in upper:
            return status
    return "UNKNOWN"


def _handoff_has_subphase(handoff_text: str, subphase_id: str) -> bool:
    if not subphase_id:
        return False
    upper = handoff_text.upper()
    return subphase_id.upper() in upper


def _session_sync_subphase(text: str) -> str:
    m = re.search(r"진행 중 sub-phase\*\*\s*\|\s*\*\*(.+?)\*\*", text)
    if m:
        return m.group(1).strip()
    m = re.search(r"진행 중 sub-phase\s*\|\s*(.+?)\s*\|", text)
    return m.group(1).strip() if m else ""


def _session_sync_is_track_a(text: str) -> bool:
    """Only compare Track A NEXT_ACTION against a Track A session snapshot."""
    active = _extract_table_field(text, "활성 트랙").upper()
    if not active:
        return True
    markers = ("TRACK A", "KR/US", "KR+US", "주식")
    return any(marker in active for marker in markers)


def _infer_next_actor(canonical: str, human_required: bool) -> str:
    if human_required:
        return "director"
    mapping = {
        "WAIT_CURSOR_IMPL": "cursor",
        "WAIT_CURSOR_VPS": "director",
        "WAIT_CLAUDE_OK": "claude",
        "WAIT_CLAUDE_HANDOFF": "claude",
        "WAIT_DIRECTOR": "director",
        "SUB_DONE": "claude",
        "CLOSED": "none",
        "DONE": "none",
        "PARK": "none",
        "IMPLEMENTATION_VERIFIED": "director",
        "FAILED_REQUIRES_REVIEW": "director",
    }
    return mapping.get(canonical, "unknown")


def _vps_hint(text: str) -> bool:
    lower = text.lower()
    return any(h in lower for h in VPS_DEPLOY_HINTS)


def _deferred_hint(text: str) -> bool:
    lower = text.lower()
    markers = (
        "보류",
        "후순위",
        "deferred",
        "postpone",
        "재rerun 금지",
        "shrink 재rerun",
        "선행",
        "director action",
    )
    return any(m in lower for m in markers)


def resolve_state(track: Track) -> ResolvedState:
    ssot, ssot_error = resolve_track_ssot(track)
    next_action_path = ssot["next_action"]
    handoff_path = ssot["handoff"]
    session_sync_path = ssot.get("session_sync")

    next_text = _read_text(next_action_path)
    handoff_text = _read_text(handoff_path)
    sync_text = _read_text(session_sync_path) if session_sync_path else ""

    if not next_text.strip():
        return ResolvedState(
            track=track,
            phase="",
            subphase="",
            subphase_id="",
            status_raw="",
            status_canonical="UNKNOWN",
            next_actor="director",
            handoff_available=False,
            blocked=True,
            block_reason="missing NEXT_ACTION.md",
            human_required=True,
            conflict=True,
            conflict_detail="NEXT_ACTION missing",
            source_files={},
            notes=["fail closed: no NEXT_ACTION"],
        )

    subphase_raw = _extract_table_field(next_text, "sub-phase")
    if not subphase_raw:
        subphase_raw = _extract_table_field(next_text, "sub_phase")

    subphase_id = normalize_subphase_id(subphase_raw)
    status_raw = _extract_table_field(next_text, "status")
    canonical = _canonical_status(status_raw)

    phase = ""
    if track == Track.A:
        phase = "KR/US"
        if sync_text and _session_sync_is_track_a(sync_text):
            phase = _extract_table_field(sync_text, "활성 트랙") or phase

    conflict = False
    conflict_detail = ""
    if track == Track.A and sync_text and _session_sync_is_track_a(sync_text):
        sync_sub = _session_sync_subphase(sync_text)
        if sync_sub and subphase_id:
            if not subphase_ids_match(subphase_id, sync_sub):
                conflict = True
                conflict_detail = f"NEXT_ACTION id={subphase_id} vs SESSION_SYNC={normalize_subphase_id(sync_sub)}"

    if ssot_error:
        conflict = True
        conflict_detail = ssot_error

    handoff_ok = _handoff_has_subphase(handoff_text, subphase_id)
    vps_hint = _vps_hint(next_text) or _vps_hint(status_raw)
    deferred = _deferred_hint(next_text) or _deferred_hint(status_raw)

    human_required = False
    blocked = False
    block_reason = ""

    if conflict:
        canonical = "CONFLICT"
        human_required = True
        blocked = True
        block_reason = conflict_detail

    if canonical == "UNKNOWN":
        human_required = True
        blocked = True
        block_reason = block_reason or f"non-canonical status: {status_raw}"

    if canonical in {"WAIT_CURSOR_VPS", "WAIT_DIRECTOR", "CONFLICT"}:
        human_required = True
        blocked = True
        block_reason = block_reason or canonical

    if canonical == "WAIT_CLAUDE_OK":
        human_required = True
        blocked = True
        block_reason = "WAIT_CLAUDE_OK — Cursor implementation not authorized"

    if canonical != "WAIT_CURSOR_IMPL":
        blocked = True
        if not block_reason:
            block_reason = f"status {canonical} — not WAIT_CURSOR_IMPL"

    if vps_hint:
        human_required = True
        blocked = True
        block_reason = block_reason or "VPS/deploy/SSH hint in NEXT_ACTION — human required"

    if deferred:
        human_required = True
        blocked = True
        block_reason = block_reason or "deferred/postponed instruction in active NEXT_ACTION"

    if track == Track.B and ("push" in status_raw.lower() or "배포" in status_raw):
        human_required = True
        blocked = True
        block_reason = block_reason or "Track B deploy/push — not autonomous"

    source_files: dict[str, SourceRef] = {}
    for key, path in ssot.items():
        if path is None:
            continue
        ref = _source_ref(path)
        if ref:
            source_files[key] = ref

    notes: list[str] = []
    if not handoff_ok:
        notes.append("Handoff section for sub-phase not found in CLAUDE_TO_CURSOR.md")

    next_actor = _infer_next_actor(canonical, human_required)

    return ResolvedState(
        track=track,
        phase=phase,
        subphase=subphase_raw,
        subphase_id=subphase_id,
        status_raw=status_raw,
        status_canonical=canonical,
        next_actor=next_actor,
        handoff_available=handoff_ok,
        blocked=blocked,
        block_reason=block_reason,
        human_required=human_required,
        conflict=conflict,
        conflict_detail=conflict_detail,
        source_files=source_files,
        vps_or_deploy_hint=vps_hint,
        deferred_hint=deferred,
        notes=notes,
    )
