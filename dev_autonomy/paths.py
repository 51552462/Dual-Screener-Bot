"""Repository paths and track SSOT layout."""

from __future__ import annotations

import re
from pathlib import Path

from dev_autonomy.types import Track

REPO_ROOT = Path(__file__).resolve().parents[1]


def _first_existing(directory: Path, candidates: tuple[str, ...]) -> Path:
    for name in candidates:
        path = directory / name
        if path.is_file():
            return path
    return directory / candidates[0]


BITGET_WORK_PHASES = REPO_ROOT / "bitget" / "docs" / "work_phases"

TRACK_SSOT: dict[Track, dict[str, Path]] = {
    Track.A: {
        "root": REPO_ROOT / "docs" / "work_phases",
        "next_action": REPO_ROOT / "docs" / "work_phases" / "NEXT_ACTION.md",
        "handoff": REPO_ROOT / "docs" / "work_phases" / "CLAUDE_TO_CURSOR.md",
        "outbox": REPO_ROOT / "docs" / "work_phases" / "CURSOR_TO_CLAUDE.md",
        "session_sync": REPO_ROOT / "docs" / "work_phases" / "00_SESSION_SYNC.md",
        "progress": REPO_ROOT / "docs" / "work_phases" / "05_진행로그.md",
    },
    Track.B: {
        "root": BITGET_WORK_PHASES,
        # The root files are the lane dashboard/index.  resolve_track_ssot()
        # selects the one active lane and then returns that lane's files.
        "next_action": BITGET_WORK_PHASES / "NEXT_ACTION.md",
        "handoff": BITGET_WORK_PHASES / "CLAUDE_TO_CURSOR.md",
        "outbox": BITGET_WORK_PHASES / "CURSOR_TO_CLAUDE.md",
        "session_sync": _first_existing(
            BITGET_WORK_PHASES, ("track_b_00_SESSION_SYNC_POINTER.md", "00_SESSION_SYNC.md")
        ),
        "progress": _first_existing(BITGET_WORK_PHASES, ("track_b_05_진행로그.md", "05_진행로그.md")),
    },
    Track.IV: {
        "root": REPO_ROOT / "docs" / "independent_verification",
        "next_action": REPO_ROOT / "docs" / "independent_verification" / "NEXT_ACTION.md",
        "handoff": REPO_ROOT / "docs" / "independent_verification" / "CLAUDE_TO_CURSOR.md",
        "outbox": REPO_ROOT / "docs" / "independent_verification" / "CURSOR_TO_CLAUDE.md",
        "session_sync": None,
        "progress": None,
    },
}

BITGET_LANES_DIR = BITGET_WORK_PHASES / "lanes"
TERMINAL_LANE_STATUSES = {"DONE", "SUB_DONE", "CLOSED", "PARK"}


def _clean_markdown_cell(value: str) -> str:
    return re.sub(r"[*`]", "", value).strip()


def bitget_dashboard_rows(path: Path | None = None) -> list[dict[str, str]]:
    """Parse only LANE_* data rows from the Bitget dashboard."""
    dashboard = path or TRACK_SSOT[Track.B]["next_action"]
    if not dashboard.is_file():
        return []
    text = dashboard.read_text(encoding="utf-8", errors="replace")
    rows: list[dict[str, str]] = []
    for raw in text.splitlines():
        if not raw.lstrip().startswith("|"):
            continue
        cells = [_clean_markdown_cell(cell) for cell in raw.strip().strip("|").split("|")]
        if len(cells) < 3 or not cells[0].upper().startswith("LANE_"):
            continue
        status_match = re.search(r"[A-Z][A-Z0-9_]+", cells[2].upper())
        rows.append(
            {
                "lane": cells[0].upper(),
                "subphase": cells[1],
                "status": status_match.group(0) if status_match else "UNKNOWN",
            }
        )
    return rows


def resolve_track_ssot(
    track: Track,
    *,
    subphase_id: str = "",
) -> tuple[dict[str, Path], str]:
    """Return concrete SSOT paths and a fail-closed resolution error."""
    ssot = dict(TRACK_SSOT[track])
    if track != Track.B:
        return ssot, ""

    rows = bitget_dashboard_rows(ssot["next_action"])
    if subphase_id:
        wanted = subphase_id.strip().upper()
        candidates = [row for row in rows if row["subphase"].strip().upper() == wanted]
    else:
        candidates = [row for row in rows if row["status"] not in TERMINAL_LANE_STATUSES]

    if len(candidates) != 1:
        detail = "no active Bitget lane" if not candidates else "multiple active Bitget lanes"
        return ssot, detail

    lane = candidates[0]["lane"]
    lane_root = BITGET_LANES_DIR / lane
    next_action = lane_root / "NEXT_ACTION.md"
    if not next_action.is_file():
        return ssot, f"active Bitget lane missing NEXT_ACTION: {lane}"

    ssot["dashboard"] = ssot["next_action"]
    ssot["root"] = lane_root
    ssot["next_action"] = next_action
    for key, filename in (
        ("handoff", "CLAUDE_TO_CURSOR.md"),
        ("outbox", "CURSOR_TO_CLAUDE.md"),
    ):
        candidate = lane_root / filename
        if candidate.is_file():
            ssot[key] = candidate
    return ssot, ""


AUTONOMY_DATA_DIR = REPO_ROOT / "data" / "dev_autonomy"
AUDIT_LOG_PATH = AUTONOMY_DATA_DIR / "audit_log.jsonl"
LOCK_PATH = AUTONOMY_DATA_DIR / ".orchestrator.lock"
SHADOW_REPORT_DIR = AUTONOMY_DATA_DIR / "shadow_reports"

AUTONOMY_WRITE_PREFIXES = (
    "dev_autonomy/",
    "data/dev_autonomy/",
    "tests/test_dev_autonomy",
)

BITGET_FORBIDDEN_ROOT_WRITES = (
    "forward/",
    "factory_pipelines.py",
    "system_auto_pilot.py",
    "performance_budget_governor.py",
    "deploy/systemd/dante-",
)

CANONICAL_STATUSES = (
    "WAIT_CLAUDE_HANDOFF",
    "WAIT_CURSOR_IMPL",
    "WAIT_CURSOR_VPS",
    "WAIT_CLAUDE_OK",
    "WAIT_DIRECTOR",
    "SUB_DONE",
    "CLOSED",
    "DONE",
    "PARK",
    "IMPLEMENTATION_VERIFIED",
    "FAILED_REQUIRES_REVIEW",
    "UNKNOWN",
    "CONFLICT",
)

DOD_IMPLEMENTATION_VERIFIED = "IMPLEMENTATION_VERIFIED"
DOD_SUB_PHASE_DONE = "SUB_PHASE_DONE"
DOD_LIVE_READY = "LIVE_READY"

VPS_DEPLOY_HINTS = (
    "vps",
    "ssh",
    "git pull",
    "git push",
    "update_factory",
    "update_bitget",
    "서버",
    "배포",
    "deploy",
)
