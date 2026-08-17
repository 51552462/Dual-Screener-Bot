"""Repository paths and track SSOT layout."""

from __future__ import annotations

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
        "next_action": _first_existing(BITGET_WORK_PHASES, ("track_b_NEXT_ACTION.md", "NEXT_ACTION.md")),
        "handoff": _first_existing(
            BITGET_WORK_PHASES, ("track_b_CLAUDE_TO_CURSOR.md", "CLAUDE_TO_CURSOR.md")
        ),
        "outbox": _first_existing(
            BITGET_WORK_PHASES, ("track_b_CURSOR_TO_CLAUDE.md", "CURSOR_TO_CLAUDE.md")
        ),
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
    "WAIT_CLAUDE_OK",
    "WAIT_DIRECTOR",
    "SUB_DONE",
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
