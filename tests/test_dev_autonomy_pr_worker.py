"""Phase 3 isolated Cursor draft-PR worker tests — no paid AI or network."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dev_autonomy.adapters import FakeClaudeVerifier
from dev_autonomy.control_plane import AutonomyEnvelope, NormalizedReport
from dev_autonomy.pr_worker import PrWorker, branch_name, validate_candidate
from dev_autonomy.types import AdapterResult, ResolvedState, Track


NOW = datetime(2026, 8, 31, 1, 0, tzinfo=timezone.utc)


def _envelope(**overrides) -> AutonomyEnvelope:
    data = {
        "envelope_id": "week-1",
        "valid_from": "2026-08-30T00:00:00Z",
        "valid_until": "2026-09-06T00:00:00Z",
        "allowed_tracks": ["A", "B", "IV"],
        "allowed_actions": ["CLAUDE_REVIEW", "CURSOR_IMPLEMENT"],
        "max_tasks_per_day": 1,
        "require_pull_request": True,
        "allow_deploy": False,
        "allow_live": False,
        "allow_merge": False,
        "allow_cursor_write": True,
        "allow_branch_push": True,
        "allow_draft_pr": True,
    }
    data.update(overrides)
    return AutonomyEnvelope.from_dict(data)


def _packet(track: str = "A") -> dict:
    return {
        "schema": "dev_autonomy.job.v1",
        "job_id": f"job-{track}-1",
        "provider": "cursor",
        "action": "CURSOR_IMPLEMENT",
        "track": track,
        "execution_authorized": False,
        "report": {
            "report_id": f"report-{track}-1",
            "source": "repository_ssot",
            "track": track,
            "observed_at": "2026-08-31T00:00:00Z",
            "source_status": "WAIT_CURSOR_IMPL",
            "metrics": {},
        },
        "decision": {
            "action": "CURSOR_IMPLEMENT",
            "execution_authorized": False,
        },
        "hard_limits": {
            "allow_live": False,
            "allow_deploy": False,
            "allow_merge": False,
            "allow_ssh": False,
            "require_pull_request": True,
        },
        "pr_policy": {
            "cursor_write_requires_envelope": True,
            "branch_push_requires_envelope": True,
            "draft_pr_only": True,
            "auto_merge": False,
        },
    }


def _state(track: Track = Track.A) -> ResolvedState:
    return ResolvedState(
        track=track,
        phase="test",
        subphase="SAFE-TEST-1",
        subphase_id="SAFE-TEST-1",
        status_raw="WAIT_CURSOR_IMPL",
        status_canonical="WAIT_CURSOR_IMPL",
        next_actor="cursor",
        handoff_available=True,
        blocked=False,
        block_reason="",
        human_required=False,
        conflict=False,
        conflict_detail="",
    )


def test_pr_worker_requires_three_explicit_envelope_capabilities():
    report = NormalizedReport(
        report_id="r",
        source="test",
        track="A",
        observed_at=NOW.isoformat(),
        source_status="WAIT_CURSOR_IMPL",
    )
    assert _envelope().allows_pr_worker(report, NOW)
    assert not _envelope(allow_cursor_write=False).allows_pr_worker(report, NOW)
    assert not _envelope(allow_branch_push=False).allows_pr_worker(report, NOW)
    assert not _envelope(allow_draft_pr=False).allows_pr_worker(report, NOW)
    with pytest.raises(ValueError, match="must be a boolean"):
        _envelope(allow_draft_pr="true")


def test_candidate_rejects_wrong_role_and_tampered_limits():
    with pytest.raises(Exception, match="cannot process"):
        validate_candidate(_packet("B"), _envelope(), role="stock", now=NOW)

    packet = _packet("A")
    packet["hard_limits"]["allow_merge"] = True
    with pytest.raises(Exception, match="unsafe hard limit"):
        validate_candidate(packet, _envelope(), role="stock", now=NOW)


def test_branch_name_is_safe_and_deterministic():
    first = branch_name(Track.B, "FULL BT / weird:phase", "same-job")
    second = branch_name(Track.B, "FULL BT / weird:phase", "same-job")
    assert first == second
    assert first.startswith("autonomy/b-full-bt-weird-phase-")
    assert " " not in first
    assert len(first) < 80


def test_worker_blocks_missing_machine_allowlist_before_cursor(tmp_path):
    packet_path = tmp_path / "job.json"
    packet_path.write_text(json.dumps(_packet("A")), encoding="utf-8")
    calls: list[Path] = []
    worker = PrWorker(
        repo_root=tmp_path,
        runtime_root=tmp_path / "runtime",
        state_provider=lambda track: _state(track),
        pack_builder=lambda _state_value: {
            "handoff_section": "prose only",
            "allowed_paths": [],
            "test_commands": ["pytest tests/test_ok.py -q"],
        },
        cursor_factory=lambda path: calls.append(path),
    )
    result = worker.run_packet(
        packet_path,
        _envelope(),
        role="stock",
        publish_draft_pr=False,
        notify_telegram=False,
        now=NOW,
    )
    assert result["phase"] == "SAFETY_BLOCKED"
    assert result["reason_code"] == "ALLOWLIST_MISSING"
    assert result["claimed"] is False
    assert calls == []


def test_worker_blocks_parent_path_in_allowlist(tmp_path):
    packet_path = tmp_path / "job.json"
    packet_path.write_text(json.dumps(_packet("A")), encoding="utf-8")
    worker = PrWorker(
        repo_root=tmp_path,
        runtime_root=tmp_path / "runtime",
        state_provider=lambda track: _state(track),
        pack_builder=lambda _state_value: {
            "handoff_section": "Allowed files:\n- ../escape.py",
            "allowed_paths": ["../escape.py"],
            "test_commands": ["pytest tests/test_ok.py -q"],
        },
    )
    result = worker.run_packet(
        packet_path,
        _envelope(),
        role="stock",
        publish_draft_pr=False,
        notify_telegram=False,
        now=NOW,
    )
    assert result["reason_code"] == "ALLOWLIST_UNSAFE"


class _WritingCursor:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root

    def availability(self):
        return True, "fake cursor"

    def run_implementation(self, context_pack):
        assert context_pack["allowed_paths"] == ["src/value.py"]
        (self.repo_root / "src/value.py").write_text("VALUE = 2\n", encoding="utf-8")
        return AdapterResult(ok=True, available=True, verdict="PASS", detail="fake cursor pass")


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _make_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    remote = tmp_path / "remote.git"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    (repo / "src").mkdir()
    (repo / "tests").mkdir()
    (repo / ".gitignore").write_text(".pytest_cache/\n__pycache__/\n*.pyc\n", encoding="utf-8")
    (repo / "src/value.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repo / "tests/test_ok.py").write_text(
        "from pathlib import Path\n\ndef test_value():\n"
        "    assert Path('src/value.py').read_text() == 'VALUE = 2\\n'\n",
        encoding="utf-8",
    )
    _git(repo, "add", ".")
    _git(repo, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "base")
    _git(tmp_path, "init", "--bare", str(remote))
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-u", "origin", "main")
    return repo, remote


def test_worker_changes_only_isolated_worktree_and_opens_draft_pr(tmp_path):
    repo, remote = _make_repo(tmp_path)
    packet_path = tmp_path / "job.json"
    packet_path.write_text(json.dumps(_packet("A")), encoding="utf-8")
    gh_log = tmp_path / "gh.log"
    fake_gh = tmp_path / "gh"
    fake_gh.write_text(
        "#!/usr/bin/env bash\n"
        f"printf '%s\\n' \"$*\" >> {gh_log}\n"
        "if [[ \"$1 $2\" == \"auth status\" ]]; then exit 0; fi\n"
        "if [[ \"$1 $2\" == \"pr create\" ]]; then "
        "echo https://github.com/example/repo/pull/123; exit 0; fi\n"
        "exit 2\n",
        encoding="utf-8",
    )
    fake_gh.chmod(0o755)

    pack = {
        "track": "A",
        "subphase": "SAFE-TEST-1",
        "status": "WAIT_CURSOR_IMPL",
        "next_action_excerpt": "bounded local implementation",
        "handoff_section": "## SAFE-TEST-1\nAllowed files:\n- src/value.py\n\npytest tests/test_ok.py -q",
        "allowed_paths": ["src/value.py"],
        "test_commands": ["pytest tests/test_ok.py -q"],
    }
    telegram: list[str] = []
    worker = PrWorker(
        repo_root=repo,
        runtime_root=tmp_path / "runtime",
        state_provider=lambda track: _state(track),
        pack_builder=lambda _state_value: pack,
        cursor_factory=lambda path: _WritingCursor(path),
        claude_factory=lambda _path: FakeClaudeVerifier("OK"),
        git_executable=shutil.which("git") or "git",
        gh_executable=str(fake_gh),
        telegram_sender=lambda text: (telegram.append(text) is None, "HTTP_200"),
    )

    result = worker.run_packet(
        packet_path,
        _envelope(),
        role="stock",
        publish_draft_pr=True,
        notify_telegram=True,
        now=NOW,
    )

    assert result["phase"] == "DRAFT_PR_OPENED"
    assert result["pr_url"].endswith("/pull/123")
    assert result["auto_merge"] is False
    assert (repo / "src/value.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    assert _git(repo, "status", "--porcelain") == ""
    assert _git(remote, "show-ref", "--verify", f"refs/heads/{result['branch']}")
    assert not list((tmp_path / "runtime/worktrees").iterdir())
    assert "pr create --draft" in gh_log.read_text(encoding="utf-8")
    assert "merge" not in gh_log.read_text(encoding="utf-8")
    assert telegram and "자동 병합" in telegram[0]
