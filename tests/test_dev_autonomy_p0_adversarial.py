"""Adversarial regression tests — 00_USER_COMMUNICATION Section 5/6."""

from __future__ import annotations

import subprocess
from pathlib import Path

from dev_autonomy.context_pack import build_claude_pack
from dev_autonomy.handoff_scope import extract_allowed_paths, path_matches_allowlist
from dev_autonomy.paths import REPO_ROOT, TRACK_SSOT, Track
from dev_autonomy.safety_guard import evaluate_post_mutation_safety
from dev_autonomy.subphase_id import normalize_subphase_id, subphase_ids_match
from dev_autonomy.types import ResolvedState, Track as TrackEnum
from dev_autonomy.validation_gate import _worktree_gate
from dev_autonomy.worktree import (
    GitBaseline,
    WorktreeSnapshot,
    capture_git_baseline,
    capture_snapshot,
    diff_snapshots,
    verify_head_unchanged,
)


def _git_init_commit(repo: Path, filename: str = "README.md", content: str = "init") -> str:
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=repo, check=True)
    (repo / filename).write_text(content, encoding="utf-8")
    subprocess.run(["git", "add", filename], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True)
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


# A — HEAD mutation after baseline (simulated commit)
def test_A_head_mutation_after_cursor_fails(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_init_commit(repo)
    baseline = capture_git_baseline(repo)
    assert baseline.ok
    (repo / "README.md").write_text("changed", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "cursor hid commit"], cwd=repo, check=True, capture_output=True)
    ok, err = verify_head_unchanged(baseline, repo)
    assert not ok
    assert "HEAD mutation" in err


# B — baseline git inspection failure
def test_B_baseline_snapshot_failure_fails_closed():
    bad = WorktreeSnapshot(paths=frozenset(), ok=False, error="simulated git failure")
    baseline = GitBaseline(head="abc", snapshot=bad, ok=False, error="simulated git failure")
    result = _worktree_gate(
        baseline,
        TrackEnum.A,
        REPO_ROOT,
        handoff_section="",
        allow_dev_autonomy_writes=False,
    )
    assert not result.passed
    assert any(
        "fail" in e.lower() or "baseline" in e.lower() or "simulated" in e.lower()
        for e in result.errors
    )


# C — post-run git inspection failure
def test_C_post_snapshot_failure_fails_closed(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_init_commit(repo)
    baseline = capture_git_baseline(repo)

    def _fail_capture(repo_root=REPO_ROOT):
        return WorktreeSnapshot(paths=frozenset(), ok=False, error="post git status failed")

    from unittest.mock import patch

    with patch("dev_autonomy.validation_gate.capture_snapshot", _fail_capture):
        result = _worktree_gate(baseline, TrackEnum.A, repo, "", False)
        assert not result.passed
        assert any(
            "post" in e.lower() or "fail" in e.lower() or "git status" in e.lower()
            for e in result.errors
        )


# D — untracked allowed-path file with forbidden content
def test_D_untracked_file_forbidden_content_fails(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_init_commit(repo)
    scripts = repo / "scripts"
    scripts.mkdir()
    allowed_file = scripts / "target.py"
    allowed_file.write_text("ENABLE_REAL_EXECUTION = True\n", encoding="utf-8")
    before = capture_snapshot(repo)
    after = capture_snapshot(repo)
    delta = diff_snapshots(before, after)
    handoff = "Allowed files:\n- scripts/target.py\n"
    dec = evaluate_post_mutation_safety(
        delta.paths,
        TrackEnum.A,
        repo_root=repo,
        handoff_section=handoff,
    )
    assert not dec.allowed
    assert dec.category == "risk_content"


# E — Handoff allowlist excludes Do NOT modify paths
def test_E_handoff_allowlist_excludes_do_not_modify():
    text = (
        "Allowed: scripts/target.py\n"
        "Do NOT modify scripts/unrelated.py\n"
    )
    allowed = extract_allowed_paths(text)
    assert allowed == ["scripts/target.py"]
    assert not path_matches_allowlist("scripts/unrelated.py", allowed)
    assert path_matches_allowlist("scripts/target.py", allowed)


# F — foo.py != pkg/foo.py
def test_F_exact_path_no_suffix_match():
    allowed = ["foo.py"]
    assert path_matches_allowlist("foo.py", allowed)
    assert not path_matches_allowlist("pkg/foo.py", allowed)


# G — pytest pass then .env created fails final gate
def test_G_post_test_env_creation_fails(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_init_commit(repo)
    baseline = capture_git_baseline(repo)
    handoff = "Allowed files:\n- scripts/target.py\n"
    pre = _worktree_gate(baseline, TrackEnum.A, repo, handoff, False)
    assert pre.passed
    (repo / ".env").write_text("SECRET=1\n", encoding="utf-8")
    post = _worktree_gate(baseline, TrackEnum.A, repo, handoff, False)
    assert not post.passed
    assert any("secret" in e.lower() or ".env" in e.lower() for e in post.errors)


# H — pytest pass then HEAD change fails
def test_H_post_test_head_change_fails(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_init_commit(repo)
    baseline = capture_git_baseline(repo)
    (repo / "scripts").mkdir()
    (repo / "scripts" / "target.py").write_text("ok\n", encoding="utf-8")
    subprocess.run(["git", "add", "scripts/target.py"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "after test commit"], cwd=repo, check=True, capture_output=True)
    handoff = "Allowed files:\n- scripts/target.py\n"
    post = _worktree_gate(baseline, TrackEnum.A, repo, handoff, False)
    assert not post.passed
    assert any("HEAD" in e for e in post.errors)


# I — A-1 vs A-10
def test_I_a1_not_matches_a10():
    assert normalize_subphase_id("A-1") == "A-1"
    assert normalize_subphase_id("A-10") == "A-10"
    assert not subphase_ids_match("A-1", "A-10")


# J — Claude pack contains actual changed paths from validation evidence
def test_J_claude_pack_contains_changed_paths():
    state = ResolvedState(
        track=TrackEnum.A,
        phase="",
        subphase="T-01",
        subphase_id="T-01",
        status_raw="WAIT_CLAUDE_OK",
        status_canonical="WAIT_CLAUDE_OK",
        next_actor="claude",
        handoff_available=True,
        blocked=False,
        block_reason="",
        human_required=False,
        conflict=False,
        conflict_detail="",
    )
    pack = build_claude_pack(
        state,
        "outbox line",
        ["scripts/target.py", "tests/test_x.py"],
        "pytest exit 0",
        test_commands=["pytest tests/test_x.py -q"],
        test_exit_code=0,
        diff_excerpt="### scripts/target.py\n+ok\n",
    )
    assert pack["changed_paths"] == ["scripts/target.py", "tests/test_x.py"]
    assert pack["test_commands"] == ["pytest tests/test_x.py -q"]
    assert pack["test_exit_code"] == 0
    assert "scripts/target.py" in pack["diff_excerpt"]


def test_norm_path_preserves_dot_env():
    from dev_autonomy.worktree import _norm_path

    assert _norm_path(".env") == ".env"
    assert _norm_path("./.env") == ".env"


def test_track_b_ssot_paths_exist():
    b = TRACK_SSOT[Track.B]
    assert b["next_action"].name == "track_b_NEXT_ACTION.md"
    assert b["next_action"].is_file()
    assert b["handoff"].name == "track_b_CLAUDE_TO_CURSOR.md"
    assert b["handoff"].is_file()
