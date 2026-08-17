"""P0-LOCAL-02 safety hardening regression tests."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from dev_autonomy.orchestrator import P0Orchestrator
from dev_autonomy.paths import LOCK_PATH, REPO_ROOT
from dev_autonomy.process_lock import OrchestratorLockError, orchestrator_lock
from dev_autonomy.safety_guard import (
    check_path_safety,
    check_text_commands,
    evaluate_post_mutation_safety,
    evaluate_pre_ai_safety,
)
from dev_autonomy.subphase_id import normalize_subphase_id, subphase_ids_match
from dev_autonomy.types import ResolvedState, Track
from dev_autonomy.validation_gate import parse_pytest_argv, run_pytest
from dev_autonomy.worktree import WorktreeSnapshot, diff_snapshots, verify_head_unchanged, GitBaseline


# 1 shell injection
def test_shell_injection_blocked_before_execution(tmp_path):
    argv, err = parse_pytest_argv("pytest --version; echo pwned")
    assert err is not None
    side = tmp_path / "side_effect.txt"
    result = run_pytest([f"pytest --version; echo pwned > {side}"], repo_root=REPO_ROOT)
    assert not result.passed
    assert not side.exists()


# 2 untracked forbidden deploy file
def test_untracked_forbidden_deploy_file_fails():
    before = WorktreeSnapshot(paths=frozenset(), ok=True)
    after = WorktreeSnapshot(
        paths=frozenset(["deploy/new_prod_unit.service"]),
        ok=True,
    )
    changed = diff_snapshots(before, after).paths
    dec = evaluate_post_mutation_safety(changed, Track.A, handoff_section="")
    assert not dec.allowed
    assert dec.category == "production"


# 3 Track A -> bitget
def test_track_a_bitget_change_fails():
    dec = evaluate_post_mutation_safety(["bitget/forward/ledger.py"], Track.A)
    assert not dec.allowed


# 4 Track B -> root forward
def test_track_b_root_forward_fails():
    dec = evaluate_post_mutation_safety(["forward/shared.py"], Track.B)
    assert not dec.allowed


# 5 Track IV unauthorized path
def test_track_iv_unauthorized_impl_path_fails():
    dec = evaluate_post_mutation_safety(["forward/ledger.py"], Track.IV)
    assert not dec.allowed


# 6 WAIT_DIRECTOR -> cursor call count 0
def test_wait_director_zero_cursor_calls():
    from dev_autonomy.worktree import WorktreeStatus

    state = ResolvedState(
        track=Track.A,
        phase="",
        subphase="X",
        subphase_id="X",
        status_raw="WAIT_DIRECTOR",
        status_canonical="WAIT_DIRECTOR",
        next_actor="director",
        handoff_available=True,
        blocked=True,
        block_reason="WAIT_DIRECTOR",
        human_required=True,
        conflict=False,
        conflict_detail="",
        deferred_hint=False,
    )
    from dev_autonomy.adapters import FakeCursorExecutor

    fake = FakeCursorExecutor()
    orch = P0Orchestrator(cursor_adapter=fake, enable_autonomous_write=True)
    wt = WorktreeStatus(False, [], False, False, "clean")
    dec = evaluate_pre_ai_safety(state, wt, mode_requires_mutation=True)
    assert not dec.allowed
    assert orch.cursor_call_count == 0


# 7 VPS/deploy task -> cursor call count 0
def test_vps_hint_blocks_pre_ai():
    from dev_autonomy.worktree import WorktreeStatus

    state = ResolvedState(
        track=Track.A,
        phase="",
        subphase="Y",
        subphase_id="Y",
        status_raw="WAIT_CURSOR_IMPL",
        status_canonical="WAIT_CURSOR_IMPL",
        next_actor="cursor",
        handoff_available=True,
        blocked=True,
        block_reason="vps",
        human_required=True,
        conflict=False,
        conflict_detail="",
        vps_or_deploy_hint=True,
        deferred_hint=False,
    )
    wt = WorktreeStatus(False, [], False, False, "clean")
    dec = evaluate_pre_ai_safety(state, wt, mode_requires_mutation=True)
    assert not dec.allowed


# 8 secret/env
def test_secret_env_modification_fails():
    dec = check_path_safety(".env.production", Track.A)
    assert dec is not None
    assert dec.category == "secret"


# 9 risk-critical diff content
def test_risk_critical_diff_content_fails(tmp_path):
    risky = tmp_path / "scripts" / "foo.py"
    risky.parent.mkdir(parents=True)
    risky.write_text("ENABLE_REAL_EXECUTION = True\n", encoding="utf-8")
    rel = risky.relative_to(tmp_path).as_posix()
    dec = evaluate_post_mutation_safety([rel], Track.A, repo_root=tmp_path)
    assert not dec.allowed
    assert dec.category == "risk_content"


# 10 deferred task
def test_deferred_task_fails():
    from dev_autonomy.worktree import WorktreeStatus

    state = ResolvedState(
        track=Track.A,
        phase="",
        subphase="Z",
        subphase_id="Z",
        status_raw="WAIT_CURSOR_IMPL",
        status_canonical="WAIT_CURSOR_IMPL",
        next_actor="cursor",
        handoff_available=True,
        blocked=True,
        block_reason="deferred",
        human_required=True,
        conflict=False,
        conflict_detail="",
        deferred_hint=True,
    )
    wt = WorktreeStatus(False, [], False, False, "clean")
    dec = evaluate_pre_ai_safety(state, wt, mode_requires_mutation=True)
    assert not dec.allowed
    assert dec.category == "deferred"


# 11 git commit attempt
def test_git_commit_command_blocked():
    dec = check_text_commands("please run git commit -m 'autonomous'")
    assert dec is not None
    assert dec.category == "deploy_vps"


# 12 active PID lock -> second controller fails
def test_active_pid_lock_blocks_second_controller():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCK_PATH.write_text(f"{os.getpid()}\n", encoding="utf-8")
    try:
        with pytest.raises(OrchestratorLockError):
            with orchestrator_lock():
                pass
    finally:
        if LOCK_PATH.exists():
            owner = int(LOCK_PATH.read_text().strip())
            if owner == os.getpid():
                LOCK_PATH.unlink(missing_ok=True)


# 13 canonical sub-phase ID normalization
def test_canonical_subphase_id_normalization():
    assert normalize_subphase_id("BULL-RECENCY-01 (iteration 3)") == "BULL-RECENCY-01"
    assert subphase_ids_match("BULL-RECENCY-01", "BULL-RECENCY-01 이터레이션 3")
    assert not subphase_ids_match("A-1", "A-10")
    assert normalize_subphase_id("A-1") == "A-1"
    assert normalize_subphase_id("A-10") == "A-10"


# 14 IMPLEMENTATION_VERIFIED not Done/live
def test_implementation_verified_not_done():
    from dev_autonomy.adapters import FakeClaudeVerifier
    from dev_autonomy.orchestrator import P0Orchestrator

    orch = P0Orchestrator(claude_adapter=FakeClaudeVerifier("OK"))
    state = ResolvedState(
        track=Track.A,
        phase="t",
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
    result = orch._claude_phase(state, "r", "ok")
    inv = result.get("dod_invariants", {})
    assert inv.get("implementation_verified") is True
    assert inv.get("sub_phase_done") is False
    assert inv.get("live_ready") is False


# 15 backtest pass does not cause promotion
def test_backtest_pass_not_live_promotion():
    from dev_autonomy.adapters import FakeClaudeVerifier
    from dev_autonomy.orchestrator import P0Orchestrator

    orch = P0Orchestrator(claude_adapter=FakeClaudeVerifier("OK"))
    state = ResolvedState(
        track=Track.A,
        phase="t",
        subphase="RP-1",
        subphase_id="RP-1",
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
    result = orch._claude_phase(state, "r", "pytest PASS backtest PASS")
    assert result["phase"] == "IMPLEMENTATION_VERIFIED"
    assert result["dod_invariants"]["backtest_pass_is_not_live_promotion"] is True
    assert result["dod_invariants"]["live_ready"] is False


# autonomous write disabled by default
def test_autonomous_write_disabled_by_default():
    orch = P0Orchestrator()
    result = orch.run_safe_single_cycle(Track.A)
    assert result["phase"] in ("SAFETY_BLOCKED", "WAIT_DIRECTOR")
    assert "AUTONOMOUS_WRITE_DISABLED" in result.get("reason", "") or result.get("phase") == "WAIT_DIRECTOR"
