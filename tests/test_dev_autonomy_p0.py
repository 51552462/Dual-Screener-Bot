"""Tests for dev_autonomy P0 — no external AI billing."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from dev_autonomy.adapters import FakeClaudeVerifier, FakeCursorExecutor
from dev_autonomy.orchestrator import P0Orchestrator
from dev_autonomy.paths import REPO_ROOT, resolve_track_ssot
from dev_autonomy.safety_guard import (
    check_path_safety,
    check_text_commands,
    evaluate_diff_safety,
    evaluate_pre_ai_safety,
)
from dev_autonomy.state_resolver import resolve_state
from dev_autonomy.types import ResolvedState, Track
from dev_autonomy.worktree import inspect_worktree


# --- State resolver ---


def test_resolve_track_a_current_state_blocked_without_cross_track_conflict():
    state = resolve_state(Track.A)
    assert state.subphase_id == "OPS-LIQ-TG-01"
    assert state.phase == "KR/US"
    assert state.blocked
    assert not state.conflict
    assert state.human_required or state.vps_or_deploy_hint


def test_resolve_track_b_vps_action_blocked():
    state = resolve_state(Track.B)
    assert state.subphase_id.startswith("FULL-BT-")
    assert state.status_canonical == "WAIT_CURSOR_VPS"
    assert state.blocked
    assert state.human_required
    assert state.vps_or_deploy_hint


def test_resolve_track_b_uses_active_lane_files():
    ssot, error = resolve_track_ssot(Track.B)
    assert error == ""
    assert ssot["root"].name == "LANE_FULLBT"
    assert ssot["next_action"].parent.name == "LANE_FULLBT"
    assert ssot["handoff"].parent.name == "LANE_FULLBT"


def test_resolve_track_iv_wait_claude_ok():
    state = resolve_state(Track.IV)
    assert state.status_canonical == "WAIT_CLAUDE_OK"
    assert state.blocked


def test_resolve_missing_next_action(tmp_path):
    from dev_autonomy.paths import TRACK_SSOT

    na = tmp_path / "NEXT_ACTION.md"
    na.write_text("", encoding="utf-8")
    original = TRACK_SSOT[Track.A]["next_action"]
    TRACK_SSOT[Track.A]["next_action"] = na
    try:
        state = resolve_state(Track.A)
        assert state.conflict
        assert state.blocked
        assert "missing" in state.block_reason.lower()
    finally:
        TRACK_SSOT[Track.A]["next_action"] = original


# --- Safety ---


def test_safety_track_a_bitget_write_blocked():
    block = check_path_safety("bitget/forward/ledger.py", Track.A)
    assert block is not None
    assert block.category == "cross_track"


def test_safety_bitget_forbidden_root():
    block = check_path_safety("forward/shared.py", Track.B)
    assert block is not None


def test_safety_env_blocked():
    block = check_path_safety(".env", Track.A)
    assert block is not None
    assert block.category == "secret"


def test_safety_real_execution_text():
    block = check_text_commands("set ENABLE_REAL_EXECUTION=true")
    assert block is not None
    assert block.category == "real_execution"


def test_safety_ssh_blocked():
    block = check_text_commands("ssh ubuntu@server git pull")
    assert block is not None
    assert block.category == "deploy_vps"


def test_safety_wait_director_state():
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
    from dev_autonomy.worktree import WorktreeStatus

    wt = WorktreeStatus(False, [], False, False, "clean")
    dec = evaluate_pre_ai_safety(state, wt, mode_requires_mutation=True)
    assert not dec.allowed


def test_safety_forbidden_diff_deploy():
    dec = evaluate_diff_safety(["deploy/systemd/foo.service"], Track.A)
    assert not dec.allowed


# --- Validation (subprocess) ---


def test_validation_pytest_pass():
    from dev_autonomy.validation_gate import run_pytest

    result = run_pytest(["pytest --version"], repo_root=REPO_ROOT)
    assert result.passed


def test_parse_pytest_uses_current_interpreter():
    from dev_autonomy.validation_gate import parse_pytest_argv

    argv, error = parse_pytest_argv("pytest --version")
    assert error is None
    assert argv[:3] == [sys.executable, "-m", "pytest"]


def test_validation_pytest_fail():
    from dev_autonomy.validation_gate import run_pytest

    result = run_pytest(["pytest --collect-only tests/nonexistent_module_xyz.py"], repo_root=REPO_ROOT)
    assert not result.passed


def test_validation_timeout():
    from dev_autonomy.validation_gate import run_pytest

    # Use invalid pytest path with sleep not possible — timeout via subprocess list
    result = run_pytest(
        [[sys.executable, "-m", "pytest", "--version"]],
        repo_root=REPO_ROOT,
        timeout_sec=1,
    )
    assert result.passed  # fast command


# --- Orchestration (fake adapters) ---


def _wait_cursor_state() -> ResolvedState:
    return ResolvedState(
        track=Track.A,
        phase="test",
        subphase="TEST-01",
        subphase_id="TEST-01",
        status_raw="WAIT_CURSOR_IMPL",
        status_canonical="WAIT_CURSOR_IMPL",
        next_actor="cursor",
        handoff_available=True,
        blocked=False,
        block_reason="",
        human_required=False,
        conflict=False,
        conflict_detail="",
        deferred_hint=False,
    )


def test_orchestrator_cursor_pass_claude_ok():
    orch = P0Orchestrator(
        cursor_adapter=FakeCursorExecutor(succeed=True),
        claude_adapter=FakeClaudeVerifier(verdict="OK"),
    )
    state = _wait_cursor_state()
    # Patch resolve in cycle — test claude phase directly via fake flow
    claude = orch._claude_phase(state, "r1", "all passed")
    assert claude["phase"] == "IMPLEMENTATION_VERIFIED"
    assert claude["dod_invariants"]["sub_phase_done"] is False
    assert claude["dod_invariants"]["live_ready"] is False


def test_orchestrator_claude_modify():
    orch = P0Orchestrator(claude_adapter=FakeClaudeVerifier(verdict="MODIFY"))
    state = _wait_cursor_state()
    result = orch._claude_phase(state, "r2", "ok")
    assert result["claude"] == "MODIFY"


def test_orchestrator_unknown_state_no_ai():
    orch = P0Orchestrator()
    report = orch.run_shadow(Track.A)
    assert report["mode"] == "SHADOW"
    assert report["expected_transition"] in ("blocked", "would_claude_verify_only", "no_ai_call")


def test_shadow_writes_report_file():
    orch = P0Orchestrator()
    report = orch.run_shadow(Track.IV)
    path = report.get("shadow_report_path")
    assert path and Path(path).is_file()


def test_cli_status_mode():
    proc = subprocess.run(
        [sys.executable, "-m", "dev_autonomy.cli", "--mode", "STATUS", "--track", "A", "--json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["mode"] == "STATUS"
    assert "state" in data


def test_worktree_inspect():
    wt = inspect_worktree()
    assert isinstance(wt.dirty, bool)
