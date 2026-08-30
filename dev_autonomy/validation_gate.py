"""Deterministic validation gate — no shell=True, pytest argv only, post-test re-check."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from dev_autonomy.paths import REPO_ROOT
from dev_autonomy.safety_guard import evaluate_post_mutation_safety
from dev_autonomy.types import Track, ValidationResult
from dev_autonomy.worktree import (
    GitBaseline,
    capture_git_baseline,
    capture_snapshot,
    diff_snapshots,
    verify_head_unchanged,
)

SHELL_METACHARACTERS = (";", "&&", "||", "|", ">", "<", "`", "$(")


def _has_shell_metachar(cmd: str) -> bool:
    return any(m in cmd for m in SHELL_METACHARACTERS)


def parse_pytest_argv(cmd: str) -> Tuple[Optional[List[str]], Optional[str]]:
    if not cmd or not cmd.strip():
        return None, "empty command"
    if _has_shell_metachar(cmd):
        return None, f"shell metacharacter blocked in: {cmd!r}"
    try:
        parts = shlex.split(cmd, posix=os.name != "nt")
    except ValueError as exc:
        return None, f"shlex parse error: {exc}"

    if not parts:
        return None, "empty argv"

    if parts[0] == "pytest":
        # Do not depend on PATH having a pytest console script.  The
        # orchestrator must validate inside the interpreter environment that
        # launched it (venv, container, or system Python).
        return [sys.executable, "-m", "pytest"] + parts[1:], None

    if parts[0] in ("python", sys.executable) or parts[0].endswith("python.exe"):
        if len(parts) >= 3 and parts[1] == "-m" and parts[2] == "pytest":
            return parts, None
        return None, f"only pytest via python -m pytest allowed: {cmd!r}"

    if parts[0].endswith("pytest") or parts[0].endswith("pytest.exe"):
        return parts, None

    return None, f"command not allowed (pytest only): {cmd!r}"


def run_pytest(
    commands: list,
    repo_root: Path = REPO_ROOT,
    timeout_sec: int = 300,
) -> ValidationResult:
    if not commands:
        return ValidationResult(
            passed=False,
            exit_code=-1,
            commands=[],
            errors=["no test commands specified"],
        )

    summaries: list[str] = []
    exit_codes: list[int] = []
    errors: list[str] = []
    all_ok = True
    labels: list[str] = []

    for cmd in commands:
        if isinstance(cmd, (list, tuple)):
            argv = list(cmd)
            label = " ".join(argv)
            if argv and argv[0] not in ("pytest", sys.executable) and not str(argv[0]).endswith("pytest"):
                if len(argv) < 4 or argv[1] != "-m" or argv[2] != "pytest":
                    all_ok = False
                    errors.append(f"argv command not pytest: {label}")
                    exit_codes.append(-1)
                    summaries.append(f"{label} -> BLOCKED (not pytest)")
                    continue
        else:
            argv, err = parse_pytest_argv(str(cmd))
            label = str(cmd)
            if err:
                all_ok = False
                errors.append(err)
                exit_codes.append(-1)
                summaries.append(f"{label} -> BLOCKED ({err})")
                continue

        labels.append(label)
        try:
            proc = subprocess.run(
                argv,
                cwd=repo_root,
                shell=False,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
                check=False,
            )
        except subprocess.TimeoutExpired:
            all_ok = False
            summaries.append(f"{label} -> TIMEOUT after {timeout_sec}s")
            exit_codes.append(-9)
            errors.append(f"TIMEOUT: {label}")
            continue

        exit_codes.append(proc.returncode)
        out = proc.stdout or ""
        err_out = proc.stderr or ""
        tail = (out[-1500:] if len(out) > 1500 else out) + (err_out[-1500:] if len(err_out) > 1500 else err_out)
        summaries.append(f"{label} -> exit {proc.returncode}\n{tail}")
        if proc.returncode != 0:
            all_ok = False
            errors.append(f"exit {proc.returncode}: {label}")

    return ValidationResult(
        passed=all_ok,
        exit_code=0 if all_ok else (exit_codes[-1] if exit_codes else -1),
        commands=labels,
        summaries=summaries,
        errors=errors if not all_ok else [],
    )


def _worktree_gate(
    baseline: GitBaseline,
    track: Track,
    repo_root: Path,
    handoff_section: str,
    allow_dev_autonomy_writes: bool,
) -> ValidationResult:
    """HEAD + snapshot delta + mutation safety — fail closed on any git error."""
    if not baseline.ok:
        return ValidationResult(
            passed=False,
            exit_code=-1,
            commands=[],
            errors=[baseline.error or "invalid git baseline"],
        )

    head_ok, head_err = verify_head_unchanged(baseline, repo_root)
    if not head_ok:
        return ValidationResult(
            passed=False,
            exit_code=-1,
            commands=[],
            errors=[head_err],
        )

    after = capture_snapshot(repo_root)
    if not after.ok:
        return ValidationResult(
            passed=False,
            exit_code=-1,
            commands=[],
            errors=[after.error],
        )

    delta = diff_snapshots(baseline.snapshot, after)
    if not delta.ok:
        return ValidationResult(
            passed=False,
            exit_code=-1,
            commands=[],
            errors=[delta.error],
        )

    changed = delta.paths
    diff_safety = evaluate_post_mutation_safety(
        changed,
        track,
        repo_root=repo_root,
        handoff_section=handoff_section,
        allow_dev_autonomy_writes=allow_dev_autonomy_writes,
    )
    if not diff_safety.allowed:
        return ValidationResult(
            passed=False,
            exit_code=-1,
            commands=[],
            forbidden_paths=[p for p in changed if p],
            changed_paths=changed,
            errors=[diff_safety.reason],
        )

    return ValidationResult(
        passed=True,
        exit_code=0,
        commands=[],
        changed_paths=changed,
    )


def validate_after_cursor(
    track: Track,
    test_commands: list,
    repo_root: Path = REPO_ROOT,
    timeout_sec: int = 300,
    baseline: Optional[GitBaseline] = None,
    handoff_section: str = "",
    allow_dev_autonomy_writes: bool = False,
) -> ValidationResult:
    if baseline is None:
        baseline = capture_git_baseline(repo_root)

    # Pre-test worktree gate (post-Cursor, pre-pytest)
    pre = _worktree_gate(baseline, track, repo_root, handoff_section, allow_dev_autonomy_writes)
    if not pre.passed:
        pre.commands = list(test_commands)
        return pre

    test_result = run_pytest(test_commands, repo_root=repo_root, timeout_sec=timeout_sec)
    test_result.changed_paths = pre.changed_paths
    if not test_result.passed:
        return test_result

    # Post-test re-check: tests may create .env, commit, forbidden files
    post = _worktree_gate(baseline, track, repo_root, handoff_section, allow_dev_autonomy_writes)
    if not post.passed:
        post.commands = test_result.commands
        post.summaries = test_result.summaries
        post.errors = post.errors or ["post-test mutation safety failed"]
        return post

    return ValidationResult(
        passed=True,
        exit_code=0,
        commands=test_result.commands,
        summaries=test_result.summaries,
        changed_paths=post.changed_paths,
    )
