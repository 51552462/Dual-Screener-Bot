"""Fail-closed Cursor worker that can publish a draft PR, never merge it.

The trusted controller owns Git and GitHub CLI calls.  Cursor runs only inside
an isolated worktree where project policy denies Git, network, secrets, deploy,
and policy changes.  A current envelope, exact Handoff allowlist, pytest pass,
and Claude ``OK`` are all required before a branch can be pushed.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from dev_autonomy.adapters import (
    ClaudeCodeVerifier,
    ClaudeVerifierAdapter,
    CursorCliExecutor,
    CursorExecutorAdapter,
)
from dev_autonomy.context_pack import build_cursor_pack
from dev_autonomy.control_plane import (
    AutonomyEnvelope,
    NormalizedReport,
    load_envelope,
)
from dev_autonomy.paths import REPO_ROOT
from dev_autonomy.safety_guard import check_path_safety, evaluate_pre_ai_safety
from dev_autonomy.state_resolver import resolve_state
from dev_autonomy.types import ResolvedState, Track
from dev_autonomy.validation_gate import validate_after_cursor
from dev_autonomy.weekday_runner import send_telegram_digest
from dev_autonomy.worktree import capture_git_baseline, collect_diff_excerpt, inspect_worktree


JOB_SCHEMA = "dev_autonomy.job.v1"
RESULT_SCHEMA = "dev_autonomy.pr_worker_result.v1"
_MAX_PACKET_BYTES = 1_000_000
_OUTPUT_LIMIT = 6000
_ROLE_TRACKS = {"stock": {"A", "IV"}, "bitget": {"B"}}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _clip(value: str, limit: int = _OUTPUT_LIMIT) -> str:
    text = str(value or "")
    return text if len(text) <= limit else text[:limit] + "\n...[truncated]"


def _slug(value: str, limit: int = 34) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return (slug or "task")[:limit].rstrip("-")


def branch_name(track: Track, subphase: str, job_id: str) -> str:
    digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()[:10]
    return f"autonomy/{track.value.lower()}-{_slug(subphase)}-{digest}"


class WorkerFailure(RuntimeError):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


class WorkerLedger:
    """Atomic job claim and daily quota ledger."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS jobs ("
                "job_id TEXT PRIMARY KEY, work_day TEXT NOT NULL, state TEXT NOT NULL, "
                "branch TEXT, pr_url TEXT, detail TEXT, updated_at TEXT NOT NULL)"
            )

    def claim(self, job_id: str, *, max_tasks: int, now: datetime) -> tuple[bool, dict[str, str]]:
        work_day = now.astimezone(timezone.utc).date().isoformat()
        stamp = now.astimezone(timezone.utc).isoformat()
        with sqlite3.connect(self.path, timeout=30) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state, branch, pr_url, detail FROM jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if row:
                conn.commit()
                return False, {
                    "state": str(row[0]),
                    "branch": str(row[1] or ""),
                    "pr_url": str(row[2] or ""),
                    "detail": str(row[3] or ""),
                }
            count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM jobs WHERE work_day=? AND state IN ('WORKING','FAILED','PR_OPENED')",
                    (work_day,),
                ).fetchone()[0]
            )
            if count >= max_tasks:
                conn.commit()
                raise WorkerFailure("DAILY_QUOTA", f"daily task limit reached ({count}/{max_tasks})")
            conn.execute(
                "INSERT INTO jobs(job_id, work_day, state, updated_at) VALUES (?, ?, 'WORKING', ?)",
                (job_id, work_day, stamp),
            )
            conn.commit()
        return True, {"state": "WORKING", "branch": "", "pr_url": "", "detail": ""}

    def lookup(self, job_id: str) -> dict[str, str] | None:
        with sqlite3.connect(self.path, timeout=30) as conn:
            row = conn.execute(
                "SELECT state, branch, pr_url, detail FROM jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "state": str(row[0]),
            "branch": str(row[1] or ""),
            "pr_url": str(row[2] or ""),
            "detail": str(row[3] or ""),
        }

    def block(self, job_id: str, *, branch: str, detail: str, now: datetime) -> None:
        work_day = now.astimezone(timezone.utc).date().isoformat()
        with sqlite3.connect(self.path, timeout=30) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO jobs(job_id, work_day, state, branch, detail, updated_at) "
                "VALUES (?, ?, 'BLOCKED', ?, ?, ?)",
                (job_id, work_day, branch, _clip(detail, 1000), now.astimezone(timezone.utc).isoformat()),
            )

    def finish(self, job_id: str, *, state: str, branch: str, pr_url: str, detail: str) -> None:
        with sqlite3.connect(self.path, timeout=30) as conn:
            conn.execute(
                "UPDATE jobs SET state=?, branch=?, pr_url=?, detail=?, updated_at=? WHERE job_id=?",
                (state, branch, pr_url, _clip(detail, 1000), _utc_now().isoformat(), job_id),
            )


def _load_packet(path: Path) -> dict[str, Any]:
    packet_path = Path(path)
    if packet_path.is_symlink() or not packet_path.is_file():
        raise WorkerFailure("PACKET_PATH", "job packet must be a regular non-symlink file")
    if packet_path.stat().st_size > _MAX_PACKET_BYTES:
        raise WorkerFailure("PACKET_SIZE", "job packet is too large")
    try:
        payload = json.loads(packet_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkerFailure("PACKET_JSON", f"invalid job packet: {type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise WorkerFailure("PACKET_JSON", "job packet must be a JSON object")
    return payload


def _report_from_packet(packet: dict[str, Any]) -> NormalizedReport:
    report = packet.get("report")
    if not isinstance(report, dict):
        raise WorkerFailure("PACKET_REPORT", "report object missing")
    try:
        return NormalizedReport(
            report_id=str(report["report_id"]),
            source=str(report.get("source") or "repository_ssot"),
            track=str(report["track"]).upper(),
            observed_at=str(report.get("observed_at") or "unknown"),
            source_status=str(report.get("source_status") or "WAIT_CURSOR_IMPL"),
            cursor_action=str(report.get("cursor_action") or ""),
            environment=str(report.get("environment") or "observation"),
            metrics=report.get("metrics") if isinstance(report.get("metrics"), dict) else {},
            flags=tuple(report.get("flags") or ()),
            payload_hash=str(report.get("payload_hash") or ""),
            schema=str(report.get("schema") or "dev_autonomy.report.v1"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkerFailure("PACKET_REPORT", f"invalid report fields: {type(exc).__name__}") from exc


def validate_candidate(
    packet: dict[str, Any],
    envelope: AutonomyEnvelope,
    *,
    role: str,
    now: datetime,
) -> tuple[Track, NormalizedReport]:
    if packet.get("schema") != JOB_SCHEMA:
        raise WorkerFailure("PACKET_SCHEMA", "unsupported job schema")
    job_id = str(packet.get("job_id") or "")
    if not job_id or len(job_id) > 500:
        raise WorkerFailure("PACKET_JOB_ID", "job_id missing or too long")
    if packet.get("provider") != "cursor" or packet.get("action") != "CURSOR_IMPLEMENT":
        raise WorkerFailure("NOT_CURSOR_JOB", "packet is not a Cursor implementation candidate")
    if packet.get("execution_authorized") is not False:
        raise WorkerFailure("PACKET_AUTHORITY", "packet authority field must remain false")

    decision = packet.get("decision")
    if not isinstance(decision, dict) or decision.get("action") != "CURSOR_IMPLEMENT":
        raise WorkerFailure("PACKET_DECISION", "decision does not authorize a Cursor candidate")
    if decision.get("execution_authorized") is not False:
        raise WorkerFailure("PACKET_DECISION", "decision authority field must remain false")

    limits = packet.get("hard_limits")
    if not isinstance(limits, dict) or limits.get("require_pull_request") is not True:
        raise WorkerFailure("PACKET_LIMITS", "pull request hard limit missing")
    for key in ("allow_live", "allow_deploy", "allow_merge", "allow_ssh"):
        if limits.get(key) is not False:
            raise WorkerFailure("PACKET_LIMITS", f"unsafe hard limit: {key}")

    pr_policy = packet.get("pr_policy")
    if not isinstance(pr_policy, dict):
        raise WorkerFailure("PACKET_PR_POLICY", "explicit draft-PR policy missing")
    required_true = ("cursor_write_requires_envelope", "branch_push_requires_envelope", "draft_pr_only")
    if any(pr_policy.get(key) is not True for key in required_true):
        raise WorkerFailure("PACKET_PR_POLICY", "draft-PR policy is incomplete")
    if pr_policy.get("auto_merge") is not False:
        raise WorkerFailure("PACKET_PR_POLICY", "auto merge must remain false")

    report = _report_from_packet(packet)
    try:
        track = Track(report.track)
    except ValueError as exc:
        raise WorkerFailure("PACKET_TRACK", "unknown track") from exc
    if role not in _ROLE_TRACKS or track.value not in _ROLE_TRACKS[role]:
        raise WorkerFailure("ROLE_SCOPE", f"role {role} cannot process Track {track.value}")
    if not envelope.allows_pr_worker(report, now):
        raise WorkerFailure("ENVELOPE_DENIED", "current envelope does not grant Cursor draft-PR capabilities")
    return track, report


def format_pr_telegram(result: dict[str, Any]) -> str:
    status = html.escape(str(result.get("phase") or "UNKNOWN"))
    track = html.escape(str(result.get("track") or "?"))
    branch = html.escape(str(result.get("branch") or ""))
    pr_url = html.escape(str(result.get("pr_url") or ""), quote=True)
    lines = [
        "🤖 <b>[Cursor 격리 PR 작업]</b>",
        f"Track {track} · <b>{status}</b>",
    ]
    if branch:
        lines.append(f"브랜치: <code>{branch}</code>")
    if pr_url:
        lines.append(f'<a href="{pr_url}">Draft PR 열기</a>')
    reason_code = html.escape(str(result.get("reason_code") or ""))
    if reason_code:
        lines.append(f"중단 사유: <code>{reason_code}</code>")
    lines.extend(
        [
            "운영 작업트리 수정: <b>없음</b>",
            "실전 주문·배포·자동 병합: <b>없음</b>",
            "검토 후 사람이 병합 여부를 결정합니다.",
        ]
    )
    return "\n".join(lines)


class PrWorker:
    def __init__(
        self,
        *,
        repo_root: Path = REPO_ROOT,
        runtime_root: Path,
        state_provider: Callable[[Track], ResolvedState] = resolve_state,
        pack_builder: Callable[[ResolvedState], dict[str, Any]] = build_cursor_pack,
        cursor_factory: Callable[[Path], CursorExecutorAdapter] | None = None,
        claude_factory: Callable[[Path], ClaudeVerifierAdapter] | None = None,
        git_executable: str = "git",
        gh_executable: str = "gh",
        telegram_sender: Callable[[str], tuple[bool, str]] = send_telegram_digest,
    ):
        self.repo_root = Path(repo_root).resolve()
        self.runtime_root = Path(runtime_root).resolve()
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.state_provider = state_provider
        self.pack_builder = pack_builder
        self.cursor_factory = cursor_factory or (
            lambda path: CursorCliExecutor(enable_write=True, repo_root=path)
        )
        self.claude_factory = claude_factory or (
            lambda path: ClaudeCodeVerifier(enabled=True, repo_root=path)
        )
        self.git_executable = git_executable
        self.gh_executable = gh_executable
        self.telegram_sender = telegram_sender
        self.ledger = WorkerLedger(self.runtime_root / "pr_worker.sqlite")
        (self.runtime_root / "worktrees").mkdir(parents=True, exist_ok=True)
        (self.runtime_root / "results").mkdir(parents=True, exist_ok=True)

    def _run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        timeout: int = 120,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        try:
            proc = subprocess.run(
                argv,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise WorkerFailure("COMMAND_FAILED", f"{Path(argv[0]).name}: {type(exc).__name__}") from exc
        if check and proc.returncode != 0:
            detail = _clip(proc.stderr or proc.stdout or f"exit={proc.returncode}", 1500)
            raise WorkerFailure("COMMAND_FAILED", f"{Path(argv[0]).name} exit={proc.returncode}: {detail}")
        return proc

    def _git(self, *args: str, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self._run([self.git_executable, *args], cwd=cwd or self.repo_root, check=check)

    def _assert_base_ready(self, *, fetch: bool) -> str:
        if shutil.which(self.git_executable) is None and not Path(self.git_executable).is_file():
            raise WorkerFailure("GIT_MISSING", "git executable not found")
        status = inspect_worktree(self.repo_root)
        if status.dirty:
            raise WorkerFailure("BASE_DIRTY", status.reason)
        branch = self._git("symbolic-ref", "--short", "HEAD").stdout.strip()
        if branch != "main":
            raise WorkerFailure("BASE_BRANCH", f"controller repository must be on main, not {branch}")
        if fetch:
            self._git("fetch", "--no-tags", "origin", "main")
        head = self._git("rev-parse", "HEAD").stdout.strip()
        remote = self._git("rev-parse", "refs/remotes/origin/main").stdout.strip()
        if head != remote:
            raise WorkerFailure("BASE_STALE", "local main must exactly match origin/main")
        return head

    def _build_context(self, track: Track) -> tuple[ResolvedState, dict[str, Any]]:
        state = self.state_provider(track)
        if state.status_canonical != "WAIT_CURSOR_IMPL" or state.blocked or state.human_required:
            raise WorkerFailure("STATE_BLOCKED", state.block_reason or state.status_canonical)
        pack = self.pack_builder(state)
        handoff = str(pack.get("handoff_section") or "")
        allowed = pack.get("allowed_paths")
        tests = pack.get("test_commands")
        if not isinstance(allowed, list) or not allowed:
            raise WorkerFailure("ALLOWLIST_MISSING", "Handoff has no machine-readable allowed files")
        if not isinstance(tests, list) or not tests:
            raise WorkerFailure("TESTS_MISSING", "Handoff has no pytest command")
        for path in allowed:
            normalized = str(path).replace("\\", "/")
            if normalized.startswith("/") or ".." in Path(normalized).parts:
                raise WorkerFailure("ALLOWLIST_UNSAFE", f"non-relative allowed path blocked: {path}")
            block = check_path_safety(normalized, track)
            if block:
                raise WorkerFailure("ALLOWLIST_UNSAFE", block.reason)
        safety = evaluate_pre_ai_safety(
            state,
            inspect_worktree(self.repo_root),
            mode_requires_mutation=True,
            handoff_excerpt=handoff,
            next_action_text=str(pack.get("next_action_excerpt") or ""),
        )
        if not safety.allowed:
            raise WorkerFailure("PRE_AI_SAFETY", safety.reason)
        return state, pack

    def _create_worktree(self, *, base_sha: str, branch: str) -> Path:
        worktree = self.runtime_root / "worktrees" / branch.replace("/", "__")
        if worktree.exists():
            raise WorkerFailure("WORKTREE_EXISTS", "isolated worktree path already exists")
        local_ref = self._git("show-ref", "--verify", f"refs/heads/{branch}", check=False)
        if local_ref.returncode == 0:
            raise WorkerFailure("BRANCH_EXISTS", "local autonomy branch already exists")
        remote_ref = self._git(
            "ls-remote",
            "--exit-code",
            "--heads",
            "origin",
            f"refs/heads/{branch}",
            check=False,
        )
        if remote_ref.returncode == 0:
            raise WorkerFailure("BRANCH_EXISTS", "remote autonomy branch already exists")
        if remote_ref.returncode not in {0, 2}:
            raise WorkerFailure("REMOTE_CHECK_FAILED", "could not verify remote branch absence")
        self._git("worktree", "add", "-b", branch, str(worktree), base_sha)
        return worktree

    def _assert_no_symlinks(self, worktree: Path, paths: list[str]) -> None:
        for raw in paths:
            target = worktree / raw
            if target.is_symlink():
                raise WorkerFailure("SYMLINK_BLOCKED", f"changed symlink blocked: {raw}")

    def _commit(self, worktree: Path, paths: list[str], state: ResolvedState) -> str:
        self._git("add", "--", *paths, cwd=worktree)
        self._git("diff", "--cached", "--check", cwd=worktree)
        quiet = self._git("diff", "--cached", "--quiet", cwd=worktree, check=False)
        if quiet.returncode == 0:
            raise WorkerFailure("NO_CHANGES", "Cursor produced no committable changes")
        if quiet.returncode != 1:
            raise WorkerFailure("GIT_DIFF_FAILED", "could not inspect staged changes")
        message = f"feat(autonomy): {state.subphase_id or state.subphase}"
        self._git(
            "-c",
            "user.name=Quant Autonomy Worker",
            "-c",
            "user.email=quant-autonomy@localhost",
            "commit",
            "-m",
            message,
            cwd=worktree,
        )
        return self._git("rev-parse", "HEAD", cwd=worktree).stdout.strip()

    def _publish_pr(
        self,
        *,
        worktree: Path,
        branch: str,
        state: ResolvedState,
        changed_paths: list[str],
        validation_commands: list[str],
    ) -> str:
        auth = self._run([self.gh_executable, "auth", "status"], cwd=worktree, check=False)
        if auth.returncode != 0:
            raise WorkerFailure("GITHUB_AUTH", "GitHub CLI is not authenticated")
        self._git("push", "--set-upstream", "origin", branch, cwd=worktree)
        title = f"autonomy: {state.subphase_id or state.subphase}"
        body = "\n".join(
            [
                "## 자동 생성 Draft PR",
                "",
                f"- Track: `{state.track.value}`",
                f"- Sub-phase: `{state.subphase_id or state.subphase}`",
                f"- 변경 파일: {len(changed_paths)}개",
                "- Cursor: 격리 worktree에서만 실행",
                "- Claude: read-only 검증 OK",
                "- 실전 주문/배포/병합: 없음",
                "",
                "## Changed paths",
                *[f"- `{path}`" for path in changed_paths],
                "",
                "## Validation",
                "- Pytest exit: `0`",
                *[f"- `{command}`" for command in validation_commands],
                "",
                "> 자동 병합되지 않습니다. 사람이 검토 후 결정하세요.",
            ]
        )
        body_path = self.runtime_root / "results" / f"pr_body_{hashlib.sha256(branch.encode()).hexdigest()[:12]}.md"
        body_path.write_text(body, encoding="utf-8")
        proc = self._run(
            [
                self.gh_executable,
                "pr",
                "create",
                "--draft",
                "--base",
                "main",
                "--head",
                branch,
                "--title",
                title,
                "--body-file",
                str(body_path),
            ],
            cwd=worktree,
        )
        match = re.search(r"https://github\.com/[^\s]+/pull/\d+", proc.stdout or "")
        if not match:
            raise WorkerFailure("PR_URL_MISSING", "GitHub CLI did not return a pull request URL")
        return match.group(0)

    def _write_result(self, job_id: str, result: dict[str, Any]) -> None:
        digest = hashlib.sha256((job_id or "unknown").encode("utf-8")).hexdigest()[:20]
        path = self.runtime_root / "results" / f"result_{digest}.json"
        temp = path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        temp.replace(path)

    def run_packet(
        self,
        packet_path: Path,
        envelope: AutonomyEnvelope,
        *,
        role: str,
        publish_draft_pr: bool,
        notify_telegram: bool,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current = now or _utc_now()
        job_id = ""
        branch = ""
        worktree: Path | None = None
        claimed = False
        result: dict[str, Any]
        try:
            packet = _load_packet(packet_path)
            job_id = str(packet.get("job_id") or "")
            if publish_draft_pr:
                existing = self.ledger.lookup(job_id)
                if existing:
                    return {
                        "schema": RESULT_SCHEMA,
                        "phase": "ALREADY_PROCESSED",
                        "job_id": job_id,
                        "track": str((packet.get("report") or {}).get("track") or ""),
                        "branch": existing["branch"],
                        "pr_url": existing["pr_url"],
                        "claimed": False,
                        "execution_authorized": False,
                    }
            track, _report = validate_candidate(packet, envelope, role=role, now=current)
            state, pack = self._build_context(track)
            branch = branch_name(track, state.subphase_id or state.subphase, job_id)
            base_sha = self._assert_base_ready(fetch=publish_draft_pr)

            if not publish_draft_pr:
                result = {
                    "schema": RESULT_SCHEMA,
                    "phase": "DRY_RUN_READY",
                    "job_id": job_id,
                    "track": track.value,
                    "branch": branch,
                    "pr_url": "",
                    "claimed": False,
                    "execution_authorized": False,
                }
                self._write_result(job_id, result)
                return result

            pre_cursor = self.cursor_factory(self.repo_root)
            pre_claude = self.claude_factory(self.repo_root)
            cursor_ok, cursor_detail = pre_cursor.availability()
            if not cursor_ok:
                raise WorkerFailure("CURSOR_UNAVAILABLE", cursor_detail)
            claude_ok, claude_detail = pre_claude.availability()
            if not claude_ok:
                raise WorkerFailure("CLAUDE_UNAVAILABLE", claude_detail)
            gh_auth = self._run([self.gh_executable, "auth", "status"], cwd=self.repo_root, check=False)
            if gh_auth.returncode != 0:
                raise WorkerFailure("GITHUB_AUTH", "GitHub CLI is not authenticated")

            claimed, existing = self.ledger.claim(
                job_id,
                max_tasks=envelope.max_tasks_per_day,
                now=current,
            )
            if not claimed:
                return {
                    "schema": RESULT_SCHEMA,
                    "phase": "ALREADY_PROCESSED",
                    "job_id": job_id,
                    "track": track.value,
                    "branch": existing["branch"],
                    "pr_url": existing["pr_url"],
                    "claimed": False,
                    "execution_authorized": False,
                }

            worktree = self._create_worktree(base_sha=base_sha, branch=branch)
            cursor = self.cursor_factory(worktree)
            claude = self.claude_factory(worktree)
            cursor_ok, cursor_detail = cursor.availability()
            if not cursor_ok:
                raise WorkerFailure("CURSOR_UNAVAILABLE", cursor_detail)
            claude_ok, claude_detail = claude.availability()
            if not claude_ok:
                raise WorkerFailure("CLAUDE_UNAVAILABLE", claude_detail)

            baseline = capture_git_baseline(worktree)
            cursor_pack = dict(pack)
            cursor_pack["job_packet"] = packet
            cursor_pack["publication_policy"] = {
                "draft_pr_only": True,
                "auto_merge": False,
                "deploy": False,
                "live": False,
            }
            cursor_result = cursor.run_implementation(cursor_pack)
            if not cursor_result.ok:
                raise WorkerFailure("CURSOR_FAILED", cursor_result.detail)

            validation = validate_after_cursor(
                track,
                list(pack.get("test_commands") or []),
                repo_root=worktree,
                baseline=baseline,
                handoff_section=str(pack.get("handoff_section") or ""),
            )
            if not validation.passed:
                raise WorkerFailure("VALIDATION_FAILED", "; ".join(validation.errors) or "pytest failed")
            if not validation.changed_paths:
                raise WorkerFailure("NO_CHANGES", "Cursor produced no changed paths")
            self._assert_no_symlinks(worktree, validation.changed_paths)

            summary = "\n".join(validation.summaries)
            diff_excerpt = collect_diff_excerpt(validation.changed_paths, repo_root=worktree)
            claude_result = claude.verify(
                {
                    "track": track.value,
                    "subphase": state.subphase_id or state.subphase,
                    "handoff_section": pack.get("handoff_section"),
                    "allowed_paths": pack.get("allowed_paths"),
                    "changed_paths": validation.changed_paths,
                    "test_commands": validation.commands,
                    "test_exit_code": validation.exit_code,
                    "validation_summary": _clip(summary, 4000),
                    "diff_excerpt": diff_excerpt,
                    "required_verdict": "OK, MODIFY, or REJECT",
                }
            )
            if not claude_result.ok or claude_result.verdict != "OK":
                raise WorkerFailure("CLAUDE_REJECTED", claude_result.detail)

            commit_sha = self._commit(worktree, validation.changed_paths, state)
            pr_url = self._publish_pr(
                worktree=worktree,
                branch=branch,
                state=state,
                changed_paths=validation.changed_paths,
                validation_commands=validation.commands,
            )
            result = {
                "schema": RESULT_SCHEMA,
                "phase": "DRAFT_PR_OPENED",
                "job_id": job_id,
                "track": track.value,
                "subphase": state.subphase_id or state.subphase,
                "branch": branch,
                "commit_sha": commit_sha,
                "pr_url": pr_url,
                "changed_paths": validation.changed_paths,
                "claimed": True,
                "execution_authorized": False,
                "auto_merge": False,
                "telegram": {"requested": notify_telegram, "sent": False, "detail": "NOT_REQUESTED"},
            }
            if notify_telegram:
                sent, detail = self.telegram_sender(format_pr_telegram(result))
                result["telegram"] = {"requested": True, "sent": sent, "detail": detail}
            self.ledger.finish(job_id, state="PR_OPENED", branch=branch, pr_url=pr_url, detail="Claude OK")
            self._write_result(job_id, result)

            if not self._git("status", "--porcelain", cwd=worktree).stdout.strip():
                self._git("worktree", "remove", str(worktree))
            return result
        except WorkerFailure as exc:
            result = {
                "schema": RESULT_SCHEMA,
                "phase": "SAFETY_BLOCKED" if not claimed else "FAILED_REQUIRES_REVIEW",
                "reason_code": exc.code,
                "reason": _clip(exc.detail, 1200),
                "job_id": job_id,
                "branch": branch,
                "worktree": str(worktree) if worktree else "",
                "claimed": claimed,
                "execution_authorized": False,
                "auto_merge": False,
            }
            if claimed and job_id:
                self.ledger.finish(job_id, state="FAILED", branch=branch, pr_url="", detail=exc.detail)
            elif publish_draft_pr and job_id:
                self.ledger.block(job_id, branch=branch, detail=exc.detail, now=current)
            if notify_telegram and publish_draft_pr and job_id:
                sent, detail = self.telegram_sender(format_pr_telegram(result))
                result["telegram"] = {"requested": True, "sent": sent, "detail": detail}
            self._write_result(job_id or str(packet_path), result)
            return result


def _envelope_ready(envelope: AutonomyEnvelope, now: datetime) -> bool:
    dummy = NormalizedReport(
        report_id="audit",
        source="audit",
        track=next(iter(sorted(envelope.allowed_tracks))),
        observed_at=now.isoformat(),
        source_status="WAIT_CURSOR_IMPL",
    )
    return envelope.allows_pr_worker(dummy, now)


def _candidate_paths(outbox_dir: Path) -> list[Path]:
    if not outbox_dir.is_dir():
        return []
    return sorted(
        path
        for path in outbox_dir.glob("job_*.json")
        if not path.name.endswith(".result.json") and path.is_file() and not path.is_symlink()
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Isolated Cursor draft-PR worker")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--job", type=Path)
    source.add_argument("--outbox-dir", type=Path)
    parser.add_argument("--envelope", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--role", choices=sorted(_ROLE_TRACKS), required=True)
    parser.add_argument("--publish-draft-pr", action="store_true")
    parser.add_argument("--notify-telegram", action="store_true")
    parser.add_argument("--max-jobs", type=int, default=1)
    parser.add_argument("--check-envelope-only", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    try:
        envelope = load_envelope(args.envelope)
        if envelope is None:
            raise WorkerFailure("ENVELOPE_MISSING", "envelope is required")
        if args.check_envelope_only:
            ready = _envelope_ready(envelope, _utc_now())
            payload = {"envelope_ready": ready, "unsafe_authority": False}
            sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
            return 0 if ready else 2

        worker = PrWorker(repo_root=args.repo_root, runtime_root=args.runtime_root)
        paths = [args.job] if args.job else _candidate_paths(args.outbox_dir or Path("."))
        results: list[dict[str, Any]] = []
        for path in paths:
            if path is None:
                continue
            try:
                packet = _load_packet(path)
            except WorkerFailure:
                packet = {}
            if packet.get("provider") != "cursor" or packet.get("action") != "CURSOR_IMPLEMENT":
                continue
            result = worker.run_packet(
                path,
                envelope,
                role=args.role,
                publish_draft_pr=args.publish_draft_pr,
                notify_telegram=args.notify_telegram,
            )
            results.append(result)
            if result.get("claimed") or result.get("phase") == "DRY_RUN_READY":
                if len(results) >= max(1, args.max_jobs):
                    break
        payload = {
            "schema": "dev_autonomy.pr_worker_run.v1",
            "mode": "PUBLISH_DRAFT_PR" if args.publish_draft_pr else "DRY_RUN",
            "results": results,
            "execution_authorized": False,
            "auto_merge": False,
        }
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        failed = any(item.get("phase") in {"SAFETY_BLOCKED", "FAILED_REQUIRES_REVIEW"} for item in results)
        return 2 if failed else 0
    except (OSError, ValueError, WorkerFailure) as exc:
        sys.stderr.write(f"pr worker configuration error: {exc}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
