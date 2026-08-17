"""Working tree cleanliness, Git baseline, and snapshot delta."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from dev_autonomy.paths import AUTONOMY_WRITE_PREFIXES, REPO_ROOT


@dataclass
class WorktreeStatus:
    dirty: bool
    dirty_paths: list[str]
    pre_existing_dirty: bool
    autonomy_only_dirty: bool
    reason: str


@dataclass
class WorktreeSnapshot:
    paths: frozenset[str]
    ok: bool
    error: str = ""


@dataclass
class GitBaseline:
    """Captured before Cursor invocation — HEAD + worktree state."""

    head: str
    snapshot: WorktreeSnapshot
    ok: bool
    error: str = ""


@dataclass
class SnapshotDeltaResult:
    paths: list[str]
    ok: bool
    error: str = ""


def _is_ignorable(path: str) -> bool:
    norm = path.replace("\\", "/")
    if norm.startswith("__pycache__/") or "/__pycache__/" in norm:
        return True
    if norm.endswith(".pyc"):
        return True
    return False


def _norm_path(path: str) -> str:
    p = path.replace("\\", "/").strip()
    while p.startswith("./"):
        p = p[2:]
    return p.lstrip("/")


def _is_autonomy_path(path: str) -> bool:
    norm = _norm_path(path)
    return any(norm.startswith(p) or f"/{p}" in norm for p in AUTONOMY_WRITE_PREFIXES)


def _parse_porcelain_z(data: bytes) -> list[str]:
    paths: list[str] = []
    if not data:
        return paths
    entries = data.split(b"\0")
    i = 0
    while i < len(entries):
        entry = entries[i]
        if not entry:
            i += 1
            continue
        try:
            text = entry.decode("utf-8", errors="replace")
        except Exception:
            i += 1
            continue
        if len(text) < 4:
            i += 1
            continue
        xy = text[:2]
        path_part = text[3:].strip()
        if xy.startswith("R") or xy.startswith("C"):
            if i + 1 < len(entries) and entries[i + 1]:
                new_path = entries[i + 1].decode("utf-8", errors="replace")
                paths.append(_norm_path(new_path))
                i += 2
                continue
        if " -> " in path_part:
            path_part = path_part.split(" -> ", 1)[1]
        paths.append(_norm_path(path_part))
        i += 1
    return paths


def get_head_commit(repo_root: Path = REPO_ROOT) -> tuple[str, bool, str]:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "git rev-parse HEAD failed").strip()
        return "", False, err
    return proc.stdout.strip(), True, ""


def capture_snapshot(repo_root: Path = REPO_ROOT) -> WorktreeSnapshot:
    proc = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
        cwd=repo_root,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "git status failed").strip()
        return WorktreeSnapshot(paths=frozenset(), ok=False, error=f"git status failed — fail closed: {err}")
    raw_paths = _parse_porcelain_z(proc.stdout or b"")
    filtered = frozenset(p for p in raw_paths if p and not _is_ignorable(p))
    return WorktreeSnapshot(paths=filtered, ok=True)


def capture_git_baseline(repo_root: Path = REPO_ROOT) -> GitBaseline:
    head, head_ok, head_err = get_head_commit(repo_root)
    snap = capture_snapshot(repo_root)
    if not head_ok:
        return GitBaseline(head="", snapshot=snap, ok=False, error=head_err or "HEAD capture failed")
    if not snap.ok:
        return GitBaseline(head=head, snapshot=snap, ok=False, error=snap.error)
    return GitBaseline(head=head, snapshot=snap, ok=True)


def verify_head_unchanged(baseline: GitBaseline, repo_root: Path = REPO_ROOT) -> tuple[bool, str]:
    if not baseline.ok:
        return False, baseline.error or "invalid baseline"
    current, ok, err = get_head_commit(repo_root)
    if not ok:
        return False, err or "HEAD inspection failed — fail closed"
    if current != baseline.head:
        return False, f"HEAD mutation detected: {baseline.head[:12]} -> {current[:12]}"
    return True, ""


def diff_snapshots(before: WorktreeSnapshot, after: WorktreeSnapshot) -> SnapshotDeltaResult:
    if not before.ok:
        return SnapshotDeltaResult([], False, before.error or "baseline snapshot failed — fail closed")
    if not after.ok:
        return SnapshotDeltaResult([], False, after.error or "post-run snapshot failed — fail closed")
    delta = after.paths - before.paths
    modified = before.paths & after.paths
    return SnapshotDeltaResult(sorted(delta | modified), True, "")


def inspect_worktree(repo_root: Path = REPO_ROOT) -> WorktreeStatus:
    snap = capture_snapshot(repo_root)
    if not snap.ok:
        return WorktreeStatus(
            dirty=True,
            dirty_paths=[],
            pre_existing_dirty=True,
            autonomy_only_dirty=False,
            reason=snap.error,
        )

    dirty_paths = sorted(snap.paths)
    if not dirty_paths:
        return WorktreeStatus(
            dirty=False,
            dirty_paths=[],
            pre_existing_dirty=False,
            autonomy_only_dirty=False,
            reason="clean",
        )

    non_autonomy = [p for p in dirty_paths if not _is_autonomy_path(p)]
    if non_autonomy:
        return WorktreeStatus(
            dirty=True,
            dirty_paths=dirty_paths,
            pre_existing_dirty=True,
            autonomy_only_dirty=False,
            reason=f"pre-existing dirty files outside autonomy scope ({len(non_autonomy)})",
        )

    return WorktreeStatus(
        dirty=True,
        dirty_paths=dirty_paths,
        pre_existing_dirty=False,
        autonomy_only_dirty=True,
        reason="dirty only within autonomy paths",
    )


def list_changed_paths(repo_root: Path = REPO_ROOT) -> list[str]:
    snap = capture_snapshot(repo_root)
    if not snap.ok:
        return []
    return sorted(snap.paths)


def collect_diff_excerpt(
    changed_paths: list[str],
    repo_root: Path = REPO_ROOT,
    max_chars: int = 4000,
) -> str:
    """Verifiable diff/content excerpt for Claude verifier — not empty when paths changed."""
    if not changed_paths:
        return ""
    chunks: list[str] = []
    remaining = max_chars
    for path in changed_paths:
        if remaining <= 0:
            break
        proc = subprocess.run(
            ["git", "diff", "HEAD", "--", path],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        text = (proc.stdout or "").strip()
        if not text:
            full = repo_root / path
            if full.is_file():
                try:
                    body = full.read_text(encoding="utf-8", errors="replace")
                    text = f"--- new/untracked {path}\n{body}"
                except OSError:
                    text = ""
        if not text:
            continue
        piece = f"### {path}\n{text[:min(1500, remaining)]}"
        chunks.append(piece)
        remaining -= len(piece) + 1
    return "\n".join(chunks)[:max_chars]
