"""Single-instance lock with PID ownership — no stale mtime takeover."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

from dev_autonomy.paths import LOCK_PATH


class OrchestratorLockError(RuntimeError):
    pass


def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_lock_pid(lock_path: Path) -> int | None:
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
        return int(raw.split()[0])
    except (OSError, ValueError):
        return None


@contextmanager
def orchestrator_lock(lock_path: Path = LOCK_PATH):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    my_pid = os.getpid()

    if lock_path.exists():
        owner = _read_lock_pid(lock_path)
        if owner is not None and _is_pid_alive(owner):
            raise OrchestratorLockError(
                f"Active lock held by PID {owner} ({lock_path}) — fail closed"
            )
        if owner is not None and not _is_pid_alive(owner):
            # Dead owner — remove stale lock only when PID is confirmed dead
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                raise OrchestratorLockError(f"Cannot clear stale lock at {lock_path}")

    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, f"{my_pid}\n".encode("utf-8"))
        finally:
            os.close(fd)
    except FileExistsError:
        owner = _read_lock_pid(lock_path)
        if owner is not None and _is_pid_alive(owner):
            raise OrchestratorLockError(
                f"Lock race — active PID {owner} ({lock_path})"
            )
        raise OrchestratorLockError(f"Lock exists but owner unknown ({lock_path})")

    try:
        yield
    finally:
        if lock_path.exists():
            owner = _read_lock_pid(lock_path)
            if owner == my_pid:
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError:
                    pass
