"""
A-LIFECAP-01 — scan/job lifetime cap (shadow-first).

CAT-N: this module must not read or write North Star ledger/snapshots.
Kill path is watchdog sweep only — dispatch never SIGTERM/SIGKILL.
"""
from __future__ import annotations

import os
import signal
import sqlite3
import time
from typing import Any, Optional

from bitget.infra.data_paths import job_lifetime_db_path
from bitget.infra.logging_setup import get_logger

logger = get_logger("bitget.job_lifetime_cap")

_HEAVY_PREFIXES = ("scan_", "daily_audit", "weekly_evolution")
_CFG_KEYS = (
    "BITGET_JOB_LIFECAP_ENABLED",
    "BITGET_JOB_LIFECAP_ENFORCE",
    "BITGET_JOB_OPS_CAP_SEC",
    "BITGET_JOB_HEAVY_CAP_SEC",
    "BITGET_JOB_KILL_GRACE_SEC",
)
_DEFAULTS: dict[str, Any] = {
    "BITGET_JOB_LIFECAP_ENABLED": True,
    "BITGET_JOB_LIFECAP_ENFORCE": False,
    "BITGET_JOB_OPS_CAP_SEC": 1800,
    "BITGET_JOB_HEAVY_CAP_SEC": 5400,
    "BITGET_JOB_KILL_GRACE_SEC": 60,
}


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _truthy(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _read_key(key: str, default: Any) -> Any:
    env = os.environ.get(key)
    if env is not None and str(env).strip() != "":
        return str(env).strip()
    try:
        from bitget.infra.config_manager import get_config_value

        v = get_config_value(key, None)
        if v is not None:
            return v
    except Exception:
        pass
    return default


def ensure_lifecap_defaults() -> None:
    """CAT-K: persist missing keys via set_config_value only (no secret path)."""
    try:
        from bitget.infra.config_manager import get_config_value, set_config_value

        for key in _CFG_KEYS:
            if get_config_value(key, None) is None:
                set_config_value(key, _DEFAULTS[key])
    except Exception:
        logger.debug("lifecap default seed skipped", exc_info=True)


def lifecap_enabled() -> bool:
    return _truthy(_read_key("BITGET_JOB_LIFECAP_ENABLED", True), True)


def lifecap_enforce() -> bool:
    return _truthy(_read_key("BITGET_JOB_LIFECAP_ENFORCE", False), False)


def job_lifetime_cap_sec(mode: str) -> int:
    m = str(mode or "").strip().lower()
    heavy = any(m.startswith(p) for p in _HEAVY_PREFIXES)
    key = "BITGET_JOB_HEAVY_CAP_SEC" if heavy else "BITGET_JOB_OPS_CAP_SEC"
    default = int(_DEFAULTS[key])
    raw = _read_key(key, default)
    try:
        return max(1, int(float(raw)))
    except (TypeError, ValueError):
        return default


def kill_grace_sec() -> int:
    raw = _read_key("BITGET_JOB_KILL_GRACE_SEC", 60)
    try:
        return max(1, int(float(raw)))
    except (TypeError, ValueError):
        return 60


def _connect() -> sqlite3.Connection:
    path = job_lifetime_db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=5.0)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_starts (
            mode TEXT PRIMARY KEY,
            pid INTEGER NOT NULL,
            started_ts REAL NOT NULL,
            started_utc TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def register_job_start(mode: str, pid: int) -> None:
    m = str(mode or "").strip().lower()
    if not m or int(pid) <= 0:
        return
    now = time.time()
    started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO job_starts (mode, pid, started_ts, started_utc)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(mode) DO UPDATE SET
                pid=excluded.pid,
                started_ts=excluded.started_ts,
                started_utc=excluded.started_utc
            """,
            (m, int(pid), now, started_utc),
        )
        conn.commit()
    finally:
        conn.close()


def unregister_job(mode: str, pid: Optional[int] = None) -> None:
    m = str(mode or "").strip().lower()
    if not m:
        return
    conn = _connect()
    try:
        if pid is None:
            conn.execute("DELETE FROM job_starts WHERE mode = ?", (m,))
        else:
            conn.execute("DELETE FROM job_starts WHERE mode = ? AND pid = ?", (m, int(pid)))
        conn.commit()
    finally:
        conn.close()


def is_same_mode_alive(mode: str) -> tuple[bool, int]:
    m = str(mode or "").strip().lower()
    if not m:
        return False, 0
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT pid, started_ts FROM job_starts WHERE mode = ?",
            (m,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return False, 0
    pid, started_ts = int(row[0]), float(row[1])
    if not _pid_alive(pid):
        unregister_job(m, pid)
        return False, 0
    age = max(0, int(time.time() - started_ts))
    return True, age


def log_job_duration(mode: str, duration_sec: float, outcome: str) -> None:
    try:
        from bitget.infra.ops_logger import insert_ops_event

        insert_ops_event(
            component=f"bitget.{mode}",
            severity="INFO",
            event="job.duration",
            payload={
                "mode": str(mode),
                "duration_sec": round(float(duration_sec), 3),
                "outcome": str(outcome)[:64],
            },
        )
    except Exception:
        logger.debug("job.duration log failed", exc_info=True)


def terminate_job(pid: int, grace_sec: int) -> None:
    pid = int(pid)
    if pid <= 0:
        return
    grace = max(1, int(grace_sec))
    if not _pid_alive(pid):
        return
    sigterm = getattr(signal, "SIGTERM", 15)
    sigkill = getattr(signal, "SIGKILL", sigterm)
    try:
        os.kill(pid, sigterm)
    except ProcessLookupError:
        return
    except OSError as ex:
        logger.warning("lifecap SIGTERM pid=%s failed: %s", pid, ex)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return
        time.sleep(0.2)
    if _pid_alive(pid):
        try:
            os.kill(pid, sigkill)
        except ProcessLookupError:
            return
        except OSError as ex:
            logger.warning("lifecap SIGKILL pid=%s failed: %s", pid, ex)


def sweep_expired_jobs() -> list[dict]:
    """Watchdog tick only. ENFORCE=false → WOULD_KILL log, no kill."""
    if not lifecap_enabled():
        return []
    ensure_lifecap_defaults()
    conn = _connect()
    try:
        rows = conn.execute("SELECT mode, pid, started_ts FROM job_starts").fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    enforce = lifecap_enforce()
    grace = kill_grace_sec()
    for mode, pid, started_ts in rows:
        pid_i = int(pid)
        age = max(0, int(time.time() - float(started_ts)))
        cap = job_lifetime_cap_sec(str(mode))
        if not _pid_alive(pid_i):
            unregister_job(str(mode), pid_i)
            continue
        if age < cap:
            continue
        rec = {
            "mode": str(mode),
            "pid": pid_i,
            "age_sec": age,
            "cap_sec": cap,
            "enforce": enforce,
        }
        out.append(rec)
        if not enforce:
            logger.warning(
                "LIFECAP WOULD_KILL mode=%s pid=%s age=%s cap=%s (ENFORCE=false)",
                mode,
                pid_i,
                age,
                cap,
            )
            continue
        logger.warning(
            "LIFECAP ENFORCE kill mode=%s pid=%s age=%s cap=%s grace=%s",
            mode,
            pid_i,
            age,
            cap,
            grace,
        )
        terminate_job(pid_i, grace)
        try:
            from bitget.watchdog import record_job_failure

            record_job_failure(str(mode), f"lifecap age={age} cap={cap}")
        except Exception:
            logger.warning("record_job_failure after lifecap kill failed mode=%s", mode, exc_info=True)
        unregister_job(str(mode), pid_i)
    return out
