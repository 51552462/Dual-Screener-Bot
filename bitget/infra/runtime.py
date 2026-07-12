"""
Bitget job runner — Step 격리, flock, cron-safe exit codes.

주식 `factory_runtime.py` 패턴; lock 경로·환경 변수는 Bitget 전용.

Clock boundary (intentional):
  - `dispatch_bitget_mode` / lock metadata → Asia/Seoul (KST) for operator-facing run_id·timestamps.
  - 24/7 trading logic, SQL lookbacks, ops_events → `bitget.infra.clock` UTC SSOT.
"""
from __future__ import annotations

import html
import logging
import os
import signal
import sys
import time
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional, Sequence, Tuple

import pytz

from bitget.bitget_scan_schedule import STAGGERED_SCAN_MODES, resolve_lock_timeout_sec
from bitget.infra.data_paths import job_lock_path, runtime_lock_path

logger = logging.getLogger(__name__)

SendFn = Callable[[str], None]


def _enqueue_on_yield_enabled() -> bool:
    """[#2] 주식 factory 양보 스킵을 Drop 대신 큐 적재(대기 후 실행)로 전환할지(기본 OFF)."""
    return str(os.environ.get("BITGET_ENQUEUE_ON_YIELD", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

BITGET_MODES = frozenset(
    {
        "scan_spot",
        "scan_futures",
        "scan_all",
        "track_positions",
        "daily_audit",
        "weekly_evolution",
        "reconcile",
        "data_refresh",
        "health",
        "ws_oms_smoke",
        "gap_heal",
        "snapshot",
        "db_backup",
        "record_baseline",
        "validate",
        "load_test",
        "cutover_check",
        "validate_all",
        "start_parallel",
    }
) | frozenset(STAGGERED_SCAN_MODES)


class JobSkipError(Exception):
    """동일 mode job이 이미 실행 중."""


@dataclass(frozen=True)
class StepSpec:
    name: str
    fn: Callable[[], None]
    critical: bool = True
    delay_after_sec: float = 0.0


@dataclass
class StepResult:
    name: str
    ok: bool
    critical: bool
    error: Optional[str] = None
    elapsed_sec: float = 0.0


@dataclass
class BitgetRunReport:
    mode: str
    run_id: str
    started_at: str
    finished_at: str
    steps: List[StepResult] = field(default_factory=list)
    skipped_lock: bool = False
    skipped_lock_detail: Optional[str] = None
    skipped_session: bool = False
    skipped_session_detail: Optional[str] = None

    @property
    def all_critical_ok(self) -> bool:
        return all(s.ok for s in self.steps if s.critical)

    @property
    def any_failure(self) -> bool:
        return any(not s.ok for s in self.steps)

    @property
    def status_label(self) -> str:
        if self.skipped_lock:
            return "SKIPPED_LOCK"
        if self.skipped_session:
            return "SKIPPED_SESSION"
        if not self.any_failure:
            return "OK"
        if self.all_critical_ok:
            return "PARTIAL_FAIL"
        return "FAIL"


def _default_lock_path(mode: str = "") -> str:
    return job_lock_path(mode)


def _lock_max_age_sec(*, holder_mode: Optional[str] = None) -> float:
    hm = str(holder_mode or "").strip().lower()
    if hm == "data_refresh":
        raw = (os.environ.get("BITGET_DATA_REFRESH_LOCK_MAX_SEC") or "3600").strip()
        try:
            return max(300.0, float(raw))
        except ValueError:
            return 3600.0
    raw = (os.environ.get("BITGET_LOCK_MAX_AGE_SEC") or "7200").strip()
    try:
        return max(60.0, float(raw))
    except ValueError:
        return 7200.0


def _lock_break_alive_on_max_age() -> bool:
    return str(os.environ.get("BITGET_LOCK_BREAK_ON_MAX_AGE", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


@dataclass(frozen=True)
class LockMetadata:
    mode: str
    started_at: str
    pid: int


def _parse_lock_metadata(path: str) -> Optional[LockMetadata]:
    try:
        with open(path, encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
    except OSError:
        return None
    if len(lines) < 2:
        return None
    mode = lines[0]
    started_at = lines[1]
    pid = 0
    if len(lines) >= 3:
        try:
            pid = int(lines[2])
        except ValueError:
            pid = 0
    return LockMetadata(mode=mode, started_at=started_at, pid=pid)


def _lock_holder_age_sec(path: str, meta: Optional[LockMetadata] = None) -> float:
    """Prefer lock metadata timestamp over file mtime (mtime can lie after touch)."""
    meta = meta if meta is not None else _parse_lock_metadata(path)
    if meta is not None and meta.started_at:
        try:
            tz = pytz.timezone("Asia/Seoul")
            started = datetime.fromisoformat(meta.started_at)
            if started.tzinfo is None:
                started = tz.localize(started)
            else:
                started = started.astimezone(tz)
            return max(0.0, time.time() - started.timestamp())
        except (ValueError, TypeError, OSError):
            pass
    return _lock_file_age_sec(path)


def _lock_file_age_sec(path: str) -> float:
    try:
        return max(0.0, time.time() - os.path.getmtime(path))
    except OSError:
        return 0.0


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    if sys.platform != "win32":
        try:
            with open(f"/proc/{pid}/status", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("State:"):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1] == "Z":
                            return False
                        break
        except OSError:
            pass
    return True


def _write_lock_metadata(lock_f, mode: str) -> None:
    tz = pytz.timezone("Asia/Seoul")
    lock_f.seek(0)
    lock_f.truncate()
    lock_f.write(f"{mode}\n{datetime.now(tz).isoformat()}\n{os.getpid()}\n")
    lock_f.flush()


def _describe_lock_holder(path: str) -> str:
    meta = _parse_lock_metadata(path)
    age = _lock_holder_age_sec(path, meta)
    if meta is None:
        return f"lock_file={path} age={age:.0f}s (metadata unreadable)"
    alive = _pid_is_alive(meta.pid)
    return (
        f"holder_mode={meta.mode} pid={meta.pid} alive={alive} "
        f"since={meta.started_at} age={age:.0f}s file={path}"
    )


def _try_nonblocking_acquire(lock_f, fcntl_mod) -> bool:
    try:
        fcntl_mod.flock(lock_f.fileno(), fcntl_mod.LOCK_EX | fcntl_mod.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def _maybe_purge_stale_lock_file(path: str) -> bool:
    if not os.path.isfile(path):
        return False
    meta = _parse_lock_metadata(path)
    if meta is not None and _pid_is_alive(meta.pid):
        return False
    if meta is None and _lock_holder_age_sec(path, meta) < 30.0:
        return False
    try:
        os.unlink(path)
        logger.warning("purged stale bitget lock file: %s", path)
        return True
    except OSError as ex:
        logger.warning("stale bitget lock purge failed %s: %s", path, ex)
        return False


def _emergency_release_lock(path: str, lock_f, fcntl_mod, *, acquired: bool) -> None:
    if sys.platform == "win32":
        return
    if acquired:
        try:
            fcntl_mod.flock(lock_f.fileno(), fcntl_mod.LOCK_UN)
        except Exception:
            pass
    try:
        lock_f.close()
    except Exception:
        pass
    try:
        if os.path.isfile(path):
            os.unlink(path)
            logger.warning("bitget lock released on shutdown: %s", path)
    except OSError as ex:
        logger.warning("bitget lock unlink on shutdown failed %s: %s", path, ex)


@contextmanager
def _lock_shutdown_guard(path: str, lock_f, fcntl_mod, acquired: bool, on_release: Callable[[], None]):
    if sys.platform == "win32" or not acquired:
        yield
        return

    prev_int = signal.getsignal(signal.SIGINT)
    prev_term = signal.getsignal(signal.SIGTERM)

    def _handler(signum, _frame):
        logger.warning("bitget lock signal %s — releasing %s", signum, path)
        _emergency_release_lock(path, lock_f, fcntl_mod, acquired=True)
        on_release()
        if signum == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128 + int(signum))

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, prev_int)
        signal.signal(signal.SIGTERM, prev_term)


def _attempt_stale_lock_self_heal(
    path: str,
    lock_f,
    *,
    requesting_mode: str,
    fcntl_mod,
) -> Tuple[bool, str]:
    meta = _parse_lock_metadata(path)
    age = _lock_holder_age_sec(path, meta)
    max_age = _lock_max_age_sec(holder_mode=meta.mode if meta else None)

    if meta is None:
        if age < 30.0:
            return False, "no metadata yet"
        reason = f"orphan lock file age={age:.0f}s"
    else:
        alive = _pid_is_alive(meta.pid)
        if alive and age <= max_age:
            return False, _describe_lock_holder(path)
        if alive and age > max_age and not _lock_break_alive_on_max_age():
            return (
                False,
                f"holder still alive pid={meta.pid} age={age:.0f}s > max={max_age:.0f}s",
            )
        if alive and age > max_age and _lock_break_alive_on_max_age():
            try:
                os.kill(meta.pid, signal.SIGTERM)
                time.sleep(2.0)
            except (ProcessLookupError, PermissionError) as ex:
                logger.warning("stale bitget lock SIGTERM pid=%s: %s", meta.pid, ex)
            if _pid_is_alive(meta.pid):
                return False, f"SIGTERM sent but pid={meta.pid} still alive"
            reason = f"stale alive holder terminated pid={meta.pid} age={age:.0f}s"
        elif not alive:
            reason = f"dead holder pid={meta.pid} mode={meta.mode} age={age:.0f}s"
        else:
            reason = f"stale holder pid={meta.pid}"

    if not _try_nonblocking_acquire(lock_f, fcntl_mod):
        if meta is not None and not _pid_is_alive(meta.pid):
            try:
                lock_f.close()
            except Exception:
                pass
            if _maybe_purge_stale_lock_file(path):
                return False, f"{reason}; lock file purged — retry open"
        return False, f"{reason}; flock still busy"

    logger.warning("bitget lock self-heal: %s → acquiring as %s", reason, requesting_mode)
    return True, reason


@contextmanager
def bitget_job_lock(
    mode: str,
    *,
    lock_path: Optional[str] = None,
    timeout_sec: float = 120.0,
):
    path = lock_path or _default_lock_path(mode)
    if sys.platform == "win32":
        yield
        return

    import fcntl

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    _maybe_purge_stale_lock_file(path)
    lock_f = open(path, "a+", encoding="utf-8")
    acquired = False
    released_box: dict[str, bool] = {"done": False}
    deadline = time.monotonic() + max(1.0, float(timeout_sec))
    last_detail = _describe_lock_holder(path)

    def _mark_released() -> None:
        released_box["done"] = True

    def _release_once() -> None:
        if released_box["done"]:
            return
        _mark_released()
        if acquired:
            try:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        try:
            lock_f.close()
        except Exception:
            pass

    try:
        while time.monotonic() < deadline:
            if _try_nonblocking_acquire(lock_f, fcntl):
                acquired = True
                break
            healed, heal_reason = _attempt_stale_lock_self_heal(
                path, lock_f, requesting_mode=mode, fcntl_mod=fcntl
            )
            if healed:
                acquired = True
                logger.info("bitget lock acquired after self-heal: %s", heal_reason)
                break
            if "lock file purged" in heal_reason:
                try:
                    lock_f.close()
                except Exception:
                    pass
                lock_f = open(path, "a+", encoding="utf-8")
                if _try_nonblocking_acquire(lock_f, fcntl):
                    acquired = True
                    logger.info("bitget lock acquired after purge reopen")
                    break
            last_detail = _describe_lock_holder(path)
            time.sleep(1.0)
        if not acquired:
            raise JobSkipError(f"bitget lock busy ({path}); mode={mode} — {last_detail}")
        _write_lock_metadata(lock_f, mode)
        with _lock_shutdown_guard(path, lock_f, fcntl, acquired, on_release=_mark_released):
            try:
                yield
            except (KeyboardInterrupt, SystemExit):
                if not released_box["done"]:
                    _emergency_release_lock(path, lock_f, fcntl, acquired=True)
                    _mark_released()
                raise
    finally:
        if acquired and not released_box["done"]:
            _release_once()
        elif not released_box["done"]:
            try:
                lock_f.close()
            except Exception:
                pass


def run_step(spec: StepSpec) -> StepResult:
    t0 = time.monotonic()
    try:
        spec.fn()
        return StepResult(
            name=spec.name,
            ok=True,
            critical=spec.critical,
            elapsed_sec=time.monotonic() - t0,
        )
    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("bitget step failed: %s", spec.name)
        return StepResult(
            name=spec.name,
            ok=False,
            critical=spec.critical,
            error=f"{e}\n{tb[-800:]}",
            elapsed_sec=time.monotonic() - t0,
        )


def format_bitget_run_telegram(report: BitgetRunReport) -> str:
    st = report.status_label
    icon = "✅" if st == "OK" else "⚠️" if st == "PARTIAL_FAIL" else "🚨"
    lines = [
        f"{icon} <b>[Bitget Job] {html.escape(report.mode, quote=False)} · {st}</b>",
        f"▪ run_id: <code>{html.escape(report.run_id, quote=False)}</code>",
        f"▪ window: {html.escape(report.started_at, quote=False)} → "
        f"{html.escape(report.finished_at, quote=False)}",
    ]
    if report.skipped_lock:
        lines.append("▪ <i>이전 동일 잡 실행 중 — 이번 회차 스킵</i>")
        if report.skipped_lock_detail:
            lines.append(
                "▪ <i>" + html.escape(report.skipped_lock_detail[:500], quote=False) + "</i>"
            )
        return "\n".join(lines) + "\n"

    ok_names = [s.name for s in report.steps if s.ok]
    fail_steps = [s for s in report.steps if not s.ok]
    if ok_names:
        lines.append("▪ <b>OK:</b> " + html.escape(", ".join(ok_names), quote=False))
    if fail_steps:
        lines.append("▪ <b>FAIL:</b>")
        for s in fail_steps:
            crit = "critical" if s.critical else "optional"
            err_short = (s.error or "unknown")[:400].replace("\n", " ")
            lines.append(
                f"  · <code>{html.escape(s.name, quote=False)}</code> "
                f"({crit}) — <i>{html.escape(err_short, quote=False)}</i>"
            )
    return "\n".join(lines) + "\n"


def _record_ops_heartbeat(mode: str, report: BitgetRunReport) -> None:
    try:
        from bitget.infra import ops_logger

        ops_logger.record_heartbeat(
            f"bitget.{mode}",
            extra={
                "status": report.status_label,
                "critical_ok": report.all_critical_ok,
            },
        )
    except Exception:
        pass


def dispatch_bitget_mode(
    mode: str,
    pipeline: Sequence[StepSpec],
    *,
    send_fn: Optional[SendFn] = None,
    skip_telegram: bool = False,
    dry_run: bool = False,
    lock_timeout_sec: Optional[float] = None,
) -> BitgetRunReport:
    tz = pytz.timezone("Asia/Seoul")
    run_id = datetime.now(tz).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    started = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    report = BitgetRunReport(mode=mode, run_id=run_id, started_at=started, finished_at=started)

    if dry_run:
        logger.info("[dry-run] mode=%s steps=%s", mode, [s.name for s in pipeline])
        report.finished_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        return report

    try:
        from bitget.bitget_schedule_guard import cron_misalignment_hint, evaluate_scan_skip, is_quiet_scan_skip

        skip, skip_reason = evaluate_scan_skip(mode)
        if skip:
            report.skipped_session = True
            report.skipped_session_detail = skip_reason
            report.finished_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
            logger.info("bitget scan skipped (%s): %s", mode, skip_reason)
            # [#2] 주식 factory 양보(yield)로 인한 스킵이면 Drop 대신 큐 적재(대기 후 실행).
            #   BITGET_ENQUEUE_ON_YIELD=1 + 큐 워커 가동 시에만 효과(기본 OFF → 동작 불변).
            #   인라인 cron 경로에서 적재 → 워커가 주식 락 풀린 뒤 픽업. 워커 경로에서
            #   호출되면 dedupe(현재 RUNNING)로 무해하게 no-op 되고, 워커가 defer 처리한다.
            if "yield_to_factory" in str(skip_reason) and _enqueue_on_yield_enabled():
                try:
                    from bitget.infra.task_orchestrator import ENGINE_BITGET, enqueue

                    qid = enqueue(ENGINE_BITGET, mode)
                    if qid is not None:
                        logger.info("bitget yield → enqueued #%s %s (await factory free)", qid, mode)
                except Exception:
                    logger.warning("bitget yield enqueue failed (mode=%s)", mode, exc_info=True)
            return report
    except Exception:
        pass

    # Mission 3: 스캔 서킷 브레이커 — 연속 실패로 회로가 OPEN 이면 좀비 재시도를 차단.
    is_scan_mode = str(mode).strip().lower().startswith("scan_")
    if is_scan_mode:
        try:
            from bitget.watchdog import is_circuit_open

            cb_open, cb_detail = is_circuit_open(mode)
            if cb_open:
                report.skipped_session = True
                report.skipped_session_detail = f"circuit_breaker_open: {cb_detail}"
                report.finished_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                logger.error("bitget scan circuit OPEN — discarding %s: %s", mode, cb_detail)
                _record_ops_heartbeat(mode, report)
                return report
        except Exception:
            pass

    effective_lock = resolve_lock_timeout_sec(mode, explicit=lock_timeout_sec)

    try:
        with bitget_job_lock(mode, timeout_sec=effective_lock):
            aborted = False
            for spec in pipeline:
                if aborted:
                    report.steps.append(
                        StepResult(
                            name=spec.name,
                            ok=False,
                            critical=spec.critical,
                            error="skipped: prior critical step failed (zombie pipeline guard)",
                        )
                    )
                    continue
                result = run_step(spec)
                report.steps.append(result)
                if not result.ok and result.critical:
                    aborted = True
                    logger.error(
                        "bitget pipeline aborted at critical step %s — "
                        "remaining steps will not execute",
                        spec.name,
                    )
                    continue
                if spec.delay_after_sec > 0:
                    time.sleep(spec.delay_after_sec)
    except JobSkipError as e:
        report.skipped_lock = True
        report.skipped_lock_detail = str(e)
        logger.warning("bitget job skipped: %s", e)
    except Exception as e:
        logger.exception("bitget dispatch outer error")
        report.steps.append(
            StepResult(name="dispatch_outer", ok=False, critical=True, error=str(e))
        )

    report.finished_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # Mission 3: 스캔 결과를 서킷 브레이커에 반영 (OK→close, FAIL→누적/OPEN).
    if is_scan_mode:
        try:
            from bitget.watchdog import record_job_failure, record_job_success

            if report.status_label == "OK":
                record_job_success(mode)
            elif report.status_label in ("FAIL", "PARTIAL_FAIL"):
                first_err = next((s.error for s in report.steps if not s.ok and s.error), "")
                opened, cb_label = record_job_failure(mode, first_err or "scan failed")
                if opened:
                    logger.error("bitget scan circuit tripped → %s", cb_label)
        except Exception:
            pass

    quiet = False
    if report.status_label == "SKIPPED_LOCK":
        alert = str(os.environ.get("BITGET_ALERT_SKIPPED_LOCK", "0")).strip().lower()
        quiet = alert not in ("1", "true", "yes", "on")
    if report.status_label in ("SKIPPED_SESSION", "SKIPPED_LOCK"):
        try:
            from bitget.bitget_schedule_guard import cron_misalignment_hint, is_quiet_scan_skip

            quiet = is_quiet_scan_skip(
                mode,
                detail=report.skipped_session_detail or report.skipped_lock_detail or "",
            )
            # Lock wait skews wall clock — drift hint is misleading on SKIPPED_LOCK.
            if report.status_label != "SKIPPED_LOCK":
                mis, hint = cron_misalignment_hint(mode)
                if mis and send_fn and not skip_telegram:
                    send_fn(f"⚠️ <b>Bitget cron drift</b>\n{hint}")
        except Exception:
            quiet = report.status_label in ("SKIPPED_SESSION", "SKIPPED_LOCK")
    if send_fn and not skip_telegram and report.status_label not in ("OK",) and not quiet:
        send_fn(format_bitget_run_telegram(report))
    _record_ops_heartbeat(mode, report)
    return report


def bitget_exit_code(report: BitgetRunReport) -> int:
    if report.skipped_lock or report.skipped_session:
        return 0
    if report.all_critical_ok:
        return 0
    return 1
