"""
Factory job runner — Step 격리, flock, PARTIAL_FAIL 텔레그램, cron-safe exit codes.
"""
from __future__ import annotations

import html
import logging
import os
import sys
import time
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional, Sequence

import pytz

logger = logging.getLogger(__name__)

SendFn = Callable[[str], None]

FACTORY_MODES = frozenset(
    {
        "scan_kr",
        "scan_us",
        "daily_audit",
        "daily_audit_kr",
        "daily_audit_us",
        "weekly_master",
    }
)


class JobSkipError(Exception):
    """동일 mode job이 이미 실행 중 — DB 이중 진입 방지."""


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
class FactoryRunReport:
    mode: str
    run_id: str
    started_at: str
    finished_at: str
    steps: List[StepResult] = field(default_factory=list)
    skipped_lock: bool = False

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
        if not self.any_failure:
            return "OK"
        if self.all_critical_ok:
            return "PARTIAL_FAIL"
        return "FAIL"


def _default_lock_path() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, ".factory_runtime.lock")


@contextmanager
def factory_job_lock(
    mode: str,
    *,
    lock_path: Optional[str] = None,
    timeout_sec: float = 120.0,
):
    """
    Ubuntu: fcntl flock. Windows 개발 환경: no-op (로컬 dry-run).
  이미 실행 중이면 JobSkipError.
    """
    path = lock_path or _default_lock_path()
    if sys.platform == "win32":
        yield
        return

    import fcntl

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    lock_f = open(path, "a+", encoding="utf-8")
    acquired = False
    deadline = time.monotonic() + max(1.0, float(timeout_sec))
    try:
        while time.monotonic() < deadline:
            try:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                time.sleep(1.0)
        if not acquired:
            raise JobSkipError(
                f"factory lock busy ({path}); mode={mode} — previous job still running"
            )
        lock_f.seek(0)
        lock_f.truncate()
        lock_f.write(f"{mode}\n{datetime.now(pytz.timezone('Asia/Seoul')).isoformat()}\n")
        lock_f.flush()
        yield
    finally:
        if acquired:
            try:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        lock_f.close()


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
        logger.exception("factory step failed: %s", spec.name)
        return StepResult(
            name=spec.name,
            ok=False,
            critical=spec.critical,
            error=f"{e}\n{tb[-800:]}",
            elapsed_sec=time.monotonic() - t0,
        )


def format_factory_run_telegram(report: FactoryRunReport) -> str:
    st = report.status_label
    icon = "✅" if st == "OK" else "⚠️" if st == "PARTIAL_FAIL" else "🚨"
    lines = [
        f"{icon} <b>[Factory Job] {html.escape(report.mode, quote=False)} · {st}</b>",
        f"▪ run_id: <code>{html.escape(report.run_id, quote=False)}</code>",
        f"▪ window: {html.escape(report.started_at, quote=False)} → "
        f"{html.escape(report.finished_at, quote=False)}",
    ]
    if report.skipped_lock:
        lines.append("▪ <i>이전 동일 잡 실행 중 — 이번 회차 스킵 (DB 보호)</i>")
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
    lines.append("▪ 다음 크론 회차는 정상 재시도됩니다.")
    return "\n".join(lines) + "\n"


def notify_factory_run(
    report: FactoryRunReport,
    *,
    send_fn: Optional[SendFn] = None,
    skip_telegram: bool = False,
) -> None:
    if skip_telegram:
        return
    st = report.status_label
    if st == "OK":
        return
    if st == "SKIPPED_LOCK":
        if send_fn:
            send_fn(format_factory_run_telegram(report))
        return
    if send_fn:
        send_fn(format_factory_run_telegram(report))


def _record_ops_heartbeat(mode: str, report: FactoryRunReport) -> None:
    try:
        import ops_logger

        ops_logger.record_heartbeat(
            f"factory.{mode}",
            extra={
                "status": report.status_label,
                "critical_ok": report.all_critical_ok,
            },
        )
    except Exception:
        pass


def dispatch_factory_mode(
    mode: str,
    pipeline: Sequence[StepSpec],
    *,
    send_fn: Optional[SendFn] = None,
    skip_telegram: bool = False,
    dry_run: bool = False,
    lock_timeout_sec: float = 120.0,
) -> FactoryRunReport:
    """
    파이프라인 순차 실행. 프로세스는 예외로 죽지 않음 — 실패는 StepResult에 기록.
    Returns report; exit code는 main()에서 all_critical_ok 기준 결정.
    """
    tz = pytz.timezone("Asia/Seoul")
    run_id = datetime.now(tz).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    started = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    report = FactoryRunReport(mode=mode, run_id=run_id, started_at=started, finished_at=started)

    if dry_run:
        logger.info("[dry-run] mode=%s steps=%s", mode, [s.name for s in pipeline])
        report.finished_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        return report

    try:
        with factory_job_lock(mode, timeout_sec=lock_timeout_sec):
            for spec in pipeline:
                result = run_step(spec)
                report.steps.append(result)
                if spec.delay_after_sec > 0:
                    time.sleep(spec.delay_after_sec)
    except JobSkipError as e:
        report.skipped_lock = True
        logger.warning("factory job skipped: %s", e)
    except Exception as e:
        logger.exception("factory dispatch outer error")
        report.steps.append(
            StepResult(
                name="dispatch_outer",
                ok=False,
                critical=True,
                error=str(e),
            )
        )

    report.finished_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    notify_factory_run(report, send_fn=send_fn, skip_telegram=skip_telegram)
    _record_ops_heartbeat(mode, report)
    return report


def factory_exit_code(report: FactoryRunReport) -> int:
    if report.skipped_lock:
        return 0
    if report.all_critical_ok:
        return 0
    return 1
