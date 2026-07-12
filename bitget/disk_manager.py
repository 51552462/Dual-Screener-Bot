import os
import re
import time

from bitget.infra.daemon_loop import DISK_MGR_POLL_SEC, DaemonLoopFrame, sleep_or_backoff
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import (
    CHART_IMAGE_RETENTION_DAYS,
    LOG_FILE_NAME,
    STAMPED_LOG_RETENTION_DAYS,
)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHARTS_DIR = os.path.join(BASE_DIR, "charts")
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"}
RETENTION_DAYS = CHART_IMAGE_RETENTION_DAYS
# bitget.sh: bitget_<mode>_YYYYMMDD_HHMMSS.log (and daemon/ws/canary variants)
_STAMPED_LOG_RE = re.compile(r"^bitget_.+_\d{8}_\d{6}\.log$")

logger = get_logger("bitget.disk_manager")


def _logs_dir() -> str:
    env = (os.environ.get("BITGET_LOG_DIR") or "").strip()
    if env:
        return env
    return os.path.join(BASE_DIR, "logs")


def is_stamped_shell_log(name: str) -> bool:
    """True only for bitget.sh stamped logs — never RotatingFileHandler bitget.log*."""
    base = os.path.basename(name or "")
    if base == LOG_FILE_NAME or base.startswith(f"{LOG_FILE_NAME}."):
        return False
    return bool(_STAMPED_LOG_RE.match(base))


def cleanup_old_chart_images(retention_days=RETENTION_DAYS):
    if not os.path.exists(CHARTS_DIR):
        return 0
    now_ts = time.time()
    ttl = int(retention_days) * 86400
    removed = 0
    for name in os.listdir(CHARTS_DIR):
        path = os.path.join(CHARTS_DIR, name)
        if not os.path.isfile(path):
            continue
        ext = os.path.splitext(name)[1].lower()
        if ext not in IMAGE_EXTS:
            continue
        try:
            age = now_ts - os.path.getmtime(path)
            if age >= ttl:
                os.remove(path)
                removed += 1
        except Exception as e:
            logger.warning("chart cleanup skip: %s (%s)", path, e)
    if removed:
        logger.info("chart cleanup removed %s files", removed)
    return removed


def cleanup_stamped_shell_logs(
    log_dir: str | None = None,
    retention_days: int | None = None,
) -> int:
    """Delete aged bitget.sh stamped logs. Never touches bitget.log rotate set."""
    root = log_dir or _logs_dir()
    if not os.path.isdir(root):
        return 0
    days = int(STAMPED_LOG_RETENTION_DAYS if retention_days is None else retention_days)
    if days < 1:
        days = int(STAMPED_LOG_RETENTION_DAYS)
    now_ts = time.time()
    ttl = days * 86400
    removed = 0
    for name in os.listdir(root):
        if not is_stamped_shell_log(name):
            continue
        path = os.path.join(root, name)
        if not os.path.isfile(path):
            continue
        try:
            age = now_ts - os.path.getmtime(path)
            if age >= ttl:
                os.remove(path)
                removed += 1
        except Exception as e:
            logger.warning("stamped log cleanup skip: %s (%s)", path, e)
    if removed:
        logger.info("stamped log cleanup removed %s files (ttl=%sd)", removed, days)
    return removed


def run_bitget_memory_retention(*, force: bool = True) -> dict[str, int]:
    """SQLite append-only 테이블 + ops_events purge (코인 SSOT)."""
    try:
        from bitget.infra.memory_retention import run_bitget_retention_sweep

        return run_bitget_retention_sweep(force=force)
    except Exception as e:
        log_exception(logger, "bitget memory retention skip: %s", e)
        return {}


def run_daily_cleanup_loop():
    logger.info("disk manager loop started")
    try:
        run_bitget_memory_retention(force=True)
        cleanup_stamped_shell_logs()
    except Exception as e:
        log_exception(logger, "bitget memory retention boot sweep skip: %s", e)
    frame = DaemonLoopFrame()
    while True:
        try:
            frame.refresh_utc()
            if frame.tick.hour == 0 and frame.dedup.day_once(frame.tick.day_key):
                cleanup_old_chart_images(RETENTION_DAYS)
                cleanup_stamped_shell_logs()
                run_bitget_memory_retention(force=True)
            frame.mark_ok()
            sleep_or_backoff(normal_sec=DISK_MGR_POLL_SEC, after_error=frame.loop_error)
        except Exception as e:
            log_exception(logger, "disk manager loop error: %s", e)
            frame.mark_error()
            sleep_or_backoff(normal_sec=DISK_MGR_POLL_SEC, after_error=frame.loop_error)
