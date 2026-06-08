import os
import time
from datetime import datetime, timezone

from bitget.infra.logging_setup import get_logger


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHARTS_DIR = os.path.join(BASE_DIR, "charts")
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"}
RETENTION_DAYS = 3

logger = get_logger("bitget.disk_manager")


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


def run_daily_cleanup_loop():
    logger.info("disk manager loop started")
    last_day = ""
    while True:
        now = datetime.now(timezone.utc)
        day_key = now.strftime("%Y-%m-%d")
        if now.hour == 0 and day_key != last_day:
            cleanup_old_chart_images(RETENTION_DAYS)
            last_day = day_key
        time.sleep(60)
