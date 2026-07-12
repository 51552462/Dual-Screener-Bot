"""
CQRS snapshot — backup `bitget_market_data.sqlite` -> `bitget_market_data_snapshot.sqlite`.

systemd oneshot / cron: `python -m bitget.infra.snapshot_service`
"""
from __future__ import annotations

import os
import sqlite3
import time

from bitget.infra.data_paths import market_data_db_path, market_data_snapshot_db_path
from bitget.infra.logging_setup import get_logger, log_exception, setup_logging

logger = get_logger("bitget.snapshot")


def backup_market_db(*, timeout_sec: float = 90.0) -> bool:
    main = market_data_db_path()
    snap = market_data_snapshot_db_path()
    if not os.path.isfile(main):
        logger.warning("snapshot skip (no main db): %s", main)
        return False

    os.makedirs(os.path.dirname(snap) or ".", exist_ok=True)
    tmp = f"{snap}.tmp.{os.getpid()}"
    if os.path.isfile(tmp):
        try:
            os.remove(tmp)
        except OSError:
            pass

    src = sqlite3.connect(main, timeout=timeout_sec)
    try:
        dst = sqlite3.connect(tmp, timeout=timeout_sec)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    os.replace(tmp, snap)
    logger.info("SQLite backup ok -> %s", snap)
    return True


def run_snapshot_job() -> dict:
    setup_logging(default_component="bitget.snapshot")
    started = time.time()
    ok = False
    err: str | None = None
    try:
        ok = backup_market_db()
    except Exception as e:
        err = str(e)
        log_exception(logger, "snapshot backup failed: %s", e)
    try:
        from bitget.infra import ops_logger

        ops_logger.record_gauge_snapshot(
            "bitget.snapshot",
            {
                "ok": ok,
                "elapsed_sec": round(time.time() - started, 3),
                "main_db": market_data_db_path(),
                "snapshot_db": market_data_snapshot_db_path(),
                "error": err,
            },
        )
    except Exception:
        pass

    return {"ok": ok, "error": err}


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Bitget CQRS market DB snapshot")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run snapshot every BITGET_SNAPSHOT_INTERVAL_SEC (default 300)",
    )
    args = parser.parse_args(argv)

    if args.loop:
        from bitget.infra.daemon_loop import sleep_or_backoff

        interval = max(30.0, float(os.environ.get("BITGET_SNAPSHOT_INTERVAL_SEC", "300")))
        setup_logging(default_component="bitget.snapshot")
        logger.info("snapshot loop interval=%.0fs", interval)
        loop_error = False
        while True:
            try:
                run_snapshot_job()
                loop_error = False
            except Exception as e:
                logger.warning("snapshot loop tick failed: %s", e)
                loop_error = True
            sleep_or_backoff(normal_sec=interval, after_error=loop_error)
        return 0

    result = run_snapshot_job()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
