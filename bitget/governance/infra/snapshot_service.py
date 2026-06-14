"""
CQRS snapshot — backup `bitget_market_data.sqlite` -> `bitget_market_data_snapshot.sqlite`.

systemd oneshot / cron: `python -m bitget.infra.snapshot_service`
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time

from bitget.infra.data_paths import market_data_db_path, market_data_snapshot_db_path
from bitget.infra.logging_setup import get_logger, setup_logging

logger = get_logger("bitget.snapshot")


def backup_market_db(*, timeout_sec: float = 90.0) -> bool:
    main = market_data_db_path()
    snap = market_data_snapshot_db_path()
    if not os.path.isfile(main):
        print(f"[bitget-snapshot] skip (no main db): {main}")
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
    print(f"[bitget-snapshot] SQLite backup ok -> {snap}")
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
        logger.warning("snapshot backup failed: %s", e)
        print(f"[bitget-snapshot] backup failed: {e}", file=sys.stderr)

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
        interval = float(os.environ.get("BITGET_SNAPSHOT_INTERVAL_SEC", "300"))
        setup_logging(default_component="bitget.snapshot")
        logger.info("snapshot loop interval=%.0fs", interval)
        while True:
            run_snapshot_job()
            time.sleep(max(30.0, interval))
        return 0

    result = run_snapshot_job()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
