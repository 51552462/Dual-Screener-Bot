"""
Bitget-isolated async Telegram daemon.

Uses `bitget_message_queue.sqlite` (not equity message_queue.sqlite).
"""
from __future__ import annotations

import os
import sys


def _patch_bitget_queue_paths() -> None:
    import telegram_message_queue as tmq
    from bitget.infra.data_paths import bitget_data_dir, message_queue_db_path

    tmq._BOT_DIR = bitget_data_dir()
    tmq.MESSAGE_QUEUE_DB_PATH = message_queue_db_path()
    tmq._schema_ready = False


def main() -> int:
    _patch_bitget_queue_paths()
    os.environ.setdefault("DANTE_ASYNC_TELEGRAM_DAEMON", "1")
    os.environ.setdefault("BITGET_ASYNC_TELEGRAM", "1")

    import async_telegram_daemon

    async_telegram_daemon.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
