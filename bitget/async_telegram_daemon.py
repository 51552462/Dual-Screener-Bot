"""
Bitget-isolated async Telegram daemon.

Uses `bitget_message_queue.sqlite` (not equity message_queue.sqlite).
Bootstrap uses BITGET_* credentials only — never equity MAIN/EQUITY_KR keys.
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


def _load_bitget_dotenv() -> None:
    try:
        from dotenv import load_dotenv

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        install = os.path.dirname(root)
        load_dotenv(os.path.join(install, ".env"))
        load_dotenv(os.path.join(root, ".env"))
    except Exception:
        pass


def _bootstrap_bitget_daemon_registration():
    """Bitget 전용 토큰·채팅 — equity async_telegram_daemon.main() 과 분리."""
    import asyncio

    import telegram_env
    from telegram_message_queue import get_telegram_daemon_registration, start_telegram_queue_daemons

    reg = get_telegram_daemon_registration()
    if reg and reg[0] and reg[2] and reg[3]:
        return reg

    _load_bitget_dotenv()
    token_main = telegram_env.get_bitget_bot_token()
    token_promo = telegram_env.get_bitget_promo_token() or token_main
    chat_id = telegram_env.get_bitget_chat_id()
    send_enabled = bool(token_main and chat_id)
    if not send_enabled:
        return None

    start_telegram_queue_daemons(
        token_main,
        token_promo or token_main,
        chat_id,
        send_enabled,
    )
    return get_telegram_daemon_registration()


def main() -> int:
    _patch_bitget_queue_paths()
    os.environ.setdefault("DANTE_ASYNC_TELEGRAM_DAEMON", "1")
    os.environ.setdefault("BITGET_ASYNC_TELEGRAM", "1")

    reg = _bootstrap_bitget_daemon_registration()
    if not reg:
        print(
            "⚠️ [bitget.async_telegram_daemon] 큐 데몬 등록 없음 — "
            "bitget/.env 또는 루트 .env 에 아래를 설정하세요.\n"
            "  BITGET_TELEGRAM_TOKEN (또는 BITGET_BOT_TOKEN)\n"
            "  BITGET_TELEGRAM_CHAT_ID (또는 BITGET_BOT_CHAT_ID)",
            file=sys.stderr,
        )
        return 2

    import async_telegram_daemon

    tm, tp, cid, en = reg
    asyncio.run(async_telegram_daemon.run_async_telegram_daemon(tm, tp, cid, en))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
