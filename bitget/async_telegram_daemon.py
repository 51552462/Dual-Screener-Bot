"""
Bitget-isolated async Telegram daemon.

Uses `bitget_message_queue.sqlite` (not equity message_queue.sqlite).
Heartbeat / gauges write to `bitget_ops_events.sqlite` so Bitget watchdog
can observe hangs and restart `dante-bitget-async` (never equity ops DB).
Bootstrap uses BITGET_* credentials only — never equity MAIN/EQUITY_KR keys.
"""
from __future__ import annotations

import asyncio
import os


def _patch_bitget_queue_paths() -> None:
    import telegram_message_queue as tmq
    from bitget.infra.data_paths import bitget_data_dir, message_queue_db_path

    tmq._BOT_DIR = bitget_data_dir()
    tmq.MESSAGE_QUEUE_DB_PATH = message_queue_db_path()
    tmq._schema_ready = False


def _patch_bitget_ops_logger() -> None:
    """Redirect root async_telegram_daemon HB/gauges → Bitget ops_events SSOT."""
    import async_telegram_daemon as atd
    import bitget.infra.ops_logger as bg_ops

    atd.ops_logger = bg_ops


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
    from bitget.infra.logging_setup import get_logger

    _patch_bitget_queue_paths()
    _patch_bitget_ops_logger()
    os.environ.setdefault("DANTE_ASYNC_TELEGRAM_DAEMON", "1")
    os.environ.setdefault("BITGET_ASYNC_TELEGRAM", "1")

    reg = _bootstrap_bitget_daemon_registration()
    if not reg:
        get_logger("bitget.async_telegram_daemon").error(
            "queue daemon registration missing — set in bitget/.env or root .env: "
            "BITGET_TELEGRAM_TOKEN (or BITGET_BOT_TOKEN), "
            "BITGET_TELEGRAM_CHAT_ID (or BITGET_BOT_CHAT_ID)"
        )
        return 2

    import async_telegram_daemon

    # Re-apply after import in case module was loaded earlier without patch
    _patch_bitget_ops_logger()

    tm, tp, cid, en = reg
    asyncio.run(async_telegram_daemon.run_async_telegram_daemon(tm, tp, cid, en))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
