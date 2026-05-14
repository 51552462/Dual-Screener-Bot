"""
텔레그램 봇 자격 증명 단일 해석 레이어.

- 값은 오직 환경 변수(.env / systemd EnvironmentFile)에서만 읽는다.
- 신규 이름(MAIN_BOT_TOKEN 등) 우선, 레거시 이름(TELEGRAM_TOKEN_MAIN 등) 폴백.
- 비어 있으면 ops_logger 에 1회만 CONFIG 경고를 남긴다(폴백 체인 끝이 비었을 때만).
"""
from __future__ import annotations

import os
import sys
from typing import Iterable

_warned: set[str] = set()


def _log_config_missing(label: str, tried: Iterable[str]) -> None:
    if label in _warned:
        return
    _warned.add(label)
    msg = f"⚠️ [CONFIG] {label} is missing (set one of: {', '.join(tried)} in .env)"
    try:
        import ops_logger

        ops_logger.insert_ops_event(
            component="telegram_env",
            severity="WARN",
            event="config.missing",
            payload={"label": label, "tried": list(tried), "msg": msg},
        )
    except Exception:
        print(msg, file=sys.stderr)


def _first_nonempty(*names: str, label: str) -> str:
    tried = [n for n in names if n]
    for n in tried:
        v = (os.environ.get(n) or "").strip()
        if v:
            return v
    _log_config_missing(label, tried)
    return ""


def get_main_token() -> str:
    return _first_nonempty(
        "MAIN_BOT_TOKEN",
        "TELEGRAM_TOKEN_MAIN",
        label="MAIN_BOT_TOKEN / TELEGRAM_TOKEN_MAIN",
    )


def get_promo_token() -> str:
    v = (os.environ.get("PROMO_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN_PROMO") or "").strip()
    if v:
        return v
    return get_main_token()


def get_factory_chat_id() -> str:
    return _first_nonempty(
        "FACTORY_CHAT_ID",
        "TELEGRAM_CHAT_ID",
        label="FACTORY_CHAT_ID / TELEGRAM_CHAT_ID",
    )


def get_report_token() -> str:
    for n in ("REPORT_BOT_TOKEN", "TELEGRAM_TOKEN_MAIN", "TELEGRAM_TOKEN"):
        v = (os.environ.get(n) or "").strip()
        if v:
            return v
    return get_main_token()


def get_report_chat_id() -> str:
    for n in ("REPORT_BOT_CHAT_ID", "TELEGRAM_CHAT_ID"):
        v = (os.environ.get(n) or "").strip()
        if v:
            return v
    return get_factory_chat_id()


def get_overseer_token() -> str:
    for n in ("OVERSEER_BOT_TOKEN", "REPORT_BOT_TOKEN"):
        v = (os.environ.get(n) or "").strip()
        if v:
            return v
    return get_main_token()


def get_overseer_chat_id() -> str:
    for n in ("OVERSEER_BOT_CHAT_ID", "REPORT_BOT_CHAT_ID", "FACTORY_CHAT_ID", "TELEGRAM_CHAT_ID"):
        v = (os.environ.get(n) or "").strip()
        if v:
            return v
    return get_factory_chat_id()


def get_watchdog_token() -> str:
    return _first_nonempty(
        "WATCHDOG_BOT_TOKEN",
        "MAIN_BOT_TOKEN",
        "TELEGRAM_TOKEN_MAIN",
        label="WATCHDOG_BOT_TOKEN / MAIN_BOT_TOKEN / TELEGRAM_TOKEN_MAIN",
    )


def get_watchdog_chat_id() -> str:
    return _first_nonempty(
        "WATCHDOG_BOT_CHAT_ID",
        "FACTORY_CHAT_ID",
        "TELEGRAM_CHAT_ID",
        label="WATCHDOG_BOT_CHAT_ID / FACTORY_CHAT_ID / TELEGRAM_CHAT_ID",
    )


def get_lab_token() -> str:
    return _first_nonempty(
        "LAB_BOT_TOKEN",
        "TELEGRAM_BOT_TOKEN",
        "TG_BOT_TOKEN",
        label="LAB_BOT_TOKEN / TELEGRAM_BOT_TOKEN / TG_BOT_TOKEN",
    )


def get_lab_chat_id() -> str:
    return _first_nonempty(
        "LAB_BOT_CHAT_ID",
        "TELEGRAM_CHAT_ID",
        "TG_CHAT_ID",
        label="LAB_BOT_CHAT_ID / TELEGRAM_CHAT_ID / TG_CHAT_ID",
    )


def get_secretary_kr_token() -> str:
    return _first_nonempty("SECRETARY_KR_BOT_TOKEN", label="SECRETARY_KR_BOT_TOKEN")


def get_secretary_us_token() -> str:
    return _first_nonempty("SECRETARY_US_BOT_TOKEN", label="SECRETARY_US_BOT_TOKEN")


def get_secretary_new_token() -> str:
    return _first_nonempty("SECRETARY_NEW_BOT_TOKEN", label="SECRETARY_NEW_BOT_TOKEN")


def get_bitget_bot_token() -> str:
    return _first_nonempty(
        "BITGET_BOT_TOKEN",
        "BITGET_TELEGRAM_TOKEN",
        "BITGET_TELEGRAM_TOKEN_MAIN",
        label="BITGET_BOT_TOKEN / BITGET_TELEGRAM_TOKEN / BITGET_TELEGRAM_TOKEN_MAIN",
    )


def get_bitget_promo_token() -> str:
    v = (os.environ.get("BITGET_BOT_PROMO_TOKEN") or os.environ.get("BITGET_TELEGRAM_TOKEN_PROMO") or "").strip()
    if v:
        return v
    return get_bitget_bot_token()


def get_bitget_chat_id() -> str:
    return _first_nonempty(
        "BITGET_BOT_CHAT_ID",
        "BITGET_TELEGRAM_CHAT_ID",
        label="BITGET_BOT_CHAT_ID / BITGET_TELEGRAM_CHAT_ID",
    )


def log_preflight_factory_telegram() -> str:
    """main.py 기동 시: 스캐너 큐용 MAIN+CHAT 이 전혀 없으면 경고. 빈 문자열 반환."""
    if get_main_token() and get_factory_chat_id():
        return ""
    _log_config_missing(
        "factory telegram (MAIN_BOT_TOKEN + FACTORY_CHAT_ID or legacy TELEGRAM_*)",
        ("MAIN_BOT_TOKEN", "FACTORY_CHAT_ID", "TELEGRAM_TOKEN_MAIN", "TELEGRAM_CHAT_ID"),
    )
    return ""
