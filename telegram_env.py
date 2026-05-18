"""
텔레그램 봇 자격 증명 단일 해석 레이어.

- 값은 오직 환경 변수(.env / systemd EnvironmentFile)에서만 읽는다.
- 신규 이름(MAIN_BOT_TOKEN 등) 우선, 레거시 이름(TELEGRAM_TOKEN_MAIN 등) 폴백.
- Equity 스캐너: 한국계열(kr.py, ema5.py, dante_krx, nulrim)은 EQUITY_KR_* → 공통 MAIN/FACTORY.
  미국계열(usa, us_5ema, nasdaq_dante, nulusa, master, us_master)은 EQUITY_US_* → 공통 MAIN/FACTORY.
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


def _env_chain_first(keys: tuple[str, ...]) -> str:
    for k in keys:
        v = (os.environ.get(k) or "").strip()
        if v:
            return v
    return ""


# ---------------------------------------------------------------------------
# Equity 스캐너: 시장별(KR / US) 오버라이드 → 없으면 공통 MAIN / FACTORY 로 폴백
# ---------------------------------------------------------------------------
def get_equity_kr_main_token() -> str:
    v = _env_chain_first(
        (
            "EQUITY_KR_MAIN_BOT_TOKEN",
            "KR_MAIN_BOT_TOKEN",
            "MAIN_BOT_TOKEN",
            "TELEGRAM_TOKEN_MAIN",
        )
    )
    if v:
        return v
    _log_config_missing(
        "EQUITY_KR scanner MAIN (EQUITY_KR_MAIN_BOT_TOKEN / KR_MAIN_BOT_TOKEN / MAIN_BOT_TOKEN / TELEGRAM_TOKEN_MAIN)",
        (
            "EQUITY_KR_MAIN_BOT_TOKEN",
            "KR_MAIN_BOT_TOKEN",
            "MAIN_BOT_TOKEN",
            "TELEGRAM_TOKEN_MAIN",
        ),
    )
    return ""


def get_equity_kr_promo_token() -> str:
    v = _env_chain_first(
        (
            "EQUITY_KR_PROMO_BOT_TOKEN",
            "KR_PROMO_BOT_TOKEN",
            "PROMO_BOT_TOKEN",
            "TELEGRAM_TOKEN_PROMO",
        )
    )
    if v:
        return v
    return get_equity_kr_main_token()


def get_equity_kr_factory_chat_id() -> str:
    v = _env_chain_first(
        (
            "EQUITY_KR_FACTORY_CHAT_ID",
            "KR_FACTORY_CHAT_ID",
            "FACTORY_CHAT_ID",
            "TELEGRAM_CHAT_ID",
        )
    )
    if v:
        return v
    _log_config_missing(
        "EQUITY_KR scanner CHAT (EQUITY_KR_FACTORY_CHAT_ID / KR_FACTORY_CHAT_ID / FACTORY_CHAT_ID / TELEGRAM_CHAT_ID)",
        (
            "EQUITY_KR_FACTORY_CHAT_ID",
            "KR_FACTORY_CHAT_ID",
            "FACTORY_CHAT_ID",
            "TELEGRAM_CHAT_ID",
        ),
    )
    return ""


def get_equity_us_main_token() -> str:
    v = _env_chain_first(
        (
            "EQUITY_US_MAIN_BOT_TOKEN",
            "US_MAIN_BOT_TOKEN",
            "MAIN_BOT_TOKEN",
            "TELEGRAM_TOKEN_MAIN",
        )
    )
    if v:
        return v
    _log_config_missing(
        "EQUITY_US scanner MAIN (EQUITY_US_MAIN_BOT_TOKEN / US_MAIN_BOT_TOKEN / MAIN_BOT_TOKEN / TELEGRAM_TOKEN_MAIN)",
        (
            "EQUITY_US_MAIN_BOT_TOKEN",
            "US_MAIN_BOT_TOKEN",
            "MAIN_BOT_TOKEN",
            "TELEGRAM_TOKEN_MAIN",
        ),
    )
    return ""


def get_equity_us_promo_token() -> str:
    v = _env_chain_first(
        (
            "EQUITY_US_PROMO_BOT_TOKEN",
            "US_PROMO_BOT_TOKEN",
            "PROMO_BOT_TOKEN",
            "TELEGRAM_TOKEN_PROMO",
        )
    )
    if v:
        return v
    return get_equity_us_main_token()


def get_equity_us_factory_chat_id() -> str:
    v = _env_chain_first(
        (
            "EQUITY_US_FACTORY_CHAT_ID",
            "US_FACTORY_CHAT_ID",
            "FACTORY_CHAT_ID",
            "TELEGRAM_CHAT_ID",
        )
    )
    if v:
        return v
    _log_config_missing(
        "EQUITY_US scanner CHAT (EQUITY_US_FACTORY_CHAT_ID / US_FACTORY_CHAT_ID / FACTORY_CHAT_ID / TELEGRAM_CHAT_ID)",
        (
            "EQUITY_US_FACTORY_CHAT_ID",
            "US_FACTORY_CHAT_ID",
            "FACTORY_CHAT_ID",
            "TELEGRAM_CHAT_ID",
        ),
    )
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
    """main.py 기동 시: KR·US·공통 중 하나라도 스캐너용 토큰+챗이 있으면 통과."""
    kr_ok = bool(get_equity_kr_main_token() and get_equity_kr_factory_chat_id())
    us_ok = bool(get_equity_us_main_token() and get_equity_us_factory_chat_id())
    gl_ok = bool(get_main_token() and get_factory_chat_id())
    if kr_ok or us_ok or gl_ok:
        return ""
    _log_config_missing(
        "factory telegram (EQUITY_KR_* / EQUITY_US_* 또는 MAIN_BOT_TOKEN + FACTORY_CHAT_ID)",
        (
            "EQUITY_KR_MAIN_BOT_TOKEN",
            "EQUITY_US_MAIN_BOT_TOKEN",
            "MAIN_BOT_TOKEN",
            "TELEGRAM_TOKEN_MAIN",
        ),
    )
    return ""
