"""
Bitget 코인 팩토리 전용 환경 변수 (주식/US·KR 팩토리와 .env 키 충돌 방지).
신규 키 우선, 레거시 BITGET_API_* / BITGET_TELEGRAM_TOKEN_MAIN 폴백.
"""
import os


def bitget_access_key() -> str:
    return (os.environ.get("BITGET_ACCESS_KEY") or os.environ.get("BITGET_API_KEY", "") or "").strip()


def bitget_secret_key() -> str:
    return (os.environ.get("BITGET_SECRET_KEY") or os.environ.get("BITGET_API_SECRET", "") or "").strip()


def bitget_passphrase() -> str:
    return (os.environ.get("BITGET_PASSPHRASE") or os.environ.get("BITGET_API_PASSPHRASE", "") or "").strip()


import telegram_env


def bitget_telegram_token() -> str:
    return telegram_env.get_bitget_bot_token()


def bitget_telegram_token_promo() -> str:
    return telegram_env.get_bitget_promo_token()


def bitget_telegram_chat_id() -> str:
    return telegram_env.get_bitget_chat_id()
