import os

from dotenv import load_dotenv

from gemini_report_cache import get_report_provider

load_dotenv()

GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:"


def generate_ai_report(symbol: str, timeframe: str):
    return get_report_provider().generate("bitget", symbol=symbol, timeframe=timeframe)
