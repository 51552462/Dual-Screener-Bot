<<<<<<< HEAD
import os

from dotenv import load_dotenv
import google.generativeai as genai

from gemini_report_cache import generate_bitget_ai_report_cached, load_gemini_api_keys

load_dotenv()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
_gemini_keys = load_gemini_api_keys()
if _gemini_keys:
    genai.configure(api_key=_gemini_keys[0])

GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:"


def generate_ai_report(symbol: str, timeframe: str):
    return generate_bitget_ai_report_cached(symbol, timeframe)
=======
import os

from dotenv import load_dotenv
import google.generativeai as genai

from gemini_report_cache import generate_bitget_ai_report_cached, load_gemini_api_keys

load_dotenv()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
_gemini_keys = load_gemini_api_keys()
if _gemini_keys:
    genai.configure(api_key=_gemini_keys[0])

GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:"


def generate_ai_report(symbol: str, timeframe: str):
    return generate_bitget_ai_report_cached(symbol, timeframe)
>>>>>>> 17d861db9e43265b5daa222305e1810d71f5fa0f
