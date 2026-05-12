import os
import time

from dotenv import load_dotenv
import google.generativeai as genai


load_dotenv()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:"


def generate_ai_report(symbol: str, timeframe: str):
    fallback = (
        "1. 섹터/테마: 디지털자산 메이저 순환 섹터\n"
        "2. 내러티브: 온체인/파생 수급이 동시에 유입되는 구간\n"
        "3. 모멘텀: 단기 추세 강화 및 변동성 확장 가능성"
    )
    prompt = f"""
너는 암호화폐 리서치 마케터다.
[{symbol}] 코인의 최신 정보를 검색해 아래 형식만 출력하라.

[본캐]
1. 섹터/테마: (코인의 산업군/서사 1줄)
2. 내러티브: (현재 시장이 반응하는 재료 1줄)
3. 모멘텀: ({timeframe} 기준 수급/변동성 관점 1줄)
"""
    raw_survival = f"{GEMINI_RAW_FALLBACK_PREFIX}\n\n{prompt}"
    if not GEMINI_API_KEY:
        return raw_survival

    for _ in range(3):
        try:
            time.sleep(3)
            gmodel = genai.GenerativeModel("gemini-2.5-flash", tools="google_search_retrieval")
            try:
                response = gmodel.generate_content(prompt)
            except Exception:
                return raw_survival
            if response and response.text:
                txt = response.text.replace("*", "").strip()
                if "[본캐]" in txt:
                    return txt.split("[본캐]", 1)[1].strip()
        except Exception:
            return raw_survival
    return raw_survival
