import os
import time

from dotenv import load_dotenv
from google import genai
from google.genai import types


load_dotenv()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


def generate_ai_report(symbol: str, timeframe: str):
    fallback = (
        "1. 섹터/테마: 디지털자산 메이저 순환 섹터\n"
        "2. 내러티브: 온체인/파생 수급이 동시에 유입되는 구간\n"
        "3. 모멘텀: 단기 추세 강화 및 변동성 확장 가능성"
    )
    if client is None:
        return fallback

    prompt = f"""
너는 암호화폐 리서치 마케터다.
[{symbol}] 코인의 최신 정보를 검색해 아래 형식만 출력하라.

[본캐]
1. 섹터/테마: (코인의 산업군/서사 1줄)
2. 내러티브: (현재 시장이 반응하는 재료 1줄)
3. 모멘텀: ({timeframe} 기준 수급/변동성 관점 1줄)
"""
    for _ in range(3):
        try:
            time.sleep(3)
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}]),
            )
            if response and response.text:
                txt = response.text.replace("*", "").strip()
                if "[본캐]" in txt:
                    return txt.split("[본캐]", 1)[1].strip()
        except Exception:
            pass
    return fallback
