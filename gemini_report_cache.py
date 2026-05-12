"""
Gemini 리포트: 로컬 SQLite 캐시 + 다중 API 키 로테이션 (무료 티어 429/쿼터 방어).
- `ai_cache.sqlite` (이 모듈과 같은 디렉터리) `report_cache(date, code, report_text)`
- `GEMINI_API_KEY="k1,k2,k3"` 콤마 구분, 3회 시도마다 키 순환 + `genai.configure`
- WAL + timeout=30 (멀티 스레드 스크리너)
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime
from typing import List, Optional, Tuple

from dotenv import load_dotenv

_AI_CACHE_LOCK = threading.Lock()

_AI_CACHE_DIR = os.path.dirname(os.path.abspath(__file__))
AI_CACHE_DB = os.path.join(_AI_CACHE_DIR, "ai_cache.sqlite")


def _ensure_cache_dir() -> None:
    d = os.path.dirname(AI_CACHE_DB)
    if d:
        os.makedirs(d, exist_ok=True)


def _cache_connection() -> sqlite3.Connection:
    _ensure_cache_dir()
    conn = sqlite3.connect(AI_CACHE_DB, timeout=30.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS report_cache (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            report_text TEXT NOT NULL,
            PRIMARY KEY (date, code)
        )
        """
    )
    return conn


def init_ai_report_cache_db() -> None:
    """선행 초기화용 (스크리너 기동 시 선택 호출)."""
    conn = _cache_connection()
    conn.close()


def load_gemini_api_keys() -> List[str]:
    load_dotenv()
    raw = os.environ.get("GEMINI_API_KEY") or ""
    return [x.strip() for x in raw.split(",") if x.strip()]


def configure_gemini_key_for_attempt(attempt: int) -> bool:
    """attempt 번째 시도에 사용할 키로 `genai.configure`. 키 없으면 False."""
    import google.generativeai as genai

    keys = load_gemini_api_keys()
    if not keys:
        return False
    genai.configure(api_key=keys[attempt % len(keys)])
    return True


def is_retryable_gemini_error(exc: BaseException) -> bool:
    """429·쿼터·일시 과부하 등 → 다음 키로 재시도."""
    msg = str(exc).lower()
    needles = (
        "429",
        "resource exhausted",
        "resourceexhausted",
        "quota",
        "rate limit",
        "too many requests",
        "503",
        "504",
        "deadline",
        "unavailable",
        "exhausted",
    )
    if any(n in msg for n in needles):
        return True
    name = type(exc).__name__.lower()
    if "resourceexhausted" in name or "aborted" in name or "deadline" in name:
        return True
    return False


def cache_get_payload(date_str: str, cache_key: str) -> Optional[str]:
    with _AI_CACHE_LOCK:
        conn = _cache_connection()
        try:
            row = conn.execute(
                "SELECT report_text FROM report_cache WHERE date = ? AND code = ?",
                (date_str, cache_key),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()


def cache_put_payload(date_str: str, cache_key: str, payload: str) -> None:
    with _AI_CACHE_LOCK:
        conn = _cache_connection()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO report_cache (date, code, report_text)
                VALUES (?, ?, ?)
                """,
                (date_str, cache_key, payload),
            )
            conn.commit()
        finally:
            conn.close()


def _decode_tuple_payload(raw: str) -> Tuple[str, str]:
    try:
        data = json.loads(raw)
        if isinstance(data, list) and len(data) >= 2:
            return str(data[0]), str(data[1])
        if isinstance(data, list) and len(data) == 1:
            return str(data[0]), ""
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return raw, ""


def _sector_block_for_prompt(code: str) -> str:
    try:
        if str(code).isdigit():
            import requests
            from bs4 import BeautifulSoup

            res = requests.get(
                f"https://finance.naver.com/item/main.naver?code={code}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5,
                verify=False,
            )
            soup = BeautifulSoup(res.text, "html.parser")
            el = soup.select_one("h4.h_sub.sub_tit7 a")
            return el.text.strip() if el else "국내 증시"
        import yfinance as yf

        tk = yf.Ticker(code)
        sector = tk.info.get("sector", "글로벌 산업")
        sector_kr_map = {
            "Technology": "테크/기술",
            "Healthcare": "헬스케어",
            "Financial Services": "금융",
            "Consumer Cyclical": "소비재",
            "Industrials": "산업재",
            "Energy": "에너지",
            "Basic Materials": "원자재",
        }
        return sector_kr_map.get(sector, sector)
    except Exception:
        return "유망 섹터"


def generate_stock_ai_report_cached(code: str, company_name: str) -> Tuple[str, str]:
    """
    주식 스크리너 공통: (본캐 본문, 해시태그 문자열) — 기존 반환 형식 유지.
    당일 동일 code 캐시 히트 시 API 미호출.
    """
    import google.generativeai as genai

    today = datetime.now().strftime("%Y-%m-%d")
    cache_key = str(code).strip()

    cached = cache_get_payload(today, cache_key)
    if cached is not None:
        return _decode_tuple_payload(cached)

    sector_kr = _sector_block_for_prompt(code)
    fb_main = f"1. 섹터: {sector_kr}\n2. 실적: 데이터 분석 중\n3. 모멘텀: 수급 유입 및 차트 반등 포착"

    prompt = f"""
            너는 주식 전문 마케터야. [{company_name} ({code})] 종목과 관련된 오늘자 최신 이슈나 테마를 검색해서 아래 양식에 맞게 딱 출력해.
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분해. 굵은 글씨(**) 금지.

            [본캐]
            1. 섹터: (어떤 테마인지 한글로 1줄 요약)
            2. 실적: (팩트 수치 한글 1줄 요약)
            3. 모멘텀: (앞으로의 호재 한글 1줄 요약)
            """

    for attempt in range(3):
        if not configure_gemini_key_for_attempt(attempt):
            return fb_main, ""

        try:
            time.sleep(4)
            gmodel = genai.GenerativeModel("gemini-2.5-flash", tools="google_search_retrieval")
            response = gmodel.generate_content(prompt)
        except Exception as e:
            if attempt < 2 and is_retryable_gemini_error(e):
                continue
            return (
                f"⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:\n\n{prompt}",
                "",
            )

        if not response or not response.text:
            continue

        report = response.text.replace("*", "").strip()
        m_part = re.search(r"\[본캐\](.*)", report, re.DOTALL)
        if not m_part:
            continue

        main = m_part.group(1).strip()
        out: Tuple[str, str] = (main, "")
        cache_put_payload(today, cache_key, json.dumps(list(out), ensure_ascii=False))
        return out

    return fb_main, ""


def generate_bitget_ai_report_cached(symbol: str, timeframe: str) -> str:
    """비트겟용 단일 문자열 리포트 (기존 반환 형식 유지)."""
    import google.generativeai as genai

    today = datetime.now().strftime("%Y-%m-%d")
    cache_key = f"{str(symbol).strip()}|{str(timeframe).strip()}"

    cached = cache_get_payload(today, cache_key)
    if cached is not None:
        main, _ = _decode_tuple_payload(cached)
        return main

    raw_survival = f"⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:\n\n"
    prompt = f"""
너는 암호화폐 리서치 마케터다.
[{symbol}] 코인의 최신 정보를 검색해 아래 형식만 출력하라.

[본캐]
1. 섹터/테마: (코인의 산업군/서사 1줄)
2. 내러티브: (현재 시장이 반응하는 재료 1줄)
3. 모멘텀: ({timeframe} 기준 수급/변동성 관점 1줄)
"""
    raw_survival_full = raw_survival + prompt

    if not load_gemini_api_keys():
        return raw_survival_full

    for attempt in range(3):
        if not configure_gemini_key_for_attempt(attempt):
            return raw_survival_full
        try:
            time.sleep(3)
            gmodel = genai.GenerativeModel("gemini-2.5-flash", tools="google_search_retrieval")
            response = gmodel.generate_content(prompt)
        except Exception as e:
            if attempt < 2 and is_retryable_gemini_error(e):
                continue
            return raw_survival_full

        if response and response.text:
            txt = response.text.replace("*", "").strip()
            if "[본캐]" in txt:
                body = txt.split("[본캐]", 1)[1].strip()
                cache_put_payload(today, cache_key, json.dumps([body, ""], ensure_ascii=False))
                return body

    return raw_survival_full
