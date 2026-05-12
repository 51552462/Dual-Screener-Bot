"""
Project 1: Sentiment Natural Language Mining Factory — 완전 격리 실크로.
- market_data.sqlite / system_config.json 미사용
- 전용 DB: ~/dante_bots/Dual-Screener-Bot/news_data.sqlite

외부 맥락(경량): 네이버 금융 헤드라인 + BOK RSS + Fed/US 금융 RSS 제목만 병합 후 Gemini.
- SQLite 스키마 고정: date PK, top_keyword_1..3, sentiment_score (변경 금지)
- PDF/무거운 파서 없음 — RSS는 requests + xml.etree 만 사용
"""
from __future__ import annotations

import html
import json
import os
import random
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urlparse
from xml.etree.ElementTree import Element
from zoneinfo import ZoneInfo

import google.generativeai as genai
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── 격리 DB ─────────────────────────────────────────────────────────────
NEWS_DB_DIR = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot")
NEWS_DB_PATH = os.path.join(NEWS_DB_DIR, "news_data.sqlite")

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "close",
    "Referer": "https://finance.naver.com/",
}

NAVER_NEWS_URLS = [
    "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258",
    "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=259",
    "https://finance.naver.com/research/company_list.naver?page=1",
]

# RSS 후보 URL(일부 404·차단 시 다음 후보 시도) — PDF·무거운 파서 없음
BOK_RSS_URLS = [
    "https://www.bok.or.kr/rss/bokNews.xml",
    "https://www.bok.or.kr/portal/bbs/B0000282/rssList.do?menuNo=200690",
]
FED_RSS_URLS = [
    "https://www.federalreserve.gov/feeds/press_all.xml",
    "https://www.federalreserve.gov/feeds/press_monetary.xml",
]
US_FINANCE_RSS_URLS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
]

# Naver + BOK + Fed + US 한 블록; 과도한 토큰 방지
MAX_HEADLINES_FOR_GEMINI = 64


def _sleep_jitter(a: float = 0.5, b: float = 1.4) -> None:
    time.sleep(random.uniform(a, b))


def _headers_for_rss(url: str) -> dict[str, str]:
    h = dict(DEFAULT_HEADERS)
    h["Accept"] = (
        "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.7"
    )
    try:
        host = urlparse(url).netloc
        if host:
            h["Referer"] = f"https://{host}/"
    except Exception:
        pass
    return h


def _local_tag(tag: str) -> str:
    if not tag:
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _clean_title_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    t = html.unescape(text)
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t if len(t) > 4 else None


def _title_from_xml_element(elem: Element) -> Optional[str]:
    """RSS/Atom에서 <title> 자식 텍스트·혼합 콘텐츠까지 평탄화."""
    try:
        raw = "".join(elem.itertext()).strip()
    except Exception:
        raw = (elem.text or "").strip()
    return _clean_title_text(raw) if raw else None


def fetch_rss_headlines_only(
    url: str,
    max_items: int = 12,
    timeout: int = 22,
) -> list[str]:
    """
    RSS 2.0 / Atom 제목만 추출. feedparser 없이 xml.etree + requests.
    실패·비정형 XML·빈 응답 시 빈 리스트 반환(호출부는 계속 진행).
    """
    titles: list[str] = []
    seen: set[str] = set()
    try:
        _sleep_jitter(0.45, 1.3)
        r = requests.get(url, headers=_headers_for_rss(url), timeout=timeout)
        r.raise_for_status()
        enc = r.encoding or None
        if not enc or enc.lower() == "iso-8859-1":
            enc = getattr(r, "apparent_encoding", None) or "utf-8"
        text = r.content.decode(enc, errors="replace")
        root = ET.fromstring(text)
    except Exception:
        return []

    # RSS 2.0: channel/item/title
    try:
        for item in root.iter():
            if _local_tag(item.tag).lower() != "item":
                continue
            for ch in item:
                if _local_tag(ch.tag).lower() == "title":
                    ct = _title_from_xml_element(ch)
                    if ct and ct not in seen:
                        seen.add(ct)
                        titles.append(ct)
                    break
            if len(titles) >= max_items:
                return titles[:max_items]
    except Exception:
        pass

    # Atom: entry/title
    if len(titles) < max_items:
        try:
            for entry in root.iter():
                if _local_tag(entry.tag).lower() != "entry":
                    continue
                for ch in entry:
                    if _local_tag(ch.tag).lower() != "title":
                        continue
                    ct = _title_from_xml_element(ch)
                    if ct and ct not in seen:
                        seen.add(ct)
                        titles.append(ct)
                    break
                if len(titles) >= max_items:
                    break
        except Exception:
            pass

    return titles[:max_items]


def _dedupe_headlines_preserve_order(items: list[str], cap: int) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
        if len(out) >= cap:
            break
    return out


def gather_rss_headlines_bok() -> list[str]:
    try:
        out: list[str] = []
        for url in BOK_RSS_URLS:
            if len(out) >= 14:
                break
            try:
                out.extend(fetch_rss_headlines_only(url, max_items=14 - len(out)))
            except Exception:
                continue
        return _dedupe_headlines_preserve_order(out, 14)
    except Exception:
        return []


def gather_rss_headlines_fed() -> list[str]:
    try:
        out: list[str] = []
        for url in FED_RSS_URLS:
            if len(out) >= 14:
                break
            try:
                out.extend(fetch_rss_headlines_only(url, max_items=14 - len(out)))
            except Exception:
                continue
        return _dedupe_headlines_preserve_order(out, 14)
    except Exception:
        return []


def gather_rss_headlines_us_finance() -> list[str]:
    try:
        out: list[str] = []
        for url in US_FINANCE_RSS_URLS:
            if len(out) >= 16:
                break
            try:
                out.extend(fetch_rss_headlines_only(url, max_items=16 - len(out)))
            except Exception:
                continue
        return _dedupe_headlines_preserve_order(out, 16)
    except Exception:
        return []


def ensure_news_db() -> None:
    os.makedirs(NEWS_DB_DIR, exist_ok=True)
    conn = sqlite3.connect(NEWS_DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_sentiment (
                date TEXT PRIMARY KEY,
                top_keyword_1 TEXT,
                top_keyword_2 TEXT,
                top_keyword_3 TEXT,
                sentiment_score REAL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def scrape_naver_finance_headlines(
    max_headlines: int = 28,
    timeout: int = 28,
) -> list[str]:
    """
    네이버 금융 주요 뉴스·리서치 목록에서 헤드라인 수집 (User-Agent·지터 적용).
    """
    seen: set[str] = set()
    titles: list[str] = []

    for url in NAVER_NEWS_URLS:
        if len(titles) >= max_headlines:
            break
        try:
            _sleep_jitter(0.6, 1.8)
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")

            candidates: list[str] = []

            for sel in (
                "div.articleSubject a",
                "td.title a",
                "a.newsTit",
                "dd.articleSubject a",
                ".mainNewsList a",
            ):
                for a in soup.select(sel):
                    t = a.get_text(strip=True)
                    if t and len(t) > 6 and t not in seen:
                        candidates.append(t)

            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "news_read" in href or "news_read.naver" in href or "/research/" in href:
                    t = a.get_text(strip=True)
                    if t and len(t) > 6 and t not in seen:
                        candidates.append(t)

            for t in candidates:
                if len(titles) >= max_headlines:
                    break
                if t in seen:
                    continue
                seen.add(t)
                titles.append(t)
        except Exception:
            continue

    return titles[:max_headlines]


def _parse_gemini_json(text: str) -> Optional[dict[str, Any]]:
    """마크다운 펜스·잡문 제거 후 JSON 오브젝트 파싱."""
    if not text:
        return None
    s = text.strip()
    try:
        if "```" in s:
            parts = re.split(r"```(?:json)?", s, flags=re.I)
            for p in parts:
                p = p.strip()
                if p.startswith("{") and "}" in p:
                    return json.loads(p)
        m = re.search(r"\{[^{}]*\}", s, re.DOTALL)
        if m:
            return json.loads(m.group(0))
        return json.loads(s)
    except Exception:
        return None


def analyze_headlines_with_gemini(headlines: list[str]) -> Optional[dict[str, Any]]:
    """Gemini로 키워드 3개 + 센티먼트 스코어(0~100) 추출."""
    load_dotenv()
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("🚨 [Sentiment Miner] GEMINI_API_KEY 가 .env 에 없습니다.")
        return None
    if not headlines:
        print("⚠️ [Sentiment Miner] 분석할 헤드라인이 없습니다.")
        return None

    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.5-flash")
    except Exception as e:
        print(f"🚨 [Sentiment Miner] Gemini 초기화 실패: {e}")
        return None

    trimmed = headlines[:MAX_HEADLINES_FOR_GEMINI]
    body = "\n".join(f"- {h}" for h in trimmed)
    prompt = f"""You are a financial NLP engine. Below is a multilingual headline bundle: Korean equity news (tag [Naver]), Bank of Korea official RSS ([BOK]), US Federal Reserve press RSS ([Fed]), and major US financial wire headlines ([US]). Each line is one headline.

{body}

Synthesize across ALL sources. Return exactly 3 trending financial keywords (short noun phrases; use Korean when reflecting Korea-dominant themes, English when US/Fed-dominant — mixed is OK) and ONE overall blended market sentiment score from 0 (Extreme Fear) to 100 (Extreme Greed) for a Korea-US overlapping equity context.

Respond with ONLY valid JSON. No markdown fences, no commentary. Use exactly this shape:
{{"top_keyword_1":"...","top_keyword_2":"...","top_keyword_3":"...","sentiment_score":55}}

sentiment_score must be an integer from 0 to 100 inclusive."""

    try:
        _sleep_jitter(0.3, 0.9)
        resp = model.generate_content(prompt)
        raw = (getattr(resp, "text", None) or "").strip()
        if not raw and getattr(resp, "candidates", None):
            try:
                raw = resp.candidates[0].content.parts[0].text.strip()
            except Exception:
                raw = ""
        data = _parse_gemini_json(raw)
        if not data:
            print("⚠️ [Sentiment Miner] Gemini 응답 JSON 파싱 실패.")
            return None

        k1 = str(data.get("top_keyword_1", "")).strip() or None
        k2 = str(data.get("top_keyword_2", "")).strip() or None
        k3 = str(data.get("top_keyword_3", "")).strip() or None
        sc = data.get("sentiment_score")
        try:
            score_f = float(sc)
            score_f = max(0.0, min(100.0, score_f))
        except (TypeError, ValueError):
            score_f = None

        if not k1 and not k2 and not k3 and score_f is None:
            return None
        return {
            "top_keyword_1": k1,
            "top_keyword_2": k2,
            "top_keyword_3": k3,
            "sentiment_score": score_f,
        }
    except Exception as e:
        print(f"⚠️ [Sentiment Miner] Gemini 호출 실패(레이트리밋·네트워크 등): {e}")
        return None


def upsert_daily_sentiment(
    row: dict[str, Any],
) -> None:
    conn = sqlite3.connect(NEWS_DB_PATH, timeout=30)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_sentiment
            (date, top_keyword_1, top_keyword_2, top_keyword_3, sentiment_score)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["date"],
                row.get("top_keyword_1"),
                row.get("top_keyword_2"),
                row.get("top_keyword_3"),
                row.get("sentiment_score"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def build_unified_headline_context() -> list[str]:
    """Naver + BOK RSS + Fed RSS + US 금융 RSS 헤드라인을 단일 리스트로(출처 태그)."""
    lines: list[str] = []

    try:
        for t in scrape_naver_finance_headlines():
            lines.append(f"[Naver] {t}")
    except Exception:
        pass

    try:
        bok = gather_rss_headlines_bok()
        for t in bok:
            lines.append(f"[BOK] {t}")
    except Exception:
        pass

    try:
        fed = gather_rss_headlines_fed()
        for t in fed:
            lines.append(f"[Fed] {t}")
    except Exception:
        pass

    try:
        us = gather_rss_headlines_us_finance()
        for t in us:
            lines.append(f"[US] {t}")
    except Exception:
        pass

    return lines[:MAX_HEADLINES_FOR_GEMINI]


def run_sentiment_mining() -> dict[str, Any]:
    """
    네이버 금융 + RSS(BOK·Fed·US) → 통합 텍스트 → Gemini 분석 → news_data.sqlite 저장.
    부분 실패 시에도 프로세스는 종료 코드 0에 가깝게 유지.
    """
    today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    out: dict[str, Any] = {
        "date": today,
        "top_keyword_1": None,
        "top_keyword_2": None,
        "top_keyword_3": None,
        "sentiment_score": None,
    }

    try:
        ensure_news_db()
    except Exception as e:
        print(f"🚨 [Sentiment Miner] DB 초기화 실패: {e}")
        return out

    headlines: list[str] = []
    try:
        headlines = build_unified_headline_context()
        n_nv = sum(1 for h in headlines if h.startswith("[Naver]"))
        n_bok = sum(1 for h in headlines if h.startswith("[BOK]"))
        n_fed = sum(1 for h in headlines if h.startswith("[Fed]"))
        n_us = sum(1 for h in headlines if h.startswith("[US]"))
        print(
            f"📰 [Sentiment Miner] 통합 헤드라인 {len(headlines)}건 "
            f"(Naver {n_nv} · BOK {n_bok} · Fed {n_fed} · US {n_us})"
        )
    except Exception as e:
        print(f"⚠️ [Sentiment Miner] 헤드라인 수집 예외: {e}")

    gem: Optional[dict[str, Any]] = None
    try:
        gem = analyze_headlines_with_gemini(headlines)
    except Exception as e:
        print(f"⚠️ [Sentiment Miner] Gemini 분석 예외: {e}")

    if gem:
        out.update(gem)

    try:
        if any(
            [
                out.get("top_keyword_1"),
                out.get("top_keyword_2"),
                out.get("top_keyword_3"),
                out.get("sentiment_score") is not None,
            ]
        ):
            upsert_daily_sentiment(out)
            print(
                f"✅ [Sentiment Miner] 저장 완료 date={today} | "
                f"k1={out.get('top_keyword_1')} | k2={out.get('top_keyword_2')} | "
                f"k3={out.get('top_keyword_3')} | score={out.get('sentiment_score')}"
            )
        else:
            print("⚠️ [Sentiment Miner] 유효 데이터 없음 — DB 쓰기 생략(메인 팩토리 무영향).")
    except Exception as e:
        print(f"🚨 [Sentiment Miner] SQLite 저장 실패: {e}")

    return out


if __name__ == "__main__":
    try:
        run_sentiment_mining()
    except Exception as e:
        print(f"🚨 [Sentiment Miner] 치명적 예외(조용히 로그만): {e}")
