"""
Project 1: Sentiment Natural Language Mining Factory — 완전 격리 실크로.
- market_data.sqlite / system_config.json 미사용
- 전용 DB: ~/dante_bots/Dual-Screener-Bot/news_data.sqlite

외부 맥락(경량): 네이버 금융 헤드라인 + BOK RSS + Fed/US 금융 RSS 제목만 병합 후 Gemini.
- SQLite 스키마 고정: date PK, top_keyword_1..3, sentiment_score (변경 금지)
- PDF/무거운 파서 없음 — RSS는 requests + xml.etree 만 사용
"""
from __future__ import annotations

import csv
import html
import json
import os
import random
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Optional, Set, Tuple
from urllib.parse import urlparse
from xml.etree.ElementTree import Element
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── 격리 DB (factory_data_dir SSOT — market_data 와 동일 데이터 루트) ─────
from factory_data_paths import factory_data_dir
from news_data_paths import news_db_path

NEWS_DB_DIR = factory_data_dir()
NEWS_DB_PATH = news_db_path()

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

try:
    from market_db_paths import market_db_read_path
except Exception:

    def market_db_read_path() -> str:
        return os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "market_data.sqlite")


# `toxic_graveyard_analyzer._sector_bucket_for_tree` 와 동일 버킷 라벨 (증권 도메인 테마 프록시).
_SECTOR_BUCKET_LABELS = (
    "반도체/IT",
    "바이오/헬스케어",
    "에너지/화학",
    "금융/지주",
    "산업재/기계",
    "소비재/엔터",
    "기타/혼합",
)

# 거시경제·금융정책 트리거만 (지수명·실적·정치 키워드 제외 — 종목 화이트리스트가 담당).
_MACRO_MARKET_TERMS: Set[str] = {
    "금리",
    "기준금리",
    "금통위",
    "한은",
    "한국은행",
    "연준",
    "ECB",
    "유럽중앙은행",
    "CPI",
    "PPI",
    "PCE",
    "GDP",
    "FOMC",
    "양적완화",
    "양적긴축",
    "QT",
    "QE",
    "채권",
    "국채",
    "회사채",
    "국고채",
    "금리인상",
    "금리인하",
    "인플레이션",
    "스태그플레이션",
    "디플레이션",
    "물가",
    "환율",
    "외환",
    "스왑",
    "유동성",
    "긴축",
    "완화",
    "기준금리동결",
}
_MACRO_MARKET_EN: Set[str] = {
    "rate",
    "rates",
    "fed",
    "fomc",
    "ecb",
    "cpi",
    "ppi",
    "pce",
    "gdp",
    "inflation",
    "deflation",
    "stagflation",
    "treasury",
    "bond",
    "bonds",
    "yield",
    "yields",
    "fx",
    "forex",
    "liquidity",
    "qe",
    "qt",
    "taper",
    "hike",
    "hikes",
}


def _compact_kr(s: str) -> str:
    return re.sub(r"\s+", "", (s or "").strip())


def _add_whitelist_tokens(raw: str, names: Set[str], symbols: Set[str]) -> None:
    t = (raw or "").strip()
    if not t:
        return
    if re.fullmatch(r"[A-Za-z.\-]+", t):
        symbols.add(t.upper().replace(".", "-"))
        for part in re.split(r"[\s\-]+", t):
            if re.fullmatch(r"[A-Za-z]{1,5}", part):
                symbols.add(part.upper())
        return
    names.add(t)
    names.add(_compact_kr(t))
    for part in re.split(r"[/|·,\s]+", t):
        p = part.strip()
        if len(p) >= 2:
            names.add(p)
            names.add(_compact_kr(p))


def _load_equity_whitelist() -> Tuple[Set[str], Set[str]]:
    """
    KRX(또는 캐시)·선택 FDR·forward_trades 등록종목명·섹터 버킷 → 명사 화이트리스트.
    """
    names: Set[str] = set()
    symbols: Set[str] = set()

    for b in _SECTOR_BUCKET_LABELS:
        _add_whitelist_tokens(b, names, symbols)

    cache_csv = os.path.join(os.path.dirname(market_db_read_path()), "krx_list_cache.csv")
    if os.path.isfile(cache_csv):
        try:
            with open(cache_csv, "r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    n = (row.get("Name") or "").strip()
                    c = (row.get("Code") or "").strip()
                    if n:
                        _add_whitelist_tokens(n, names, symbols)
                    if c.isdigit():
                        symbols.add(c.zfill(6))
        except Exception:
            pass

    dbp = market_db_read_path()
    if os.path.isfile(dbp):
        uri = "file:" + os.path.abspath(dbp).replace("\\", "/") + "?mode=ro"
        try:
            conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=30)
            try:
                cur = conn.execute(
                    "SELECT DISTINCT COALESCE(NULLIF(trim(name),''), ''), "
                    "COALESCE(NULLIF(trim(code),''), '') FROM forward_trades WHERE market='KR'"
                )
                for nm, cd in cur.fetchall():
                    if nm:
                        _add_whitelist_tokens(str(nm), names, symbols)
                    if cd and str(cd).strip().isdigit():
                        symbols.add(str(cd).strip().zfill(6))
            finally:
                conn.close()
        except Exception:
            pass

    try:
        import FinanceDataReader as fdr  # type: ignore

        for tag in ("KOSPI", "KOSDAQ"):
            try:
                lst = fdr.StockListing(tag)
            except Exception:
                continue
            if lst is None or getattr(lst, "empty", True):
                continue
            code_col = "Code" if "Code" in lst.columns else ("Symbol" if "Symbol" in lst.columns else None)
            name_col = "Name" if "Name" in lst.columns else None
            if code_col:
                for v in lst[code_col].astype(str):
                    v = v.strip()
                    if v.isdigit():
                        symbols.add(v.zfill(6))
            if name_col:
                for v in lst[name_col].astype(str):
                    _add_whitelist_tokens(str(v).strip(), names, symbols)
            for col in ("Industry", "업종", "Sector", "sector", "분류"):
                if col in lst.columns:
                    for v in lst[col].astype(str):
                        _add_whitelist_tokens(str(v).strip(), names, symbols)

        for tag in ("NASDAQ", "NYSE", "AMEX"):
            try:
                lst = fdr.StockListing(tag)
            except Exception:
                continue
            if lst is None or getattr(lst, "empty", True) or "Symbol" not in lst.columns:
                continue
            name_col = "Name" if "Name" in lst.columns else None
            for sym in lst["Symbol"].astype(str):
                s = sym.strip().upper().replace(".", "-")
                if re.fullmatch(r"[A-Z.\-]+", s) and 1 <= len(s) <= 6:
                    symbols.add(s)
            if name_col:
                for v in lst[name_col].astype(str):
                    _add_whitelist_tokens(str(v).strip(), names, symbols)
    except Exception:
        pass

    return names, symbols


def _keyword_matches_equity_whitelist(keyword: str, names: Set[str], symbols: Set[str]) -> bool:
    k = (keyword or "").strip()
    if len(k) < 2:
        return False

    if re.fullmatch(r"[A-Za-z][A-Za-z.\-0-9]*", k):
        ku = k.upper().replace(".", "-")
        if ku in symbols:
            return True

    c = _compact_kr(k)
    if c in names:
        return True

    for n in names:
        if len(n) < 3:
            continue
        if n in c or (len(c) >= 4 and c in n):
            return True
    return False


def _keyword_matches_macro_finance(keyword: str) -> bool:
    k = (keyword or "").strip()
    if len(k) < 2:
        return False
    c = _compact_kr(k)
    if c in _MACRO_MARKET_TERMS or k in _MACRO_MARKET_TERMS:
        return True
    for t in _MACRO_MARKET_TERMS:
        if len(t) >= 2 and t in c:
            return True
    tokens = re.findall(r"[A-Za-z]{2,}|[가-힣]{2,}", k)
    for t in tokens:
        if t in _MACRO_MARKET_TERMS:
            return True
        low = t.lower()
        if low in _MACRO_MARKET_EN:
            return True
    return False


def filter_gemini_keywords_through_whitelist(result: dict[str, Any]) -> dict[str, Any]:
    """Gemini 산출 키워드를 등록 종목·섹터·거시금융 교집합으로 정제 (정치·일반 사회 이슈 드롭)."""
    if not isinstance(result, dict):
        return result
    names, symbols = _load_equity_whitelist()
    out = dict(result)
    kept: list[Optional[str]] = []
    for key in ("top_keyword_1", "top_keyword_2", "top_keyword_3"):
        raw = out.get(key)
        s = str(raw).strip() if raw is not None else ""
        if not s:
            kept.append(None)
            continue
        if _keyword_matches_equity_whitelist(s, names, symbols) or _keyword_matches_macro_finance(s):
            kept.append(s)
        else:
            kept.append(None)
    out["top_keyword_1"], out["top_keyword_2"], out["top_keyword_3"] = kept[0], kept[1], kept[2]
    return out


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
        import google.generativeai as genai

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.5-flash")
    except Exception as e:
        print(f"🚨 [Sentiment Miner] Gemini 초기화 실패: {e}")
        return None

    trimmed = headlines[:MAX_HEADLINES_FOR_GEMINI]
    body = "\n".join(f"- {h}" for h in trimmed)
    prompt = f"""You are a financial NLP engine for securities markets only. Below is a multilingual headline bundle: Korean equity news (tag [Naver]), Bank of Korea official RSS ([BOK]), US Federal Reserve press RSS ([Fed]), and major US financial wire headlines ([US]). Each line is one headline.

{body}

Rules for the 3 keywords:
- Each MUST name something tradeable or a market-wide macro term only: a specific listed stock issuer / ADR name, a KOSPI/KOSDAQ industry sector, a major US ticker theme, OR a standard macro label (rates, inflation, FX, indices). 
- NEVER output political figures, elections, welfare/social policy debates, or non-market current events. If headlines are dominated by such noise, still pick the closest *market* terms or leave a keyword blank by using an empty string.

Return exactly 3 keyword slots (short noun phrases; Korean for Korea-dominant, English for US/Fed-dominant) and ONE overall blended market sentiment score from 0 (Extreme Fear) to 100 (Extreme Greed) for a Korea–US overlapping equity context.

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
        filtered = filter_gemini_keywords_through_whitelist(
            {
                "top_keyword_1": k1,
                "top_keyword_2": k2,
                "top_keyword_3": k3,
                "sentiment_score": score_f,
            }
        )
        fk1 = filtered.get("top_keyword_1")
        fk2 = filtered.get("top_keyword_2")
        fk3 = filtered.get("top_keyword_3")
        dropped = [x for x, (a, b) in enumerate([(k1, fk1), (k2, fk2), (k3, fk3)]) if a and not b]
        if dropped:
            print("⚠️ [Sentiment Miner] 화이트리스트 탈락(keyword slot) — 원문 일부 제거됨.")
        return {
            "top_keyword_1": fk1,
            "top_keyword_2": fk2,
            "top_keyword_3": fk3,
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
