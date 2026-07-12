import os
import random
import re
import sqlite3
import time
from collections import Counter

from bs4 import BeautifulSoup

from bitget.infra.clock import utc_date_key
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.network_retry import http_get

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
]

from bitget.infra.shared_db_connector import get_connection

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NEWS_DB_PATH = os.path.join(BASE_DIR, "news_data.sqlite")
FGI_URL = "https://api.alternative.me/fng/?limit=1"
CRYPTO_NEWS_URL = "https://cointelegraph.com/tags/markets"
logger = get_logger("bitget.sentiment_miner")


def _headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9,ko-KR;q=0.8",
    }


def init_news_db():
    conn = get_connection(NEWS_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_sentiment (
            date TEXT PRIMARY KEY,
            top_keyword_1 TEXT,
            top_keyword_2 TEXT,
            top_keyword_3 TEXT,
            top_keyword_4 TEXT,
            top_keyword_5 TEXT,
            sentiment_score REAL,
            fear_greed_value REAL,
            fear_greed_label TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def fetch_fear_greed():
    r = http_get(
        FGI_URL,
        op="sentiment.fear_greed",
        throttle_key="http.alternative.fng",
        throttle_interval_sec=0.35,
        timeout=12.0,
        headers=_headers(),
        default=None,
        swallow=True,
    )
    if r is None:
        return 50.0, "Neutral"
    try:
        j = r.json().get("data", [{}])[0]
        val = float(j.get("value", 50))
        cls = str(j.get("value_classification", "Neutral"))
        return val, cls
    except Exception as e:
        log_exception(logger, "fear/greed parse failed: %s", e)
        return 50.0, "Neutral"


def fetch_crypto_news_titles():
    r = http_get(
        CRYPTO_NEWS_URL,
        op="sentiment.crypto_news",
        throttle_key="http.cointelegraph.news",
        throttle_interval_sec=0.45,
        timeout=15.0,
        headers=_headers(),
        default=None,
        swallow=True,
    )
    if r is None:
        return []
    try:
        time.sleep(random.uniform(0.8, 1.7))
        soup = BeautifulSoup(r.text, "html.parser")
        titles = []
        for tag in soup.select("a span"):
            t = (tag.get_text() or "").strip()
            if len(t) >= 15:
                titles.append(t)
            if len(titles) >= 80:
                break
        return titles
    except Exception as e:
        log_exception(logger, "crypto news parse failed: %s", e)
        return []


def extract_keywords(titles):
    stop_words = {"bitcoin", "crypto", "market", "price", "token", "coin", "today"}
    words = []
    for t in titles:
        c = re.sub(r"[^a-zA-Z0-9가-힣\s]", " ", t.lower())
        for w in c.split():
            if len(w) > 2 and w not in stop_words:
                words.append(w)
    top = [k for k, _ in Counter(words).most_common(5)]
    while len(top) < 5:
        top.append("none")
    return top


def run_sentiment_mining():
    logger.info("[sentiment] fear/greed + crypto news mining")
    init_news_db()
    today = utc_date_key()
    titles = fetch_crypto_news_titles()
    keys = extract_keywords(titles)
    fgi, label = fetch_fear_greed()

    # 탐욕 지수 0~100을 기본 sentiment_score로 사용
    sentiment_score = float(fgi)
    conn = get_connection(NEWS_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO daily_sentiment
        (date, top_keyword_1, top_keyword_2, top_keyword_3, top_keyword_4, top_keyword_5,
         sentiment_score, fear_greed_value, fear_greed_label)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (today, keys[0], keys[1], keys[2], keys[3], keys[4], sentiment_score, fgi, label),
    )
    conn.commit()
    conn.close()
    logger.info("sentiment saved: FGI=%.1f(%s) keywords=%s", fgi, label, keys)


if __name__ == "__main__":
    run_sentiment_mining()
