import os
import random
import re
import sqlite3
import time
from collections import Counter
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from bitget_rate_limit_guard import throttle

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NEWS_DB_PATH = os.path.join(BASE_DIR, "news_data.sqlite")
FGI_URL = "https://api.alternative.me/fng/?limit=1"
CRYPTO_NEWS_URL = "https://cointelegraph.com/tags/markets"


def _headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9,ko-KR;q=0.8",
    }


def init_news_db():
    conn = sqlite3.connect(NEWS_DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
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
    try:
        r = requests.get(FGI_URL, headers=_headers(), timeout=12)
        j = r.json().get("data", [{}])[0]
        val = float(j.get("value", 50))
        cls = str(j.get("value_classification", "Neutral"))
        return val, cls
    except Exception:
        return 50.0, "Neutral"


def fetch_crypto_news_titles():
    try:
        throttle("http.cointelegraph.news", 0.45)
        r = requests.get(CRYPTO_NEWS_URL, headers=_headers(), timeout=15)
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
    except Exception:
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
    print("🧠 [Bitget Sentiment Miner] 공포/탐욕 + 크립토 뉴스 마이닝...")
    init_news_db()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    titles = fetch_crypto_news_titles()
    keys = extract_keywords(titles)
    fgi, label = fetch_fear_greed()

    # 탐욕 지수 0~100을 기본 sentiment_score로 사용
    sentiment_score = float(fgi)
    conn = sqlite3.connect(NEWS_DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
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
    print(f"✅ 저장 완료: FGI={fgi:.1f}({label}) | keywords={keys}")


if __name__ == "__main__":
    run_sentiment_mining()
