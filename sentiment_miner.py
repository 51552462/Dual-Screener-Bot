import sqlite3
import time
import random
import requests
from bs4 import BeautifulSoup
from collections import Counter
import re
import os
from datetime import datetime

# 🛡️ 스텔스 위장 명찰 (다양한 OS와 브라우저)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]


def get_stealth_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive",
    }


# 1. 완벽히 분리된 뉴스 센티먼트 데이터 댐 (독립 DB)
NEWS_DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'news_data.sqlite')

def init_news_db():
    """뉴스 센티먼트 전용 DB와 테이블을 생성합니다."""
    conn = sqlite3.connect(NEWS_DB_PATH, timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_sentiment (
            date TEXT PRIMARY KEY,
            top_keyword_1 TEXT,
            top_keyword_2 TEXT,
            top_keyword_3 TEXT,
            top_keyword_4 TEXT,
            top_keyword_5 TEXT,
            sentiment_score REAL -- 긍정/부정(탐욕/공포) 점수 (0~100)
        )
    ''')
    conn.commit()
    conn.close()

def fetch_naver_finance_news():
    """네이버 금융 많이 본 뉴스 제목을 크롤링합니다."""
    url = "https://finance.naver.com/news/news_list.naver?mode=RANK"

    try:
        response = requests.get(url, headers=get_stealth_headers(), timeout=15)
        time.sleep(random.uniform(0.8, 2.1))
        soup = BeautifulSoup(response.content, 'html.parser')

        titles = []
        for a_tag in soup.select('.hotNewsList a'):
            title = a_tag.get('title') or a_tag.text
            if title:
                titles.append(title.strip())
        return titles
    except Exception as e:
        print(f"⚠️ 뉴스 크롤링 실패: {e}")
        return []

def extract_top_keywords(titles):
    """뉴스 제목에서 명사 위주의 핵심 키워드를 추출하고 빈도수를 셉니다."""
    words = []
    stop_words = ['특징주', '주가', '상승', '하락', '급등', '코스피', '코스닥', '종목', '투자', '증시', '마감']

    for title in titles:
        clean_title = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', title)
        for word in clean_title.split():
            if len(word) > 1 and word not in stop_words:
                words.append(word)

    if not words:
        return ["데이터없음"] * 5

    counter = Counter(words)
    top_5 = [item[0] for item in counter.most_common(5)]

    while len(top_5) < 5:
        top_5.append("없음")

    return top_5

def run_sentiment_mining():
    print("🧠 [센티먼트 마이닝 공장 가동] 대중의 심리와 키워드 스캔 중...")
    init_news_db()

    today_str = datetime.now().strftime('%Y-%m-%d')
    titles = fetch_naver_finance_news()

    if not titles:
        print("🚨 수집된 뉴스가 없습니다. 마이닝을 종료합니다.")
        return

    top_keywords = extract_top_keywords(titles)

    positive_words = ['상한가', '급등', '수주', '돌파', '흑자', '최대']
    negative_words = ['하한가', '급락', '악재', '적자', '쇼크', '매도']

    pos_count = sum(1 for t in titles for p in positive_words if p in t)
    neg_count = sum(1 for t in titles for n in negative_words if n in t)

    total_sentiment = 50
    if (pos_count + neg_count) > 0:
        total_sentiment = round((pos_count / (pos_count + neg_count)) * 100, 1)

    try:
        conn = sqlite3.connect(NEWS_DB_PATH, timeout=60)
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute('''
            INSERT OR REPLACE INTO daily_sentiment
            (date, top_keyword_1, top_keyword_2, top_keyword_3, top_keyword_4, top_keyword_5, sentiment_score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (today_str, top_keywords[0], top_keywords[1], top_keywords[2], top_keywords[3], top_keywords[4], total_sentiment))
        conn.commit()
        conn.close()

        print(f"✅ [{today_str}] 센티먼트 DB 저장이 완료되었습니다.")
        print(f" ↳ 🔥 오늘의 핫 키워드: {', '.join(top_keywords)}")
        print(f" ↳ 🌡️ 시장 심리 온도: {total_sentiment}점 (높을수록 탐욕/긍정)")
    except Exception as e:
        print(f"🚨 DB 저장 에러: {e}")

if __name__ == "__main__":
    run_sentiment_mining()
