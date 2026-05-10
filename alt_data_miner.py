import sqlite3
import pandas as pd
import yfinance as yf
import os
from datetime import datetime
import time

# 1. 완벽히 분리된 새로운 데이터 댐 (독립 DB)
ALT_DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'alt_data.sqlite')

def init_alt_db():
    """대체 데이터 전용 DB와 테이블을 생성합니다."""
    conn = sqlite3.connect(ALT_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS macro_daily (
            date TEXT PRIMARY KEY,
            usd_krw REAL,       -- 원/달러 환율
            us_10y_yield REAL,  -- 미국 10년물 국채 금리 (시장 금리 벤치마크)
            vix_index REAL,     -- VIX 지수 (공포/탐욕 프록시)
            dxy_index REAL,     -- 달러 인덱스 (글로벌 유동성)
            put_call_ratio REAL,-- 풋/콜 비율 (추후 고도화 크롤링을 위한 빈칸 세팅)
            fed_rate_prob REAL  -- 연준 인상 확률 (추후 고도화 크롤링을 위한 빈칸 세팅)
        )
    ''')
    conn.commit()
    conn.close()

def fetch_yfinance_data(ticker):
    """yfinance에서 가장 최근 종가를 안전하게 가져옵니다."""
    try:
        data = yf.Ticker(ticker).history(period="5d")
        if not data.empty:
            return round(float(data['Close'].iloc[-1]), 4)
    except Exception as e:
        print(f"⚠️ {ticker} 데이터 수집 실패: {e}")
    return None

def run_alternative_data_mining():
    print("📡 [대체 데이터 마이닝 공장 가동] 글로벌 거시경제 지표 스캔 중...")
    init_alt_db()
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # 안정적인 글로벌 벤치마크 데이터 수집
    usd_krw = fetch_yfinance_data("KRW=X")    # 원/달러 환율
    us_10y = fetch_yfinance_data("^TNX")      # 미국 10년물 국채 금리
    vix = fetch_yfinance_data("^VIX")         # VIX (공포 지수)
    dxy = fetch_yfinance_data("DX-Y.NYB")     # 달러 인덱스
    
    # 크롤링이 필요한 심화 데이터는 초기값 0.0으로 세팅 (추후 기능 확장 시 업데이트)
    put_call_ratio = 0.0 
    fed_rate_prob = 0.0  

    # DB 저장 (INSERT OR REPLACE로 중복 방지)
    try:
        conn = sqlite3.connect(ALT_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO macro_daily 
            (date, usd_krw, us_10y_yield, vix_index, dxy_index, put_call_ratio, fed_rate_prob)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (today_str, usd_krw, us_10y, vix, dxy, put_call_ratio, fed_rate_prob))
        conn.commit()
        conn.close()
        
        print(f"✅ [{today_str}] 대체 데이터 댐에 저장이 완료되었습니다.")
        print(f" ↳ 환율: {usd_krw}원 | 국채금리: {us_10y}% | VIX: {vix} | 달러인덱스: {dxy}")
    except Exception as e:
        print(f"🚨 DB 저장 에러: {e}")

if __name__ == "__main__":
    run_alternative_data_mining()
