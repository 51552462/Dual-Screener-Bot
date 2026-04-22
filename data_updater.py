# 1_Data_Pipeline / data_updater.py
import yfinance as yf
import FinanceDataReader as fdr
import pandas as pd
import sqlite3
import os
import concurrent.futures
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 💡 DB 파일이 저장될 절대 경로 세팅
DB_PATH = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Quant_System', 'market_data.sqlite')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# 🇺🇸 미국장 리스트 추출
def get_us_tickers():
    print("🇺🇸 미국장 종목 리스트 수집 중...")
    df = pd.concat([
        fdr.StockListing('NASDAQ').assign(Market='NASDAQ'),
        fdr.StockListing('NYSE').assign(Market='NYSE'),
        fdr.StockListing('AMEX').assign(Market='AMEX')
    ])
    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
    return df[['Symbol', 'Name', 'Market']].drop_duplicates(subset=['Symbol']).dropna()

# 🇰🇷 한국장 리스트 추출
def get_kr_tickers():
    print("🇰🇷 한국장 종목 리스트 수집 중...")
    df = fdr.StockListing('KRX')
    df['Code'] = df['Code'].astype(str).str.zfill(6)
    # 스팩, 리츠 등 잡주 필터링
    filtered_df = df[~df['Name'].str.contains('스팩|ETN|ETF|우$|홀딩스|리츠', regex=True)].copy()
    return filtered_df[['Code', 'Name', 'Market']].dropna()

# 개별 종목 데이터 다운로드 및 DB 저장 엔진
def update_single_ticker(row, country, conn):
    if country == 'US':
        sym = row['Symbol']
        table_name = f"US_{sym}"
        try:
            df = yf.download(sym, period="3y", interval="1d", progress=False)
            if df.empty: return False
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        except: return False
    else: # KR
        sym = row['Code']
        table_name = f"KR_{sym}"
        try:
            # 한국장은 FDR 사용 (속도 및 안정성 우수)
            start_date = (datetime.now() - pd.Timedelta(days=1000)).strftime('%Y-%m-%d')
            df = fdr.DataReader(sym, start_date)
            if df.empty: return False
        except: return False

    try:
        # 데이터 정규화 및 저장
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
        df.reset_index(inplace=True)
        df.rename(columns={'Date': 'Date', 'index': 'Date'}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
        # 💡 SQLite에 덮어쓰기 저장 (기존 데이터 최신화)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        return True
    except: return False

# 메인 업데이트 실행기
def run_daily_db_update():
    print(f"\n🛢️ 글로벌 퀀트 로컬 데이터베이스 갱신 시작 (경로: {DB_PATH})")
    
    us_list = get_us_tickers()
    kr_list = get_kr_tickers()
    
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    
    print("\n⏳ [1/2] 미국장 데이터 갱신 중... (야후 파이낸스 접속)")
    us_success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(update_single_ticker, row, 'US', conn): row['Symbol'] for _, row in us_list.iterrows()}
        import sys
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.result(): us_success += 1
            sys.stdout.write(f"\r진행률: {i+1}/{len(us_list)} (성공: {us_success}개)")
            sys.stdout.flush()

    print("\n\n⏳ [2/2] 한국장 데이터 갱신 중... (KRX 접속)")
    kr_success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(update_single_ticker, row, 'KR', conn): row['Code'] for _, row in kr_list.iterrows()}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.result(): kr_success += 1
            sys.stdout.write(f"\r진행률: {i+1}/{len(kr_list)} (성공: {kr_success}개)")
            sys.stdout.flush()
# (기존 run_daily_db_update 내부)
    print("\n⏳ [0/2] 벤치마크 지수(VIX, SPY, QQQ, KOSPI, KOSDAQ) 갱신 중...")
    try:
        # 미국 지수
        idx_us = yf.download("SPY QQQ ^VIX", period="3y", interval="1d", group_by="ticker", progress=False)
        for tk, tbl in zip(['SPY', 'QQQ', '^VIX'], ['US_SPY', 'US_QQQ', 'US_VIX']):
            if tk in idx_us.columns.levels[0]:
                df_temp = idx_us[tk].dropna().reset_index()
                df_temp.rename(columns={'Date': 'Date', 'index': 'Date'}, inplace=True)
                df_temp['Date'] = pd.to_datetime(df_temp['Date']).dt.strftime('%Y-%m-%d')
                df_temp.to_sql(tbl, conn, if_exists='replace', index=False)
        
        # 한국 지수 (KODEX ETF 대용)
        for tk, tbl in zip(['069500', '229200'], ['KR_KOSPI_IDX', 'KR_KOSDAQ_IDX']):
            df_temp = fdr.DataReader(tk, (pd.Timestamp.now() - pd.Timedelta(days=1000)).strftime('%Y-%m-%d')).reset_index()
            df_temp['Date'] = pd.to_datetime(df_temp['Date']).dt.strftime('%Y-%m-%d')
            df_temp.to_sql(tbl, conn, if_exists='replace', index=False)
        print("✅ 벤치마크 지수 DB 저장 완료!")
    except Exception as e:
        print(f"⚠️ 벤치마크 지수 갱신 실패: {e}")
        
    conn.close()
    print(f"\n\n✅ DB 업데이트 완료! (미국: {us_success}개 / 한국: {kr_success}개 안전 저장 완료)")

if __name__ == "__main__":
    run_daily_db_update()
