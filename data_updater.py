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

# 💡 [핵심 픽스] Ubuntu 서버 환경에 맞춘 정확한 DB 절대 경로 세팅
DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')
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
    filtered_df = df[~df['Name'].str.contains('스팩|ETN|ETF|우$|홀딩스|리츠', regex=True)].copy()
    return filtered_df[['Code', 'Name', 'Market']].dropna()

# 개별 종목 데이터 다운로드 및 DB 저장 엔진 (💡 인자에서 conn 제거)
def update_single_ticker(row, country): 
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
            start_date = (datetime.now() - pd.Timedelta(days=1000)).strftime('%Y-%m-%d')
            df = fdr.DataReader(sym, start_date)
            if df.empty: return False
        except: return False

    try:
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
        
        # 👇👇 [수정] V26.0 시계열 왜곡 방지 및 횡단면 동기화 👇👇
        # 1. 거래량 결측치는 명백한 거래 없음이므로 0으로 채움
        df['Volume'] = df['Volume'].fillna(0)
        # 2. 가격 결측치는 거래 정지 상태이므로 이전 종가로 채움 (Forward Fill)
        # 3. 데이터 맨 앞부분의 무의미한 결측치만 최종 제거
        df = df.ffill().dropna()
        # 👆👆 [수정 끝] 👆👆
        
        df.reset_index(inplace=True)
        df.rename(columns={'Date': 'Date', 'index': 'Date'}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
        # 💡 [핵심] 각 스레드가 독립적인 출입문 생성 및 Timeout 대기열(Queue) 확보
        local_conn = sqlite3.connect(DB_PATH, timeout=30)
        local_conn.execute("PRAGMA journal_mode=WAL;")       # 동시 읽기/쓰기 허용
        local_conn.execute("PRAGMA synchronous=NORMAL;")     # WAL 모드 최적화 (속도 향상)
        
        # 👇👇 [V102.6 버그 픽스] Replace(DROP) 폭파 방지 및 무정지 Append 엔진 👇👇
        try:
            # 1. 뼈대(테이블)는 살려두고 기존 데이터 알맹이만 조용히 삭제 (DB Lock 방지)
            local_conn.execute(f'DELETE FROM "{table_name}"')
        except sqlite3.OperationalError:
            # 처음 수집하는 종목이라 테이블이 아예 없다면 조용히 패스
            pass
        
        # 2. Append 모드로 알맹이만 안전하게 주입 (다른 봇들이 0.1초도 멈추지 않음)
        df.to_sql(table_name, local_conn, if_exists='append', index=False)
        # 👆👆 [패치 완료] 👆👆

        local_conn.close()
        return True
    except: return False
# 메인 업데이트 실행기
def run_daily_db_update():
    print(f"\n🛢️ 글로벌 퀀트 로컬 데이터베이스 갱신 시작 (경로: {DB_PATH})")
    
    us_list = get_us_tickers()
    kr_list = get_kr_tickers()
    
    # 💡 [순서 교정] 0/2 벤치마크 지수 먼저 실행 (독립 연결 사용)
    print("\n⏳ [0/2] 벤치마크 지수(VIX, SPY, QQQ, KOSPI, KOSDAQ) 갱신 중...")
    try:
        bm_conn = sqlite3.connect(DB_PATH, timeout=30)
        bm_conn.execute("PRAGMA journal_mode=WAL;")
        
        idx_us = yf.download("SPY QQQ ^VIX", period="3y", interval="1d", group_by="ticker", progress=False)
        for tk, tbl in zip(['SPY', 'QQQ', '^VIX'], ['US_SPY', 'US_QQQ', 'US_VIX']):
            if tk in idx_us.columns.levels[0]:
                df_temp = idx_us[tk].dropna().reset_index()
                df_temp.rename(columns={'Date': 'Date', 'index': 'Date'}, inplace=True)
                df_temp['Date'] = pd.to_datetime(df_temp['Date']).dt.strftime('%Y-%m-%d')
                
                # 👇👇 [V102.6] 지수 데이터 역시 무정지 Append 적용 👇👇
                try: bm_conn.execute(f'DELETE FROM "{tbl}"')
                except: pass
                df_temp.to_sql(tbl, bm_conn, if_exists='append', index=False)
        
        for tk, tbl in zip(['069500', '229200'], ['KR_KOSPI_IDX', 'KR_KOSDAQ_IDX']):
            df_temp = fdr.DataReader(tk, (pd.Timestamp.now() - pd.Timedelta(days=1000)).strftime('%Y-%m-%d')).reset_index()
            df_temp['Date'] = pd.to_datetime(df_temp['Date']).dt.strftime('%Y-%m-%d')
            
            # 👇👇 [V102.6] 한국 지수 데이터 무정지 Append 적용 👇👇
            try: bm_conn.execute(f'DELETE FROM "{tbl}"')
            except: pass
            df_temp.to_sql(tbl, bm_conn, if_exists='append', index=False)
            
        bm_conn.close()
        print("✅ 벤치마크 지수 DB 저장 완료!")
    except Exception as e:
        print(f"⚠️ 벤치마크 지수 갱신 실패: {e}")

    # 1/2 미국장 (스레드 실행부 conn 제거)
    print("\n⏳ [1/2] 미국장 데이터 갱신 중... (야후 파이낸스 접속)")
    us_success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(update_single_ticker, row, 'US'): row['Symbol'] for _, row in us_list.iterrows()}
        import sys
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.result(): us_success += 1
            sys.stdout.write(f"\r진행률: {i+1}/{len(us_list)} (성공: {us_success}개)")
            sys.stdout.flush()

    # 2/2 한국장 (스레드 실행부 conn 제거)
    print("\n\n⏳ [2/2] 한국장 데이터 갱신 중... (KRX 접속)")
    kr_success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(update_single_ticker, row, 'KR'): row['Code'] for _, row in kr_list.iterrows()}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.result(): kr_success += 1
            sys.stdout.write(f"\r진행률: {i+1}/{len(kr_list)} (성공: {kr_success}개)")
            sys.stdout.flush()

    print(f"\n\n✅ DB 업데이트 완료! (미국: {us_success}개 / 한국: {kr_success}개 안전 저장 완료)")

if __name__ == "__main__":
    run_daily_db_update()
