# 1_Data_Pipeline / data_updater.py
import yfinance as yf
import FinanceDataReader as fdr
import pandas as pd
import sqlite3
import os
import time
import random
import concurrent.futures
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

from yf_download_flatten import flatten_yf_download_df

from market_db_paths import MARKET_DATA_DB_PATH, MARKET_DATA_SNAPSHOT_PATH

# 💡 [핵심 픽스] Ubuntu 서버 환경에 맞춘 정확한 DB 절대 경로 세팅
DB_PATH = MARKET_DATA_DB_PATH
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# 벤치마크·지수 테이블 (티커 OHLCV가 아님) — 고아 정리에서 제외
_ORPHAN_CLEANUP_PROTECTED_TABLES = frozenset(
    {"US_SPY", "US_QQQ", "US_VIX", "KR_KOSPI_IDX", "KR_KOSDAQ_IDX"}
)


def cleanup_orphan_tables(
    conn: sqlite3.Connection,
    us_list: pd.DataFrame,
    kr_list: pd.DataFrame,
) -> int:
    """
    sqlite_master에서 KR_% / US_% 테이블을 나열하고, 이번 배치의 생존 티커 집합과 대조해
    상장폐지·티커 변경 등으로 남은 고아 테이블만 DROP 한다.
    """
    alive_us: set[str] = set()
    if us_list is not None and not us_list.empty and "Symbol" in us_list.columns:
        for sym in us_list["Symbol"].astype(str):
            s = str(sym).strip()
            if s:
                alive_us.add(f"US_{s}")

    alive_kr: set[str] = set()
    if kr_list is not None and not kr_list.empty and "Code" in kr_list.columns:
        for code in kr_list["Code"].astype(str):
            c = str(code).strip().zfill(6)
            alive_kr.add(f"KR_{c}")

    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND (name LIKE 'US_%' OR name LIKE 'KR_%')"
    )
    dropped = 0
    for (name,) in cur.fetchall():
        if not name or name in _ORPHAN_CLEANUP_PROTECTED_TABLES:
            continue
        if name.startswith("US_") and name not in alive_us:
            conn.execute(f'DROP TABLE IF EXISTS "{name}"')
            dropped += 1
        elif name.startswith("KR_") and name not in alive_kr:
            conn.execute(f'DROP TABLE IF EXISTS "{name}"')
            dropped += 1
    conn.commit()
    return dropped

def save_data_safely(conn, table_name, df):
    """테이블 본체를 유지한 채 데이터만 원자적으로 교체한다."""
    temp_table = f"{table_name}__tmp_new"
    backup_table = f"{table_name}__tmp_old"

    # 신규 데이터를 임시 테이블에 먼저 준비
    df.to_sql(temp_table, conn, if_exists='replace', index=False)

    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f'DROP TABLE IF EXISTS "{backup_table}"')

        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        ).fetchone() is not None

        if table_exists:
            conn.execute(f'ALTER TABLE "{table_name}" RENAME TO "{backup_table}"')

        conn.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
        conn.execute(f'DROP TABLE IF EXISTS "{backup_table}"')
        conn.commit()
    except Exception:
        conn.rollback()
        conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        raise

# 🇺🇸 미국장 리스트 추출
def get_us_tickers():
    print("🇺🇸 미국장 종목 리스트 수집 중...")
    nas = fdr.StockListing('NASDAQ').assign(Market='NASDAQ')
    time.sleep(random.uniform(0.3, 0.7))
    nyse = fdr.StockListing('NYSE').assign(Market='NYSE')
    time.sleep(random.uniform(0.3, 0.7))
    amex = fdr.StockListing('AMEX').assign(Market='AMEX')
    df = pd.concat([nas, nyse, amex])
    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
    return df[['Symbol', 'Name', 'Market']].drop_duplicates(subset=['Symbol']).dropna()

# 🇰🇷 한국장 리스트 추출 (FDR → krx_list_cache.csv → sqlite KR_* 3단계 생존)
def get_kr_tickers():
    from krx_list_survival import collect_krx_list_survival

    print("🇰🇷 한국장 종목 리스트 수집 중...")
    junk = r"스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권|KODEX|TIGER|KBSTAR|ACE|ARIRANG|KOSEF|HANARO|SOL|TIMEFOLIO|WOORI|히어로즈|마이티|디딤|BNK|PLUS"
    try:
        df, _src = collect_krx_list_survival(db_path=DB_PATH, junk_pattern=junk, fdr_module=fdr)
    except Exception:
        df = pd.DataFrame()
    # 2차 방어선: 시장 전체 스캔이 깨졌다면 코스피라도 강제 확보
    if df is None or len(df) < 50:
        try:
            print("⚠️ KRX 리스트 부족(<50). 코스피 강제 수집(FDR) 시도...")
            kospi = fdr.StockListing("KOSPI")
            if kospi is not None and not kospi.empty:
                kospi = kospi.copy()
                if "Symbol" in kospi.columns and "Code" not in kospi.columns:
                    kospi["Code"] = kospi["Symbol"]
                if "종목코드" in kospi.columns and "Code" not in kospi.columns:
                    kospi["Code"] = kospi["종목코드"]
                if "회사명" in kospi.columns and "Name" not in kospi.columns:
                    kospi = kospi.rename(columns={"회사명": "Name"})
                if "종목명" in kospi.columns and "Name" not in kospi.columns:
                    kospi = kospi.rename(columns={"종목명": "Name"})
                kospi["Code"] = kospi["Code"].astype(str).str.strip().str.zfill(6)
                if "Market" not in kospi.columns:
                    kospi["Market"] = "KOSPI"
                kospi = kospi[~kospi["Name"].astype(str).str.contains(junk, regex=True)].copy()
                df = kospi[["Code", "Name", "Market"]].dropna()
        except Exception:
            pass
    if df is None or df.empty:
        return pd.DataFrame(columns=["Code", "Name", "Market"])
    return df[["Code", "Name", "Market"]].dropna()

# 개별 종목 데이터 다운로드 및 DB 저장 엔진 (💡 인자에서 conn 제거)
def update_single_ticker(row, country): 
    if country == 'US':
        sym = row['Symbol']
        table_name = f"US_{sym}"
        try:
            df = yf.download(sym, period="3y", interval="1d", progress=False)
            if df.empty: return False
            df = flatten_yf_download_df(df)
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
        # 거래정지 착시: 최근 3거래일 종가 동일 + 거래량 0이면 ffill 금지(가짜 응축 방지)
        tail3 = df.tail(3)
        if len(tail3) >= 3 and tail3['Close'].notna().all():
            if tail3['Close'].nunique() == 1 and (tail3['Volume'].astype(float) <= 0).all():
                return False
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
        
        try:
            local_conn.execute(f'DELETE FROM "{table_name}"')
        except sqlite3.OperationalError:
            pass
        
        df.to_sql(table_name, local_conn, if_exists='append', index=False)
        return True
    except:
        return False
    finally:
        try:
            local_conn.close()
        except:
            pass
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
        time.sleep(random.uniform(0.3, 0.7))
        for tk, tbl in zip(['SPY', 'QQQ', '^VIX'], ['US_SPY', 'US_QQQ', 'US_VIX']):
            if tk in idx_us.columns.levels[0]:
                df_temp = flatten_yf_download_df(idx_us[tk].copy()).dropna().reset_index()
                df_temp.rename(columns={'Date': 'Date', 'index': 'Date'}, inplace=True)
                df_temp['Date'] = pd.to_datetime(df_temp['Date']).dt.strftime('%Y-%m-%d')
                
                # 👇👇 [V102.6] 지수 데이터도 안전한 원자 교체 방식으로 저장 👇👇
                save_data_safely(bm_conn, tbl, df_temp)
        
        for tk, tbl in zip(['069500', '229200'], ['KR_KOSPI_IDX', 'KR_KOSDAQ_IDX']):
            df_temp = fdr.DataReader(tk, (pd.Timestamp.now() - pd.Timedelta(days=1000)).strftime('%Y-%m-%d')).reset_index()
            df_temp['Date'] = pd.to_datetime(df_temp['Date']).dt.strftime('%Y-%m-%d')
            
            # 👇👇 [V102.6] 한국 지수 데이터도 안전한 원자 교체 방식으로 저장 👇👇
            save_data_safely(bm_conn, tbl, df_temp)
            time.sleep(random.uniform(0.3, 0.7))

        bm_conn.close()
        print("✅ 벤치마크 지수 DB 저장 완료!")
    except Exception as e:
        print(f"⚠️ 벤치마크 지수 갱신 실패: {e}")

    # 1/2 미국장 (스레드 실행부 conn 제거)
    print("\n⏳ [1/2] 미국장 데이터 갱신 중... (야후 파이낸스 접속)")
    us_success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(update_single_ticker, row, 'US'): row['Symbol'] for _, row in us_list.iterrows()}
        import sys
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.result(): us_success += 1
            sys.stdout.write(f"\r진행률: {i+1}/{len(us_list)} (성공: {us_success}개)")
            sys.stdout.flush()

    # 2/2 한국장 (스레드 실행부 conn 제거)
    print("\n\n⏳ [2/2] 한국장 데이터 갱신 중... (KRX 접속)")
    kr_success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(update_single_ticker, row, 'KR'): row['Code'] for _, row in kr_list.iterrows()}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.result(): kr_success += 1
            sys.stdout.write(f"\r진행률: {i+1}/{len(kr_list)} (성공: {kr_success}개)")
            sys.stdout.flush()

    print(f"\n\n✅ DB 업데이트 완료! (미국: {us_success}개 / 한국: {kr_success}개 안전 저장 완료)")
    snap = create_read_only_snapshot()
    if snap:
        print(f"📸 읽기 전용 스냅샷 갱신 완료: {snap}")
    else:
        print("⚠️ 읽기 전용 스냅샷 생성 스킵(메인 DB 없음 또는 복제 실패).")

    try:
        oc = sqlite3.connect(DB_PATH, timeout=60)
        oc.execute("PRAGMA journal_mode=WAL;")
        n_drop = cleanup_orphan_tables(oc, us_list, kr_list)
        oc.close()
        n_us = len(us_list) if us_list is not None and not us_list.empty else 0
        n_kr = len(kr_list) if kr_list is not None and not kr_list.empty else 0
        print(f"🧹 고아 티커 테이블 정리 완료: DROP {n_drop}개 (배치 티커 US {n_us} / KR {n_kr} 기준 대조)")
    except Exception as e:
        print(f"⚠️ 고아 테이블 정리 스킵/실패: {e}")


def create_read_only_snapshot():
    """
    market_data.sqlite 의 읽기 전용 복제본(market_data_snapshot.sqlite)을 만든다.
    우선 sqlite3.Connection.backup (WAL 환경에서도 안전한 온라인 복제),
    실패 시 shutil.copy2 로 폴백한다.
    """
    import shutil

    if not os.path.isfile(MARKET_DATA_DB_PATH):
        return None
    os.makedirs(os.path.dirname(MARKET_DATA_SNAPSHOT_PATH), exist_ok=True)
    tmp_path = MARKET_DATA_SNAPSHOT_PATH + ".building"
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except OSError:
        pass

    try:
        src = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=60.0)
        try:
            src.execute("PRAGMA journal_mode=WAL;")
            dst = sqlite3.connect(tmp_path, timeout=60.0)
            try:
                dst.execute("PRAGMA journal_mode=WAL;")
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
        os.replace(tmp_path, MARKET_DATA_SNAPSHOT_PATH)
        return MARKET_DATA_SNAPSHOT_PATH
    except Exception as e:
        print(f"⚠️ [스냅샷] backup API 실패, copy2 폴백 시도: {e}")
        try:
            shutil.copy2(MARKET_DATA_DB_PATH, tmp_path)
            os.replace(tmp_path, MARKET_DATA_SNAPSHOT_PATH)
            return MARKET_DATA_SNAPSHOT_PATH
        except Exception as e2:
            print(f"🚨 [스냅샷] 생성 실패: {e2}")
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            return None


if __name__ == "__main__":
    run_daily_db_update()
