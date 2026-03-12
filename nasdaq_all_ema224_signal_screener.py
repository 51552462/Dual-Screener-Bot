# Dante_US_Dual_Hyper_Screener.py
import os
import re
import time
import threading
import queue
from datetime import datetime
import pytz
from io import StringIO
import numpy as np
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg') # GUI 에러 원천 차단
import matplotlib.pyplot as plt
import requests
import warnings
import urllib3
import yfinance as yf
import FinanceDataReader as fdr
import logging

# ==========================================
# [보안 & 에러 숨김]
# ==========================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ================== Telegram ==================
TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

# ================== 폴더 설정 ==================
TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Dual_Screener')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]', '_', s)

# ================== 🇺🇸 스마트 기업 팩트 분석기 (US Version) ==================
def get_us_smart_report(ticker_str: str) -> tuple:
    sector = "정보 없음"
    earnings_trend = "뚜렷한 실적 추세 없음"
    news_summary = "최근 주요 뉴스 없음"
    
    try:
        tk = yf.Ticker(ticker_str)
        info = tk.info
        
        # 1. 섹터 추출
        sector = info.get('sector', '정보 없음')
        
        # 2. 실적 턴어라운드 추출 (EPS 성장률 기준)
        growth = info.get('earningsGrowth', 0)
        if growth is None: growth = 0
        
        if growth > 0.1:
            earnings_trend = f"📈 실적 성장 및 턴어라운드 (EPS 분기성장률: +{growth*100:.1f}%)"
        elif growth < -0.1:
            earnings_trend = f"📉 실적 부진 (EPS 분기성장률: {growth*100:.1f}%)"
        else:
            earnings_trend = "📊 보합 (특이사항 없음)"

        # 3. 최신 현지 뉴스 헤드라인 2줄 추출
        news = tk.news
        if news:
            headlines = []
            for n in news[:2]:
                headlines.append(f"- {n['title']}")
            news_summary = "\n".join(headlines)

    except: pass
    return sector, earnings_trend, news_summary

# ================== 미국 증시 티커 리스트 고속 수집 ==================
def get_us_ticker_list():
    print("🇺🇸 미국 증시(NASDAQ, NYSE, AMEX) 티커 리스트를 수집합니다...")
    try:
        df_ndq = fdr.StockListing('NASDAQ')
        df_nyse = fdr.StockListing('NYSE')
        df_amex = fdr.StockListing('AMEX')
        df = pd.concat([df_ndq, df_nyse, df_amex])
        
        df = df[df['Symbol'].str.isalpha()] # 워런트, 유닛 제외
        
        # 야후 파이낸스 호환을 위해 '.'을 '-'로 변환 (예: BRK.B -> BRK-B)
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except Exception as e:
        print(f"티커 수집 에러: {e}")
        return pd.DataFrame()

# ================== 텔레그램 전송 데몬 ==================
def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
            
        img_path, caption = item
        if len(caption) > 1000: caption = caption[:980] + "\n\n...(내용이 너무 길어 생략됨)"

        if SEND_TELEGRAM:
            for attempt in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                            params={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                            files={"photo": f}, timeout=20, verify=False
                        )
                    if res.status_code == 200: 
                        break
                    elif res.status_code == 429: time.sleep(3)
                    else: break 
                except Exception as e:
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

sender_thread = threading.Thread(target=telegram_sender_daemon, daemon=True)
sender_thread.start()

# ================== 지표 계산 (NumPy 초고속 C-엔진) ==================
def add_emas(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        d[f'EMA{n}'] = d['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    return d

# ================== [로직 1] 역배열 (1시간봉) - 100% 동기화 ==================
def compute_inverse_1h(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, None, df_raw, {}
    df = add_emas(df_raw)
    
    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    
    # ⭐️ 속도 최적화: 판다스 대신 순수 NumPy로 직전 3봉 평균 거래량 계산
    v_1 = np.roll(vol_arr, 1); v_1[0] = 0
    v_2 = np.roll(vol_arr, 2); v_2[:2] = 0
    v_3 = np.roll(vol_arr, 3); v_3[:3] = 0
    avgvol3_arr = (v_1 + v_2 + v_3) / 3

    ema10, ema20, ema30 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    bullish = close_arr > open_arr
    alignedNow = (ema10 > ema20) & (ema20 > ema30)
    
    # ⭐️ Infinity 무한대 에러 완벽 방어 처리
    with np.errstate(invalid='ignore'):
        volSpike = vol_arr > (np.nan_to_num(avgvol3_arr, nan=1.0) * 3)
    
    prev_close = np.roll(close_arr, 1); prev_close[0] = 0
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    cross224 = (close_arr > ema224) & (prev_close <= prev_ema224)
    
    signal1 = cross224 & bullish & alignedNow & volSpike
    
    # 파인스크립트 ta.barssince 완벽 동기화
    s1_T_3 = np.roll(signal1, 3); s1_T_3[:3] = False
    s1_T_2 = np.roll(signal1, 2); s1_T_2[:2] = False
    s1_T_1 = np.roll(signal1, 1); s1_T_1[0] = False
    s1_T_0 = signal1
    bars_since_s1_is_3 = s1_T_3 & (~s1_T_2) & (~s1_T_1) & (~s1_T_0)
    
    holdNow = (close_arr > ema224) & (ema10 > ema20) & (ema20 > ema30)
    holdNow_1 = np.roll(holdNow, 1); holdNow_1[0] = False
    holdNow_2 = np.roll(holdNow, 2); holdNow_2[:2] = False
    
    hold3 = bars_since_s1_is_3 & holdNow & holdNow_1 & holdNow_2
    allAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224) & (ema224 > ema448)
    signal2 = hold3 & allAligned

    is_s1, is_s2 = signal1[-1], signal2[-1]
    if not (is_s1 or is_s2): return False, None, df, {}
        
    sig_type = "S2 (유지 + 448 완전 정배열)" if is_s2 else "S1 (224 돌파 + 3배 거래량)"
    
    # ⭐️ 0으로 나누기 에러 차단
    safe_avg_vol = avgvol3_arr[-1] if avgvol3_arr[-1] > 0 else 1
    dbg = {"last_close": float(close_arr[-1]), "vol_spike": float(vol_arr[-1]/safe_avg_vol), "sig_type": sig_type}
    return True, sig_type, df, dbg

# ================== [로직 2] 정배열 (일봉) - 100% 동기화 ==================
def compute_aligned_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, None, df_raw, {}
    df = add_emas(df_raw)
    
    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    
    v_1 = np.roll(vol_arr, 1); v_1[0] = 0
    v_2 = np.roll(vol_arr, 2); v_2[:2] = 0
    v_3 = np.roll(vol_arr, 3); v_3[:3] = 0
    avgvol3_arr = (v_1 + v_2 + v_3) / 3

    ema10, ema20, ema30 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values
    
    isBullish = close_arr > open_arr
    
    # ⭐️ Infinity 무한대 에러 완벽 방어 처리
    with np.errstate(invalid='ignore'):
        volSpike5 = vol_arr >= (np.nan_to_num(avgvol3_arr, nan=1.0) * 5)
        
    condBase = isBullish & volSpike5
    
    align112 = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112)
    align224 = align112 & (ema112 > ema224)
    align448 = align224 & (ema224 > ema448)
    
    prev_align448 = np.roll(align448, 1); prev_align448[0] = False
    
    signal3 = condBase & align448 & (~prev_align448)
    signal2 = condBase & align224 & (~signal3)
    signal1 = condBase & align112 & (~align224)
    
    is_s1, is_s2, is_s3 = signal1[-1], signal2[-1], signal3[-1]
    if not (is_s1 or is_s2 or is_s3): return False, None, df, {}
         
    if is_s3: sig_type = "S3 (448 완전 정배열 완성)"
    elif is_s2: sig_type = "S2 (224 정배열 상태)"
    else: sig_type = "S1 (112 정배열 상태)"
    
    safe_avg_vol = avgvol3_arr[-1] if avgvol3_arr[-1] > 0 else 1
    dbg = {"last_close": float(close_arr[-1]), "vol_spike": float(vol_arr[-1]/safe_avg_vol), "sig_type": sig_type}
    return True, sig_type, df, dbg

# ================== 차트 저장 ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, timeframe: str) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            # ⭐️ 파일명 버그 수정: name 변수 추가
            safe = sanitize_filename(f"{code}_{name}_{timeframe}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            apds = [
                mpf.make_addplot(df_cut["EMA10"], color='red', width=1),
                mpf.make_addplot(df_cut["EMA20"], color='orange', width=1),
                mpf.make_addplot(df_cut["EMA30"], color='yellow', width=1),
                mpf.make_addplot(df_cut["EMA60"], color='green', width=1),
                mpf.make_addplot(df_cut["EMA112"], color='blue', width=1),
                mpf.make_addplot(df_cut["EMA224"], color='navy', width=2),
                mpf.make_addplot(df_cut["EMA448"], color='purple', width=2),
            ]

            tf_str = "1H(역배열)" if timeframe == '1h' else "1D(정배열)"
            title = f"[{dbg['sig_type']}] US Market: {code} ({tf_str})\nClose: ${dbg['last_close']:.2f} | 거래량 {dbg['vol_spike']:.1f}배"
            
            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            return None

# ================== 🚀 야후 API 그룹 다운로드 엔진 ==================
def scan_market(timeframe: str):
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    
    tf_label = "1시간봉" if timeframe == '1h' else "일봉"
    logic_name = "역배열(1시간봉)" if timeframe == '1h' else "정배열(일봉)"
    
    print(f"\n🇺🇸 [월스트리트 듀얼 스캔 가동] 총 {len(stock_list)}개 종목 '{logic_name}' 초고속 스캔 시작!")

    ticker_to_info = {}
    for _, row in stock_list.iterrows():
        ticker = row['Symbol']
        ticker_to_info[ticker] = {'code': ticker, 'name': row['Name']}
    
    tickers = list(ticker_to_info.keys())
    chunk_size = 100 
    period = "730d" if timeframe == '1h' else "3y"

    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        tickers_str = " ".join(chunk)
        
        df_batch = yf.download(tickers_str, interval=timeframe, period=period, group_by="ticker", progress=False, threads=True)
        
        for ticker in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(ticker, {})
            if not info: continue
            name, code = info.get('name', ''), info.get('code', '')

            try:
                if len(chunk) == 1: df_ticker = df_batch.copy()
                else: df_ticker = df_batch[ticker].copy()

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('America/New_York').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= 1.0:
                    tracker['analyzed'] += 1
                    
                    if timeframe == '1h': hit, sig_type, df, dbg = compute_inverse_1h(df_ticker)
                    else: hit, sig_type, df, dbg = compute_aligned_1d(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg, timeframe)
                        
                        if chart_path:
                            sector, earnings_trend, news_summary = get_us_smart_report(code) 
                            emoji = "🇺🇸"
                            
                            caption = (
                                f"{emoji} [{dbg['sig_type']}] ({tf_label})\n\n"
                                f"[{code}] {name}\n"
                                f"- 현재가: ${dbg['last_close']:.2f}\n"
                                f"- 거래량: 직전 3봉 평균 대비 {dbg['vol_spike']:.1f}배 폭발\n\n"
                                f"💡 [US Fact Check 리포트]\n"
                                f"🔸 섹터: {sector}\n"
                                f"🔸 실적: {earnings_trend}\n"
                                f"🔸 현지 뉴스 헤드라인:\n{news_summary}\n\n"
                                f"Time(NY): {datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            telegram_queue.put((chart_path, caption))
                            
            except Exception as e:
                pass
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    dt = time.time() - t0
    print(f"\n✅ [6번 봇: 미국장 듀얼 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 미국 서머타임(DST) 적용 스케줄러 ==================
def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [6번 봇: US 듀얼 스캐너 대기 모드 - 분산 완료]")
    print("   - [역배열/1H] 미국 현지시간(NY) 기준: 매시 40분 실행 (한국장 봇과 충돌 없음)")
    print("   - [정배열/1D] 미국 현지시간(NY) 기준: 16:10 1회 실행 (장 마감 직후)")
    print("   (서머타임 여부를 시스템이 자동 계산하여 실행합니다.)\n")
    
    while True:
        now_ny = datetime.now(ny_tz)
        
        # 💡 [시간 분산] 매시 40분에 1시간봉 검사
        if now_ny.minute == 10 and (9 <= now_ny.hour <= 15) and now_ny.hour != 14:
            print(f"🚀 [US 역배열/1H 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1h')
            print("💤 1H 스캔 완료. 다음 타임까지 대기합니다...")
            time.sleep(50 * 60) 
            
        # 💡 [시간 분산] 미장 마감 10분 뒤 (16:10) 일봉 검사
        elif now_ny.hour == 14 and now_ny.minute == 10:
            print(f"🚀 [US 정배열/1D 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1d')
            print("💤 1D 스캔 완료. 내일 개장까지 대기합니다...")
            time.sleep(50 * 60)
            
        else:
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()








