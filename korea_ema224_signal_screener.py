# Dante_Dual_Hyper_Screener_Smart_Final.py
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
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import warnings
import urllib3
import yfinance as yf
import logging
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Dual_Screener')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ⭐️ 뉴스 제거 및 실적 팩트 체크로 교체
def get_company_fact_report(code: str) -> tuple:
    sector, outlook, growth = "정보 없음", "정보 없음", "정보 없음"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url_naver = f"https://finance.naver.com/item/main.naver?code={code}"
        res_naver = requests.get(url_naver, headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
                
        url_fn = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}"
        res_fn = requests.get(url_fn, headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if len(tags) >= 1: outlook = tags[0].text.strip()
            if len(tags) >= 2: growth = tags[1].text.strip()
    except: pass
    return sector, outlook, growth

def get_krx_list_kind():
    print("KRX KIND 서버에서 종목 리스트를 가져옵니다...")
    try:
        url_ks = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt"
        df_ks = pd.read_html(StringIO(requests.get(url_ks, verify=False, timeout=10).text), header=0)[0]
        df_ks['Market'] = 'KOSPI'
        
        url_kq = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt"
        df_kq = pd.read_html(StringIO(requests.get(url_kq, verify=False, timeout=10).text), header=0)[0]
        df_kq['Market'] = 'KOSDAQ'
        
        df = pd.concat([df_ks, df_kq])
        df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
        df = df.rename(columns={'종목코드': 'Code', '회사명': 'Name'})
        df = df[~df['Name'].str.contains('스팩|ETN|ETF|우$|홀딩스|리츠', regex=True)]
        return df[['Code', 'Name', 'Market']].dropna()
    except: return pd.DataFrame()

def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
            
        img_path, caption = item
        if len(caption) > 1000: caption = caption[:980] + "\n\n...(생략됨)"

        if SEND_TELEGRAM:
            for attempt in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                            params={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                            files={"photo": f}, timeout=20, verify=False
                        )
                    if res.status_code == 200: break
                    elif res.status_code == 429: time.sleep(3)
                    else: break 
                except Exception as e:
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

def add_emas(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        d[f'EMA{n}'] = d['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    d['AvgVol3'] = d['Volume'].shift(1).rolling(3, min_periods=1).mean()
    return d

def compute_inverse_1h(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, None, df_raw, {}
    df = add_emas(df_raw)
    
    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    avgvol3_arr = df['AvgVol3'].values
    ema10, ema20, ema30 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    bullish = close_arr > open_arr
    alignedNow = (ema10 > ema20) & (ema20 > ema30)
    
    with np.errstate(invalid='ignore'):
        volSpike = vol_arr > (np.nan_to_num(avgvol3_arr, nan=1.0) * 3)
    
    prev_close = np.roll(close_arr, 1); prev_close[0] = 0
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    cross224 = (close_arr > ema224) & (prev_close <= prev_ema224)
    
    signal1 = cross224 & bullish & alignedNow & volSpike
    s1_shift3 = np.roll(signal1, 3); s1_shift3[:3] = False 
    
    holdNow = (close_arr > ema224) & (ema10 > ema20) & (ema20 > ema30)
    holdNow_1 = np.roll(holdNow, 1); holdNow_1[0] = False
    holdNow_2 = np.roll(holdNow, 2); holdNow_2[:2] = False
    
    hold3 = s1_shift3 & holdNow & holdNow_1 & holdNow_2
    allAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224) & (ema224 > ema448)
    signal2 = hold3 & allAligned 

    is_s1, is_s2 = signal1[-1], signal2[-1]
    if not (is_s1 or is_s2): return False, None, df, {}
        
    sig_type = "💥 Y (유지)" if is_s2 else "🎯 Y (신규)"
    
    dbg = {"last_close": float(close_arr[-1]), "sig_type": sig_type}
    return True, sig_type, df, dbg 

def compute_aligned_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, None, df_raw, {}
    df = add_emas(df_raw)
    
    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    avgvol3_arr = df['AvgVol3'].values
    ema10, ema20, ema30 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values 
    
    isBullish = close_arr > open_arr
    
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
         
    if is_s3: sig_type = "🎯 J (448 완성)"
    elif is_s2: sig_type = "🎯 J (224 상태)"
    else: sig_type = "🎯 J (112 상태)" 
    
    dbg = {"last_close": float(close_arr[-1]), "sig_type": sig_type}
    return True, sig_type, df, dbg 

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, timeframe: str) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}_{timeframe}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png") 

            df_cut = df.iloc[-DISPLAY_BARS:].copy()

            tf_str = "1H" if timeframe == '1h' else "1D"
            title = f"[{dbg['sig_type']}] {code} {name} ({tf_str})\nClose: {dbg['last_close']:,.0f}" 
            
            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            # ⭐️ 차트 선 전부 제거
            mpf.plot(df_cut, type="candle", volume=True, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            return None 

def scan_market(timeframe: str):
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    
    t0 = time.time() 
    tf_label = "1시간봉" if timeframe == '1h' else "일봉"
    logic_name = "Y (1시간봉)" if timeframe == '1h' else "J (일봉)" 
    
    print(f"\n⚡ [궁극의 그룹 스캔 가동] 총 {len(stock_list)}개 종목 '{logic_name}' 초고속 스캔 시작!") 

    ticker_to_info = {}
    for _, row in stock_list.iterrows():
        ticker = f"{row['Code']}.KS" if row['Market'] == 'KOSPI' else f"{row['Code']}.KQ"
        ticker_to_info[ticker] = {'code': row['Code'], 'name': row['Name']} 
    
    tickers = list(ticker_to_info.keys())
    chunk_size = 100 
    period = "730d" if timeframe == '1h' else "3y" 

    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0} 

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        tickers_str = " ".join(chunk)
        
        df_batch = yf.download(tickers_str, interval=timeframe, period=period, group_by="ticker", progress=False, threads=False) 
        
        for ticker in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info[ticker]
            name, code = info['name'], info['code'] 

            try:
                if len(chunk) == 1: df_ticker = df_batch.copy()
                else: df_ticker = df_batch[ticker].copy() 

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('Asia/Seoul').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')] 

                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= 1000:
                    tracker['analyzed'] += 1 
                    
                    if timeframe == '1h': hit, sig_type, df, dbg = compute_inverse_1h(df_ticker)
                    else: hit, sig_type, df, dbg = compute_aligned_1d(df_ticker) 
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg, timeframe) 
                        
                        if chart_path:
                            sector, outlook, growth = get_company_fact_report(code) 
                            emoji = "🔥" if timeframe == '1h' else "💎" 
                            
                            # ⭐️ 초깔끔 팩트 리포트
                            caption = (
                                f"{emoji} [{dbg['sig_type']}] ({tf_label})\n\n"
                                f"🏢 [{name}] ({code})\n"
                                f"💰 현재가: {dbg['last_close']:,.0f}원\n\n"
                                f"💡 [기업 팩트 체크]\n"
                                f"🔸 섹터: {sector}\n"
                                f"🔸 전망: {outlook}\n"
                                f"🔸 실적: {growth}\n\n"
                                f"⏰ {datetime.now(pytz.timezone('Asia/Seoul')).strftime('%m-%d %H:%M')}"
                            ) 
                            telegram_queue.put((chart_path, caption)) 
                            
            except Exception as e:
                pass 
        
        if tracker['scanned'] % 200 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)") 

    dt = time.time() - t0
    print(f"\n✅ [{logic_name}] 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n") 

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [3번 봇: 한국장 EMA224 대기 모드]")
    
    while True:
        now_kr = datetime.now(kr_tz)
        if now_kr.minute == 42 and (9 <= now_kr.hour <= 15) and now_kr.hour != 14:
            print(f"🚀 [3번 봇 1H 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1h')
            time.sleep(60) 
            
        elif now_kr.hour == 14 and now_kr.minute == 30:
            print(f"🚀 [3번 봇 1D 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1d')
            time.sleep(60) 
            
        else: 
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
