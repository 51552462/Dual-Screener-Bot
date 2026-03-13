# Dante_Ohdole_1H_FactCheck_Final.py
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Ohdole_1H')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ⭐️ 뉴스 제거, 다른 봇과 동일한 섹터/실적 팩트 체크 적용
def get_company_fact_report(code: str) -> tuple:
    sector, outlook, growth = "정보 없음", "정보 없음", "정보 없음"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
                
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
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

MIN_PRICE = 1000
MIN_TRANS_MONEY = 300_000_000  
VOL_MUL = 1.0                  

def compute_ohdole_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_signal", df_raw, {}

    df = df_raw.copy()
    
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA112'] = df['Close'].rolling(window=112).mean()
    df['MA224'] = df['Close'].rolling(window=224).mean()
    df['MA448'] = df['Close'].rolling(window=448).mean()

    close_arr = df['Close'].values
    open_arr = df['Open'].values
    high_arr = df['High'].values
    vol_arr = df['Volume'].values
    
    ma5 = df['MA5'].values
    ma112 = df['MA112'].values
    ma224 = df['MA224'].values
    ma448 = df['MA448'].values

    money_curr = close_arr * vol_arr

    is_downtrend = (ma448 > ma224) & (ma224 > ma112)
    is_basement = close_arr < ma112
    is_env_ok = is_downtrend & is_basement

    prev_vol = np.roll(vol_arr, 1); prev_vol[0] = np.inf
    is_vol_ok = vol_arr >= (prev_vol * VOL_MUL)
    is_money_ok = money_curr >= MIN_TRANS_MONEY
    is_price_ok = close_arr >= MIN_PRICE
    is_power_ok = is_vol_ok & is_money_ok & is_price_ok

    prev_ma5 = np.roll(ma5, 1); prev_ma5[0] = np.inf
    prev_close = np.roll(close_arr, 1); prev_close[0] = 0
    is_breakout = (close_arr > ma5) & (prev_close <= prev_ma5)
    
    prev_high1 = np.roll(high_arr, 1); prev_high1[0] = np.inf
    prev_high2 = np.roll(high_arr, 2); prev_high2[:2] = np.inf
    high_prev_2 = np.maximum(prev_high1, prev_high2)
    is_engulfing = (close_arr > open_arr) & (close_arr > high_prev_2)
    
    sig_1 = is_env_ok & is_power_ok & is_breakout & is_engulfing

    is_yangbong = close_arr > open_arr
    threshold = open_arr + ((close_arr - open_arr) * 0.33)
    is_riding = ma5 <= threshold
    
    sig_2 = is_env_ok & is_power_ok & is_yangbong & is_riding & (~sig_1)

    sig1_hit = sig_1[-1]
    sig2_hit = sig_2[-1]

    if not (sig1_hit or sig2_hit):
        return False, "no_signal", df, {}

    if sig1_hit: sig_type = "🔥 E 1번 (장악형)"
    else: sig_type = "✅ E 2번 (안착형)"

    dbg = {"last_close": float(close_arr[-1]), "sig_type": sig_type}
    return True, sig_type, df, dbg

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()

            title = f"[{dbg['sig_type']}] {code} {name} (1H)\nClose: {dbg['last_close']:,.0f}원"

            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':', rc={'font.family': plt.rcParams['font.family']})

            plt.close('all')
            # ⭐️ 차트 선 전부 제거
            mpf.plot(df_cut, type="candle", volume=True, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            return None

def scan_market():
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n⚡ [오돌이 하이퍼 스캔 가동] 총 {len(stock_list)}개 종목 '1시간봉' 초고속 병렬 스캔 시작!")

    ticker_to_info = {}
    for _, row in stock_list.iterrows():
        ticker = f"{row['Code']}.KS" if row['Market'] == 'KOSPI' else f"{row['Code']}.KQ"
        ticker_to_info[ticker] = {'code': row['Code'], 'name': row['Name']}
    
    tickers = list(ticker_to_info.keys())
    chunk_size = 40 
    period = "730d" 

    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=3)
    session.mount('https://', adapter)

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        tickers_str = " ".join(chunk)
        
        df_batch = pd.DataFrame()
        for attempt in range(4):
            try:
                df_batch = yf.download(tickers_str, interval="1h", period=period, group_by="ticker", progress=False, threads=False, session=session)
                if df_batch is not None and not df_batch.empty:
                    break
            except: pass
            time.sleep(1.5)
            
        if df_batch is None or df_batch.empty:
            tracker['scanned'] += len(chunk)
            continue
        
        for ticker in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(ticker)
            if not info: continue
            name, code = info['name'], info['code']

            try:
                if len(chunk) == 1: 
                    df_ticker = df_batch.copy()
                else: 
                    if isinstance(df_batch.columns, pd.MultiIndex):
                        if ticker not in df_batch.columns.get_level_values(0): continue 
                        df_ticker = df_batch[ticker].copy()
                    else: continue

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('Asia/Seoul').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= MIN_PRICE:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_ohdole_signal(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                        
                        if chart_path:
                            sector, outlook, growth = get_company_fact_report(code) 
                            emoji = "🇰🇷"
                            
                            # ⭐️ 초깔끔 팩트 리포트 
                            caption = (
                                f"{emoji} [{dbg['sig_type']}] (1H)\n\n"
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
    print(f"\n✅ [4번 봇: E 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [4번 봇: 오돌이(E) 대기 모드]")
    
    while True:
        now_kr = datetime.now(kr_tz)
        if now_kr.minute == 22 and (9 <= now_kr.hour <= 15) and now_kr.hour != 14:
            print(f"🚀 [4번 봇 1H 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market()
            time.sleep(60) 
        else: 
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
