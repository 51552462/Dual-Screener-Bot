# Dante_Reverse_Breakout_Hyper_Batch_Fixed_Final.py
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Reverse_Breakout_Dual')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

def get_company_fact_report(code: str) -> tuple:
    sector, outlook, growth = "정보 없음", "기업 현황 데이터를 불러올 수 없습니다.", "최근 실적 데이터를 불러올 수 없습니다."
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url_naver = f"https://finance.naver.com/item/main.naver?code={code}"
        res_naver = requests.get(url_naver, headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            sector_tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if sector_tag: sector = sector_tag.text.strip()
                
        url_fn = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}"
        res_fn = requests.get(url_fn, headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            summary_tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if len(summary_tags) >= 1: outlook = summary_tags[0].text.strip()
            if len(summary_tags) >= 2: growth = summary_tags[1].text.strip()
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

EMA112_LEN, EMA224_LEN, EMA448_LEN = 112, 224, 448
MIN_PRICE = 1000                 
MIN_AVG_VALUE_20D = 500_000_000  
VALUE_SPIKE_MULT = 1.6           
CLOSE_TOP_FRAC = 0.68            
ACC_LOOKBACK = 20           
PRE_LOOKBACK = 8            

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["Value"] = d["Close"] * d["Volume"]
    d["ValueMA20"] = d["Value"].rolling(20, min_periods=1).mean()
    d["EMA112"] = d["Close"].ewm(span=EMA112_LEN, adjust=False, min_periods=0).mean()
    d["EMA224"] = d["Close"].ewm(span=EMA224_LEN, adjust=False, min_periods=0).mean()
    d["EMA448"] = d["Close"].ewm(span=EMA448_LEN, adjust=False, min_periods=0).mean()
    d["AvgVol3"] = d["Volume"].shift(1).rolling(3, min_periods=1).mean()
    return d

def compute_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_data", df_raw, {}

    df = add_indicators(df_raw)

    condPrice = df['Close'] >= MIN_PRICE
    condLiquidity = df['ValueMA20'] >= MIN_AVG_VALUE_20D
    condBearAlign = (df['EMA112'] < df['EMA224']) & (df['EMA224'] < df['EMA448'])
    condBullAlign = (df['EMA112'] > df['EMA224']) & (df['EMA224'] > df['EMA448'])
    condHold112 = df['Close'] > df['EMA112']

    is_under = df['Close'].shift(1) < df['EMA112'].shift(1)
    condCrossEvent = is_under.rolling(window=PRE_LOOKBACK).sum() > 0

    isAccBull = df['Close'] > df['Open']
    rng = df['High'] - df['Low']
    
    with np.errstate(divide='ignore', invalid='ignore'):
        closePos = np.where(rng > 0, (df['Close'] - df['Low']) / rng, 0)
    
    isAccCandle = isAccBull & (df['Value'] >= VALUE_SPIKE_MULT * df['ValueMA20']) & (closePos >= CLOSE_TOP_FRAC)
    condHasAcc = isAccCandle.rolling(window=ACC_LOOKBACK).sum() > 0

    avgVol3_arr = df['AvgVol3'].values
    vol_arr = df['Volume'].values
    with np.errstate(invalid='ignore'):
        condVolSpike = vol_arr >= (np.nan_to_num(avgVol3_arr, nan=1.0) * 3)

    isCurrentBullish = df['Close'] > df['Open']

    signalBase = condPrice & condLiquidity & condBearAlign & condHold112 & condCrossEvent & condHasAcc & condVolSpike & isCurrentBullish

    if not signalBase.iloc[-1]:
        return False, "no_signal", df, {}

    bull_align_arr = condBullAlign.values
    signal_base_arr = signalBase.values
    
    signalCount = 0
    for i in range(len(bull_align_arr)):
        if bull_align_arr[i]:
            signalCount = 0
        if signal_base_arr[i]:
            signalCount += 1

    isSubsequentSignal = signalBase.iloc[-1] and (signalCount > 1)
    signal_type = "💥 연속 P" if isSubsequentSignal else "🎯 첫 P"

    dbg = {
        "last_close": float(df["Close"].iloc[-1]),
        "signal_type": signal_type
    }
    
    return True, signal_type, df, dbg

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, timeframe: str) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}_{timeframe}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            tf_str = "1H" if timeframe == '1h' else "1D"
            title = f"[{dbg['signal_type']}] {code} {name} ({tf_str})\nClose: {dbg['last_close']:,.0f}"

            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':', rc={'font.family': plt.rcParams['font.family']})

            plt.close('all')
            # ⭐️ 차트 선 전부 제거 (오직 캔들과 거래량만)
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
    print(f"\n⚡ [궁극의 그룹 스캔 가동] 총 {len(stock_list)}개 종목 '{tf_label}' 초고속 병렬 스캔 시작!")

    ticker_to_info = {}
    for _, row in stock_list.iterrows():
        ticker = f"{row['Code']}.KS" if row['Market'] == 'KOSPI' else f"{row['Code']}.KQ"
        ticker_to_info[ticker] = {'code': row['Code'], 'name': row['Name']}
    
    tickers = list(ticker_to_info.keys())
    chunk_size = 40 
    period = "730d" if timeframe == '1h' else "3y"

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
                df_batch = yf.download(tickers_str, interval=timeframe, period=period, group_by="ticker", progress=False, threads=False, session=session)
                if df_batch is not None and not df_batch.empty:
                    break
            except Exception:
                pass
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
                        if ticker not in df_batch.columns.get_level_values(0): 
                            continue 
                        df_ticker = df_batch[ticker].copy()
                    else:
                        continue

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('Asia/Seoul').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_signal(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg, timeframe)
                        
                        if chart_path:
                            sector, outlook, growth = get_company_fact_report(code) 
                            emoji = "🇰🇷"
                            
                            # ⭐️ 초깔끔 팩트 리포트
                            caption = (
                                f"{emoji} [{dbg['signal_type']}] ({tf_label})\n\n"
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
    print(f"\n✅ [2번 봇 {tf_label} 스캔 완료] 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [2번 봇: 한국장 역매공파 대기 모드]")
    
    while True:
        now_kr = datetime.now(kr_tz)
        if now_kr.minute == 12 and (9 <= now_kr.hour <= 15) and now_kr.hour != 14:
            print(f"🚀 [2번 봇 1H 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1h')
            time.sleep(60) 
            
        elif now_kr.hour == 14 and now_kr.minute == 20:
            print(f"🚀 [2번 봇 1D 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1d')
            time.sleep(60)
            
        else: 
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
