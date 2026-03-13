# Dante_US_Bowl_Hyper_Screener.py
import os
import re
import time
import threading
import queue
from datetime import datetime
import pytz
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
import FinanceDataReader as fdr
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Bowl_Dual')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]', '_', s)

def get_us_smart_report(ticker_str: str) -> tuple:
    sector = "정보 없음"
    earnings_trend = "정보 없음"
    try:
        tk = yf.Ticker(ticker_str)
        info = tk.info
        sector = info.get('sector', '정보 없음')
        growth = info.get('earningsGrowth', 0)
        
        if growth is None: growth = 0
        if growth > 0.1:
            earnings_trend = f"📈 실적 성장/턴어라운드 (분기 EPS: +{growth*100:.1f}%)"
        elif growth < -0.1:
            earnings_trend = f"📉 실적 부진 (분기 EPS: {growth*100:.1f}%)"
        else:
            earnings_trend = "📊 보합 (특이사항 없음)"
    except: pass
    return sector, earnings_trend

def get_us_ticker_list():
    print("🇺🇸 미국 증시(NASDAQ, NYSE, AMEX) 티커 리스트를 수집합니다...")
    try:
        df_ndq = fdr.StockListing('NASDAQ')
        df_nyse = fdr.StockListing('NYSE')
        df_amex = fdr.StockListing('AMEX')
        df = pd.concat([df_ndq, df_nyse, df_amex])
        df = df[df['Symbol'].str.isalpha()]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except Exception as e:
        return pd.DataFrame()

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

MIN_PRICE_USD = 1.0               
MIN_MONEY_USD = 1_000_000         

def compute_bowl_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_signal", df_raw, {}

    df = df_raw.copy()

    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['StdDev'] = df['Close'].rolling(window=20).std(ddof=1) 
    df['BB_Upper'] = df['MA20'] + (df['StdDev'] * 2)

    high9 = df['High'].rolling(9).max()
    low9 = df['Low'].rolling(9).min()
    tenkan = (high9 + low9) / 2
    
    high26 = df['High'].rolling(26).max()
    low26 = df['Low'].rolling(26).min()
    kijun = (high26 + low26) / 2
    
    spanA = (tenkan + kijun) / 2
    spanB = (df['High'].rolling(52).max() + df['Low'].rolling(52).min()) / 2
    
    df['Senkou1'] = spanA.shift(25)
    df['Senkou2'] = spanB.shift(25)
    df['CloudTop'] = df[['Senkou1', 'Senkou2']].max(axis=1)

    df['VolAvg20'] = df['Volume'].rolling(20).mean()
    df['AvgVol3'] = df['Volume'].shift(1).rolling(3).mean()

    mean120 = df['Close'].rolling(120).mean().shift(5)
    std120 = df['Close'].rolling(120).std(ddof=1).shift(5)
    
    mean60 = df['Close'].rolling(60).mean().shift(5)
    std60 = df['Close'].rolling(60).std(ddof=1).shift(5)
    
    with np.errstate(divide='ignore', invalid='ignore'):
        condBox6m = (std120 / mean120) < 0.20
        condBox3m = (std60 / mean60) < 0.20

    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    avgvol3_arr = df['AvgVol3'].values
    volavg20_arr = df['VolAvg20'].values
    bb_upper = df['BB_Upper'].values
    cloud_top = df['CloudTop'].values
    
    ema10, ema20, ema30 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    isCat2 = condBox6m.values
    isCat1 = (~isCat2) & condBox3m.values
    hasBox = isCat1 | isCat2

    isBullish = close_arr > open_arr
    prev_close = np.roll(close_arr, 1); prev_close[0] = np.inf
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    
    condEma = (close_arr > ema224) & (prev_close < prev_ema224 * 1.05)
    condCloud = close_arr > cloud_top
    condBb = close_arr >= bb_upper * 0.98
    condVol = vol_arr > volavg20_arr * 2.0
    condNotOverheated = close_arr <= ema224 * 1.15
    
    with np.errstate(invalid='ignore'):
        condVolSpike = vol_arr >= (np.nan_to_num(avgvol3_arr, nan=1.0) * 5)
    
    condMoney = (close_arr * vol_arr) >= MIN_MONEY_USD
    condPriceUsd = close_arr >= MIN_PRICE_USD

    signalBase = condPriceUsd & condMoney & isBullish & condEma & condCloud & condBb & condVol & condNotOverheated & hasBox & condVolSpike
    
    if not signalBase[-1]: 
        return False, "no_signal", df, {}

    signalCat2 = signalBase & isCat2
    signalCat1 = signalBase & isCat1

    isAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224)

    cat2_Normal = signalCat2 & (~isAligned)
    cat2_Strong = signalCat2 & isAligned
    cat1_Normal = signalCat1 & (~isAligned)
    cat1_Strong = signalCat1 & isAligned

    if cat2_Strong[-1]: sig_type = "💥 B (정배열 강조)"
    elif cat1_Strong[-1]: sig_type = "💥 B (정배열 강조)"
    elif cat2_Normal[-1]: sig_type = "🎯 B (일반)"
    elif cat1_Normal[-1]: sig_type = "🎯 B (일반)"
    else: sig_type = "🎯 B"

    dbg = {
        "last_close": float(close_arr[-1]), 
        "sig_type": sig_type
    }
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
            title = f"[{dbg['sig_type']}] US Market: {code} ({tf_str})\nClose: ${dbg['last_close']:.2f}"
            
            mc = mpf.make_marketcolors(up='green', down='red', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            # ⭐️ 선, 구름대 전부 제거
            mpf.plot(df_cut, type="candle", volume=True, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            return None

def scan_market(timeframe: str):
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    tf_label = "1시간봉" if timeframe == '1h' else "일봉"
    print(f"\n🇺🇸 [월스트리트 B 스캔] 총 {len(stock_list)}개 종목 '{tf_label}' 초고속 스캔 시작!")

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
       
        df_batch = yf.download(tickers_str, interval=timeframe, period=period, group_by="ticker", progress=False, threads=False)
        
        for ticker in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(ticker, {})
            if not info: continue
            name, code = info.get('name', ''), info.get('code', '')

            try:
                if len(chunk) == 1: df_ticker = df_batch.copy()
                else: 
                    if ticker not in df_batch.columns.get_level_values(0): continue
                    df_ticker = df_batch[ticker].copy()

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('America/New_York').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= 1.0:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_bowl_signal(df_ticker)
                
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg, timeframe)
                  
                        if chart_path:
                            sector, earnings_trend = get_us_smart_report(code) 
                            emoji = "🇺🇸"
                            
                            # ⭐️ 초깔끔 팩트 리포트
                            caption = (
                                f"{emoji} [{dbg['sig_type']}] ({tf_label})\n\n"
                                f"🏢 [{code}] {name}\n"
                                f"💰 현재가: ${dbg['last_close']:.2f}\n\n"
                                f"💡 [기업 팩트 체크]\n"
                                f"🔸 섹터: {sector}\n"
                                f"🔸 실적: {earnings_trend}\n\n"
                                f"⏰ {datetime.now(pytz.timezone('America/New_York')).strftime('%m-%d %H:%M')}"
                            )
                            telegram_queue.put((chart_path, caption))
                            
            except Exception as e:
                pass
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    dt = time.time() - t0
    print(f"\n✅ [9번 봇: B 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [9번 봇: US B 대기 모드]")
    
    while True:
        now_ny = datetime.now(ny_tz)
        
        if now_ny.minute == 43 and (9 <= now_ny.hour <= 15) and now_ny.hour != 14:
            print(f"🚀 [US B 1H 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1h')
            time.sleep(60) 
            
        elif now_ny.hour == 14 and now_ny.minute == 30:
            print(f"🚀 [US B 1D 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1d')
            time.sleep(60)
            
        else:
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
