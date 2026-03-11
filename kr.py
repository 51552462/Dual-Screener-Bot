# Dante_KRX_Bowl_Hybrid_Perfect.py
import os
import re
import time
import threading
import queue
import json
import concurrent.futures
from datetime import datetime, timedelta
import pytz
from io import StringIO
import numpy as np
import pandas as pd
import mplfinance as mpf

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import platform

if platform.system() == 'Windows':
    plt.rcParams['font.family'] = 'Malgun Gothic'
elif platform.system() == 'Darwin': 
    plt.rcParams['font.family'] = 'AppleGothic'
else:
    plt.rcParams['font.family'] = 'NanumGothic'
plt.rcParams['axes.unicode_minus'] = False

import requests
import warnings
import urllib3
import yfinance as yf
from bs4 import BeautifulSoup
import logging

try:
    from tvDatafeed import TvDatafeed, Interval
except ImportError:
    print("❌ tvdatafeed 라이브러리가 없습니다.")
    os._exit(1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('tvDatafeed.main').setLevel(logging.CRITICAL)
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Bobgeureut_Dual_KRX')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)
STATE_PATH = os.path.join(TOP_FOLDER, "state_krx_bobgeureut.json")

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

def clear_old_charts():
    try:
        for f in os.listdir(CHART_FOLDER):
            if f.endswith(".png"): os.remove(os.path.join(CHART_FOLDER, f))
    except: pass

def _load_state():
    if not os.path.exists(STATE_PATH): return {"tickers": {}}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f: return json.load(f)
    except: return {"tickers": {}}

def _save_state(state: dict):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def _update_streak(state: dict, key: str, today_str: str) -> int:
    rec = state["tickers"].get(key, {"streak": 0, "last_date": ""})
    last_date = rec.get("last_date", "")
    streak = int(rec.get("streak", 0))
    today = datetime.strptime(today_str, "%Y-%m-%d")
    yday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    if last_date == yday_str: new_streak = streak + 1
    elif last_date == today_str: new_streak = streak 
    else: new_streak = 1
    rec["streak"] = new_streak
    rec["last_date"] = today_str
    state["tickers"][key] = rec
    return new_streak

def _streak_badge(streak: int) -> str:
    if streak <= 1: return ""
    return "  ⚠️중첩2" if streak == 2 else "  🔥중첩3" if streak == 3 else f"  🚀중첩{min(streak, 10)}"

def get_company_fact_report(code: str) -> tuple:
    sector, outlook, growth = "정보 없음", "정보 없음", "정보 없음"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=3, verify=False)
        if res_naver.status_code == 200:
            tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=3, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if len(tags) >= 1: outlook = tags[0].text.strip()
            if len(tags) >= 2: growth = tags[1].text.strip()
    except: pass
    return sector, outlook, growth

def get_krx_list_kind():
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

TV_POOL_SIZE = 5
tv_pool = queue.Queue()
def initialize_tv_pool():
    for _ in range(TV_POOL_SIZE):
        try: tv_pool.put(TvDatafeed())
        except: pass

def get_tv_1h_ohlcv(ticker: str):
    tv_instance = tv_pool.get()
    df = None
    for attempt in range(3):
        try:
            time.sleep(0.05)
            df = tv_instance.get_hist(symbol=ticker, exchange='KRX', interval=Interval.in_1_hour, n_bars=600)
            if df is not None and not df.empty:
                df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
                df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                break
        except:
            time.sleep(1)
            try: tv_instance = TvDatafeed()
            except: pass
    tv_pool.put(tv_instance)
    return df

def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
        img_path, caption = item
        if len(caption) > 1000: caption = caption[:980] + "\n\n...(생략됨)"
        if SEND_TELEGRAM:
            for _ in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        if requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": caption}, files={"photo": f}, timeout=20, verify=False).status_code == 200: break
                except: time.sleep(1.5)
        telegram_queue.task_done()
threading.Thread(target=telegram_sender_daemon, daemon=True).start()

def compute_bobgeureut(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    ma20 = df['Close'].rolling(20, min_periods=1).mean().values
    stddev = df['Close'].rolling(20, min_periods=1).std(ddof=1).values
    bbUpper = ma20 + (stddev * 2)

    tenkan = (pd.Series(h).rolling(9, min_periods=1).max() + pd.Series(l).rolling(9, min_periods=1).min()) / 2
    kijun = (pd.Series(h).rolling(26, min_periods=1).max() + pd.Series(l).rolling(26, min_periods=1).min()) / 2
    spanA = (tenkan + kijun) / 2
    spanB = (pd.Series(h).rolling(52, min_periods=1).max() + pd.Series(l).rolling(52, min_periods=1).min()) / 2
    senkou1 = np.roll(spanA.values, 25); senkou1[:25] = np.nan
    senkou2 = np.roll(spanB.values, 25); senkou2[:25] = np.nan
    cloudTop = np.fmax(senkou1, senkou2)

    df['Senkou1'], df['Senkou2'], df['CloudTop'], df['BB_Upper'] = senkou1, senkou2, cloudTop, bbUpper

    mean120 = pd.Series(c).rolling(120, min_periods=1).mean().values
    std120 = pd.Series(c).rolling(120, min_periods=1).std(ddof=1).values
    mean120_shifted = np.roll(mean120, 5); mean120_shifted[:5] = np.nan
    std120_shifted = np.roll(std120, 5); std120_shifted[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox6m = (std120_shifted / mean120_shifted) < 0.20

    mean60 = pd.Series(c).rolling(60, min_periods=1).mean().values
    std60 = pd.Series(c).rolling(60, min_periods=1).std(ddof=1).values
    mean60_shifted = np.roll(mean60, 5); mean60_shifted[:5] = np.nan
    std60_shifted = np.roll(std60, 5); std60_shifted[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox3m = (std60_shifted / mean60_shifted) < 0.20

    isCat2 = condBox6m
    isCat1 = (~condBox6m) & condBox3m
    hasBox = isCat1 | isCat2

    ema10, ema20, ema30, ema60, ema112, ema224 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values, df['EMA112'].values, df['EMA224'].values
    condPrice = c >= 1000
    isBullish = c > o
    prev_c = np.roll(c, 1); prev_c[0] = np.inf
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    condEma = (c > ema224) & (prev_c < prev_ema224 * 1.05)
    condCloud = (c > cloudTop) & (~np.isnan(cloudTop))
    condBb = c >= bbUpper * 0.98
    volAvg = pd.Series(v).rolling(20, min_periods=1).mean().values
    condVol = v > volAvg * 2.0
    condNotOverheated = c <= ema224 * 1.15
    
    v_1 = np.roll(v, 1); v_1[0] = 0
    v_2 = np.roll(v, 2); v_2[:2] = 0
    v_3 = np.roll(v, 3); v_3[:3] = 0
    avgVol3 = (v_1 + v_2 + v_3) / 3
    condVolSpike = v >= (avgVol3 * 5)

    signalBase = condPrice & isBullish & condEma & condCloud & condBb & condVol & condNotOverheated & hasBox & condVolSpike
    if not signalBase[-1]: return False, "", df, {}

    signalCat2 = signalBase & isCat2
    signalCat1 = signalBase & isCat1
    isAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224)

    if (signalCat2 & isAligned)[-1]: sig_type = "💥Cat2(정배열) - 120봉 횡보"
    elif (signalCat1 & isAligned)[-1]: sig_type = "💥Cat1(정배열) - 60봉 횡보"
    elif (signalCat2 & (~isAligned))[-1]: sig_type = "Cat2(Buy V5) - 120봉 횡보"
    elif (signalCat1 & (~isAligned))[-1]: sig_type = "Cat1(Buy V5) - 60봉 횡보"
    else: sig_type = "밥그릇 돌파"

    return True, sig_type, df, {"close": float(c[-1]), "ema224": float(ema224[-1]), "vol_spike": float(v[-1]/max(1, avgVol3[-1]))}

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, title_text: str) -> str:
    with chart_lock:
        try:
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{int(time.time()*1000)}.png")
            dfc = df.iloc[-DISPLAY_BARS:].copy()
            apds = [
                mpf.make_addplot(dfc["EMA112"], color='green', width=1),
                mpf.make_addplot(dfc["EMA224"], color='black', width=2),
                mpf.make_addplot(dfc["BB_Upper"], color='red', type='scatter', markersize=5),
                mpf.make_addplot(dfc["Senkou1"], color='aqua', alpha=0.3, width=1),
                mpf.make_addplot(dfc["Senkou2"], color='aqua', alpha=0.3, width=1),
            ]
            fill_between = dict(y1=dfc['Senkou1'].values, y2=dfc['Senkou2'].values, alpha=0.1, color='aqua')
            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':', rc={'font.family': plt.rcParams['font.family']})
            plt.close('all')
            mpf.plot(dfc, type="candle", volume=True, addplot=apds, fill_between=fill_between, title=title_text, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            return path
        except: return None

def scan_krx_1h():
    clear_old_charts()
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    state = _load_state()
    today_str = datetime.now(pytz.timezone('Asia/Seoul')).strftime("%Y-%m-%d")
    console_lock = threading.Lock()

    def worker(row):
        code, name = row["Code"], row["Name"]
        df = get_tv_1h_ohlcv(code)
        if df is not None and len(df) >= 500 and df['Close'].iloc[-1] >= 1000:
            hit, sig_type, df_res, dbg = compute_bobgeureut(df)
            with console_lock:
                tracker['analyzed'] += 1
                if hit:
                    tracker['hits'] += 1
                    badge = _streak_badge(_update_streak(state, f"{code}:1H", today_str))
                    title = f"[{sig_type}] {name} (1H)\nClose:{dbg['close']:.0f}  EMA224:{dbg['ema224']:.0f}  VolSpike:{dbg['vol_spike']:.1f}x"
                    path = save_chart(df_res, code, name, tracker['hits'], title)
                    if path:
                        sec, out, grow = get_company_fact_report(code)
                        msg = f"🔥 [{sig_type}] (1H)\n\n[{name}] ({code}) {badge}\n- 현재가: {dbg['close']:,.0f}원\n- 224일선: {dbg['ema224']:,.0f}원\n- 거래량: {dbg['vol_spike']:.1f}배\n\n💡 [팩트 체크]\n🔸 섹터: {sec}\n🔸 전망: {out}\n🔸 실적: {grow}\n\nTime: {datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')}"
                        telegram_queue.put((path, msg))
        with console_lock:
            tracker['scanned'] += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=TV_POOL_SIZE) as executor:
        executor.map(worker, [row for _, row in stock_list.iterrows()])
    _save_state(state)
    print(f"\n✅ [1번 봇 1H 완료] 정상분석: {tracker['analyzed']} | 포착: {tracker['hits']} | 시간: {(time.time() - t0)/60:.1f}분")

def scan_krx_1d():
    clear_old_charts()
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    state = _load_state()
    today_str = datetime.now(pytz.timezone('Asia/Seoul')).strftime("%Y-%m-%d")
    ticker_to_info = {f"{row['Code']}.KS" if row['Market'] == 'KOSPI' else f"{row['Code']}.KQ": {'code': row['Code'], 'name': row['Name']} for _, row in stock_list.iterrows()}
    tickers = list(ticker_to_info.keys())

    for i in range(0, len(tickers), 100):
        chunk = tickers[i:i+100]
        df_batch = yf.download(" ".join(chunk), interval="1d", period="3y", group_by="ticker", progress=False, threads=True)
        for tk in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info[tk]
            try:
                if len(chunk) == 1: df = df_batch.copy()
                else:
                    if tk not in df_batch.columns.get_level_values(0): continue 
                    df = df_batch[tk].copy()
                df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                if df.index.tzinfo is not None: df.index = df.index.tz_convert('Asia/Seoul').tz_localize(None)
                df = df[~df.index.duplicated(keep='last')]
                if len(df) >= 500 and df['Close'].iloc[-1] >= 1000:
                    tracker['analyzed'] += 1
                    hit, sig_type, df_res, dbg = compute_bobgeureut(df)
                    if hit:
                        tracker['hits'] += 1
                        badge = _streak_badge(_update_streak(state, f"{info['code']}:1d", today_str))
                        title = f"[{sig_type}] {info['name']} (1D)\nClose:{dbg['close']:.0f}  EMA224:{dbg['ema224']:.0f}  VolSpike:{dbg['vol_spike']:.1f}x"
                        path = save_chart(df_res, info['code'], info['name'], tracker['hits'], title)
                        if path:
                            sec, out, grow = get_company_fact_report(info['code'])
                            msg = f"💎 [{sig_type}] (1D)\n\n[{info['name']}] ({info['code']}) {badge}\n- 현재가: {dbg['close']:,.0f}원\n- 224일선: {dbg['ema224']:,.0f}원\n- 거래량: {dbg['vol_spike']:.1f}배\n\n💡 [팩트 체크]\n🔸 섹터: {sec}\n🔸 전망: {out}\n🔸 실적: {grow}\n\nTime: {datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')}"
                            telegram_queue.put((path, msg))
            except: pass
    _save_state(state)
    print(f"\n✅ [1번 봇 1D 완료] 정상분석: {tracker['analyzed']} | 포착: {tracker['hits']} | 시간: {(time.time() - t0)/60:.1f}분")

# ================== ⏰ [1번 봇 스케줄러] 정각 / 15:30 ==================
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [1번 봇: 한국장 밥그릇 대기 모드 - 분산 완료]")
    print("   - [1H 스캔] 매시 00분 (정각)")
    print("   - [1D 스캔] 매일 15:30")
    
    while True:
        now_kr = datetime.now(kr_tz)
        
        if now_kr.minute == 31:
            print(f"🚀 [1번 봇 1H 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_krx_1h()
            time.sleep(50 * 60) 
            
        elif now_kr.hour == 15 and now_kr.minute == 00:
            print(f"🚀 [1번 봇 1D 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_krx_1d()
            time.sleep(50 * 60)
            
        else: 
            time.sleep(10)

if __name__ == "__main__":
    initialize_tv_pool()
    run_scheduler()
