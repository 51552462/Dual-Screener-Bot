# Dante_US_Bowl_1D_Pro.py
import os, re, time, threading, queue
from datetime import datetime
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import warnings, urllib3
import yfinance as yf
import FinanceDataReader as fdr
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN    = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Bowl_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

def get_us_smart_report(ticker_str: str) -> tuple:
    sector, earnings_trend = "정보 없음", "정보 없음"
    try:
        tk = yf.Ticker(ticker_str)
        info = tk.info
        sector = info.get('sector', '정보 없음')
        growth = info.get('earningsGrowth', 0)
        if growth is None: growth = 0
        if growth > 0.1: earnings_trend = f"실적 성장/턴어라운드 (분기 EPS: +{growth*100:.1f}%)"
        elif growth < -0.1: earnings_trend = f"실적 부진 (분기 EPS: {growth*100:.1f}%)"
        else: earnings_trend = "보합세 (특이사항 없음)"
    except: pass
    return sector, earnings_trend

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
        img_path, caption = item
        if SEND_TELEGRAM:
            for _ in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": caption}, files={"photo": f}, timeout=20, verify=False)
                    if res.status_code == 200: break
                    elif res.status_code == 429: time.sleep(3)
                except: time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()
threading.Thread(target=telegram_sender_daemon, daemon=True).start()

# ⭐️ 신뢰도 스코어링 시스템
def calculate_trust_score(c, e60, signal_arr):
    score = 5 
    lookback = min(100, len(c))
    for i in range(len(c) - lookback, len(c) - 1):
        if signal_arr[i]:
            valid = True
            entry_price = c[i]
            for j in range(i + 1, len(c)):
                if c[j] < e60[j] or c[j] >= entry_price * 1.15:
                    valid = False
                    break
            if valid: score += 2 
    return min(10, score)

def compute_bobgeureut_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    ema10, ema20, ema30, ema60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    ema112, ema224, ema448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    ma20 = df['Close'].rolling(20).mean().values
    stddev = df['Close'].rolling(20).std(ddof=1).values
    bb_upper = ma20 + (stddev * 2)

    tenkan = (pd.Series(h).rolling(9).max() + pd.Series(l).rolling(9).min()) / 2
    kijun = (pd.Series(h).rolling(26).max() + pd.Series(l).rolling(26).min()) / 2
    spanA = (tenkan + kijun) / 2
    spanB = (pd.Series(h).rolling(52).max() + pd.Series(l).rolling(52).min()) / 2
    senkou1 = np.roll(spanA.values, 25); senkou1[:25] = np.nan
    senkou2 = np.roll(spanB.values, 25); senkou2[:25] = np.nan
    cloud_top = np.fmax(senkou1, senkou2)

    volavg20 = pd.Series(v).rolling(20).mean().values
    avgvol3 = pd.Series(v).shift(1).rolling(3).mean().values

    mean120 = pd.Series(c).rolling(120).mean().shift(5).values
    std120 = pd.Series(c).rolling(120).std(ddof=1).shift(5).values
    mean60 = pd.Series(c).rolling(60).mean().shift(5).values
    std60 = pd.Series(c).rolling(60).std(ddof=1).shift(5).values
    
    with np.errstate(divide='ignore', invalid='ignore'):
        isCat2 = (std120 / mean120) < 0.20
        isCat1 = (~isCat2) & ((std60 / mean60) < 0.20)
    hasBox = isCat1 | isCat2

    isBullish = c > o
    prev_c = np.roll(c, 1); prev_c[0] = np.inf
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    
    condEma = (c > ema224) & (prev_c < prev_ema224 * 1.05)
    condCloud = c > cloud_top
    condBb = c >= bb_upper * 0.98
    condVol = v > volavg20 * 2.0
    condNotOverheated = c <= ema224 * 1.15
    
    with np.errstate(invalid='ignore'): condVolSpike = v >= (np.nan_to_num(avgvol3, nan=1.0) * 5)
    
    condMoney = (c * v) >= 1_000_000
    condPrice = c >= 1.0

    signalBase = condPrice & condMoney & isBullish & condEma & condCloud & condBb & condVol & condNotOverheated & hasBox & condVolSpike
    
    if not signalBase[-1]: return False, "", df, {}

    signalCat2 = signalBase & isCat2
    signalCat1 = signalBase & isCat1
    isAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224)

    if (signalCat2 & isAligned)[-1] or (signalCat1 & isAligned)[-1]: sig_type = "💥 B (J 강조)"
    else: sig_type = "🎯 B (일반)"

    trust_score = calculate_trust_score(c, ema60, signalBase)

    return True, sig_type, df, {"last_close": float(c[-1]), "score": trust_score}

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{int(time.time()*1000)}.png")
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            title = f"[{dbg['sig_type']}] US Market: {code} (1D)\nClose: ${dbg['last_close']:.2f}"
            mc = mpf.make_marketcolors(up='green', down='red', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')
            plt.close('all')
            # ⭐️ 모든 선, 구름대 완벽 제거 (캔들+거래량만)
            mpf.plot(df_cut, type="candle", volume=True, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            return path
        except: return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 B(밥그릇) 초고속 스캔 시작!")

    ticker_to_info = {row['Symbol']: {'code': row['Symbol'], 'name': row['Name']} for _, row in stock_list.iterrows()}
    tickers = list(ticker_to_info.keys())
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}

    for i in range(0, len(tickers), 100):
        chunk = tickers[i:i+100]
        df_batch = yf.download(" ".join(chunk), interval="1d", period="3y", group_by="ticker", progress=False, threads=False)
        
        for tk in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(tk)
            if not info: continue
            name, code = info['name'], info['code']

            try:
                if len(chunk) == 1: df_ticker = df_batch.copy()
                else: 
                    if tk not in df_batch.columns.get_level_values(0): continue
                    df_ticker = df_batch[tk].copy()

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                if df_ticker.index.tzinfo is not None: df_ticker.index = df_ticker.index.tz_convert('America/New_York').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_bobgeureut_1d(df_ticker)
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                        if chart_path:
                            sector, earnings_trend = get_us_smart_report(code) 
                            caption = (
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg['last_close']:.2f}\n"
                                f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                                f"📉 [매수/손절 전략]\n"
                                f"- 양봉 길이만큼 분할매수\n"
                                f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                                f"⭐ 알고리즘 신뢰도: {dbg['score']} / 10점\n\n"
                                f"💡 [기업 팩트체크]\n"
                                f"🔸 섹터: {sector}\n"
                                f"🔸 전망: 전문가 분석 요망\n"
                                f"🔸 실적: {earnings_trend}\n"
                            )
                            telegram_queue.put((chart_path, caption))
            except: pass
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    print(f"\n✅ [미국장 B 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [미국장 B 스케줄러 (1D 전용)] 미국시간 09:30 / 11:30 / 14:30 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 9 and now_ny.minute == 30) or \
           (now_ny.hour == 11 and now_ny.minute == 30) or \
           (now_ny.hour == 14 and now_ny.minute == 30):
            print(f"🚀 [B 1D 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
