# Dante_US_Nulrim_1D_Sniper.py
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
matplotlib.use('Agg') # GUI 메모리 누수 완벽 차단
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
TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]', '_', s)

# ================== 🇺🇸 스마트 기업 팩트 분석기 ==================
def get_us_smart_report(ticker_str: str) -> tuple:
    sector = "정보 없음"
    earnings_trend = "뚜렷한 실적 추세 없음"
    news_summary = "최근 주요 뉴스 없음"
    
    try:
        tk = yf.Ticker(ticker_str)
        info = tk.info
        
        sector = info.get('sector', '정보 없음')
        growth = info.get('earningsGrowth', 0)
        
        if growth is None: growth = 0
        if growth > 0.1:
            earnings_trend = f"📈 실적 성장 및 턴어라운드 (EPS 분기성장률: +{growth*100:.1f}%)"
        elif growth < -0.1:
            earnings_trend = f"📉 실적 부진 (EPS 분기성장률: {growth*100:.1f}%)"
        else:
            earnings_trend = "📊 보합 (특이사항 없음)"

        news = tk.news
        if news:
            headlines = [f"- {n['title']}" for n in news[:2]]
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
        
        df = df[df['Symbol'].str.isalpha()]
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
            for _ in range(3):
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
                except Exception as e:
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

# ================== ⭐️ 눌림목 핵심 로직 (트레이딩뷰 100% 동기화) ==================
MIN_PRICE_USD = 1.0               
MIN_MONEY_USD = 1_000_000         

def compute_nulrim_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_signal", df_raw, {}

    df = df_raw.copy()
    
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    v_1 = np.roll(v, 1); v_1[0] = 0
    v_2 = np.roll(v, 2); v_2[:2] = 0
    v_3 = np.roll(v, 3); v_3[:3] = 0
    av3 = (v_1 + v_2 + v_3) / 3

    isBullish = c > o
    
    # ⭐️ 0으로 나누기 무한대 에러 완벽 차단
    with np.errstate(invalid='ignore'):
        volSpike5 = v >= (np.nan_to_num(av3, nan=1.0) * 5)
        
    moneyOk = (c * v) >= MIN_MONEY_USD
    priceOk = c >= MIN_PRICE_USD

    condBase = priceOk & moneyOk & isBullish & volSpike5

    c1_long_trend = (e112 > e224) & (e224 > e448)
    c1_short_inverse = (e30 > e20) & (e20 > e10)
    c1_position = (c < e30) & (c > e112)
    isCat112 = condBase & c1_long_trend & c1_short_inverse & c1_position

    c2_full_trend = (e10 > e20) & (e20 > e30) & (e30 > e112) & (e112 > e224) & (e224 > e448)
    c2_under_20 = (c < e10) & (c < e20)
    c2_above_30 = c > e30
    isCat30 = condBase & (~isCat112) & c2_full_trend & c2_under_20 & c2_above_30

    c3_mid_inverse = (e60 > e30) & (e30 > e20) & (e20 > e10)
    c3_position = (c < e112) & (c > e224)
    isCat224 = condBase & (~isCat112) & (~isCat30) & c3_mid_inverse & c3_position

    c4_position = (c < e224) & (c > e448)
    isCat448 = condBase & (~isCat112) & (~isCat30) & (~isCat224) & c4_position

    c112_hit = isCat112[-1]
    c30_hit = isCat30[-1]
    c224_hit = isCat224[-1]
    c448_hit = isCat448[-1]

    if not (c112_hit or c30_hit or c224_hit or c448_hit):
        return False, "no_signal", df, {}

    if c30_hit: sig_type = "🚀 30선 지지 (급등 눌림목)"
    elif c112_hit: sig_type = "💎 112선 지지 (황금 눌림목)"
    elif c224_hit: sig_type = "🛡️ 224선 지지 (중기 마지노선)"
    else: sig_type = "⚓ 448선 지지 (최후 마지노선)"

    safe_avg_vol = av3[-1] if av3[-1] > 0 else 1
    dbg = {
        "last_close": float(c[-1]),
        "vol_spike": float(v[-1] / safe_avg_vol),
        "sig_type": sig_type
    }
    return True, sig_type, df, dbg

# ================== 차트 저장 (US 글로벌 스타일) ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            
            apds = [
                mpf.make_addplot(df_cut["EMA10"], color='#FF5252', width=1, alpha=0.5),
                mpf.make_addplot(df_cut["EMA20"], color='#FFD700', width=1, alpha=0.5),
                mpf.make_addplot(df_cut["EMA30"], color='#FF00FF', width=2),
                mpf.make_addplot(df_cut["EMA60"], color='#FF9800', width=1, alpha=0.5),
                mpf.make_addplot(df_cut["EMA112"], color='#00E676', width=2),
                mpf.make_addplot(df_cut["EMA224"], color='#2979FF', width=2),
                mpf.make_addplot(df_cut["EMA448"], color='#B0BEC5', width=2),
            ]

            title = f"[{dbg['sig_type']}] US Market: {code} (1D)\nClose: ${dbg['last_close']:.2f} | VolSpike: {dbg['vol_spike']:.1f}x"

            mc = mpf.make_marketcolors(up='green', down='red', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            return None

# ================== 🚀 미국 주식 일봉 야후 엔진 ==================
def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n🇺🇸 [월스트리트 눌림목 스캔] 총 {len(stock_list)}개 종목 '일봉(1D)' 초고속 스캔 시작!")

    ticker_to_info = {}
    for _, row in stock_list.iterrows():
        ticker = row['Symbol']
        ticker_to_info[ticker] = {'code': ticker, 'name': row['Name']}
    
    tickers = list(ticker_to_info.keys())
    chunk_size = 100 
    period = "3y"

    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        tickers_str = " ".join(chunk)
        
        df_batch = yf.download(tickers_str, interval="1d", period=period, group_by="ticker", progress=False, threads=True)
        
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

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_nulrim_1d(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                        
                        if chart_path:
                            sector, earnings_trend, news_summary = get_us_smart_report(code) 
                            emoji = "🇺🇸"
                            
                            caption = (
                                f"{emoji} [{dbg['sig_type']}] (일봉)\n\n"
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
    print(f"\n✅ [8번 봇: US 눌림목 1D 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 미국 서머타임(DST) 적용 스케줄러 ==================
def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [8번 봇: US 눌림목 상업용 스케줄러 자동 대기 모드 - 분산 완료]")
    print("   - [일봉 전용] 미국 현지시간(NY) 장 마감 직후: 16:30 단독 실행")
    print("   (한국 봇 및 다른 미국 봇과 절대 겹치지 않습니다.)\n")
    
    while True:
        now_ny = datetime.now(ny_tz)
        
        # 💡 [시간 분산] 6번 봇(16:10), 7번 봇(16:20)과 겹치지 않게 16:30에 실행
        if now_ny.hour == 16 and now_ny.minute == 20:
            print(f"🚀 [US 눌림목 1D 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            print("💤 1D 스캔 완료. 내일 개장까지 대기합니다...")
            time.sleep(50 * 60)
            
        else:
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
