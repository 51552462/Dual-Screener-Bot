# Dante_US_Nulrim_1D_Sniper_V2_NewLogic.py
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

# ================== ⭐️ 신규 눌림목 로직 (트레이딩뷰 100% 동기화) ==================
MIN_PRICE_USD = 1.0               
MIN_MONEY_USD = 1_000_000         

def compute_nulrim_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_signal", df_raw, {}

    df = df_raw.copy()
    
    # 1. EMA 설정
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    df['Lowest5'] = df['Low'].rolling(5).min()

    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    av3 = df['AvgVol3'].values
    lowest5 = df['Lowest5'].values
    
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # ⭐️ 스캐너용 기본 안전 필터
    moneyOk = (c * v) >= MIN_MONEY_USD
    priceOk = c >= MIN_PRICE_USD
    with np.errstate(invalid='ignore'):
        volSpike = v >= (np.nan_to_num(av3, nan=1.0) * 3)
    isBullish = c > o

    # 3. 배열 상태 정의
    align112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    align224 = align112 & (e112 > e224)
    align448 = align224 & (e224 > e448)

    # 4. 장기 기준선 유지 상태 정의
    longKeep448 = e224 > e448 
    longKeep224 = e112 > e224 
    longKeep112 = e60 > e112  

    prev_align448 = np.roll(align448, 1); prev_align448[0] = False
    prev_align224 = np.roll(align224, 1); prev_align224[0] = False
    prev_align112 = np.roll(align112, 1); prev_align112[0] = False
    
    prev_longKeep448 = np.roll(longKeep448, 1); prev_longKeep448[0] = False
    prev_longKeep224 = np.roll(longKeep224, 1); prev_longKeep224[0] = False
    prev_longKeep112 = np.roll(longKeep112, 1); prev_longKeep112[0] = False

    # 5. 기본 시그널 (S1, S2, S3)
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    s2 = align224 & (~prev_align224) & prev_longKeep224 & (e224 < e448) & isBullish
    s3 = align112 & (~prev_align112) & prev_longKeep112 & (e112 < e224) & isBullish

    # 6. 정밀 필터링 돌파 시그널 (S4, S5) 조건 검사
    prev_c = np.roll(c, 1); prev_c[0] = 0
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    dipped20 = lowest5 < e20
    raw_s5 = align448 & (~prev_align448) & dipped20 & (c > e10) & isBullish & (~s1)

    # ⭐️ 쿨타임 (5봉) 적용 시뮬레이터
    s4 = np.zeros_like(c, dtype=bool)
    s5 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100

    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i
        if raw_s5[i] and not s4[i] and (i - last_pullback_bar > 5):
            s5[i] = True
            last_pullback_bar = i

    # 7. 최종 타점 판별 (스캐너 필터와 결합)
    cond_base = moneyOk & priceOk & volSpike
    
    hit1 = s1[-1] and cond_base[-1]
    hit2 = s2[-1] and cond_base[-1]
    hit3 = s3[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1]
    hit5 = s5[-1] and cond_base[-1]

    if not (hit1 or hit2 or hit3 or hit4 or hit5):
        return False, "no_signal", df, {}

    if hit5: sig_type = "S5 (지연 돌파 확정)"
    elif hit4: sig_type = "S4 (정배열 눌림 돌파)"
    elif hit1: sig_type = "S1 (448 재정렬 양봉)"
    elif hit2: sig_type = "S2 (224 재정렬 양봉)"
    elif hit3: sig_type = "S3 (112 재정렬 양봉)"
    else: sig_type = "새로운 눌림"

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
                mpf.make_addplot(df_cut["EMA10"], color='red', width=1),
                mpf.make_addplot(df_cut["EMA20"], color='orange', width=1),
                mpf.make_addplot(df_cut["EMA30"], color='yellow', width=1),
                mpf.make_addplot(df_cut["EMA60"], color='green', width=1),
                mpf.make_addplot(df_cut["EMA112"], color='blue', width=1),
                mpf.make_addplot(df_cut["EMA224"], color='navy', width=2),
                mpf.make_addplot(df_cut["EMA448"], color='purple', width=2),
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
    print(f"\n🇺🇸 [월스트리트 신규 눌림목 스캔] 총 {len(stock_list)}개 종목 '일봉(1D)' 초고속 스캔 시작!")

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
        
        # ⭐️ 핵심 패치: 야후 멀티스레딩 버그 원천 차단을 위해 threads=False 적용
        df_batch = yf.download(tickers_str, interval="1d", period=period, group_by="ticker", progress=False, threads=False)
        
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

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_nulrim_1d(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                        
                        if chart_path:
                            sector, earnings_trend, news_summary = get_us_smart_report(code) 
                            
                            # 로직별 맞춤형 코멘트
                            msg = ""
                            if "S1" in sig_type: msg = "S1: 224, 448선 정배열 유지 중, 꼬였던 단기 이평선이 다시 448선까지 완벽한 정배열을 이루는 양봉이 떴습니다!"
                            elif "S2" in sig_type: msg = "S2: 112, 224선 정배열 유지 중, 꼬였던 단기 이평선이 다시 224선까지 정배열을 이루는 양봉이 떴습니다!"
                            elif "S3" in sig_type: msg = "S3: 60, 112선 정배열 유지 중, 꼬였던 단기 이평선이 다시 112선까지 정배열을 이루는 양봉이 떴습니다!"
                            elif "S4" in sig_type: msg = "S4: 완전 정배열 상태에서 20일선 눌림 후 10일선 위로 강하게 돌파하는 양봉이 떴습니다!"
                            elif "S5" in sig_type: msg = "S5: 최근 눌림 이후, 이평선이 완전 정배열로 딱 맞춰지며 상승을 확정 짓는 타점이 떴습니다!"

                            emoji = "🇺🇸"
                            caption = (
                                f"{emoji} 🔥 [{dbg['sig_type']}] (일봉)\n\n"
                                f"[{code}] {name}\n"
                                f"- 현재가: ${dbg['last_close']:.2f}\n"
                                f"- 거래량: 3일 평균 대비 {dbg['vol_spike']:.1f}배\n\n"
                                f"📢 [알고리즘 브리핑]\n"
                                f"{msg}\n\n"
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
    print(f"\n✅ [8번 봇: US 신규 눌림목 1D 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 미국 서머타임(DST) 적용 스케줄러 ==================
def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [8번 봇: US 신규 눌림목 상업용 스케줄러 대기 모드 - 분산 완료]")
    print("   - [일봉 전용] 미국 현지시간(NY) 장 마감 직후: 16:30 단독 실행")
    
    while True:
        now_ny = datetime.now(ny_tz)
        if now_ny.hour == 14 and now_ny.minute == 0:
            print(f"🚀 [US 신규 눌림목 1D 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            print("💤 1D 스캔 완료. 내일 개장까지 대기합니다...")
            time.sleep(50 * 60)
        else:
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()

