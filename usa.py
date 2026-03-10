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
TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Bowl_Dual')
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

        # 3. 최신 현지 뉴스 헤드라인 추출
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
        # 미국 3대장 거래소 모두 수집
        df_ndq = fdr.StockListing('NASDAQ')
        df_nyse = fdr.StockListing('NYSE')
        df_amex = fdr.StockListing('AMEX')
        df = pd.concat([df_ndq, df_nyse, df_amex])
        
        # 우선주, 워런트, 유닛 등 이상한 티커(기호 포함) 필터링
        df = df[df['Symbol'].str.isalpha()]
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except Exception as e:
        print(f"티커 수집 에러: {e}")
        return pd.DataFrame()

# ================== 텔레그램 전송 데몬 (마크다운 에러 100% 방지) ==================
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
                        print(f"\n📲 [텔레그램 전송 성공] {img_path}")
                        break
                    elif res.status_code == 429: time.sleep(3)
                    else: 
                        print(f"\n❌ [텔레그램 서버 에러] {res.status_code}: {res.text}")
                        break 
                except Exception as e:
                    print(f"\n❌ [파이썬 통신 에러] {e}")
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

sender_thread = threading.Thread(target=telegram_sender_daemon, daemon=True)
sender_thread.start()

# ================== 🇺🇸 밥그릇 핵심 로직 (트레이딩뷰 100% 동기화) ==================
MIN_PRICE_USD = 1.0               # 페니스탁(1달러 미만) 방지
MIN_MONEY_USD = 1_000_000         # 일 거래대금 최소 100만 달러 (약 13억원)

def compute_bowl_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_signal", df_raw, {}

    df = df_raw.copy()

    # 1. EMA 계산
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    # 2. 볼린저밴드 ⭐️ (ddof=1 적용: 파인스크립트 표본표준편차와 100% 동일)
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['StdDev'] = df['Close'].rolling(window=20).std(ddof=1) 
    df['BB_Upper'] = df['MA20'] + (df['StdDev'] * 2)

    # 3. 일목균형표
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

    # 4. 거래량 및 횡보(박스권) 조건 ⭐️ (ddof=1 적용 완벽 동기화)
    df['VolAvg20'] = df['Volume'].rolling(20).mean()
    df['AvgVol3'] = df['Volume'].shift(1).rolling(3).mean()

    mean120 = df['Close'].rolling(120).mean().shift(5)
    std120 = df['Close'].rolling(120).std(ddof=1).shift(5)
    condBox6m = (std120 / mean120) < 0.20

    mean60 = df['Close'].rolling(60).mean().shift(5)
    std60 = df['Close'].rolling(60).std(ddof=1).shift(5)
    condBox3m = (std60 / mean60) < 0.20

    # ========================================================
    # 🚀 NumPy C-엔진 전환 (연산 속도 극대화)
    # ========================================================
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
    condVolSpike = vol_arr >= (avgvol3_arr * 5)
    
    # 🇺🇸 미국 시장 안전 필터 (달러 거래대금 및 주가)
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

    if cat2_Strong[-1]: sig_type = "💥 Cat2 (정배열 강조) - 6m"
    elif cat1_Strong[-1]: sig_type = "💥 Cat1 (정배열 강조) - 3m"
    elif cat2_Normal[-1]: sig_type = "🎯 Cat2 (일반 돌파) - 6m"
    elif cat1_Normal[-1]: sig_type = "🎯 Cat1 (일반 돌파) - 3m"
    else: sig_type = "밥그릇 돌파"

    dbg = {
        "last_close": float(close_arr[-1]), 
        "ema224": float(ema224[-1]), 
        "vol_spike": float(vol_arr[-1] / max(1, avgvol3_arr[-1])), 
        "sig_type": sig_type
    }
    return True, sig_type, df, dbg

# ================== 차트 저장 (US 글로벌 스타일) ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, timeframe: str) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{timeframe}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            senkou1, senkou2 = df_cut['Senkou1'].values, df_cut['Senkou2'].values

            apds = [
                mpf.make_addplot(df_cut["EMA112"], color='green', width=1),
                mpf.make_addplot(df_cut["EMA224"], color='black', width=2),
                mpf.make_addplot(df_cut["BB_Upper"], color='red', type='scatter', markersize=5),
                mpf.make_addplot(df_cut["Senkou1"], color='aqua', alpha=0.3, width=1),
                mpf.make_addplot(df_cut["Senkou2"], color='aqua', alpha=0.3, width=1),
            ]

            fill_between = dict(y1=senkou1, y2=senkou2, alpha=0.1, color='aqua')
            tf_str = "1H" if timeframe == '1h' else "1D"
            title = f"[{dbg['sig_type']}] US Market: {code} ({tf_str})\nClose: ${dbg['last_close']:.2f}  EMA224: ${dbg['ema224']:.2f}  VolSpike: {dbg['vol_spike']:.1f}x"
            
            # 🇺🇸 미국 차트는 상승이 초록, 하락이 빨강입니다.
            mc = mpf.make_marketcolors(up='green', down='red', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, fill_between=fill_between, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            print(f"\n❌ [차트 그리기 실패] {code}: {e}")
            return None

# ================== 🚀 미국 주식 전용 야후 하이퍼 엔진 ==================
def scan_market(timeframe: str):
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    tf_label = "1시간봉" if timeframe == '1h' else "일봉"
    print(f"\n🇺🇸 [월스트리트 밥그릇 스캔] 총 {len(stock_list)}개 종목 '{tf_label}' 초고속 스캔 시작!")

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

                # 500봉 확보 및 1달러 이상 필터링
                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= 1.0:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_bowl_signal(df_ticker)
                    
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
    print(f"\n✅ [{tf_label} 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 미국 서머타임(DST) 적용 스케줄러 ==================
def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [US 밥그릇 상업용 스케줄러 자동 대기 모드]")
    print("   - [1시간봉] 미국 현지시간(NY) 기준: 정규장 중 매시 35분 실행 (예: 10:35, 11:35...)")
    print("   - [일봉] 미국 현지시간(NY) 장 마감 직후: 16:05 실행")
    print("   (서머타임 여부를 시스템이 자동 계산하여 실행합니다.)\n")
    
    while True:
        # 미국 뉴욕 현지 시간 가져오기 (서머타임 자동 반영)
        now_ny = datetime.now(ny_tz)
        
        # 1. 1시간봉 스캔 (뉴욕장 09:30 ~ 16:00 사이, 매시 35분마다)
        # 예: 10:35, 11:35, 12:35, 13:35, 14:35, 15:35
        if now_ny.minute == 35 and (10 <= now_ny.hour <= 15):
            print(f"🚀 [US 1H 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1h')
            print("💤 1H 스캔 완료. 다음 타임까지 대기합니다...")
            time.sleep(50 * 60) 
            
        # 2. 일봉 스캔 (뉴욕장 마감 직후 16:05 한 번만 실행)
        elif now_ny.hour == 16 and now_ny.minute == 5:
            print(f"🚀 [US 1D 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1d')
            print("💤 1D 스캔 완료. 내일 개장까지 대기합니다...")
            time.sleep(50 * 60)
            
        else:
            time.sleep(10)

if __name__ == "__main__":
    scan_market('1h')
    scan_market('1d')
    run_scheduler()