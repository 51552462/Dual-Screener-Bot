# Dante_US_Dual_Reverse_Hyper.py
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
TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Dual_Reverse')
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
            earnings_trend = f"📈 실적 성장/턴어라운드 (분기 EPS: +{growth*100:.1f}%)"
        elif growth < -0.1:
            earnings_trend = f"📉 실적 부진 (분기 EPS: {growth*100:.1f}%)"
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
        
        df = df[df['Symbol'].str.isalpha()] # 워런트, 유닛 제외
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
                    if res.status_code == 200: 
                        print(f"\n📲 [텔레그램 전송 성공] {img_path}")
                        break
                    elif res.status_code == 429: time.sleep(3)
                    else: break 
                except Exception as e:
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

# ================== 지표 계산 공통 함수 ==================
def add_emas(df: pd.DataFrame) -> pd.DataFrame:
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    return df

# ================== ⭐️ [로직 1] 역매공파 1시간봉 (PineScript 100% 동기화) ==================
def compute_inverse_1h(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "no_signal", df_raw, {}
    df = add_emas(df_raw)
    
    c = df['Close'].values
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    v = df['Volume'].values
    
    ema112 = df['EMA112'].values
    ema224 = df['EMA224'].values
    ema448 = df['EMA448'].values

    # 1. 파라미터 셋업 (미국 달러 기준)
    minPrice = 3.0
    minValMA = 5_000_000
    spikeMult = 1.6
    closeTopFrac = 0.68
    
    # 2. 거래대금 (cValue) 및 20MA
    cValue = c * v
    valMa20 = pd.Series(cValue).rolling(20, min_periods=1).mean().values
    
    # 3. 직전 3봉 평균 거래량 (NumPy 속도 최적화)
    v_1 = np.roll(v, 1); v_1[0] = 0
    v_2 = np.roll(v, 2); v_2[:2] = 0
    v_3 = np.roll(v, 3); v_3[:3] = 0
    avgVol3 = (v_1 + v_2 + v_3) / 3

    # 4. 필수 필터 
    condPrice = c >= minPrice
    condLiquidity = valMa20 >= minValMA
    condBearAlign = (ema112 < ema224) & (ema224 < ema448)
    condHold112 = c > ema112

    # 5. EMA112 이탈 확인 (직전 8봉)
    condCrossEvent = np.zeros(len(c), dtype=bool)
    for i in range(1, 9):
        shifted_c = np.roll(c, i)
        shifted_c[:i] = np.inf # 과거 데이터 없는 인덱스 오류 방지
        shifted_ema112 = np.roll(ema112, i)
        condCrossEvent |= (shifted_c < shifted_ema112)

    # 6. 매집봉 판별 및 구간(20봉) 탐색
    isAccBull = c > o
    rng = h - l
    with np.errstate(divide='ignore', invalid='ignore'):
        closePos = np.where(rng > 0, (c - l) / rng, 0)
    isAccCandle = isAccBull & (cValue >= (spikeMult * valMa20)) & (closePos >= closeTopFrac)
    condHasAcc = pd.Series(isAccCandle).rolling(window=20, min_periods=1).sum().values > 0

    # 7. 거래량 3배 폭발 & 양봉
    condVolSpike = v >= (avgVol3 * 3)
    isCurrentBullish = c > o

    # 8. 최종 타점
    signalBase = condPrice & condLiquidity & condBearAlign & condHold112 & condCrossEvent & condHasAcc & condVolSpike & isCurrentBullish

    # ⭐️ 9. 연산 최적화: 타점이 오늘 안 떴으면 카운터 루프 돌릴 필요 없이 즉시 종료
    if not signalBase[-1]: 
        return False, "no_signal", df, {}

    # 정배열 -> 역배열 사이클 추적 (연속 타점 구분)
    condBullAlign = (ema112 > ema224) & (ema224 > ema448)
    
    signal_counts = np.zeros(len(c), dtype=int)
    count = 0
    for i in range(len(c)):
        if condBullAlign[i]:
            count = 0
        if signalBase[i]:
            count += 1
        signal_counts[i] = count

    isSubsequentSignal = signalBase[-1] and (signal_counts[-1] > 1)
    sig_type = "💥연속 역매공파" if isSubsequentSignal else "🎯첫 역매공파"

    return True, sig_type, df, {"close": c[-1], "vol_spike": v[-1]/max(1, avgVol3[-1])}

# ================== [로직 2] 정배열 일봉 ==================
def compute_aligned_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "no_signal", df_raw, {}
    df = add_emas(df_raw)
    
    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    
    # 미국 최소 주가(3달러) 필터
    if c[-1] < 3.0: return False, "no_signal", df, {}

    # NumPy 직전 3봉 평균 (Pandas 병목 제거)
    v_1 = np.roll(v, 1); v_1[0] = 0
    v_2 = np.roll(v, 2); v_2[:2] = 0
    v_3 = np.roll(v, 3); v_3[:3] = 0
    av3 = (v_1 + v_2 + v_3) / 3

    e10, e20, e30, e60, e112, e224, e448 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values
    
    condBase = (c > o) & (v >= av3 * 5)
    
    a112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    a224 = a112 & (e112 > e224)
    a448 = a224 & (e224 > e448)
    
    prev_a448 = np.roll(a448, 1); prev_a448[0] = False
    
    s3 = condBase & a448 & (~prev_a448)
    s2 = condBase & a224 & (~s3)
    s1 = condBase & a112 & (~a224)
    
    if not (s1[-1] or s2[-1] or s3[-1]): return False, "no_signal", df, {}
    sig_type = "S3 (448 완전 정배열 완성)" if s3[-1] else "S2 (224 정배열 상태)" if s2[-1] else "S1 (112 정배열 상태)"
    
    return True, sig_type, df, {"close": float(c[-1]), "vol_spike": float(v[-1]/max(1, av3[-1]))}

# ================== 차트 저장 ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, timeframe: str) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{timeframe}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            
            # 1D(정배열)와 1H(역매공파) 차트 선 두께/색상 분리
            if timeframe == '1d':
                apds = [
                    mpf.make_addplot(df_cut["EMA10"], color='red', width=1),
                    mpf.make_addplot(df_cut["EMA20"], color='orange', width=1),
                    mpf.make_addplot(df_cut["EMA30"], color='yellow', width=1),
                    mpf.make_addplot(df_cut["EMA60"], color='green', width=1),
                    mpf.make_addplot(df_cut["EMA112"], color='blue', width=1),
                    mpf.make_addplot(df_cut["EMA224"], color='navy', width=2),
                    mpf.make_addplot(df_cut["EMA448"], color='purple', width=2),
                ]
            else:
                apds = [
                    mpf.make_addplot(df_cut["EMA112"], color='blue', width=1),
                    mpf.make_addplot(df_cut["EMA224"], color='navy', width=1),
                    mpf.make_addplot(df_cut["EMA448"], color='purple', width=2),
                ]

            tf_str = "1H(역매공파)" if timeframe == '1h' else "1D(정배열)"
            title = f"[{dbg['sig_type']}] US Market: {code} ({tf_str})\nClose: ${dbg['last_close']:.2f} | 거래량 {dbg['vol_spike']:.1f}배"
            
            # 🇺🇸 미국 차트는 상승이 초록, 하락이 빨강입니다.
            mc = mpf.make_marketcolors(up='green', down='red', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            print(f"\n❌ [차트 그리기 실패] 종목: {code} | 사유: {e}")
            return None

# ================== 🚀 야후 API 그룹 다운로드 엔진 ==================
def scan_market(timeframe: str):
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    tf_label = "1시간봉" if timeframe == '1h' else "일봉"
    logic_name = "역매공파(1시간봉)" if timeframe == '1h' else "정배열(일봉)"
    
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
        
        # 야후 파이낸스 고속 다운로드 (미국 주식에 최적화됨)
        df_batch = yf.download(tickers_str, interval=timeframe, period=period, group_by="ticker", progress=False, threads=True)
        
        for ticker in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(ticker, {})
            if not info: continue
            name, code = info.get('name', ''), info.get('code', '')

            try:
                # 데이터 분리 오류 완벽 방어
                if len(chunk) == 1:
                    df_ticker = df_batch.copy()
                else:
                    if ticker not in df_batch.columns.get_level_values(0): continue
                    df_ticker = df_batch[ticker].copy()

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('America/New_York').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                # 500봉 확보 및 미국 최저 주가(3달러) 필터
                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= 3.0:
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
    print(f"\n✅ [{logic_name} 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 미국 서머타임(DST) 적용 스케줄러 ==================
def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [US 듀얼 하이브리드 상업용 스케줄러 자동 대기 모드]")
    print("   - [역매공파/1H] 미국 현지시간(NY) 기준: 정규장 중 매시 35분 실행 (예: 10:35, 11:35...)")
    print("   - [정배열/1D] 미국 현지시간(NY) 장 마감 직후: 16:05 1회 실행")
    print("   (서머타임 여부를 시스템이 자동 계산하여 실행합니다.)\n")
    
    while True:
        now_ny = datetime.now(ny_tz)
        
        if now_ny.minute == 35 and (10 <= now_ny.hour <= 15):
            print(f"🚀 [US 역매공파/1H 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1h')
            print("💤 1H 스캔 완료. 다음 타임까지 대기합니다...")
            time.sleep(50 * 60) 
            
        elif now_ny.hour == 16 and now_ny.minute == 5:
            print(f"🚀 [US 정배열/1D 정규 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1d')
            print("💤 1D 스캔 완료. 내일 개장까지 대기합니다...")
            time.sleep(50 * 60)
            
        else:
            time.sleep(10)

if __name__ == "__main__":
    print("==================================================")
    print("💡 [강제 1회 스캔] 1H(역매공파) -> 1D(정배열) 순차 테스트")
    scan_market('1h')
    scan_market('1d')
    print("==================================================")
    
    run_scheduler()