# Dante_Reverse_Breakout_Hyper_Batch_Fixed_Final.py
import os
import re
import time
import threading
import queue
from datetime import datetime
from io import StringIO
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
import logging
from bs4 import BeautifulSoup
import traceback

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
# 💡 [수정완료] 서버 환경에 맞춰 현재 폴더(./charts)에 바로 저장하도록 변경
CHART_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ================== 기업 팩트 리포트 자동 추출 ==================
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

# ================== KRX 종목 리스트 고속 수집 ==================
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

# ================== 텔레그램 전송 데몬 ==================
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
                    print(f"\n⚠️ [파이썬 통신 에러] {e}")
                    time.sleep(2)
            time.sleep(1.5)
            
        # 💡 [수정완료] 텔레그램 전송 완료(또는 실패) 직후 해당 차트 즉시 삭제! (용량 0 유지)
        try:
            if os.path.exists(img_path):
                os.remove(img_path)
                print(f"🗑️ [용량 확보] 전송 완료된 차트 삭제: {img_path}")
        except:
            pass
      
        telegram_queue.task_done()

sender_thread = threading.Thread(target=telegram_sender_daemon, daemon=True)
sender_thread.start()

# ================== 파라미터 셋업 ==================
EMA112_LEN = 112
EMA224_LEN = 224
EMA448_LEN = 448

MIN_PRICE = 1000                 
MIN_AVG_VALUE_20D = 500_000_000  

VALUE_SPIKE_MULT = 1.6           
CLOSE_TOP_FRAC = 0.68            
ACC_LOOKBACK = 20           
PRE_LOOKBACK = 8            

# ================== ⭐️ 기준 타점 100% 동기화 (파인스크립트 대입 완료) ==================
def compute_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_data", df_raw, {}

    df = df_raw.copy()

    # 1. 파인스크립트(ta.ema) 완벽 호환 수식
    for n in [112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    # NumPy C-엔진 배열 변환 (연산 속도 극대화)
    close_arr = df['Close'].values
    open_arr = df['Open'].values
    high_arr = df['High'].values
    low_arr = df['Low'].values
    vol_arr = df['Volume'].values
    
    ema112 = df['EMA112'].values
    ema224 = df['EMA224'].values
    ema448 = df['EMA448'].values

    # 파인스크립트: cValue = close * volume
    cValue = close_arr * vol_arr
    # 파인스크립트: valMa20 = ta.sma(cValue, 20)
    valMa20 = pd.Series(cValue).rolling(20, min_periods=1).mean().values

    # ⭐️ 파인스크립트: avgVol3 = (volume[1] + volume[2] + volume[3]) / 3 완벽 대입
    v_1 = np.roll(vol_arr, 1); v_1[0] = 0
    v_2 = np.roll(vol_arr, 2); v_2[:2] = 0
    v_3 = np.roll(vol_arr, 3); v_3[:3] = 0
    avgVol3 = (v_1 + v_2 + v_3) / 3

    # 필터 검증
    condPrice = close_arr >= MIN_PRICE
    condLiquidity = valMa20 >= MIN_AVG_VALUE_20D

    # 배열 검증
    condBearAlign = (ema112 < ema224) & (ema224 < ema448)
    condBullAlign = (ema112 > ema224) & (ema224 > ema448)
    condHold112 = close_arr > ema112

    # ⭐️ 파인스크립트: for i = 1 to preLookback (직전 8봉 검증) 완벽 대입
    condCrossEvent = np.zeros(len(close_arr), dtype=bool)
    for i in range(1, PRE_LOOKBACK + 1):
        shifted_close = np.roll(close_arr, i)
        shifted_ema112 = np.roll(ema112, i)
        shifted_close[:i] = np.inf # 과거 데이터 부족구간 방어
        condCrossEvent |= (shifted_close < shifted_ema112)

    # ⭐️ 파인스크립트: 매집봉 (isAccCandle) 완벽 대입
    isAccBull = close_arr > open_arr
    rng = high_arr - low_arr
    closePos = np.where(rng > 0, (close_arr - low_arr) / rng, 0)
    isAccCandle = isAccBull & (cValue >= (VALUE_SPIKE_MULT * valMa20)) & (closePos >= CLOSE_TOP_FRAC)
    
    # ⭐️ 파인스크립트: math.sum(isAccCandle ? 1 : 0, accLookback) > 0 완벽 대입
    condHasAcc = pd.Series(isAccCandle).rolling(window=ACC_LOOKBACK, min_periods=1).sum().values > 0

    # ⭐️ 파인스크립트: 거래량 3배 폭발 완벽 대입
    condVolSpike = vol_arr >= (avgVol3 * 3)
    isCurrentBullish = close_arr > open_arr

    # 최종 타점 베이스
    signalBase = condPrice & condLiquidity & condBearAlign & condHold112 & condCrossEvent & condHasAcc & condVolSpike & isCurrentBullish

    if not signalBase[-1]:
        return False, "no_signal", df, {}

    # ⭐️ 파인스크립트: 정배열 진입 시 사이클(signalCount) 0 초기화 완벽 대입
    signalCount = 0
    for i in range(len(close_arr)):
        if condBullAlign[i]:
            signalCount = 0
        if signalBase[i]:
            signalCount += 1

    isSubsequentSignal = signalBase[-1] and (signalCount > 1)
    signal_type = "💥연속 역매공파" if isSubsequentSignal else "🎯첫 역매공파"

    dbg = {
        "last_close": float(close_arr[-1]),
        "vma20": float(valMa20[-1]),
        "ema112": float(ema112[-1]),
        "ema224": float(ema224[-1]),
        "ema448": float(ema448[-1]),
        "signal_type": signal_type,
        "signal_count": signalCount,
        "vol_spike": float(vol_arr[-1] / max(1, avgVol3[-1]))
    }
    
    return True, signal_type, df, dbg

# ================== 차트 저장 ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, timeframe: str) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}_{timeframe}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            apds = [
                mpf.make_addplot(df_cut["EMA112"], width=1, color='blue'),
                mpf.make_addplot(df_cut["EMA224"], width=1, color='navy'),
                mpf.make_addplot(df_cut["EMA448"], width=2, color='purple'),
            ]

            tf_str = "1H" if timeframe == '1h' else "1D"
            title = (
                f"[{dbg['signal_type']}] {code} {name} ({tf_str})\n"
                f"Close:{dbg['last_close']:.0f}  EMA112:{dbg['ema112']:.0f}  EMA224:{dbg['ema224']:.0f}  EMA448:{dbg['ema448']:.0f}"
             )

            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':', rc={'font.family': plt.rcParams['font.family']})

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            print(f"\n❌ [차트 그리기 실패] 종목: {name}({code}) | 사유: {e}")
            return None

# ================== 🚀 야후 API 그룹 다운로드 엔진 (구조 원본 유지) ==================
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
    chunk_size = 100 
    period = "730d" if timeframe == '1h' else "3y"

    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        tickers_str = " ".join(chunk)
        
        df_batch = yf.download(tickers_str, interval=timeframe, period=period, group_by="ticker", progress=False, threads=True)
        
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

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_signal(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg, timeframe)
                        
                        if chart_path:
                            sector, outlook, growth = get_company_fact_report(code) 
                            emoji = "💥" if dbg['signal_count'] > 1 else "✅"
                            
                            caption = (
                                f"{emoji} [{dbg['signal_type']}] ({tf_label})\n\n"
                                f"[{name}] ({code})\n"
                                f"- 현재가: {dbg['last_close']:,.0f}원\n"
                                f"- 타점 기록: {dbg['signal_count']}번째 시그널\n"
                                f"- 거래량: 직전 3봉 평균 대비 {dbg['vol_spike']:.1f}배 폭발\n"
                                f"- 유동성(20MA): {int(dbg['vma20'])//100_000_000:,}억 원\n\n"
                                f"💡 [시장 뷰 & 기업 분석]\n"
                                f"- 섹터: {sector}\n"
                                f"- 전망: {outlook}\n"
                                f"- 실적: {growth}\n\n"
                                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            telegram_queue.put((chart_path, caption))
                            
            except Exception as e:
                # 💡 [수정완료] 에러 숨김(pass) 해제 -> 어떤 종목에서 에러 났는지 로그에 출력
                print(f"⚠️ [에러 발생] {ticker}: {e}")
        
        if tracker['scanned'] % 200 == 0 or tracker['scanned'] == len(tickers):
             print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    dt = time.time() - t0
    print(f"\n✅ [{tf_label} 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 스케줄러 ==================
def run_scheduler():
    # 💡 [수정완료] 타임존 완벽 적용 및 시작 시간 35분으로 세팅
    import pytz
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [역매공파 1H & 1D 상업용 스케줄러 자동 대기 모드]")
    print("   - [1시간봉] 매일 08:35 ~ 14:35 (매시 35분마다)")
    print("   - [일봉] 매일 13:35 (오후 1시 35분 딱 1번 추가 실행)")
    
    while True:
        now = datetime.now(kr_tz)
        if now.minute == 35 and (8 <= now.hour <= 14):
             print(f"🚀 [1H 정규 스캔 시작] 현재 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}")
             scan_market('1h')
             if now.hour == 13:
                 print(f"🚀 [1D 정규 스캔 시작] (오후 1:35) 현재 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                 scan_market('1d')
             print("💤 스캔 완료. 다음 타임(1시간 뒤)까지 대기합니다...")
             time.sleep(50 * 60) 
        else: time.sleep(10)

if __name__ == "__main__":
    # 💡 [수정완료] 메인 파일(main.py)에서 충돌 방지를 위해 즉시 실행 코드는 제거하고 스케줄러만 호출
    run_scheduler()
