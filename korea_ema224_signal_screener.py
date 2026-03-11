# Dante_Dual_Hyper_Screener_Smart_Final.py
import os
import re
import time
import threading
import queue
from datetime import datetime
import pytz  # 💡 [추가] 서버 시간 꼬임 방지용 한국 시간 동기화
from io import StringIO
import numpy as np
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg') # GUI 에러 원천 차단 [cite: 88]
import matplotlib.pyplot as plt
import requests
import warnings
import urllib3
import yfinance as yf
import logging
from bs4 import BeautifulSoup

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
TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Dual_Screener')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ================== ⭐️ 스마트 기업 팩트 분석기 ==================
def get_smart_company_report(code: str, name: str) -> tuple:
    sector = "정보 없음"
    earnings_trend = "뚜렷한 실적 추세 없음"
    news_summary = "최근 1주일 주요 호재/특징주 뉴스 없음" [cite: 89]
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        # 1. 섹터 추출 (네이버)
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=3, verify=False)
        if res_naver.status_code == 200:
            soup = BeautifulSoup(res_naver.text, 'html.parser')
            tag = soup.select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip() [cite: 89, 90]

        # 2. 실적 우상향 판별 (FnGuide)
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=3, verify=False)
        if res_fn.status_code == 200:
            soup = BeautifulSoup(res_fn.text, 'html.parser')
            tags = soup.select('ul#bizSummaryContent > li')
            if len(tags) >= 2:
                growth_text = tags[1].text.strip()
                if any(x in growth_text for x in ["증가", "흑자", "개선", "상승", "호조", "성장"]):
                    earnings_trend = "📈 실적 턴어라운드 및 우상향 진행 중"
                elif any(x in growth_text for x in ["감소", "적자", "악화", "하락", "부진"]):
                    earnings_trend = "📉 실적 부진 및 악화 진행 중"
                else:
                    earnings_trend = "보합 (특이사항 없음)" [cite: 90, 91, 92]

        # 3. 최근 호재 뉴스 추출 (네이버 금융)
        news_url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        res_news = requests.get(news_url, headers=headers, timeout=3, verify=False)
        if res_news.status_code == 200:
            soup = BeautifulSoup(res_news.text, 'html.parser')
            news_links = soup.select('.title a')
            
            headlines = []
            for link in news_links:
                title = link.text.strip()
                if any(kw in title for keyword in ["특징주", "강세", "급등", "상한가", "수주", "계약", "돌파", "호실적", "최대", "AI"] for kw in [keyword]):
                    if title not in headlines:
                        headlines.append("- " + title)
                if len(headlines) >= 2: break
            
            if not headlines and news_links:
                for link in news_links[:2]:
                    headlines.append("- " + link.text.strip())
            
            if headlines:
                news_summary = "\n".join(headlines) [cite: 96]

    except: pass
    return sector, earnings_trend, news_summary [cite: 96, 97]

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
    except: return pd.DataFrame() [cite: 97, 98]

# ================== 텔레그램 전송 데몬 ==================
def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
            
        img_path, caption = item
        if len(caption) > 1000: caption = caption[:980] + "\n\n...(내용이 너무 길어 생략됨)" [cite: 98, 99]

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
                    else: 
                        break 
                except Exception as e:
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done() [cite: 99, 100, 101, 102, 103, 104]

sender_thread = threading.Thread(target=telegram_sender_daemon, daemon=True)
sender_thread.start() [cite: 104]

# ================== 지표 계산 및 로직 ==================
def add_emas(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        d[f'EMA{n}'] = d['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    d['AvgVol3'] = d['Volume'].shift(1).rolling(3, min_periods=1).mean()
    return d [cite: 104]

# [로직 1] 역배열 (1시간봉) - S1, S2
def compute_inverse_1h(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, None, df_raw, {}
    df = add_emas(df_raw)
    
    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    avgvol3_arr = df['AvgVol3'].values
    ema10, ema20, ema30 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values [cite: 104, 105]

    bullish = close_arr > open_arr
    alignedNow = (ema10 > ema20) & (ema20 > ema30)
    
    # ⭐️ 0으로 나누기 및 NaN 무한대 에러 완벽 방어 처리 ⭐️
    with np.errstate(invalid='ignore'):
        volSpike = vol_arr > (np.nan_to_num(avgvol3_arr, nan=1.0) * 3)
    
    prev_close = np.roll(close_arr, 1); prev_close[0] = 0
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    cross224 = (close_arr > ema224) & (prev_close <= prev_ema224) [cite: 105, 106, 107]
    
    signal1 = cross224 & bullish & alignedNow & volSpike
    
    s1_shift3 = np.roll(signal1, 3); s1_shift3[:3] = False [cite: 107, 108]
    
    holdNow = (close_arr > ema224) & (ema10 > ema20) & (ema20 > ema30)
    holdNow_1 = np.roll(holdNow, 1); holdNow_1[0] = False
    holdNow_2 = np.roll(holdNow, 2); holdNow_2[:2] = False [cite: 108, 109, 110]
    
    hold3 = s1_shift3 & holdNow & holdNow_1 & holdNow_2
    allAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224) & (ema224 > ema448)
    signal2 = hold3 & allAligned [cite: 110]

    is_s1, is_s2 = signal1[-1], signal2[-1]
    if not (is_s1 or is_s2): return False, None, df, {}
        
    sig_type = "S2 (유지 + 448 완전 정배열)" if is_s2 else "S1 (224 돌파 + 3배 거래량)"
    
    # ⭐️ 0으로 나누기 방어 (max 1 적용) ⭐️
    dbg = {"last_close": float(close_arr[-1]), "vol_spike": float(vol_arr[-1]/max(1, avgvol3_arr[-1])), "sig_type": sig_type}
    return True, sig_type, df, dbg [cite: 110, 111]

# [로직 2] 정배열 (일봉) - S1, S2, S3
def compute_aligned_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, None, df_raw, {}
    df = add_emas(df_raw)
    
    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    avgvol3_arr = df['AvgVol3'].values
    ema10, ema20, ema30 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values [cite: 111, 112]
    
    isBullish = close_arr > open_arr
    
    # ⭐️ 0으로 나누기 및 NaN 무한대 에러 완벽 방어 처리 ⭐️
    with np.errstate(invalid='ignore'):
        volSpike5 = vol_arr >= (np.nan_to_num(avgvol3_arr, nan=1.0) * 5)
    
    condBase = isBullish & volSpike5
    
    align112 = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112)
    align224 = align112 & (ema112 > ema224)
    align448 = align224 & (ema224 > ema448) [cite: 112]
    
    prev_align448 = np.roll(align448, 1); prev_align448[0] = False [cite: 112, 113]
    
    signal3 = condBase & align448 & (~prev_align448)
    signal2 = condBase & align224 & (~signal3)
    signal1 = condBase & align112 & (~align224) [cite: 113]
    
    is_s1, is_s2, is_s3 = signal1[-1], signal2[-1], signal3[-1]
    if not (is_s1 or is_s2 or is_s3): return False, None, df, {}
         
    if is_s3: sig_type = "S3 (448 완전 정배열 완성)"
    elif is_s2: sig_type = "S2 (224 정배열 상태)"
    else: sig_type = "S1 (112 정배열 상태)" [cite: 113, 114]
    
    # ⭐️ 0으로 나누기 방어 (max 1 적용) ⭐️
    dbg = {"last_close": float(close_arr[-1]), "vol_spike": float(vol_arr[-1]/max(1, avgvol3_arr[-1])), "sig_type": sig_type}
    return True, sig_type, df, dbg [cite: 114]

# ================== 차트 저장 ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, timeframe: str) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}_{timeframe}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png") [cite: 114, 115]

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            apds = [
                mpf.make_addplot(df_cut["EMA10"], color='red', width=1),
                mpf.make_addplot(df_cut["EMA20"], color='orange', width=1),
                mpf.make_addplot(df_cut["EMA30"], color='yellow', width=1),
                mpf.make_addplot(df_cut["EMA60"], color='green', width=1),
                mpf.make_addplot(df_cut["EMA112"], color='blue', width=1),
                mpf.make_addplot(df_cut["EMA224"], color='navy', width=2),
                mpf.make_addplot(df_cut["EMA448"], color='purple', width=2),
            ] [cite: 115, 116]

            tf_str = "1H(역배열)" if timeframe == '1h' else "1D(정배열)"
            title = f"[{dbg['sig_type']}] {code} {name} ({tf_str})\nClose:{dbg['last_close']:.0f} | 거래량 {dbg['vol_spike']:.1f}배" [cite: 116, 117, 118]
            
            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':') [cite: 118]

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all') [cite: 118, 119]
            
            return path
        except Exception as e:
            return None [cite: 119]

# ================== 🚀 야후 API 그룹 다운로드 엔진 ==================
def scan_market(timeframe: str):
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    
    t0 = time.time() [cite: 119]
    
    tf_label = "1시간봉" if timeframe == '1h' else "일봉"
    logic_name = "역배열(1시간봉)" if timeframe == '1h' else "정배열(일봉)" [cite: 120]
    
    print(f"\n⚡ [궁극의 그룹 스캔 가동] 총 {len(stock_list)}개 종목 '{logic_name}' 초고속 스캔 시작!") [cite: 120]

    ticker_to_info = {}
    for _, row in stock_list.iterrows():
        ticker = f"{row['Code']}.KS" if row['Market'] == 'KOSPI' else f"{row['Code']}.KQ"
        ticker_to_info[ticker] = {'code': row['Code'], 'name': row['Name']} [cite: 120]
    
    tickers = list(ticker_to_info.keys())
    chunk_size = 100 
    period = "730d" if timeframe == '1h' else "3y" [cite: 121]

    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0} [cite: 121]

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        tickers_str = " ".join(chunk)
        
        df_batch = yf.download(tickers_str, interval=timeframe, period=period, group_by="ticker", progress=False, threads=True) [cite: 121]
        
        for ticker in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info[ticker]
            name, code = info['name'], info['code'] [cite: 122]

            try:
                if len(chunk) == 1: df_ticker = df_batch.copy()
                else: df_ticker = df_batch[ticker].copy() [cite: 122, 123]

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('Asia/Seoul').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')] [cite: 123, 124]

                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= 1000:
                    tracker['analyzed'] += 1 [cite: 124]
                    
                    if timeframe == '1h': hit, sig_type, df, dbg = compute_inverse_1h(df_ticker)
                    else: hit, sig_type, df, dbg = compute_aligned_1d(df_ticker) [cite: 124, 125]
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg, timeframe) [cite: 125, 126]
                        
                        if chart_path:
                            sector, earnings_trend, news_summary = get_smart_company_report(code, name) 
                            emoji = "🔥" if timeframe == '1h' else "💎" [cite: 126, 127, 128]
                            
                            caption = (
                                f"{emoji} [{dbg['sig_type']}] ({tf_label})\n\n"
                                f"[{name}] ({code})\n"
                                f"- 현재가: {dbg['last_close']:,.0f}원\n"
                                f"- 거래량: 직전 3봉 평균 대비 {dbg['vol_spike']:.1f}배 폭발\n\n"
                                f"💡 [팩트 체크 리포트]\n"
                                f"🔸 섹터: {sector}\n"
                                f"🔸 실적: {earnings_trend}\n"
                                f"🔸 최근 1주 주요 뉴스:\n{news_summary}\n\n"
                                f"Time: {datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')}"
                            ) [cite: 128, 129, 130, 131, 132]
                            telegram_queue.put((chart_path, caption)) [cite: 132]
                            
            except Exception as e:
                pass [cite: 133]
        
        if tracker['scanned'] % 200 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)") [cite: 133]

    dt = time.time() - t0
    print(f"\n✅ [{logic_name}] 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n") [cite: 133, 134]

# ================== ⏰ [3번 봇 스케줄러] 매시 20분 / 매일 16:00 ==================
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [3번 봇: 한국장 EMA224 듀얼 자동 스케줄러 대기 모드 - 분산 완료]")
    print("   - [1H 스캔] 매시 20분마다 (서버 부하 분산)")
    print("   - [1D 스캔] 매일 16:00 (장 마감 및 데이터 안정화 직후)") 
    
    while True:
        now_kr = datetime.now(kr_tz)
        
        # 💡 매시 20분에 1시간봉 스캔 (1, 2번 봇과 격차)
        if now_kr.minute == 25:
            print(f"🚀 [3번 봇 1H 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1h')
            time.sleep(50 * 60) 
            
        # 💡 매일 16:00에 일봉 스캔 (1, 2번 봇과 격차)
        elif now_kr.hour == 15 and now_kr.minute == 30:
            print(f"🚀 [3번 봇 1D 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market('1d')
            time.sleep(50 * 60) 
            
        else: 
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()

