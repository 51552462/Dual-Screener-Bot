# Dante_Ohdole_1H_FactCheck_Final.py
import os
import re
import time
import threading
import queue
from datetime import datetime
import pytz  # 💡 [추가] 서버 시간 꼬임 방지용
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
TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Ohdole_1H')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ================== ⭐️ 진짜 팩트 코멘트 추출기 ==================
def get_smart_company_report(code: str, name: str) -> tuple:
    sector = "정보 없음"
    earnings_detail = "실적 코멘트를 불러올 수 없습니다."
    news_summary = "최근 주요 뉴스가 없습니다."
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        # 1. 섹터 추출 (네이버)
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=3, verify=False)
        if res_naver.status_code == 200:
            soup = BeautifulSoup(res_naver.text, 'html.parser')
            tag = soup.select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()

        # 2. 실적 코멘트 원문 추출 (FnGuide)
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=3, verify=False)
        if res_fn.status_code == 200:
            soup = BeautifulSoup(res_fn.text, 'html.parser')
            tags = soup.select('ul#bizSummaryContent > li')
            if len(tags) >= 2:
                raw_text = tags[1].text.strip()
                sentences = [s.strip() for s in raw_text.split('. ') if s.strip()]
                summary = '. '.join(sentences[:2])
                if not summary.endswith('.'): summary += '.'
                
                if any(x in summary for x in ["증가", "흑자", "개선", "상승", "호조", "성장", "최대"]):
                    earnings_detail = f"📈 [개선] {summary}"
                elif any(x in summary for x in ["감소", "적자", "악화", "하락", "부진"]):
                    earnings_detail = f"📉 [부진] {summary}"
                else:
                    earnings_detail = f"📊 [기타] {summary}"

        # 3. 최근 호재 뉴스 실제 제목 추출 (네이버 금융)
        news_url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        res_news = requests.get(news_url, headers=headers, timeout=3, verify=False)
        if res_news.status_code == 200:
            soup = BeautifulSoup(res_news.text, 'html.parser')
            news_links = soup.select('.title a')
            
            headlines = []
            for link in news_links:
                title = link.text.strip()
                if any(kw in title for keyword in ["특징주", "강세", "급등", "상한가", "수주", "계약", "돌파", "호실적", "최대", "AI", "공급", "MOU", "체결"] for kw in [keyword]):
                    if title not in headlines:
                        headlines.append("- " + title)
                if len(headlines) >= 2: break
            
            if not headlines and news_links:
                for link in news_links[:2]:
                    title = link.text.strip()
                    if title not in headlines:
                        headlines.append("- " + title)
            
            if headlines:
                news_summary = "\n".join(headlines)

    except: pass
    return sector, earnings_detail, news_summary

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
                        break
                    elif res.status_code == 429: time.sleep(3)
                    else: break 
                except Exception as e:
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

sender_thread = threading.Thread(target=telegram_sender_daemon, daemon=True)
sender_thread.start()

# ================== 파라미터 셋업 (오돌이 100% 룰) ==================
MIN_PRICE = 1000
MIN_TRANS_MONEY = 300_000_000  
VOL_MUL = 1.0                  

# ================== 오돌이 핵심 로직 (SMA 기반 + 초고속 NumPy 연산) ==================
def compute_ohdole_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_signal", df_raw, {}

    df = df_raw.copy()
    
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA112'] = df['Close'].rolling(window=112).mean()
    df['MA224'] = df['Close'].rolling(window=224).mean()
    df['MA448'] = df['Close'].rolling(window=448).mean()

    close_arr = df['Close'].values
    open_arr = df['Open'].values
    high_arr = df['High'].values
    vol_arr = df['Volume'].values
    
    ma5 = df['MA5'].values
    ma112 = df['MA112'].values
    ma224 = df['MA224'].values
    ma448 = df['MA448'].values

    money_curr = close_arr * vol_arr

    is_downtrend = (ma448 > ma224) & (ma224 > ma112)
    is_basement = close_arr < ma112
    is_env_ok = is_downtrend & is_basement

    prev_vol = np.roll(vol_arr, 1); prev_vol[0] = np.inf
    is_vol_ok = vol_arr >= (prev_vol * VOL_MUL)
    is_money_ok = money_curr >= MIN_TRANS_MONEY
    is_price_ok = close_arr >= MIN_PRICE
    is_power_ok = is_vol_ok & is_money_ok & is_price_ok

    prev_ma5 = np.roll(ma5, 1); prev_ma5[0] = np.inf
    prev_close = np.roll(close_arr, 1); prev_close[0] = 0
    is_breakout = (close_arr > ma5) & (prev_close <= prev_ma5)
    
    prev_high1 = np.roll(high_arr, 1); prev_high1[0] = np.inf
    prev_high2 = np.roll(high_arr, 2); prev_high2[:2] = np.inf
    high_prev_2 = np.maximum(prev_high1, prev_high2)
    is_engulfing = (close_arr > open_arr) & (close_arr > high_prev_2)
    
    sig_1 = is_env_ok & is_power_ok & is_breakout & is_engulfing

    is_yangbong = close_arr > open_arr
    threshold = open_arr + ((close_arr - open_arr) * 0.33)
    is_riding = ma5 <= threshold
    
    sig_2 = is_env_ok & is_power_ok & is_yangbong & is_riding & (~sig_1)

    sig1_hit = sig_1[-1]
    sig2_hit = sig_2[-1]

    if not (sig1_hit or sig2_hit):
        return False, "no_signal", df, {}

    if sig1_hit: sig_type = "🔥 오돌이 1번 (장악형)"
    else: sig_type = "✅ 오돌이 2번 (안착형)"

    # ⭐️ 0으로 나누기 (Infinity) 완벽 방어
    safe_prev_vol = max(1, prev_vol[-1]) if prev_vol[-1] != np.inf else 1
    vol_ratio = (vol_arr[-1] / safe_prev_vol) * 100

    dbg = {
        "last_close": float(close_arr[-1]),
        "trans_money": float(money_curr[-1]),
        "vol_ratio": float(vol_ratio),
        "sig_type": sig_type
    }
 
    return True, sig_type, df, dbg

# ================== 차트 저장 ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            
            apds = [
                mpf.make_addplot(df_cut["MA5"], color='#FF5252', width=2),
                mpf.make_addplot(df_cut["MA20"], color='#FFD700', width=2),
                mpf.make_addplot(df_cut["MA112"], color='#00E676', width=1, linestyle='--'),
                mpf.make_addplot(df_cut["MA224"], color='#2979FF', width=1, linestyle='--'),
                mpf.make_addplot(df_cut["MA448"], color='gray', width=1, linestyle='--'),
            ]

            title = f"[{dbg['sig_type']}] {code} {name} (1H)\nClose: {dbg['last_close']:.0f} | 거래대금: {dbg['trans_money']/100_000_000:.1f}억"

            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            return None

# ================== 🚀 야후 API 그룹 다운로드 엔진 ==================
def scan_market():
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n⚡ [오돌이 하이퍼 스캔 가동] 총 {len(stock_list)}개 종목 '1시간봉' 초고속 병렬 스캔 시작!")

    ticker_to_info = {}
    for _, row in stock_list.iterrows():
        ticker = f"{row['Code']}.KS" if row['Market'] == 'KOSPI' else f"{row['Code']}.KQ"
        ticker_to_info[ticker] = {'code': row['Code'], 'name': row['Name']}
    
    tickers = list(ticker_to_info.keys())
    chunk_size = 100 
    period = "730d" 

    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        tickers_str = " ".join(chunk)
        
        df_batch = yf.download(tickers_str, interval="1h", period=period, group_by="ticker", progress=False, threads=True)
        
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

                if len(df_ticker) >= 500 and df_ticker['Close'].iloc[-1] >= MIN_PRICE:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_ohdole_signal(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                        
                        if chart_path:
                            sector, earnings_detail, news_summary = get_smart_company_report(code, name) 
                            
                            caption = (
                                f"[{dbg['sig_type']}]\n\n"
                                f"[{name}] ({code})\n"
                                f"- 현재가: {dbg['last_close']:,.0f}원\n"
                                f"- 거래량 유지: 어제 대비 {dbg['vol_ratio']:.0f}%\n"
                                f"- 거래대금: {dbg['trans_money']/100_000_000:.1f}억 원\n\n"
                                f"💡 [팩트 체크 리포트]\n"
                                f"🔸 섹터: {sector}\n"
                                f"🔸 실적 코멘트:\n{earnings_detail}\n\n"
                                f"🔸 최근 1주 주요 뉴스:\n{news_summary}\n\n"
                                f"Time: {datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            telegram_queue.put((chart_path, caption))
                            
            except Exception as e:
                pass
        
        if tracker['scanned'] % 200 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    dt = time.time() - t0
    print(f"\n✅ [4번 봇: 오돌이 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ [4번 봇 스케줄러] 매시 30분 작동 ==================
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [4번 봇: 오돌이 1H 상업용 스케줄러 대기 모드 - 분산 완료]")
    print("   - [1시간봉] 매시 30분마다 단독 실행 (서버 부하 분산)")
    
    while True:
        now_kr = datetime.now(kr_tz)
        
        # 💡 매시 30분에 작동 (1번 00분, 2번 10분, 3번 20분과 완벽 분리)
        if now_kr.minute == 35:
            print(f"🚀 [4번 봇 1H 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market()
            print("💤 스캔 완료. 다음 타임(1시간 뒤)까지 대기합니다...")
            time.sleep(50 * 60) 
        else: 
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
