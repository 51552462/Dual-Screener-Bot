# Dante_US_Nulrim_1D_AI_Pro.py
import os, re, time, threading, queue, concurrent.futures
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
from google import genai

# ==========================================
# 🔑 Gemini API 키 세팅 (여기에 대표님 키 입력!)
# ==========================================
GEMINI_API_KEY = "AIzaSyDn624Gw7cWw4nIBE65jbvA8HLbmbYuVOY"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

def generate_ai_report(ticker_str: str, company_name: str) -> str:
    try:
        tk = yf.Ticker(ticker_str)
        info = tk.info
        
        sector = info.get('sector', '정보 없음')
        industry = info.get('industry', '정보 없음')
        market_cap = info.get('marketCap', '정보 없음')
        if isinstance(market_cap, int): market_cap = f"${market_cap / 1_000_000_000:.2f}B (십억 달러)"
        
        eps = info.get('trailingEps', '정보 없음')
        revenue_growth = info.get('revenueGrowth', '정보 없음')
        business_summary = info.get('longBusinessSummary', '정보 없음')[:800] 
        
        financials = f"EPS: {eps}, 매출성장률: {revenue_growth}"

        prompt = f"""
        너는 월스트리트의 냉철하고 전문적인 탑 애널리스트야.
        아래 종목의 데이터를 바탕으로 팩트 중심의 핵심 투자 메모를 작성해.
        추상적이거나 감정적인 표현은 철저히 배제하고, 기관 보고서처럼 간결하고 명확하게 써.

        [종목 정보]
        - 종목명: {company_name} ({ticker_str})
        - 섹터: {sector} / 산업군: {industry}
        - 시가총액: {market_cap}
        - 실적 및 재무: {financials}
        - 비즈니스 요약: {business_summary}

        [출력 양식] (반드시 아래 번호와 항목명에 맞춰서 작성할 것)
        1. 섹터 종류: (간단한 설명)
        2. 업계 점유율/규모: (시총 규모 및 지위)
        3. 최근 실적: (흑자/적자 여부, 핵심 지표)
        4. 미래 모멘텀: (파이프라인, 기대감 등)
        5. 기업 전망: (짧고 굵은 전망)
        """
        
        client = genai.Client(api_key=GEMINI_API_KEY)
        # ⭐️ 100% 멈추지 않는 초안정적 1.5 모델 복구 (무한대기 방지) ⭐️
        response = client.models.generate_content(
            model='gemini-1.5-flash',
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        return "⚠️ 기업 팩트 데이터를 불러오거나 AI 요약 중 오류가 발생했습니다. (직접 분석 요망)"

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

# 순수 발송 데몬만 남겨서 스레드 꼬임 완벽 차단
threading.Thread(target=telegram_sender_daemon, daemon=True).start()

MIN_PRICE_USD = 1.0               
MIN_MONEY_USD = 1_000_000         

def calculate_trust_score(c, e60, s1_arr, s2_arr, s4_arr):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   

    lookback = min(100, len(c))
    for i in range(len(c) - lookback, len(c) - 1):
        if s1_arr[i] or s2_arr[i] or s4_arr[i]:
            valid = True
            entry_price = c[i]
            for j in range(i + 1, len(c)):
                if c[j] < e60[j] or c[j] >= entry_price * 1.15:
                    valid = False; break
            if valid: score += 2 
    return max(1, min(10, score))

def compute_nulrim_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    
    c, o, v = df['Close'].values, df['Open'].values, df['Volume'].values
    av3 = df['AvgVol3'].values
    
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    moneyOk = (c * v) >= MIN_MONEY_USD
    priceOk = c >= MIN_PRICE_USD
    with np.errstate(invalid='ignore'): volSpike = v >= (np.nan_to_num(av3, nan=1.0) * 3)
    isBullish = c > o

    align112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    align224 = align112 & (e112 > e224)
    align448 = align224 & (e224 > e448)

    longKeep448 = e224 > e448 
    longKeep224 = e112 > e224 
    longKeep112 = e60 > e112  

    prev_align448 = np.roll(align448, 1); prev_align448[0] = False
    prev_align224 = np.roll(align224, 1); prev_align224[0] = False
    prev_align112 = np.roll(align112, 1); prev_align112[0] = False
    
    prev_longKeep448 = np.roll(longKeep448, 1); prev_longKeep448[0] = False
    prev_longKeep224 = np.roll(longKeep224, 1); prev_longKeep224[0] = False
    prev_longKeep112 = np.roll(longKeep112, 1); prev_longKeep112[0] = False

    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    s2 = align224 & (~prev_align224) & prev_longKeep224 & (e224 < e448) & isBullish
    
    prev_c = np.roll(c, 1); prev_c[0] = 0
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    
    s4 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i

    cond_base = moneyOk & priceOk & volSpike
    hit1 = s1[-1] and cond_base[-1]
    hit2 = s2[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1]

    if not (hit1 or hit2 or hit4): return False, "", df, {}

    if hit4: sig_type = "V (S4: 돌파)"
    elif hit2: sig_type = "V (S2: 224 재정렬)"
    else: sig_type = "V (S1: 448 재정렬)"

    trust_score = calculate_trust_score(c, e60, s1, s2, s4)

    return True, sig_type, df, {"last_close": float(c[-1]), "score": trust_score}

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{timestamp_ms}.png")
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            title = f"[🎯 {dbg['sig_type']}] US Market: {code} (1D)\nClose: ${dbg['last_close']:.2f}"
            mc = mpf.make_marketcolors(up='green', down='red', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')
            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            return path
        except: return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 V(눌림목) 스캔 시작!")

    ticker_to_info = {row['Symbol']: {'code': row['Symbol'], 'name': row['Name']} for _, row in stock_list.iterrows()}
    tickers = list(ticker_to_info.keys())
    chunk_size = 100 
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        df_batch = None
        fallback_dict = {}

        try:
            df_batch = yf.download(" ".join(chunk), interval="1d", period="3y", group_by="ticker", progress=False, threads=False)
        except:
            def fetch_single(tk):
                try:
                    df_s = yf.download(tk, interval="1d", period="3y", progress=False, threads=False)
                    if not df_s.empty: fallback_dict[tk] = df_s
                except: pass
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                executor.map(fetch_single, chunk)
        
        for tk in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(tk)
            if not info: continue
            name, code = info['name'], info['code']

            try:
                if df_batch is not None:
                    if len(chunk) == 1: df_ticker = df_batch.copy()
                    else: 
                        if tk not in df_batch.columns.get_level_values(0): continue
                        df_ticker = df_batch[tk].copy()
                else:
                    df_ticker = fallback_dict.get(tk)
                    if df_ticker is None or df_ticker.empty: continue

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                if df_ticker.index.tzinfo is not None: df_ticker.index = df_ticker.index.tz_convert('America/New_York').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_nulrim_1d(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                        if chart_path:
                            ai_fact_check = generate_ai_report(code, name)
                            
                            caption = (
                                f"🎯 [{dbg['sig_type']}]\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg['last_close']:.2f}\n"
                                f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                                f"📉 [매수/손절 전략]\n"
                                f"- 양봉 길이만큼 분할매수\n"
                                f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                                f"⭐ 알고리즘 신뢰도: {dbg['score']} / 10점\n\n"
                                f"💡 [기업 팩트체크]\n"
                                f"{ai_fact_check}\n\n"
                                f"⚠️ [전문가 코멘트]\n"
                                f"본 분석은 실시간 데이터 기반 팩트 요약본입니다. 시장 상황과 개인의 관점에 따라 해석이 다를 수 있으므로, 반드시 개별적인 추가 분석을 권장합니다.\n"
                                f"\n💬 궁금한 점이 있다면 채팅창에 '/질문 내용' 을 입력해 보세요!"
                            )
                            telegram_queue.put((chart_path, caption))
            except: pass
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    dt = time.time() - t0
    print(f"\n✅ [8번 봇: US V 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [4번 미국장 검색기] 10:30 / 12:30 / 15:30 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 10 and now_ny.minute == 30) or (now_ny.hour == 12 and now_ny.minute == 30) or (now_ny.hour == 15 and now_ny.minute == 30):
            print(f"🚀 [4번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
