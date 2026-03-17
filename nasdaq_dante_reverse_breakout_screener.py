# Dante_US_P_1D_AI_Interactive_Pro.py
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

# ⭐️ 구글 최신 통합 라이브러리로 세대 교체 ⭐️
from google import genai

# ==========================================
# 🔑 Gemini API 키 세팅 (여기에 대표님 키 입력!)
# ==========================================
GEMINI_API_KEY = "AIzaSyAagV9SDlZ72CUmYK8JDZaP937CeHrqV7Q"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_P_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

# ⭐️ 실시간 팩트 요약기 (최신 모델 적용) ⭐️
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
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        return "⚠️ 기업 팩트 데이터를 불러오거나 AI 요약 중 오류가 발생했습니다. (직접 분석 요망)"

# ⭐️ 양방향 Q&A 텔레그램 리스너 ⭐️
last_update_id = 0
def telegram_interactive_daemon():
    global last_update_id
    client = genai.Client(api_key=GEMINI_API_KEY)
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            params = {"offset": last_update_id + 1, "timeout": 10}
            res = requests.get(url, params=params, timeout=15).json()
            
            if res.get("ok"):
                for item in res.get("result", []):
                    last_update_id = item["update_id"]
                    msg = item.get("message", {})
                    chat_id = msg.get("chat", {}).get("id")
                    text = msg.get("text", "")
                    
                    if str(chat_id) == str(TELEGRAM_CHAT_ID) and text.startswith("/질문"):
                        question = text.replace("/질문", "").strip()
                        if question:
                            prompt = f"너는 월스트리트의 냉철한 탑 애널리스트야. 다음 주식 관련 질문에 팩트 기반으로 짧고 명확하게 답변해줘. 종목 추천은 하지 말고 분석만 제공해.\n질문: {question}"
                            ai_res = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                            
                            reply_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                            requests.post(reply_url, json={"chat_id": chat_id, "text": f"🤖 [AI 비서 팩트체크]\n\n{ai_res.text.strip()}", "reply_to_message_id": msg.get("message_id")})
        except Exception as e:
            time.sleep(2)
        time.sleep(2)

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
threading.Thread(target=telegram_interactive_daemon, daemon=True).start()

def calculate_trust_score(c, e60, signal_arr):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   

    lookback = min(100, len(c))
    for i in range(len(c) - lookback, len(c) - 1):
        if signal_arr[i]:
            valid = True
            entry_price = c[i]
            for j in range(i + 1, len(c)):
                if c[j] < e60[j] or c[j] >= entry_price * 1.15:
                    valid = False; break
            if valid: score += 2 
    return max(1, min(10, score)) 

def compute_inverse_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    av3 = df['AvgVol3'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    moneyOk = (c * v) >= 1_000_000 
    priceOk = c >= 1.0

    condBearAlign = (ema112 < ema224) & (ema224 < ema448)
    condHold112 = c > ema112

    condCrossEvent = np.zeros(len(c), dtype=bool)
    for i in range(1, 9):
        shifted_c = np.roll(c, i); shifted_c[:i] = np.inf 
        shifted_ema112 = np.roll(ema112, i)
        condCrossEvent |= (shifted_c < shifted_ema112)

    isAccBull = c > o
    rng = h - l
    with np.errstate(divide='ignore', invalid='ignore'):
        closePos = np.where(rng > 0, (c - l) / rng, 0)
        
    valMa20 = pd.Series(c*v).rolling(20, min_periods=1).mean().values
    isAccCandle = isAccBull & ((c*v) >= (1.6 * valMa20)) & (closePos >= 0.68)
    condHasAcc = pd.Series(isAccCandle).rolling(window=20, min_periods=1).sum().values > 0

    with np.errstate(invalid='ignore'):
        condVolSpike = v >= (np.nan_to_num(av3, nan=1.0) * 3)

    signalBase = priceOk & moneyOk & condBearAlign & condHold112 & condCrossEvent & condHasAcc & condVolSpike & (c > o)
    if not signalBase[-1]: return False, "", df, {}

    condBullAlign = (ema112 > ema224) & (ema224 > ema448)
    signalCount = 0
    for i in range(len(c)):
        if condBullAlign[i]: signalCount = 0
        if signalBase[i]: signalCount += 1

    sig_type = "P (연속)" if signalCount > 1 else "P (신규)"
    trust_score = calculate_trust_score(c, ema60, signalBase)

    return True, sig_type, df, {"last_close": float(c[-1]), "score": trust_score}

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{int(time.time()*1000)}.png")
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
    print(f"\n🇺🇸 [일봉 전용] 미국장 P(역매공파) 스캔 (AI 양방향 비서 가동중) 시작!")

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
                    hit, sig_type, df, dbg = compute_inverse_1d(df_ticker)
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
    print(f"\n✅ [미국장 P 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [미국장 P 스케줄러] 09:10 / 11:10 / 14:10 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if now_ny.hour in [9, 11, 14] and now_ny.minute == 20:
            print(f"🚀 [P 1D 스캔 시작] 미국 현지시간: {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
