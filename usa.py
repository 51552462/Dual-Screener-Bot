# Dante_US_Bowl_1D_AI_Pro.py
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
from dotenv import load_dotenv

load_dotenv() 
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("🚨 API 키를 찾을 수 없습니다! .env 파일을 확인해 주세요.")

client = genai.Client(api_key=GEMINI_API_KEY)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Bowl_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

# ⭐️ AI 에러 원인 추적기 및 3회 재시도 로직 ⭐️
def generate_ai_report(ticker_str: str, company_name: str) -> str:
    for attempt in range(3):
        try:
            tk = yf.Ticker(ticker_str)
            info = tk.info
            sector = info.get('sector', '정보 없음')
            industry = info.get('industry', '정보 없음')
            market_cap = info.get('marketCap', '정보 없음')
            if isinstance(market_cap, int): market_cap = f"${market_cap / 1_000_000_000:.2f}B"
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

            [출력 양식]
            1. 섹터 종류: (간단한 설명)
            2. 업계 점유율/규모: (시총 규모 및 지위)
            3. 최근 실적: (흑자/적자 여부, 핵심 지표)
            4. 미래 모멘텀: (파이프라인, 기대감 등)
            5. 기업 전망: (짧고 굵은 전망)
            """
            response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
            return response.text.strip()
        except Exception as e: 
            print(f"❌ [{company_name}] AI 에러 (시도 {attempt+1}/3): {e}")
            time.sleep(3)
            
    return f"⚠️ AI 요약 실패\n(진짜 에러 원인: {e})"

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
        
        safe_caption = caption[:1000] + "\n...(글자수 제한으로 요약됨)" if len(caption) > 1000 else caption

        if SEND_TELEGRAM:
            is_success = False
            for _ in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, files={"photo": f}, timeout=20, verify=False)
                    if res.status_code == 200: 
                        print(f"\n✅ 텔레그램 전송 성공: {img_path}")
                        is_success = True
                        break
                    elif res.status_code == 429: 
                        print("\n⚠️ 텔레그램 전송 지연 (429 에러). 3초 대기...")
                        time.sleep(3)
                    else:
                        print(f"\n❌ 텔레그램 서버 거부 (HTTP {res.status_code}): {res.text}")
                        time.sleep(2)
                except Exception as e:
                    print(f"\n❌ 텔레그램 전송 중 예외 발생: {e}")
                    time.sleep(2)
            if not is_success:
                print(f"\n⚠️ 최종 텔레그램 전송 실패 - 대상 파일: {img_path}")
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

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
                    valid = False
                    break
            if valid: score += 2 
    return max(1, min(10, score))

def compute_bobgeureut_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    ema10, ema20, ema30, ema60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    ema112, ema224, ema448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    ma20 = pd.Series(c).rolling(20).mean().values
    stddev = pd.Series(c).rolling(20).std(ddof=1).values
    bb_upper = ma20 + (stddev * 2)

    tenkan = (pd.Series(h).rolling(9).max() + pd.Series(l).rolling(9).min()) / 2
    kijun = (pd.Series(h).rolling(26).max() + pd.Series(l).rolling(26).min()) / 2
    spanA = (tenkan + kijun) / 2
    spanB = (pd.Series(h).rolling(52).max() + pd.Series(l).rolling(52).min()) / 2
    senkou1 = np.roll(spanA, 25); senkou1[:25] = np.nan
    senkou2 = np.roll(spanB, 25); senkou2[:25] = np.nan
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

    if (signalCat2 & isAligned)[-1] or (signalCat1 & isAligned)[-1]: sig_type = "B (J 강조)"
    else: sig_type = "B (일반)"

    trust_score = calculate_trust_score(c, ema60, signalBase)

    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "s67_count": int(s67_counts[-1])}

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
    print(f"\n🇺🇸 [일봉 전용] 미국장 3번(밥그릇) 스캔 시작!")

    ticker_to_info = {row['Symbol']: {'code': row['Symbol'], 'name': row['Name']} for _, row in stock_list.iterrows()}
    tickers = list(ticker_to_info.keys())
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    chunk_size = 100 

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

            # 💡 숨은 에러 추적 및 공감형 카피라이팅 적용
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
                    hit, sig_type, df, dbg = compute_bobgeureut_1d(df_ticker)
                    
                    if hit:
                        tracker['hits'] += 1
                        chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                        if chart_path:
                            ai_fact_check = generate_ai_report(code, name)
                            
                            # 💡 J강조(골든타점)와 일반 타점 분기
                            if "J 강조" in dbg['sig_type']:
                                caption = (
                                    f"🏢 {name} ({code})\n"
                                    f"💰 현재가: ${dbg['last_close']:.2f}\n\n"
                                    f"✨ [본격적인 가치 회복의 서막]\n"
                                    f"오랜 기다림 끝에 기업의 내재 가치가 빛을 발하기 시작하는 결정적 순간입니다. 시장의 흐름과 함께 안정적인 우상향을 기대하며 발걸음을 맞춰보세요.\n\n"
                                    f"⚖️ [건강한 매매를 위한 가이드]\n"
                                    f"• 여유로운 접근: 현재가부터 천천히 모아가며 마음의 여유를 가지세요.\n"
                                    f"• 원칙 대응: 약속된 지지라인(-5%) 이탈 시에는 기계적으로 대응하여 소중한 자산을 보호합니다.\n\n"
                                    f"💡 [AI 비즈니스 요약]\n"
                                    f"{ai_fact_check}\n\n"
                                    f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요."
                                )
                            else:
                                caption = (
                                    f"🏢 {name} ({code})\n"
                                    f"💰 현재가: ${dbg['last_close']:.2f}\n\n"
                                    f"🌱 [흙 속의 진주, 비상을 위한 준비]\n"
                                    f"당장의 화려함보다는 묵묵히 내실을 다져온 기업입니다. 조급한 매매보다는 '관심종목'에 조용히 담아두고, 기업의 진정한 가치가 시장에서 인정받는 과정을 여유롭게 지켜보시길 권해드립니다.\n\n"
                                    f"💡 [AI 비즈니스 요약]\n"
                                    f"{ai_fact_check}\n\n"
                                    f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요."
                                )
                            telegram_queue.put((chart_path, caption))
                            print(f"\n✅ [{name}] 텔레그램 전송 대기열에 추가 완료!")
                        else:
                            print(f"\n⚠️ [{name}] 차트 생성 실패로 텔레그램 전송 취소.")
            except Exception as e:
                print(f"\n❌ [{name}] 처리 중 에러 발생: {e}")
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    # 💡 텔레그램 전송 완료 보장 대기 (조기 퇴근 방지)
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()

    dt = time.time() - t0
    print(f"\n✅ [미국장 3번 B 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ⭐️ 3번 스케줄러 세팅 (10:30, 12:30, 14:30) ⭐️
def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [3번 미국장 검색기] 10:30 / 12:30 / 14:30 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 10 and now_ny.minute == 30) or (now_ny.hour == 12 and now_ny.minute == 30) or (now_ny.hour == 14 and now_ny.minute == 30):
            print(f"🚀 [3번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
