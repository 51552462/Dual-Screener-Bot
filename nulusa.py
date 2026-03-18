# Dante_US_Nulrim_1D_AI_Pro.py
import os, re, time, threading, queue, concurrent.futures
from datetime import datetime, timedelta
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
from google.genai import types
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

# ⭐️ 기모으는중(미니폼) + 정식 폼 분기 생성기 및 3회 재시도 ⭐️
def generate_ai_report(ticker_str: str, company_name: str, is_gathering: bool = False) -> str:
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
            today_date = datetime.now().strftime('%Y년 %m월 %d일')

            # S6, S7 (기모으는중) 숏 폼 지시
            if is_gathering:
                prompt = f"""
                오늘 날짜는 {today_date}이야. 구글 검색을 활용해 다음 미국 주식의 정보를 아래 양식에 맞춰 딱 3줄로 한국어로 요약해.
                [종목 정보] {company_name} ({ticker_str}) / 섹터: {sector}, {industry} / 실적: {financials}
                
                [출력 양식] (마크다운 기호 없이 텍스트로만)
                기업 이름: {company_name} ({ticker_str})
                주도섹터: (현재 시장에서 어떤 테마/섹터로 엮이는지 1문장)
                실적: (최근 실적 요약 1문장)
                """
            # S1, S2, S3, S4 롱 폼 지시
            else:
                prompt = f"""
                너는 월스트리트의 냉철하고 전문적인 탑 애널리스트야. 오늘 날짜는 {today_date}이야. 
                반드시 최신 구글 검색 결과를 바탕으로 팩트 중심의 투자 메모를 작성해.
                
                [종목 정보] {company_name} ({ticker_str}) / 섹터: {sector} / 시가총액: {market_cap} / 실적: {financials}
                [비즈니스 요약] {business_summary}

                [출력 양식]
                1. 섹터 종류: (간단한 설명)
                2. 업계 점유율/규모: (시총 규모 및 지위)
                3. 최근 실적: (흑자/적자 여부, 핵심 지표)
                4. 미래 모멘텀: (파이프라인, 최신 호재/악재 등)
                5. 기업 전망: (짧고 굵은 전망)
                """
            
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
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

MIN_PRICE_USD = 3.0               
MIN_MONEY_USD = 5_000_000         

def calculate_trust_score(c, e60, *sig_arrays):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   

    lookback = min(100, len(c))
    for i in range(len(c) - lookback, len(c) - 1):
        is_sig = any(arr[i] for arr in sig_arrays)
        if is_sig:
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
    df['Lowest5'] = df['Low'].rolling(5).min()
    
    c, o, h, v = df['Close'].values, df['Open'].values, df['High'].values, df['Volume'].values
    av3 = df['AvgVol3'].values
    lowest5 = df['Lowest5'].values
    
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
    s3 = align112 & (~prev_align112) & prev_longKeep112 & (e112 < e224) & isBullish
    
    prev_c = np.roll(c, 1); prev_c[0] = 0
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish

    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1); prev_shortBull[0] = False
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish

    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    s4 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i

    # ⭐️ 10% 상승 실패 시 별 누적기 & 리셋 로직 ⭐️
    s67_counts = np.zeros(len(c), dtype=int)
    current_s67_count = 0
    wait_idx = -1

    for i in range(len(c)):
        if wait_idx != -1:
            if i <= wait_idx + 3:
                # 3봉 이내 10% 상승하면 성공! -> 리셋
                if h[i] >= c[wait_idx] * 1.10:
                    current_s67_count = 0
                    wait_idx = -1
            if i == wait_idx + 3 and wait_idx != -1:
                # 3봉 이후에도 못 오르면 실패! -> 누적
                wait_idx = -1

        if s6[i] or s7[i]: current_s67_count += 1
        if s1[i] or s2[i] or s3[i] or s4[i]:
            s67_counts[i] = current_s67_count
            wait_idx = i

    cond_base = moneyOk & priceOk & volSpike
    
    # ⭐️ 미국장: 오직 S1, S2, S4, S7만 발송하도록 필터링
    hit1 = s1[-1] and cond_base[-1]
    hit2 = s2[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1]
    hit7 = s7[-1] and cond_base[-1]

    if not (hit1 or hit2 or hit4 or hit7): return False, "", df, {}

    if hit4: sig_type = "V (S4: 돌파)"
    elif hit7: sig_type = "V (S7: 눌림)"
    elif hit2: sig_type = "V (S2: 224 재정렬)"
    else: sig_type = "V (S1: 448 재정렬)"

    trust_score = calculate_trust_score(c, e60, s1, s2, s3, s4, s6, s7)

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
    print(f"\n🇺🇸 [일봉 전용] 미국장 4번(눌림목) 스캔 시작!")

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

            # 💡 기존의 except: pass를 없애고 에러 추적 및 프리미엄 카피라이팅 적용
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
                            # ⭐️ 기모으는중 폼 삭제 및 S1/S2/S4/S7 프리미엄 감성 멘트 적용
                            ai_text = generate_ai_report(code, name, False)
                            
                            grade_val = (dbg['score'] // 2) + dbg['s67_count']
                            if grade_val >= 5: premium_grade = "Class S (최상위 기대주)"
                            elif grade_val >= 3: premium_grade = "Class A (우수 기대주)"
                            else: premium_grade = "Class B (관심 기대주)"

                            if "S4" in dbg['sig_type']:
                                intro_title = "🚀 [한 단계 도약을 위한 에너지 발산]"
                                intro_desc = "새로운 궤도로 진입하며 한 단계 레벨업을 시도하는 역동적이고 의미 있는 순간입니다."
                            elif "S7" in dbg['sig_type']:
                                intro_title = "☕ [안정적인 여정을 위한 숨고르기]"
                                intro_desc = "바쁘게 달려온 뒤 건강하게 쉬어가는 자리입니다. 달리는 말에 무리하게 올라타기보다, 물을 마실 때 편안하게 함께하기 좋은 시점입니다."
                            else:
                                intro_title = "🌅 [새로운 흐름의 자연스러운 시작]"
                                intro_desc = "오랜 기간 시장의 테스트를 견뎌내고, 비로소 긍정적인 방향으로 방향타를 돌린 든든한 구간입니다."

                            caption = (
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg['last_close']:.2f}\n\n"
                                f"{intro_title}\n"
                                f"{intro_desc}\n\n"
                                f"📊 [자체 평가 종합 등급]: {premium_grade}\n"
                                f"(수많은 데이터 속에서 기업의 잠재력과 현재의 흐름을 종합한 고유 지표입니다)\n\n"
                                f"⚖️ [건강한 매매를 위한 가이드]\n"
                                f"• 여유로운 접근: 한 번에 조급하게 진입하기보다, 천천히 비중을 늘려가며 마음의 평정을 유지하세요.\n"
                                f"• 원칙 대응: 미리 정해둔 기준 라인(-5%)을 벗어나면 미련 없이 리스크를 관리합니다.\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_text}\n\n"
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

    # 💡 텔레그램 큐가 텅 빌 때까지 강제로 기다려주는 핵심 로직!
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()

    dt = time.time() - t0
    print(f"\n✅ [미국장 4번 V 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ⭐️ 4번 스케줄러 세팅 (11:00, 13:00, 15:00) ⭐️
def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [4번 미국장 검색기] 11:00 / 13:00 / 15:00 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 11 and now_ny.minute == 0) or (now_ny.hour == 13 and now_ny.minute == 0) or (now_ny.hour == 15 and now_ny.minute == 0):
            print(f"🚀 [4번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
