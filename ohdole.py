# Dante_Ohdole_1D_AI_Pro.py
import os, re, time, threading, queue, concurrent.futures
from datetime import datetime, timedelta
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import warnings, urllib3
from bs4 import BeautifulSoup
from io import StringIO
import FinanceDataReader as fdr

from google import genai
from google.genai import types
from dotenv import load_dotenv

# ==========================================
# 🔑 .env 안전 파일 방식 적용
# ==========================================
load_dotenv() 
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("🚨 API 키를 찾을 수 없습니다! .env 파일을 확인해 주세요.")

client = genai.Client(api_key=GEMINI_API_KEY)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

TELEGRAM_TOKEN    = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Pro_System')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ⭐️ AI 에러 원인 추적기 (last_err_msg 버그 픽스) ⭐️
def generate_kr_ai_report(code: str, company_name: str) -> str:
    sector, summary = "정보 없음", "정보 없음"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
    except: pass
                
    try:
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if tags: summary = " ".join([t.text.strip() for t in tags])
    except: pass

    today_date = datetime.now().strftime('%Y년 %m월 %d일')
    prompt = f"""
    너는 여의도의 냉철하고 전문적인 탑 애널리스트야.
    오늘 날짜는 {today_date}이야. 반드시 최신 구글 검색 결과를 바탕으로 팩트 중심의 핵심 투자 메모를 작성해.
    추상적이거나 감정적인 표현은 철저히 배제하고, 기관 보고서처럼 간결하고 명확하게 써.

    [종목 정보]
    - 종목명: {company_name} ({code})
    - 네이버금융 섹터분류: {sector}
    - 에프앤가이드 비즈니스 요약(실적포함): {summary[:1000]}

    [출력 양식]
    1. 섹터 종류: (간단한 설명)
    2. 업계 점유율/규모: (비즈니스 개요 및 지위)
    3. 최근 실적: (요약본에 나타난 실적 증감 및 핵심 지표)
    4. 미래 모멘텀: (주요 사업 파이프라인, 기대감 등)
    5. 기업 전망: (짧고 굵은 전망)
    """
    
    last_err_msg = ""
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash', 
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
            if response and response.text:
                return response.text.strip()
        except Exception as e: 
            last_err_msg = str(e)
            print(f"❌ [{company_name}] AI 에러 (시도 {attempt+1}/3): {last_err_msg}")
            time.sleep(3) 
            
    return f"⚠️ AI 요약 3회 재시도 실패\n(진짜 에러 원인: {last_err_msg})"

def get_krx_list_kind():
    try:
        df_ks = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt", verify=False, timeout=10).text), header=0)[0]
        df_ks['Market'] = 'KOSPI'
        df_kq = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt", verify=False, timeout=10).text), header=0)[0]
        df_kq['Market'] = 'KOSDAQ'
        df = pd.concat([df_ks, df_kq])
        df['Code'] = df['종목코드'].astype(str).str.zfill(6)
        df = df.rename(columns={'회사명': 'Name'})
        return df[~df['Name'].str.contains('스팩|ETN|ETF|우$|홀딩스|리츠', regex=True)][['Code', 'Name', 'Market']].dropna()
    except: return pd.DataFrame()

def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
        img_path, caption = item
        
        safe_caption = caption[:1000] + "\n...(글자수 제한으로 요약됨)" if len(caption) > 1000 else caption
        
        if SEND_TELEGRAM:
            for _ in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, files={"photo": f}, timeout=20, verify=False)
                    if res.status_code == 200: break
                    elif res.status_code == 429: time.sleep(3)
                except: time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

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

def compute_ohdole_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    # 1. 트레이딩뷰 지수이동평균(EMA) 세팅 (3, 10, 20일선 및 신뢰도 점수용 60일선)
    df['EMA3'] = df['Close'].ewm(span=3, adjust=False, min_periods=0).mean()
    df['EMA10'] = df['Close'].ewm(span=10, adjust=False, min_periods=0).mean()
    df['EMA20'] = df['Close'].ewm(span=20, adjust=False, min_periods=0).mean()
    df['EMA60'] = df['Close'].ewm(span=60, adjust=False, min_periods=0).mean()

    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    ema3 = df['EMA3'].values
    ema10 = df['EMA10'].values
    ema20 = df['EMA20'].values
    ema60 = df['EMA60'].values

    # ⭐️ 잡주 필터링 (동전주 및 거래대금 미달 종목 제외)
    money_curr = c * v
    is_money_ok = money_curr >= 100_000_000
    is_price_ok = c >= 1000

    # ==========================================
    # 💡 신버전 시그널 조건 정의
    # ==========================================
    # 조건 1: 완벽한 단기 역배열 상태 (20일선 > 10일선 > 3일선)
    isBearishAlign = (ema20 > ema10) & (ema10 > ema3)

    # 조건 2: 현재 캔들이 양봉 (매수세)
    isBullish = c > o

    # 조건 3: 종가 기준으로 3일선을 상방 돌파 (크로스업)
    prev_c = np.roll(c, 1)
    prev_c[0] = 0
    prev_ema3 = np.roll(ema3, 1)
    prev_ema3[0] = np.inf
    isCrossUp = (prev_c <= prev_ema3) & (c > ema3)

    # ==========================================
    # 💡 최종 시그널 산출
    # ==========================================
    signal = isBearishAlign & isBullish & isCrossUp & is_money_ok & is_price_ok

    if not signal[-1]: 
        return False, "", df, {}

    sig_type = "E (3일선 돌파)"
    trust_score = calculate_trust_score(c, ema60, signal)
    
    # 기존 코드에 남아있던 s67_counts 참조 에러 방지 (오돌이는 해당 로직이 없으므로 0 처리)
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "s67_count": 0}
    
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{int(time.time()*1000)}.png")
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            title = f"[🎯 {dbg['sig_type']}] {code} {name} (1D)\nClose: {dbg['last_close']:,.0f}원"
            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':', rc={'font.family': plt.rcParams['font.family']})
            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            return path
        except: return None

def scan_market_1d():
    stock_list = get_krx_list_kind()
    if stock_list.empty: return

    print(f"\n⚡ [일봉 전용] 한국장 2번(오돌이) 스캔 시작! (무적 방어막 탑재 🛡️)")
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    
    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    
    def worker(row_tuple):
        _, row = row_tuple
        name, code = row["Name"], row["Code"]
        df_raw = None
        is_valid = False
        hit, sig_type, df, dbg = False, "", None, {}
        
        # ⭐️ 일꾼 절대 사망 방지 방어막 (NaN 제거 및 예외처리) ⭐️
        try:
            df_raw = fdr.DataReader(code, start_date)
            if df_raw is not None and not df_raw.empty:
                df_raw = df_raw[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            if is_valid: 
                hit, sig_type, df, dbg = compute_ohdole_1d(df_raw)
        except Exception:
            pass # 계산 꼬이는 불량주식 무시하고 무조건 전진

        hit_rank = 0
        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")
            if hit:
                tracker['hits'] += 1
                hit_rank = tracker['hits']
                
        if hit:
            chart_path = save_chart(df, code, name, hit_rank, dbg)
            if chart_path:
                ai_fact_check = generate_kr_ai_report(code, name)
                caption = (
                    f"🎯 [{dbg['sig_type']}]\n\n"
                    f"🏢 {name} ({code})\n"
                    f"💰 현재가: {dbg['last_close']:,.0f}원\n"
                    f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                    f"📉 [매수/손절 전략]\n"
                    f"- 양봉 길이만큼 분할매수\n"
                    f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                    f"⭐ 알고리즘 신뢰도: {dbg['score']} / 10점\n\n"
                    f"💡 [기업 팩트체크]\n"
                    f"{ai_fact_check}\n\n"
                    f"⚠️ [전문가 코멘트]\n"
                    f"본 분석은 실시간 데이터 기반 팩트 요약본입니다. 시장 상황과 개인의 관점에 따라 해석이 다를 수 있으므로, 반드시 개별적인 추가 분석을 권장합니다.\n"
                    f"\n💬 이 종목이 궁금하다면 채팅창에 '/질문 내용' 을 입력해 보세요!"
                )
                telegram_queue.put((chart_path, caption))

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        executor.map(worker, list(stock_list.iterrows()))
        
    # ⭐️ 텔레그램 전송 완료 보장 대기 ⭐️
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()
        
    print(f"\n✅ [한국장 2번 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

# ⭐️ 2번 스케줄러 세팅 (09:30, 12:00, 14:30) ⭐️
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [2번 검색기] 09:30 / 12:00 / 14:30 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 9 and now_kr.minute == 30) or (now_kr.hour == 12 and now_kr.minute == 0) or (now_kr.hour == 14 and now_kr.minute == 30):
            print(f"🚀 [2번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    # run_scheduler()  # 💡 스케줄러를 잠시 끄고
    scan_market_1d()   # 🚀 스캔 함수를 직접 호출하여 즉시 실행
