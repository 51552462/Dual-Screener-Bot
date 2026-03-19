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
import random

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

TELEGRAM_TOKEN    = "8004222500:AAFS9rPPtiQiNx4SxGgYOnODFGULqLTNO8M"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

# ⭐️ 당일 중복 발송 방지용 기억 장치 추가
sent_today = set()
last_run_date = ""

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Pro_System')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

ai_request_lock = threading.Lock()

def generate_kr_ai_report(code: str, company_name: str) -> str:
    sector = "정보 없음"
    summary_parts = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 1. 팩트 데이터 크롤링 (네이버 & 에프앤가이드)
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
            if tags: 
                summary_parts = [t.text.strip() for t in tags]
    except: pass

    # 2. 팩트 데이터 정제 (전망은 버리고 실적만 추출)
    performance = "실적 팩트가 제공되지 않았습니다."
    
    if len(summary_parts) >= 2:
        performance = summary_parts[1].replace("동사는", f"[{company_name}]은(는)")
    elif len(summary_parts) == 1:
        performance = summary_parts[0].replace("동사는", f"[{company_name}]은(는)")

    # 3. 전망을 없앤 초간단 팩트체크 구성
    final_report = (
        f"💡 [기업 핵심 팩트 (FnGuide 공식 데이터)]\n"
        f"📌 주요 섹터/테마: {sector}\n\n"
        f"📈 [최근 실적 (우상향 여부)]\n"
        f"✔️ {performance}"
    )

    return final_report
    
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
    
    # 1. 트레이딩뷰 지수이동평균(EMA) 세팅 (3, 10일선 및 신뢰도 점수용 60일선)
    df['EMA3'] = df['Close'].ewm(span=3, adjust=False, min_periods=0).mean()
    df['EMA10'] = df['Close'].ewm(span=10, adjust=False, min_periods=0).mean()
    df['EMA60'] = df['Close'].ewm(span=60, adjust=False, min_periods=0).mean()

    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    ema3 = df['EMA3'].values
    ema10 = df['EMA10'].values
    ema60 = df['EMA60'].values

    # ⭐️ 잡주 필터링 (동전주 및 거래대금 미달 종목 제외)
    money_curr = c * v
    is_money_ok = money_curr >= 100_000_000
    is_price_ok = c >= 1000

    # ==========================================
    # 💡 신버전 시그널 조건 정의 (3/10 단기 정배열 턴 시그널)
    # ==========================================
    # 조건 1: 현재 캔들이 무조건 양봉일 때만 (매수세 유입)
    isBullish = c > o

    # 조건 2: 3일선이 10일선을 상향 돌파 (골든크로스)하며 정배열 시작
    prev_ema3 = np.roll(ema3, 1)
    prev_ema3[0] = 0
    prev_ema10 = np.roll(ema10, 1)
    prev_ema10[0] = np.inf
    
    isCrossUp = (prev_ema3 <= prev_ema10) & (ema3 > ema10)

    # ==========================================
    # 💡 최종 시그널 산출
    # ==========================================
    # 양봉 + 3/10 골든크로스 + 거래대금 조건 동시 만족 시
    signal = isBullish & isCrossUp & is_money_ok & is_price_ok

   if not signal[-1]: 
        return False, "", df, {}

    # 💡 괄호 내용 지우고 'E' 로만 출력되도록 수정
    sig_type = "E"
    trust_score = calculate_trust_score(c, ema60, signal)
    
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
    # 💡 위에서 만든 기억 장치를 스캔 함수 안으로 불러옵니다.
    global sent_today, last_run_date
    
    kr_tz = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(kr_tz).strftime('%Y-%m-%d')
    
    # ⭐️ 매일 자정이 지나 날짜가 바뀌면, 어제 보냈던 발송 기록을 깨끗하게 지웁니다.
    if today_str != last_run_date:
        sent_today.clear()
        last_run_date = today_str

    stock_list = get_krx_list_kind()
    if stock_list.empty: return

    print(f"\n⚡ [일봉 전용] 한국장 2번(오돌이) 스캔 시작! (당일 중복 발송 방지 가동 🛡️)")
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
        
        try:
            df_raw = fdr.DataReader(code, start_date)
            if df_raw is not None and not df_raw.empty:
                df_raw = df_raw[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            if is_valid: 
                hit, sig_type, df, dbg = compute_ohdole_1d(df_raw)
        except Exception:
            pass 

        hit_rank = 0
        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 당일 신규 포착: {tracker['hits']}개)")
            
            # ⭐️ 핵심: 당일 중복 발송 차단 로직
            if hit:
                if code in sent_today:
                    hit = False # 오늘 오전이나 방금 전 턴에 이미 보냈던 종목이면 취소하고 조용히 넘어갑니다.
                else:
                    tracker['hits'] += 1
                    hit_rank = tracker['hits']
                    sent_today.add(code) # 신규 포착된 종목은 '오늘 보낸 명단'에 도장을 찍어둡니다.
                
        if hit:
            chart_path = save_chart(df, code, name, hit_rank, dbg)
            if chart_path:
                ai_fact_check = generate_kr_ai_report(code, name)
                caption = (
                    f"🎯 [{dbg['sig_type']}]\n\n"
                    f"🏢 {name} ({code})\n"
                    f"💰 현재가: {dbg['last_close']:,.0f}원\n"
                    f"🎯 추천: 단타, 스윙\n\n"
                    f"📉 [매수/손절 전략]\n"
                    f"- 양봉 길이만큼 분할매수\n"
                    f"- 양봉 시가 이탈 or 진입 후 2봉 연속 음봉 발생 시 손절\n\n"
                    f"{ai_fact_check}\n\n"
                    f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요."
                )
                telegram_queue.put((chart_path, caption))

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()
        
    print(f"\n✅ [한국장 2번 스캔 완료] 신규 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

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
