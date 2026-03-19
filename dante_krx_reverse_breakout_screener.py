# Dante_KRX_Reverse_Breakout_1D_AI_Pro.py
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
    ema112, ema224, ema448, ema60 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values, df['EMA60'].values
    av3 = df['AvgVol3'].values

    moneyOk = (c * v) >= 100_000_000
    priceOk = c >= 1000
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
    
    # ⭐️ 3봉 내 15% 상승 실패 시 누적, 성공 시 리셋 로직 ⭐️
    p_counts = np.zeros(len(c), dtype=int)
    current_p_count = 0
    wait_idx = -1

    for i in range(len(c)):
        # 추세가 완전히 우상향(정배열)으로 바뀌면 카운트 초기화
        if condBullAlign[i]: 
            current_p_count = 0
            wait_idx = -1

        if wait_idx != -1:
            # 타점 발생 후 3봉 이내에 고가가 15% 이상 상승했는지 체크
            if i <= wait_idx + 3:
                if h[i] >= c[wait_idx] * 1.15: # 15% 달성 시 리셋 (시세 분출 완료)
                    current_p_count = 0
                    wait_idx = -1
            # 3봉이 지났는데도 15% 도달을 못했으면 누적 유지 (에너지 응축 중)
            if i == wait_idx + 3 and wait_idx != -1:
                wait_idx = -1

        # 타점 발생 시 카운트 올리고 대기열에 올림
        if signalBase[i]:
            current_p_count += 1
            wait_idx = i
            
        p_counts[i] = current_p_count

    sig_type = "P (연속)" if p_counts[-1] > 1 else "P (신규)"
    trust_score = calculate_trust_score(c, ema60, signalBase)
    
    # 💡 p_count(누적 횟수)를 텔레그램으로 넘겨줌
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "p_count": int(p_counts[-1])}

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

    print(f"\n⚡ [일봉 전용] 한국장 3번(역매공파) 스캔 시작! (무적 방어막 탑재 🛡️)")
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
                hit, sig_type, df, dbg = compute_inverse_1d(df_raw)
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
                            ai_fact_check = generate_ai_report(code, name)
                            
                            # 💡 계산된 누적 횟수를 가져옵니다.
                            p_count = dbg.get('p_count', 1)
                            
                            # ⭐️ 3회 이상 누적 시 : 강력한 시세 분출 임박 카피라이팅
                            if p_count >= 3:
                                intro_title = "🌟 [진입타점]"
                                intro_desc = "기준을 잡고 거기에 맞춰서 대응하고 매매하기."
                            # 1~2회 포착 시 : 기존 턴어라운드 초기 카피라이팅
                            else:
                                intro_title = "💎 [관심종목]"
                                intro_desc = "무관심할때 조금씩 관심 가져주기."

                            caption = (
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg['last_close']:.2f}\n\n"
                                f"{intro_title}\n"
                                f"{intro_desc}\n\n"
                                f"⚖️ [건강한 투자를 위한 기준]\n"
                                f"• 관심종목 편입: 타이밍이 올때까지 천천히 기다리세요.\n"
                                f"• 단기 진입 시: 실전 매매에 참여하신다면, 진입 시가 이탈 시 칼 같은 손절 필수.\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_fact_check}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요."
                            )
                            telegram_queue.put((chart_path, caption))
                            print(f"\n✅ [{name}] 텔레그램 전송 대기열에 추가 완료 (누적 타점: {p_count}회)")
        
    # ⭐️ 텔레그램 전송 완료 보장 대기 ⭐️
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()
        
    print(f"\n✅ [한국장 3번 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

# ⭐️ 3번 스케줄러 세팅 (10:00, 12:30, 15:00) ⭐️
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [3번 검색기] 10:00 / 12:30 / 15:00 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 10 and now_kr.minute == 0) or (now_kr.hour == 12 and now_kr.minute == 30) or (now_kr.hour == 15 and now_kr.minute == 0):
            print(f"🚀 [3번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
