# Dante_Nulrim_1D_LS_AI_Interactive_Pro.py
import os, re, time, json, threading, queue, concurrent.futures
from datetime import datetime
import pytz
from io import StringIO
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
from requests.adapters import HTTPAdapter
import warnings, urllib3
from bs4 import BeautifulSoup

# ⭐️ 구글 최신 통합 라이브러리로 세대 교체 ⭐️
from google import genai

# ==========================================
# 🔑 Gemini API 키 세팅 (여기에 대표님 키 입력!)
# ==========================================
GEMINI_API_KEY = "AIzaSyAagV9SDlZ72CUmYK8JDZaP937CeHrqV7Q"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

TELEGRAM_TOKEN    = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

APP_KEY = "PSIY0DPy5PI0DMO2VN8T5bg9V37DRQSLwVu2"
APP_SECRET = "4Hj8Exqp92VH3gZ2INjjOMhK7VHtBUDz"

def get_ls_token():
    url = "https://openapi.ls-sec.co.kr:8080/oauth2/token"
    headers = {"content-type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecretkey": APP_SECRET, "scope": "oob"}
    try:
        res = requests.post(url, headers=headers, data=data, timeout=10, verify=False)
        if res.status_code == 200: return res.json().get("access_token")
    except: pass
    return None

class LSApiRateLimiter:
    def __init__(self):
        self.timestamps = []
        self.lock = threading.Lock()
    def wait(self):
        with self.lock:
            now = time.time()
            self.timestamps = [t for t in self.timestamps if now - t < 1.05]
            if len(self.timestamps) >= 3:
                sleep_time = 1.05 - (now - self.timestamps[0])
                if sleep_time > 0: time.sleep(sleep_time)
            self.timestamps.append(time.time())

ls_limiter = LSApiRateLimiter()
global_session = requests.Session()
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=1)
global_session.mount('https://', adapter)

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ⭐️ [기존] 한국장 실시간 팩트 요약기 (최신 모델 적용) ⭐️
def generate_kr_ai_report(code: str, company_name: str) -> str:
    sector = "정보 없음"
    summary = "정보 없음"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
                
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if tags: summary = " ".join([t.text.strip() for t in tags])

        prompt = f"""
        너는 여의도의 냉철하고 전문적인 탑 애널리스트야.
        아래 한국 주식의 실제 크롤링 데이터를 바탕으로 팩트 중심의 핵심 투자 메모를 작성해.
        추상적이거나 감정적인 표현은 철저히 배제하고, 기관 보고서처럼 간결하고 명확하게 써.

        [종목 정보]
        - 종목명: {company_name} ({code})
        - 네이버금융 섹터분류: {sector}
        - 에프앤가이드 비즈니스 요약(실적포함): {summary[:1000]}

        [출력 양식] (반드시 아래 번호와 항목명에 맞춰서 작성할 것)
        1. 섹터 종류: (간단한 설명)
        2. 업계 점유율/규모: (비즈니스 개요 및 지위)
        3. 최근 실적: (요약본에 나타난 실적 증감 및 핵심 지표)
        4. 미래 모멘텀: (주요 사업 파이프라인, 기대감 등)
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

# ⭐️ [신규 추가] 양방향 Q&A 텔레그램 리스너 ⭐️
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
                            prompt = f"너는 여의도의 냉철한 탑 애널리스트야. 다음 주식 관련 질문에 팩트 기반으로 짧고 명확하게 답변해줘. 종목 추천은 하지 말고 분석만 제공해.\n질문: {question}"
                            ai_res = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                            
                            reply_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                            requests.post(reply_url, json={"chat_id": chat_id, "text": f"🤖 [AI 비서 팩트체크]\n\n{ai_res.text.strip()}", "reply_to_message_id": msg.get("message_id")})
        except Exception as e:
            time.sleep(2)
        time.sleep(2)

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
        if SEND_TELEGRAM:
            for _ in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": caption}, files={"photo": f}, timeout=20, verify=False)
                    if res.status_code == 200: break
                    elif res.status_code == 429: time.sleep(3)
                    else: break 
                except Exception as e: time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

# 두 개의 일꾼을 백그라운드에 가동
threading.Thread(target=telegram_sender_daemon, daemon=True).start()
# threading.Thread(target=telegram_interactive_daemon, daemon=True).start()

MIN_PRICE = 1000                 
MIN_TRANS_MONEY = 100_000_000  

def calculate_trust_score(c, e60, s1_arr, s2_arr, s3_arr, s4_arr, s5_arr, s6_arr, s7_arr):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   

    lookback = min(100, len(c))
    for i in range(len(c) - lookback, len(c) - 1):
        if s1_arr[i] or s2_arr[i] or s3_arr[i] or s4_arr[i] or s5_arr[i] or s6_arr[i] or s7_arr[i]:
            valid = True
            entry_price = c[i]
            for j in range(i + 1, len(c)):
                if c[j] < e60[j] or c[j] >= entry_price * 1.15:
                    valid = False; break
            if valid: score += 2 
    return max(1, min(10, score))

def compute_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    df['Lowest5'] = df['Low'].rolling(5).min()

    c, o, v = df['Close'].values, df['Open'].values, df['Volume'].values
    av3 = df['AvgVol3'].values
    lowest5 = df['Lowest5'].values
    
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    moneyOk = (c * v) >= MIN_TRANS_MONEY
    priceOk = c >= MIN_PRICE
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
    dipped20 = lowest5 < e20
    raw_s5 = align448 & (~prev_align448) & dipped20 & (c > e10) & isBullish & (~s1)

    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1); prev_shortBull[0] = False
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish

    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    s4 = np.zeros_like(c, dtype=bool)
    s5 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i
        if raw_s5[i] and not s4[i] and (i - last_pullback_bar > 5):
            s5[i] = True
            last_pullback_bar = i

    cond_base = moneyOk & priceOk & volSpike
    hit1 = s1[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1]
    hit7 = s7[-1] and cond_base[-1]

    if not (hit1 or hit4 or hit7): return False, "", df, {}

    if hit7: sig_type = "V (S7: 중기턴)"
    elif hit4: sig_type = "V (S4: 돌파)"
    else: sig_type = "V (S1: 448 재정렬)"

    trust_score = calculate_trust_score(c, e60, s1, s2, s3, s4, s5, s6, s7)

    return True, sig_type, df, {"last_close": float(c[-1]), "score": trust_score}

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{timestamp_ms}.png")
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
    token = get_ls_token()
    if not token: return

    print(f"\n⚡ [일봉 전용] LS증권 V 스캔 (AI 양방향 비서 가동중) 시작!")

    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    url = "https://openapi.ls-sec.co.kr:8080/stock/chart"
    tr_cd = "t8413"
    
    def worker(row_tuple):
        _, row = row_tuple
        name, code = row["Name"], row["Code"]
        body = {f"{tr_cd}InBlock": {"shcode": code, "gubun": "2", "qrycnt": 600, "sdate": "", "edate": "99999999", "comp_yn": "N"}}
        headers = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {token}", "tr_cd": tr_cd, "tr_cont": "N"}

        df_raw = None
        for _ in range(5): 
            ls_limiter.wait()
            try:
                res = global_session.post(url, headers=headers, data=json.dumps(body), timeout=7, verify=False)
                if res.status_code == 200:
                    data = res.json()
                    if "IGW" in data.get("rsp_cd", "") or data.get("rsp_msg", "") == "조회건수제한":
                        time.sleep(2); continue 
                    items = data.get("t8413OutBlock1", [])
                    if items:
                        records = [{'Date': pd.to_datetime(r.get('date', '') + '000000', format='%Y%m%d%H%M%S'), 'Open': float(r.get('open', 0)), 'High': float(r.get('high', 0)), 'Low': float(r.get('low', 0)), 'Close': float(r.get('close', 0)), 'Volume': float(r.get('jdiff_vol', r.get('volume', 0)))} for r in items]
                        df_raw = pd.DataFrame(records).dropna(subset=['Date']).sort_values('Date').reset_index(drop=True).set_index('Date')
                    break
            except: time.sleep(1)

        is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
        hit, sig_type, df, dbg = False, "", None, {}
        if is_valid: hit, sig_type, df, dbg = compute_signal(df_raw)

        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

            if hit:
                tracker['hits'] += 1
                chart_path = save_chart(df, code, name, tracker['hits'], dbg)
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
                        f"\n💬 궁금한 점이 있다면 채팅창에 '/질문 내용' 을 입력해 보세요!"
                    )
                    telegram_queue.put((chart_path, caption))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(worker, list(stock_list.iterrows()))
    print(f"\n✅ [5번 봇: KRX V 스캔 완료] 포착: {tracker['hits']}개\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [5번 봇: 한국장 V 스케줄러] 09:40 / 11:40 / 14:40 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if now_kr.hour in [9, 11, 14] and now_kr.minute == 40:
            print(f"🚀 [KRX V 1D 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
