# Dante_KRX_Bowl_1D_AI_Pro.py
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
import traceback
from google import genai

# ==========================================
# 🔑 Gemini API 키 세팅
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Pro_System')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ⭐️ AI 리포트 생성기 (최신 2.5-flash로 안정화)
def generate_kr_ai_report(code: str, company_name: str) -> str:
    sector, summary = "정보 없음", "정보 없음"
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
        아래 한국 주식 데이터를 바탕으로 팩트 중심의 짦은 요약 메모를 작성해.
        [종목 정보]
        - 종목명: {company_name} ({code})
        - 섹터: {sector}
        - 비즈니스 요약: {summary[:800]}

        [출력 양식]
        1. 섹터: (간단설명)
        2. 지위: (점유율/규모)
        3. 실적: (핵심지표)
        4. 모멘텀: (기대감)
        5. 전망: (짧고 굵게)
        """
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        return response.text.strip()
    except Exception as e:
        print(f"❌ AI 생성 에러 ({code}): {e}")
        return "⚠️ 기업 팩트 데이터를 불러오거나 AI 요약 중 오류가 발생했습니다."

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

# ⭐️ 혁신 포인트: 발송 데몬이 AI를 호출하고 발송까지 책임집니다. 스캐너는 터치 안 함!
def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
        chart_path, code, name, dbg = item  # 캡션 완성본이 아니라 기본 데이터만 받아옴

        if SEND_TELEGRAM:
            print(f"🤖 [{name}] 텔레그램 발송 준비 및 AI 요약 중...")
            # 여기서 AI 리포트 생성 (스캐너 속도에 전혀 지장 없음)
            ai_fact_check = generate_kr_ai_report(code, name)
            
            caption = (
                f"🎯 [{dbg['sig_type']}]\n\n"
                f"🏢 {name} ({code})\n"
                f"💰 현재가: {dbg['last_close']:,.0f}원\n"
                f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                f"📉 [매수/손절 전략]\n"
                f"- 양봉 길이만큼 분할매수\n"
                f"- 마지막 분할매수에서 -5% 손절\n\n"
                f"⭐ 신뢰도: {dbg['score']} / 10점\n\n"
                f"💡 [팩트체크]\n"
                f"{ai_fact_check}\n\n"
                f"💬 이 종목이 궁금하다면 채팅창에 '/질문 내용' 을 입력해 보세요!"
            )

            # 텔레그램 1024자 제한 방어
            safe_caption = caption[:1000] + "\n...(글자수 제한으로 요약됨)" if len(caption) > 1000 else caption

            for _ in range(3):
                try:
                    with open(chart_path, 'rb') as f:
                        res = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, files={"photo": f}, timeout=20, verify=False)
                    if res.status_code == 200:
                        print(f"✅ [{name}] 텔레그램 발송 성공!")
                        break
                    elif res.status_code == 429: time.sleep(3)
                    else: 
                        print(f"❌ 텔레그램 발송 거부됨: {res.text}")
                        break
                except Exception as e: 
                    print(f"❌ 텔레그램 통신 에러: {e}")
                    time.sleep(2)
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

def compute_bobgeureut(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    
    ma20 = pd.Series(c).rolling(20, min_periods=1).mean().values
    stddev = pd.Series(c).rolling(20, min_periods=1).std(ddof=1).values
    bbUpper = ma20 + (stddev * 2)

    tenkan = (pd.Series(h).rolling(9, min_periods=1).max() + pd.Series(l).rolling(9, min_periods=1).min()) / 2
    kijun = (pd.Series(h).rolling(26, min_periods=1).max() + pd.Series(l).rolling(26, min_periods=1).min()) / 2
    spanA = (tenkan + kijun) / 2
    spanB = (pd.Series(h).rolling(52, min_periods=1).max() + pd.Series(l).rolling(52, min_periods=1).min()) / 2
    senkou1 = np.roll(spanA.values, 25); senkou1[:25] = np.nan
    senkou2 = np.roll(spanB.values, 25); senkou2[:25] = np.nan
    cloudTop = np.fmax(senkou1, senkou2)

    mean120 = pd.Series(c).rolling(120, min_periods=1).mean().values
    std120 = pd.Series(c).rolling(120, min_periods=1).std(ddof=1).values
    mean120_s = np.roll(mean120, 5); mean120_s[:5] = np.nan
    std120_s = np.roll(std120, 5); std120_s[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox6m = (std120_s / mean120_s) < 0.20

    mean60 = pd.Series(c).rolling(60, min_periods=1).mean().values
    std60 = pd.Series(c).rolling(60, min_periods=1).std(ddof=1).values
    mean60_s = np.roll(mean60, 5); mean60_s[:5] = np.nan
    std60_s = np.roll(std60, 5); std60_s[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox3m = (std60_s / mean60_s) < 0.20

    isCat2 = condBox6m
    isCat1 = (~condBox6m) & condBox3m
    hasBox = isCat1 | isCat2

    ema10, ema20, ema30, ema60, ema112, ema224 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values, df['EMA112'].values, df['EMA224'].values
    condPrice = c >= 1000
    isBullish = c > o
    prev_c = np.roll(c, 1); prev_c[0] = np.inf
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    condEma = (c > ema224) & (prev_c < prev_ema224 * 1.05)
    condCloud = (c > cloudTop) & (~np.isnan(cloudTop))
    condBb = c >= bbUpper * 0.98
    volAvg = pd.Series(v).rolling(20, min_periods=1).mean().values
    condVol = v > volAvg * 2.0
    condNotOverheated = c <= ema224 * 1.15
    
    v_1 = np.roll(v, 1); v_1[0] = 0
    v_2 = np.roll(v, 2); v_2[:2] = 0
    v_3 = np.roll(v, 3); v_3[:3] = 0
    avgVol3 = (v_1 + v_2 + v_3) / 3
    condVolSpike = v >= (avgVol3 * 5)

    signalBase = condPrice & isBullish & condEma & condCloud & condBb & condVol & condNotOverheated & hasBox & condVolSpike
    if not signalBase[-1]: return False, "", df, {}

    signalCat2 = signalBase & isCat2
    signalCat1 = signalBase & isCat1
    isAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224)

    if (signalCat2 & isAligned)[-1] or (signalCat1 & isAligned)[-1]: sig_type = "B (J 강조)"
    else: sig_type = "B (일반)"

    trust_score = calculate_trust_score(c, ema60, signalBase)
    return True, sig_type, df, {"last_close": float(c[-1]), "score": trust_score, "sig_type": sig_type}

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
    token = get_ls_token()
    if not token: return

    print(f"\n⚡ [일봉 전용] 한국장 B 스캔 시작! (속도 원상복구 & 텔레그램 100% 보장)")
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
        if is_valid: hit, sig_type, df, dbg = compute_bobgeureut(df_raw)

        hit_rank = 0
        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")
            if hit:
                tracker['hits'] += 1
                hit_rank = tracker['hits']

        # ⭐️ 스캐너는 캡션을 만들지 않고 데이터만 대기줄에 던지고 바로 다음 종목으로 넘어감! (속도 저하 0%)
        if hit:
            chart_path = save_chart(df, code, name, hit_rank, dbg)
            if chart_path:
                telegram_queue.put((chart_path, code, name, dbg))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(worker, list(stock_list.iterrows()))
    print(f"\n✅ [한국장 B 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [한국장 B 스케줄러] 09:00 / 11:00 / 14:00 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if now_kr.hour in [9, 11, 14] and now_kr.minute == 0:
            print(f"🚀 [B 1D 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d() # 즉시 1회 스캔 
    run_scheduler()
