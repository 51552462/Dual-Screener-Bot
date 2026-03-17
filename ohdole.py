# Dante_Ohdole_1D_LS_Pro.py
import os, re, time, json, threading, queue, concurrent.futures
from datetime import datetime
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
from requests.adapters import HTTPAdapter
import warnings, urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

TELEGRAM_TOKEN    = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

# ================== LS증권 OpenAPI 세팅 ==================
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

def shorten_text(text):
    if not text or text == "정보 없음": return "특이사항 없음"
    res = text.split('.')[0].strip()
    return res[:40] + "..." if len(res) > 40 else res

def get_company_fact_report(code: str) -> tuple:
    sector, outlook, growth = "정보 없음", "정보 없음", "정보 없음"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if len(tags) >= 1: outlook = shorten_text(tags[0].text.strip())
            if len(tags) >= 2: growth = shorten_text(tags[1].text.strip())
    except: pass
    return sector, outlook, growth

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
                except: time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()
threading.Thread(target=telegram_sender_daemon, daemon=True).start()

def calculate_trust_score(c, e60, *sig_arrays):
    score = 5 
    lookback = min(100, len(c))
    for i in range(len(c) - lookback, len(c) - 1):
        is_sig = any(arr[i] for arr in sig_arrays)
        if is_sig:
            valid = True
            entry_price = c[i]
            for j in range(i + 1, len(c)):
                if c[j] < e60[j] or c[j] >= entry_price * 1.15:
                    valid = False
                    break
            if valid: score += 2 
    return min(10, score) 

def compute_ohdole_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    df['MA112'] = df['Close'].rolling(window=112).mean()
    df['MA224'] = df['Close'].rolling(window=224).mean()
    df['MA448'] = df['Close'].rolling(window=448).mean()

    c, o, h, v = df['Close'].values, df['Open'].values, df['High'].values, df['Volume'].values
    ma5, ma60, ma112, ma224, ma448 = df['MA5'].values, df['MA60'].values, df['MA112'].values, df['MA224'].values, df['MA448'].values

    money_curr = c * v
    is_downtrend = (ma448 > ma224) & (ma224 > ma112)
    is_basement = c < ma112
    is_env_ok = is_downtrend & is_basement

    prev_vol = np.roll(v, 1); prev_vol[0] = np.inf
    is_vol_ok = v >= (prev_vol * 1.0)
    is_money_ok = money_curr >= 100_000_000
    is_price_ok = c >= 1000
    is_power_ok = is_vol_ok & is_money_ok & is_price_ok

    prev_ma5 = np.roll(ma5, 1); prev_ma5[0] = np.inf
    prev_c = np.roll(c, 1); prev_c[0] = 0
    is_breakout = (c > ma5) & (prev_c <= prev_ma5)
    
    prev_high1 = np.roll(h, 1); prev_high1[0] = np.inf
    prev_high2 = np.roll(h, 2); prev_high2[:2] = np.inf
    high_prev_2 = np.maximum(prev_high1, prev_high2)
    is_engulfing = (c > o) & (c > high_prev_2)
    
    sig_1 = is_env_ok & is_power_ok & is_breakout & is_engulfing

    is_yangbong = c > o
    threshold = o + ((c - o) * 0.33)
    is_riding = ma5 <= threshold
    sig_2 = is_env_ok & is_power_ok & is_yangbong & is_riding & (~sig_1)

    sig1_hit, sig2_hit = sig_1[-1], sig_2[-1]
    if not (sig1_hit or sig2_hit): return False, "", df, {}

    sig_type = "🔥 E (장악형)" if sig1_hit else "✅ E (안착형)"
    trust_score = calculate_trust_score(c, ma60, sig_1, sig_2)

    return True, sig_type, df, {"last_close": float(c[-1]), "score": trust_score}

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{int(time.time()*1000)}.png")
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            title = f"[{dbg['sig_type']}] {code} {name} (1D)\nClose: {dbg['last_close']:,.0f}원"
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

    print(f"\n⚡ [일봉 전용] 한국장 E(오돌이) LS OpenAPI 스캔 시작!")
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
        if is_valid: hit, sig_type, df, dbg = compute_ohdole_1d(df_raw)

        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")
            if hit:
                tracker['hits'] += 1
                chart_path = save_chart(df, code, name, tracker['hits'], dbg)
                if chart_path:
                    sector, outlook, growth = get_company_fact_report(code)
                    caption = (
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg['last_close']:,.0f}원\n"
                        f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                        f"📉 [매수/손절 전략]\n"
                        f"- 양봉 길이만큼 분할매수\n"
                        f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                        f"⭐ 알고리즘 신뢰도: {dbg['score']} / 10점\n\n"
                        f"💡 [기업 팩트체크]\n"
                        f"🔸 섹터: {sector}\n"
                        f"🔸 전망: {outlook}\n"
                        f"🔸 실적: {growth}\n"
                    )
                    telegram_queue.put((chart_path, caption))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(worker, list(stock_list.iterrows()))
    print(f"\n✅ [한국장 E 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [한국장 E 스케줄러 (1D 전용)] 09:30 / 11:30 / 14:30 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 9 and now_kr.minute == 10) or \
           (now_kr.hour == 11 and now_kr.minute == 10) or \
           (now_kr.hour == 14 and now_kr.minute == 10):
            print(f"🚀 [E 1D 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
