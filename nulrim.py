# Dante_Nulrim_1D_LS_Sniper_V2_NewLogic.py
import os
import re
import time
import json
import threading
import queue
import concurrent.futures
from datetime import datetime
import pytz
from io import StringIO
import numpy as np
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg') # GUI 메모리 누수 완벽 차단
import matplotlib.pyplot as plt
import requests
from requests.adapters import HTTPAdapter
import warnings
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# ================== Telegram ==================
TELEGRAM_TOKEN    = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

# ================== LS증권 OpenAPI 세팅 ==================
APP_KEY = "PSIY0DPy5PI0DMO2VN8T5bg9V37DRQSLwVu2"
APP_SECRET = "4Hj8Exqp92VH3gZ2INjjOMhK7VHtBUDz"

def get_ls_token():
    print("🔑 LS증권 OpenAPI 인증 토큰 발급 중...")
    url = "https://openapi.ls-sec.co.kr:8080/oauth2/token"
    headers = {"content-type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecretkey": APP_SECRET, "scope": "oob"}
    try:
        res = requests.post(url, headers=headers, data=data, timeout=10, verify=False)
        if res.status_code == 200:
            print("✅ LS API 통신망 연결 성공!")
            return res.json().get("access_token")
    except Exception as e:
        print(f"❌ 토큰 발급 에러: {e}")
    return None

# ================== 스마트 속도 제어기 ==================
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

# ================== 폴더 및 유틸 ==================
TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ================== 기업 팩트 리포트 ==================
def get_company_fact_report(code: str) -> tuple:
    sector, outlook, growth = "정보 없음", "기업 현황 데이터를 불러올 수 없습니다.", "최근 실적 데이터를 불러올 수 없습니다."
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            tag = BeautifulSoup(res_naver.text, 'html.parser').select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
                
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if len(tags) >= 1: outlook = tags[0].text.strip()
            if len(tags) >= 2: growth = tags[1].text.strip()
    except: pass
    return sector, outlook, growth

# ================== KRX 종목 수집 ==================
def get_krx_list_kind():
    print("KRX KIND 서버에서 종목 리스트를 가져옵니다...")
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

# ================== 텔레그램 데몬 ==================
def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
        img_path, caption = item
        if len(caption) > 1000: caption = caption[:980] + "\n\n...(내용이 너무 길어 생략됨)"

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

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

# ================== ⭐️ 신규 눌림목 로직 (트레이딩뷰 100% 동기화) ==================
MIN_PRICE = 1000                 
MIN_TRANS_MONEY = 100_000_000  

def compute_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_data", df_raw, {}

    df = df_raw.copy()
    
    # 1. EMA 설정
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    df['Lowest5'] = df['Low'].rolling(5).min()

    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    av3 = df['AvgVol3'].values
    lowest5 = df['Lowest5'].values
    
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # ⭐️ 스캐너용 기본 안전 필터 (거래대금 1억 이상, 3배 거래량 터진 양봉)
    moneyOk = (c * v) >= MIN_TRANS_MONEY
    priceOk = c >= MIN_PRICE
    with np.errstate(invalid='ignore'):
        volSpike = v >= (np.nan_to_num(av3, nan=1.0) * 3)
    isBullish = c > o

    # 3. 배열 상태 정의
    align112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    align224 = align112 & (e112 > e224)
    align448 = align224 & (e224 > e448)

    # 4. 장기 기준선 유지 상태 정의
    longKeep448 = e224 > e448 
    longKeep224 = e112 > e224 
    longKeep112 = e60 > e112  

    prev_align448 = np.roll(align448, 1); prev_align448[0] = False
    prev_align224 = np.roll(align224, 1); prev_align224[0] = False
    prev_align112 = np.roll(align112, 1); prev_align112[0] = False
    
    prev_longKeep448 = np.roll(longKeep448, 1); prev_longKeep448[0] = False
    prev_longKeep224 = np.roll(longKeep224, 1); prev_longKeep224[0] = False
    prev_longKeep112 = np.roll(longKeep112, 1); prev_longKeep112[0] = False

    # 5. 기본 시그널 (S1, S2, S3)
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    s2 = align224 & (~prev_align224) & prev_longKeep224 & (e224 < e448) & isBullish
    s3 = align112 & (~prev_align112) & prev_longKeep112 & (e112 < e224) & isBullish

    # 6. 정밀 필터링 돌파 시그널 (S4, S5) 조건 검사
    prev_c = np.roll(c, 1); prev_c[0] = 0
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    dipped20 = lowest5 < e20
    raw_s5 = align448 & (~prev_align448) & dipped20 & (c > e10) & isBullish & (~s1)

    # ⭐️ 쿨타임 (5봉) 적용 시뮬레이터
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

    # 7. 최종 타점 판별 (스캐너 필터와 결합)
    cond_base = moneyOk & priceOk & volSpike
    
    hit1 = s1[-1] and cond_base[-1]
    hit2 = s2[-1] and cond_base[-1]
    hit3 = s3[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1]
    hit5 = s5[-1] and cond_base[-1]

    if not (hit1 or hit2 or hit3 or hit4 or hit5):
        return False, "no_signal", df, {}

    if hit5: sig_type = "S5 (지연 돌파 확정)"
    elif hit4: sig_type = "S4 (정배열 눌림 돌파)"
    elif hit1: sig_type = "S1 (448 재정렬 양봉)"
    elif hit2: sig_type = "S2 (224 재정렬 양봉)"
    elif hit3: sig_type = "S3 (112 재정렬 양봉)"
    else: sig_type = "새로운 눌림"

    safe_avg_vol = av3[-1] if av3[-1] > 0 else 1
    
    dbg = {
        "last_close": float(c[-1]),
        "vol_spike": float(v[-1] / safe_avg_vol),
        "sig_type": sig_type
    }
    
    return True, sig_type, df, dbg

# ================== 차트 저장 ==================
chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            safe = sanitize_filename(f"{code}_{name}")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{safe}_{timestamp_ms}.png")

            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            
            # 파인스크립트 컬러 완벽 매칭
            apds = [
                mpf.make_addplot(df_cut["EMA10"], color='red', width=1),
                mpf.make_addplot(df_cut["EMA20"], color='orange', width=1),
                mpf.make_addplot(df_cut["EMA30"], color='yellow', width=1),
                mpf.make_addplot(df_cut["EMA60"], color='green', width=1),
                mpf.make_addplot(df_cut["EMA112"], color='blue', width=1),
                mpf.make_addplot(df_cut["EMA224"], color='navy', width=2),
                mpf.make_addplot(df_cut["EMA448"], color='purple', width=2),
            ]

            title = f"[{dbg['sig_type']}] {code} {name} (일봉)\nClose:{dbg['last_close']:.0f} | 거래량 {dbg['vol_spike']:.1f}배 폭발"

            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            return None

# ================== 🚀 초고속 20차선 LS증권 하이브리드 엔진 ==================
def scan_market_1d():
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    
    token = get_ls_token()
    if not token: 
        print("❌ LS증권 토큰 발급 실패로 스캔 불가")
        return

    print(f"\n⚡ [일봉 전용] LS증권 20차선 초고속 병렬 스캔 시작!")

    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    url = "https://openapi.ls-sec.co.kr:8080/stock/chart"
    tr_cd = "t8413"
    outblock_key = "t8413OutBlock1"

    def worker(row_tuple):
        _, row = row_tuple
        name, code = row["Name"], row["Code"]
        
        body = {
            f"{tr_cd}InBlock": {
                "shcode": code,
                "gubun": "2", 
                "qrycnt": 600, 
                "sdate": "",
                "edate": "99999999",
                "comp_yn": "N"
            }
        }

        headers = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {token}", "tr_cd": tr_cd, "tr_cont": "N"}

        df_raw = None
        for retry in range(3): 
            ls_limiter.wait()
            try:
                res = global_session.post(url, headers=headers, data=json.dumps(body), timeout=5, verify=False)
                if res.status_code == 200:
                    data = res.json()
                    if "IGW" in data.get("rsp_cd", ""):
                        time.sleep(1)
                        continue 
                        
                    items = data.get(outblock_key, [])
                    if items:
                        records = [{'Date': pd.to_datetime(r.get('date', '') + '000000', format='%Y%m%d%H%M%S', errors='coerce'), 'Open': float(r.get('open', 0)), 'High': float(r.get('high', 0)), 'Low': float(r.get('low', 0)), 'Close': float(r.get('close', 0)), 'Volume': float(r.get('jdiff_vol', r.get('volume', 0)))} for r in items]
                        df_raw = pd.DataFrame(records).dropna(subset=['Date']).sort_values('Date').reset_index(drop=True).set_index('Date')
                    break
            except: pass

        is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
        hit, sig_type, df, dbg = False, "", None, {}
        
        if is_valid:
            hit, sig_type, df, dbg = compute_signal(df_raw)

        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

            if hit:
                tracker['hits'] += 1
                rank = tracker['hits']
                chart_path = save_chart(df, code, name, rank, dbg)
                if chart_path:
                    sector, outlook, growth = get_company_fact_report(code)
                    
                    # 로직별 맞춤형 코멘트
                    msg = ""
                    if "S1" in sig_type: msg = "S1: 224, 448선 정배열 유지 중, 꼬였던 단기 이평선이 다시 448선까지 완벽한 정배열을 이루는 양봉이 떴습니다!"
                    elif "S2" in sig_type: msg = "S2: 112, 224선 정배열 유지 중, 꼬였던 단기 이평선이 다시 224선까지 정배열을 이루는 양봉이 떴습니다!"
                    elif "S3" in sig_type: msg = "S3: 60, 112선 정배열 유지 중, 꼬였던 단기 이평선이 다시 112선까지 정배열을 이루는 양봉이 떴습니다!"
                    elif "S4" in sig_type: msg = "S4: 완전 정배열 상태에서 20일선 눌림 후 10일선 위로 강하게 돌파하는 양봉이 떴습니다!"
                    elif "S5" in sig_type: msg = "S5: 최근 눌림 이후, 이평선이 완전 정배열로 딱 맞춰지며 상승을 확정 짓는 타점이 떴습니다!"

                    caption = (
                        f"🔥 [{dbg['sig_type']}] (일봉)\n\n"
                        f"[{name}] ({code})\n"
                        f"- 현재가: {dbg['last_close']:,.0f}원\n"
                        f"- 거래량: 3일 평균 대비 {dbg['vol_spike']:.1f}배\n\n"
                        f"📢 [알고리즘 브리핑]\n"
                        f"{msg}\n\n"
                        f"💡 [시장 뷰 & 기업 분석]\n"
                        f"- 섹터: {sector}\n"
                        f"- 전망: {outlook}\n"
                        f"- 실적: {growth}\n\n"
                        f"Time: {datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    telegram_queue.put((chart_path, caption))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(worker, list(stock_list.iterrows()))

    dt = time.time() - t0
    print(f"\n✅ [5번 봇: 신규 눌림목 1D 스캔 완료] 탐색: {tracker['scanned']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 스케줄러 ==================
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [5번 봇: 신규 눌림목 1D 스케줄러 대기 모드]")
    print("   - [일봉 전용] 매일 16:10 (장 마감 직후 단독 실행)")
    
    while True:
        now_kr = datetime.now(kr_tz)
        if now_kr.hour == 14 and now_kr.minute == 0:
            print(f"🚀 [5번 봇 1D 정규 스캔 시작] 현재 시간: {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            print("💤 스캔 완료. 다음 타임(내일)까지 대기합니다...")
            time.sleep(50 * 60) 
        else: 
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()

