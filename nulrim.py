# Dante_Nulrim_1D_LS_Sniper_Final.py
import os
import re
import time
import json
import threading
import queue
import concurrent.futures
from datetime import datetime
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
        res = requests.post(url, headers=headers, data=data, timeout=10)
        if res.status_code == 200:
            print("✅ LS API 통신망 연결 성공!")
            return res.json().get("access_token")
    except: pass
    return None

# ================== ⭐️ 스마트 속도 제어기 (12분 단축의 핵심) ==================
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
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.timestamps.append(time.time())

ls_limiter = LSApiRateLimiter()

# 글로벌 고속 세션 (TCP Handshake 병목 제거)
global_session = requests.Session()
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=1)
global_session.mount('https://', adapter)

# ================== 폴더 및 유틸 ==================
CHART_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'charts')
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

# ================== KRX 종목 리스트 고속 수집 ==================
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
    except Exception as e:
        print(f"종목 수집 실패: {e}")
        return pd.DataFrame()

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
                        res = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                            params={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                            files={"photo": f}, timeout=20, verify=False
                        )
                    if res.status_code == 200: 
                        print(f"\n📲 [텔레그램 전송 성공] {img_path}")
                        break
                    elif res.status_code == 429: time.sleep(3)
                    else: 
                        print(f"\n❌ [텔레그램 에러] {res.status_code}: {res.text}")
                        break 
                except Exception as e:
                    print(f"\n⚠️ [통신 에러] {e}")
                    time.sleep(2)
            time.sleep(1.5)
            
        try:
            if os.path.exists(img_path):
                os.remove(img_path)
                print(f"🗑️ [용량 확보] 전송 완료된 차트 삭제: {img_path}")
        except:
            pass
            
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

# ================== 파라미터 셋업 ==================
MIN_PRICE = 1000                 
MIN_TRANS_MONEY = 100_000_000  

# ================== ⭐️ 눌림목 핵심 로직 (초고속 NumPy 기반) ==================
def compute_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500:
        return False, "no_data", df_raw, {}

    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()

    close_arr = df['Close'].values
    open_arr = df['Open'].values
    vol_arr = df['Volume'].values
    avgvol3_arr = df['AvgVol3'].values
    
    ema10 = df['EMA10'].values
    ema20 = df['EMA20'].values
    ema30 = df['EMA30'].values
    ema60 = df['EMA60'].values
    ema112 = df['EMA112'].values
    ema224 = df['EMA224'].values
    ema448 = df['EMA448'].values

    isBullish = close_arr > open_arr
    volSpike5 = vol_arr >= (avgvol3_arr * 5)
    moneyOk = (close_arr * vol_arr) >= MIN_TRANS_MONEY
    priceOk = close_arr >= MIN_PRICE

    condBase = priceOk & moneyOk & isBullish & volSpike5

    c1_long_trend = (ema112 > ema224) & (ema224 > ema448)
    c1_short_inverse = (ema30 > ema20) & (ema20 > ema10)
    c1_position = (close_arr < ema30) & (close_arr > ema112)
    isCat112 = condBase & c1_long_trend & c1_short_inverse & c1_position

    c2_full_trend = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema112) & (ema112 > ema224) & (ema224 > ema448)
    c2_under_20 = (close_arr < ema10) & (close_arr < ema20)
    c2_above_30 = close_arr > ema30
    isCat30 = condBase & (~isCat112) & c2_full_trend & c2_under_20 & c2_above_30

    c3_mid_inverse = (ema60 > ema30) & (ema30 > ema20) & (ema20 > ema10)
    c3_position = (close_arr < ema112) & (close_arr > ema224)
    isCat224 = condBase & (~isCat112) & (~isCat30) & c3_mid_inverse & c3_position

    c4_position = (close_arr < ema224) & (close_arr > ema448)
    isCat448 = condBase & (~isCat112) & (~isCat30) & (~isCat224) & c4_position

    c112_hit = isCat112[-1]
    c30_hit = isCat30[-1]
    c224_hit = isCat224[-1]
    c448_hit = isCat448[-1]

    if not (c112_hit or c30_hit or c224_hit or c448_hit):
        return False, "no_signal", df, {}

    if c30_hit: sig_type = "🚀 30선 지지 (급등 눌림목)"
    elif c112_hit: sig_type = "💎 112선 지지 (황금 눌림목)"
    elif c224_hit: sig_type = "🛡️ 224선 지지 (중기 마지노선)"
    else: sig_type = "⚓ 448선 지지 (최후 마지노선)"

    safe_avg_vol = avgvol3_arr[-1] if avgvol3_arr[-1] > 0 else 1
    
    dbg = {
        "last_close": float(close_arr[-1]),
        "vol_spike": float(vol_arr[-1] / safe_avg_vol),
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
            
            apds = [
                mpf.make_addplot(df_cut["EMA10"], color='#FF5252', width=1, alpha=0.5),
                mpf.make_addplot(df_cut["EMA20"], color='#FFD700', width=1, alpha=0.5),
                mpf.make_addplot(df_cut["EMA30"], color='#FF00FF', width=2),
                mpf.make_addplot(df_cut["EMA60"], color='#FF9800', width=1, alpha=0.5),
                mpf.make_addplot(df_cut["EMA112"], color='#00E676', width=2),
                mpf.make_addplot(df_cut["EMA224"], color='#2979FF', width=2),
                mpf.make_addplot(df_cut["EMA448"], color='#B0BEC5', width=2),
            ]

            title = f"[{dbg['sig_type']}] {code} {name} (일봉)\nClose:{dbg['last_close']:.0f} | 거래량 {dbg['vol_spike']:.1f}배 폭발"

            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':')

            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, addplot=apds, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            print(f"\n❌ [차트 생성 실패] {name}({code}): {e}")
            return None

# ================== 🚀 초고속 20차선 LS증권 하이브리드 엔진 ==================
def scan_market_1d():
    stock_list = get_krx_list_kind()
    if stock_list.empty: return
    
    token = get_ls_token()
    if not token: 
        print("❌ LS증권 토큰 발급 실패로 스캔 불가")
        return

    print(f"\n⚡ [일봉 전용] LS증권 20차선 초고속 병렬 스캔 시작! (예상 소요시간 13~14분 컷)")

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
                res = global_session.post(url, headers=headers, data=json.dumps(body), timeout=5)
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
            except Exception as e:
                print(f"⚠️ [LS증권 통신 에러] {name}({code}): {e}")

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
                    caption = (
                        f"[{dbg['sig_type']}] (일봉)\n\n"
                        f"[{name}] ({code})\n"
                        f"- 현재가: {dbg['last_close']:,.0f}원\n"
                        f"- 거래량: 직전 3거래일 평균 대비 {dbg['vol_spike']:.1f}배 폭발\n\n"
                        f"💡 [시장 뷰 & 기업 분석]\n"
                        f"- 섹터: {sector}\n"
                        f"- 전망: {outlook}\n"
                        f"- 실적: {growth}\n\n"
                        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    telegram_queue.put((chart_path, caption))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(worker, list(stock_list.iterrows()))

    dt = time.time() - t0
    print(f"\n✅ [일봉 스캔 완료] 탐색: {tracker['scanned']}개 | 정상 분석: {tracker['analyzed']}개 | 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

# ================== ⏰ 스케줄러 ==================
def run_scheduler():
    import pytz
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [눌림목 1D 상업용 스케줄러 자동 대기 모드]")
    print("   - [일봉] 매일 15:00 딱 1번 실행")
    
    while True:
        now = datetime.now(kr_tz)
        if now.hour == 15 and now.minute == 0:
             print(f"🚀 [1D 정규 스캔 시작] 현재 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}")
             scan_market_1d()
             print("💤 스캔 완료. 다음 타임(내일)까지 대기합니다...")
             time.sleep(50 * 60) 
        else: 
            time.sleep(10)

if __name__ == "__main__":
    # 💡 [수정완료] 시작하자마자 수동으로 1회 강제 실행되도록 추가
    print("\n💡 [수동 테스트] 서버 켜지자마자 일봉 눌림목 즉시 1회 스캔 실행!\n")
    scan_market_1d()
    
    run_scheduler()
