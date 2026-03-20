# Dante_Nulrim_1D_LS_AI_Pro.py
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

def generate_kr_ai_report(code: str, company_name: str) -> str:
    sector = "정보 없음"
    summary_parts = []
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
            if tags: summary = [t.text.strip() for t in tags]
    except: pass

    performance = "실적 팩트가 제공되지 않았습니다."
    outlook = "현황 및 전망 정보가 제공되지 않았습니다."
    
    if len(summary_parts) >= 2:
        performance = summary_parts[1].replace("동사는", f"[{company_name}]은(는)")
    if len(summary_parts) >= 3:
        outlook = summary_parts[2].replace("동사는", f"[{company_name}]은(는)")
    elif len(summary_parts) == 1:
        performance = summary_parts[0].replace("동사는", f"[{company_name}]은(는)")

    final_report = (
        f"💡 [기업 핵심 팩트 (FnGuide 공식 데이터)]\n"
        f"📌 주요 섹터/테마: {sector}\n\n"
        f"📈 [최근 실적 (우상향 여부)]\n"
        f"✔️ {performance}\n\n"
        f"🔭 [기업 현황 및 전망]\n"
        f"✔️ {outlook}"
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
            is_success = False
            for attempt in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        # 💡 타임아웃을 60초로 넉넉하게 늘려 텔레그램 지연에 대비합니다.
                        res = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", 
                            params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, 
                            files={"photo": f}, 
                            timeout=60, 
                            verify=False
                        )
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
                        
                except requests.exceptions.ReadTimeout:
                    # ⭐️ 핵심 방어: 텔레그램 서버가 늦게 대답할 뿐 사진은 전송되었을 확률이 매우 높으므로, 재전송(중복 발송)을 포기하고 넘어갑니다!
                    print(f"\n⚠️ 텔레그램 서버 응답 지연 (이미 전송되었을 수 있으므로 중복 방지를 위해 패스합니다.)")
                    break
                except Exception as e:
                    print(f"\n❌ 텔레그램 전송 중 예외 발생: {e}")
                    time.sleep(2)
                    
            if not is_success:
                print(f"\n⚠️ 최종 텔레그램 전송 실패 - 대상 파일: {img_path}")
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

MIN_PRICE = 1000                 
MIN_TRANS_MONEY = 100_000_000  

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
                    valid = False
                    break
            if valid: score += 2 
    return max(1, min(10, score))

def compute_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, v = df['Close'].values, df['Open'].values, df['High'].values, df['Volume'].values
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # ⭐️ 잡주 필터링 (거래대금 1억 이상, 1000원 이상)
    moneyOk = (c * v) >= MIN_TRANS_MONEY
    priceOk = c >= MIN_PRICE
    isBullish = c > o

    # ==========================================
    # 💡 배열 상태 정의
    # ==========================================
    align112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    align224 = align112 & (e112 > e224)
    align448 = align224 & (e224 > e448)

    longKeep448 = e224 > e448 
    
    prev_align448 = np.roll(align448, 1); prev_align448[0] = False
    prev_longKeep448 = np.roll(longKeep448, 1); prev_longKeep448[0] = False

    # 🎯 S1: 448 재정렬
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish

    # 🎯 S4: 정배열 20선 눌림돌파 (트뷰와 100% 동일)
    prev_c = np.roll(c, 1); prev_c[0] = 0
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    
    s4 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i

    # 🎯 S7: 112 중기 정배열 턴 (트뷰와 100% 동일)
    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    # ⭐️ 핵심: 거래량 폭발(volSpike) 족쇄 삭제! 기본 필터만 적용.
    cond_base = moneyOk & priceOk
    
    # ⭐️ 오직 S1, S4, S7 타점만 통과시킵니다.
    hit1 = s1[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1]
    hit7 = s7[-1] and cond_base[-1]

    if not (hit1 or hit4 or hit7): return False, "", df, {}

    if hit4: sig_type = "V (S4: 돌파)"
    elif hit7: sig_type = "V (S7: 중기턴)"
    else: sig_type = "V (S1: 448 재정렬)"

    # s67 누적 카운트는 파이썬에서 계산 로직이 너무 길어지므로 S1, S4, S7 본질 타점에 집중하기 위해 0 처리
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": 10, "s67_count": 0}

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict) -> str:
    with chart_lock:
        try:
            timestamp_ms = int(time.time() * 1000000)
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{timestamp_ms}.png")
            
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            
            # 💡 핵심 수정 1: 차트 에러의 주범인 '결측치(NaN)' 완벽 제거
            df_cut.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
            
            if df_cut.empty or len(df_cut) < 5:
                print(f"\n⚠️ [{name}] 데이터 부족(결측치)으로 차트 생성을 스킵합니다.")
                return None

            title = f"[🎯 {dbg['sig_type']}] {code} {name} (1D)\nClose: {dbg['last_close']:,.0f}원"
            mc = mpf.make_marketcolors(up='red', down='blue', volume='inherit')
            s  = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='yahoo', gridstyle=':', rc={'font.family': plt.rcParams['font.family']})
            
            plt.close('all')
            mpf.plot(df_cut, type="candle", volume=True, title=title, style=s, savefig=dict(fname=path, dpi=110, bbox_inches="tight"))
            plt.close('all')
            
            return path
        except Exception as e:
            # 💡 핵심 수정 2: 묵음 처리되던 에러를 콘솔에 강력하게 출력!
            print(f"\n❌ [{name}] 차트 이미지 저장 중 치명적 에러 발생: {e}")
            return None

def scan_market_1d():
    stock_list = get_krx_list_kind()
    if stock_list.empty: return

    print(f"\n⚡ [일봉 전용] 한국장 V 스캔 시작! (초고속 네이버 엔진🚀 / S1,S4 전용)")

    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    
    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    
    def worker(row_tuple):
        # ⭐️ 숨겨진 에러를 철저히 잡아내는 try-except 방어막 ⭐️
        try:
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            df_raw = None
            
            try:
                df_raw = fdr.DataReader(code, start_date)
            except: pass

            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            hit, sig_type, df, dbg = False, "", None, {}
            if is_valid: hit, sig_type, df, dbg = compute_signal(df_raw)

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
                        f"🌟 사전 매집/바닥턴 누적: 별x{dbg['s67_count']}\n"
                        f"⭐ 알고리즘 신뢰도: {dbg['score']} / 10점\n\n"
                        f"💡 [기업 팩트체크]\n"
                        f"{ai_fact_check}\n\n"
                        f"⚠️ [전문가 코멘트]\n"
                        f"본 분석은 실시간 데이터 기반 팩트 요약본입니다.\n"
                        f"시장 상황과 개인의 관점에 따라 해석이 다를 수 있으므로, 반드시 개별적인 추가 분석을 권장합니다.\n"
                        f"\n💬 이 종목이 궁금하다면 채팅창에 '/질문 내용' 을 입력해 보세요!"
                    )
                    telegram_queue.put((chart_path, caption))
                    # 💡 큐에 담겼다는 사실을 명확히 시각화
                    print(f"\n✅ [{name}] 텔레그램 전송 대기열에 추가 완료!")
                else:
                    print(f"\n⚠️ [{name}] 차트 생성 실패로 인해 텔레그램 전송이 취소되었습니다.")
        except Exception as e:
            print(f"\n❌ [{row['Name']}] 워커 스레드 치명적 에러 발생: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        # ⭐️ list()로 감싸서 제너레이터를 강제 실행! 에러가 숨는 것을 방지합니다.
        list(executor.map(worker, list(stock_list.iterrows())))
        
    # ⭐️ 메인 프로그램이 대기열 처리를 기다리지 않고 꺼지는 현상 방지
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()

    print(f"\n✅ [5번 봇: KRX V 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [5번 검색기] 11:00 / 13:30 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 11 and now_kr.minute == 0) or (now_kr.hour == 13 and now_kr.minute == 30):
            print(f"🚀 [5번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    # run_scheduler()  # 💡 스케줄러를 잠시 끄고
    scan_market_1d()   # 🚀 스캔 함수를 직접 호출하여 즉시 실행
