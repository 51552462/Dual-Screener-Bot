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
import matplotlib.font_manager as fm

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
            is_success = False
            for attempt in range(3):
                try:
                    # 💡 이미지가 있으면 [차트+글] 전송
                    if img_path: 
                        with open(img_path, 'rb') as f:
                            res = requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", 
                                params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, 
                                files={"photo": f}, 
                                timeout=60, verify=False
                            )
                    # 💡 이미지가 없으면 [텍스트만] 깔끔하게 전송
                    else:
                        res = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", 
                            json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_caption}, 
                            timeout=60, verify=False
                        )

                    if res.status_code == 200: 
                        if img_path: print(f"\n✅ 텔레그램 전송 성공: {img_path}")
                        is_success = True
                        break
                    elif res.status_code == 429: 
                        time.sleep(3)
                except requests.exceptions.ReadTimeout:
                    print(f"\n⚠️ 텔레그램 서버 응답 지연 (중복 방지를 위해 패스합니다.)")
                    break
                except: 
                    time.sleep(2)
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
    
    # 1. 트레이딩뷰 지수이동평균(EMA) 세팅 (5, 30일선 및 장기선 세팅)
    df['EMA5'] = df['Close'].ewm(span=5, adjust=False, min_periods=0).mean()
    df['EMA30'] = df['Close'].ewm(span=30, adjust=False, min_periods=0).mean()
    df['EMA60'] = df['Close'].ewm(span=60, adjust=False, min_periods=0).mean() # 신뢰도 계산용
    df['EMA112'] = df['Close'].ewm(span=112, adjust=False, min_periods=0).mean()
    df['EMA224'] = df['Close'].ewm(span=224, adjust=False, min_periods=0).mean()
    df['EMA448'] = df['Close'].ewm(span=448, adjust=False, min_periods=0).mean()

    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    ema5 = df['EMA5'].values
    ema30 = df['EMA30'].values
    ema60 = df['EMA60'].values
    ema112 = df['EMA112'].values
    ema224 = df['EMA224'].values
    ema448 = df['EMA448'].values

    # ⭐️ 잡주 필터링 (동전주 및 거래대금 미달 종목 제외)
    money_curr = c * v
    is_money_ok = money_curr >= 100_000_000
    is_price_ok = c >= 1000

    # 💡 1. 공통 조건: 양봉 필수
    isBullish = c > o

    # 💡 2. 핵심 필터: 112, 224, 448일선 완벽 정배열 (대세 상승장)
    macroBull = (ema112 > ema224) & (ema224 > ema448)

    # 💡 3. 돌파 로직: 5일선이 30일선을 "확실하게 돌파"
    prev_ema5 = np.roll(ema5, 1)
    prev_ema5[0] = 0
    prev_ema30 = np.roll(ema30, 1)
    prev_ema30[0] = np.inf
    
    isStrictCrossUp30 = (prev_ema5 < prev_ema30) & (ema5 > ema30)

    # 💡 4. 최종 시그널 산출
    signal = isStrictCrossUp30 & isBullish & macroBull & is_money_ok & is_price_ok

    if not signal[-1]: 
        return False, "", df, {}

    # 결과지에 찍힐 이름은 기존 요청대로 "E" 유지
    sig_type = "E"
    trust_score = calculate_trust_score(c, ema60, signal)
    
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "s67_count": 0}
    
chart_lock = threading.Lock()

def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, show_volume=False) -> str:
    with chart_lock:
        try:
            plt.rcParams['font.family'] = 'NanumGothic'
            plt.rcParams['axes.unicode_minus'] = False
            
            timestamp_ms = int(time.time() * 1000)
            vol_suffix = "wVol" if show_volume else "noVol"
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{timestamp_ms}_{vol_suffix}.png")
            
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            df_cut.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
            
            if df_cut.empty or len(df_cut) < 5: return None

            # 1. 데이터 계산
            c = df_cut['Close'].iloc[-1]
            o = df_cut['Open'].iloc[-1]
            h = df_cut['High'].iloc[-1]
            l = df_cut['Low'].iloc[-1]
            v = int(df_cut['Volume'].iloc[-1])
            
            prev_c = df_cut['Close'].iloc[-2] if len(df_cut) > 1 else c
            diff = c - prev_c
            diff_pct = (diff / prev_c) * 100 if prev_c != 0 else 0
            
            sign = "▲" if diff > 0 else ("▼" if diff < 0 else "-")
            
            # 💡 다크테마용 네온 색상 세팅
            bg_color = '#131722'      # 고급스러운 다크 네이비/블랙
            grid_color = '#2A2E39'    # 은은한 그리드
            text_main = '#FFFFFF'     # 메인 텍스트 (흰색)
            text_sub = '#8A91A5'      # 서브 텍스트 (회색)
            color_up = '#FF3B69'      # 네온 레드 (상승)
            color_down = '#00B4D8'    # 네온 블루 (하락)
            
            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)

            # 💡 황금색(Gold) 시그널 화살표
            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut['High'].max() - df_cut['Low'].min()) * 0.04 
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - y_offset
            ap = mpf.make_addplot(signal_marker, type='scatter', markersize=300, marker='^', color='#FFD700', alpha=1.0)

            # 💡 프리미엄 다크 스타일 캔들 & 그리드 세팅
            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge='inherit', wick='inherit', volume='inherit')
            s = mpf.make_mpf_style(
                marketcolors=mc, 
                facecolor=bg_color, edgecolor=bg_color, figcolor=bg_color, 
                gridcolor=grid_color, gridstyle='--', y_on_right=True,
                rc={
                    'font.family': plt.rcParams['font.family'], 
                    'text.color': text_main, 
                    'axes.labelcolor': text_sub, 
                    'xtick.color': text_sub, 
                    'ytick.color': text_sub
                }
            )
            
            plt.close('all')
            
            # 차트 뼈대 생성
            fig, axes = mpf.plot(
                df_cut, type="candle", volume=show_volume, addplot=ap,
                style=s, figsize=(11, 6.5), tight_layout=False, returnfig=True
            )

            # 상단 여백 조절 및 대시보드 텍스트 삽입
            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            
            # 좌측 상단 (종목명)
            fig.text(0.05, 0.93, f"{code} | {name}", fontsize=22, fontweight='bold', color=text_main, ha='left')
            fig.text(0.05, 0.88, "1D / KRX", fontsize=12, color=text_sub, ha='left')

            # 우측 상단 (현재가 및 등락)
            right_text1 = f"Close: {c:,.0f} ({sign} {abs(diff):,.0f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, 0.93, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            # 우측 하단 디테일
            right_text2 = f"Vol: {v:,}  |  O: {o:,.0f}  H: {h:,.0f}  L: {l:,.0f}"
            fig.text(0.95, 0.88, right_text2, fontsize=12, color=text_sub, ha='right')

            # 💡 좌측 하단 전문성 강조용 워터마크
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            # 200 DPI 초고화질 렌더링
            fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            
            return path
        except Exception as e:
            print(f"\n❌ [{name}] 차트 에러: {e}")
            return None
            
def scan_market_1d():
    global sent_today, last_run_date
    kr_tz = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(kr_tz).strftime('%Y-%m-%d')
    
    if today_str != last_run_date:
        sent_today.clear()
        last_run_date = today_str

    stock_list = get_krx_list_kind()
    if stock_list.empty: return

    print(f"\n⚡ [일봉 전용] 오돌이 스캔 시작! (당일 중복 차단 🛡️)")
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()

    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    
    def worker(row_tuple):
        try:
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            df_raw = None
            
            try:
                df_raw = fdr.DataReader(code, start_date)
            except: pass

            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            hit, sig_type, df, dbg = False, "", None, {}
            if is_valid: hit, sig_type, df, dbg = compute_ohdole_1d(df_raw)

            hit_rank = 0
            with console_lock:
                tracker['scanned'] += 1
                if is_valid: tracker['analyzed'] += 1 
                if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                    print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 당일 신규 포착: {tracker['hits']}개)")

                if hit:
                    if code in sent_today:
                        hit = False 
                    else:
                        tracker['hits'] += 1
                        hit_rank = tracker['hits']
                        sent_today.add(code) 
                    
            if hit:
                # 1️⃣ 본캐용 (차트+거래량 표시)
                main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True)
                if main_chart_path:
                    ai_fact_check = generate_kr_ai_report(code, name)
                    
                    main_caption = (
                        f"🎯 [{dbg['sig_type']}]\n\n"
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg['last_close']:,.0f}원\n"
                        f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                        f"📉 [매수/손절 전략]\n"
                        f"- 양봉 길이만큼 분할매수\n"
                        f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                        f"⭐ 알고리즘 신뢰도: {dbg['score']} / 10점\n\n"
                        f"{ai_fact_check}\n\n"
                        f"💬 이 종목이 궁금하다면 채팅창에 '/질문 내용' 을 입력해 보세요!"
                    )
                    telegram_queue.put((main_chart_path, main_caption))

                   # ==========================================
                    # 2️⃣ 쓰레드(Threads) 복붙용 결과지 (진짜 텍스트만)
                    # ==========================================
                    threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False) 

                    if threads_chart_path:
                        # 💡 요청하신 대로 해시태그, 홍보 문구, 제목 싹 다 지웠습니다.
                        threads_caption = (
                            f"🏢 종목명: {name} ({code})\n"
                            f"💰 현재가: {dbg['last_close']:,.0f}원\n\n"
                            f"💡 시장의 주목을 받기 전, 검색기에 발굴된 종목입니다. 투자의 참고 자료로 활용해 보세요!"
                        )
                        telegram_queue.put((threads_chart_path, threads_caption))

                    print(f"\n✅ [{name}] 본캐용 + 쓰레드용 결과지 2개 모두 추가 완료!")
        except Exception as e:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()

    print(f"\n✅ [오돌이 스캔 완료] 신규 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

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
    run_scheduler()
