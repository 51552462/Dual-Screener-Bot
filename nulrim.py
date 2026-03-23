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
import matplotlib.font_manager as fm

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

# ⭐️ 당일 중복 발송 방지용 기억 장치 (반드시 함수들 바깥, 파일 위쪽에 선언되어야 합니다!)
sent_today = set()
last_run_date = ""

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

def generate_kr_ai_report(code: str, company_name: str) -> str:
    sector = "정보 없음"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    # 1. 듀얼 엔진 데이터 수집 (에프앤가이드 + 네이버금융 백업)
    fn_summary = []
    naver_summary = []

    try:
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            soup = BeautifulSoup(res_naver.text, 'html.parser')
            tag = soup.select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
            # 네이버 기업개요 백업용 추출
            summary_tags = soup.select('.summary_info p')
            if summary_tags: naver_summary = [t.text.strip() for t in summary_tags if t.text.strip()]
    except: pass

    try:
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if tags: fn_summary = [t.text.strip() for t in tags if t.text.strip()]
    except: pass

    # 2. 데이터 최적화 (에프앤가이드 실패 시 네이버 데이터로 완벽 방어)
    target_summary = fn_summary if fn_summary else naver_summary

    performance = "실적 데이터 일시적 수집 지연"
    outlook = "추가 전망 데이터가 요약본에 포함되지 않은 종목입니다."

    if len(target_summary) >= 2:
        performance = target_summary[1].replace("동사는", f"[{company_name}]은(는)")
    if len(target_summary) >= 3:
        outlook = target_summary[2].replace("동사는", f"[{company_name}]은(는)")
    elif len(target_summary) == 1:
        performance = target_summary[0].replace("동사는", f"[{company_name}]은(는)")

    # 3. 중복 타이틀 제거 및 전문가 스타일의 깔끔한 출력
    final_report = (
        f"💡 [기업 핵심 팩트]\n"
        f"📌 주요 섹터/테마: {sector}\n\n"
        f"📈 [최근 실적 및 비즈니스 현황]\n"
        f"✔️ {performance}\n\n"
        f"🔭 [향후 모멘텀 및 전망]\n"
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

   # 🎯 S1, S4, S7 기존 로직 (S1과 S4는 계산은 하되 마지막에 버립니다)
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish

    prev_c = np.roll(c, 1); prev_c[0] = 0
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    s4 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i

    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    # 🚨 대표님 파일에 S2, S3, S5, S6 로직이 없어서 임시로 빈 칸을 만들어 둡니다!
    # (나중에 S6 실제 조건을 여기에 넣으시면 됩니다)
    s2 = np.zeros_like(c, dtype=bool)
    s3 = np.zeros_like(c, dtype=bool)
    s5 = np.zeros_like(c, dtype=bool)
    s6 = np.zeros_like(c, dtype=bool) 

    # ==========================================
    # 💡 [핵심] S6 누적 및 리셋 로직
    # ==========================================
    s6_counts = np.zeros(len(c), dtype=int)
    current_s6_count = 0
    
    for i in range(len(c)):
        # 1️⃣ S6이 아닌 다른 시그널(S1~S5, S7)이 중간에 하나라도 뜨면 S6 카운터 0으로 완전 리셋!
        if s1[i] or s2[i] or s3[i] or s4[i] or s5[i] or s7[i]:
            current_s6_count = 0
        
        # 2️⃣ S6이 뜨면 카운터 +1 누적
        if s6[i]:
            current_s6_count += 1
            
        s6_counts[i] = current_s6_count

    # ⭐️ 잡주 필터링 (거래대금 1억 이상, 1000원 이상)
    cond_base = moneyOk & priceOk

    # 3️⃣ S1, S4는 쳐다보지도 않고, 오직 S6과 S7이 떴을 때만 'hit(포착)' 처리
    hit6 = s6[-1] and cond_base[-1]
    hit7 = s7[-1] and cond_base[-1]

    if not (hit6 or hit7): 
        return False, "", df, {}

    # 4️⃣ S6 누적 횟수에 따른 시그널 강조 카피라이팅
    if hit6:
        if s6_counts[-1] >= 2:
            sig_type = f"💥 S6 (바닥 다지기 누적 {s6_counts[-1]}회 포착!)"
        else:
            sig_type = "🌱 S6 (바닥 다지기 첫 진입)"
    else:
        sig_type = "🚀 S7 (추세 전환 돌파)"

    # 본질 타점만 통과했으므로 신뢰도 10점 만점 고정
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": 10, "s6_count": int(s6_counts[-1])}

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

            c = df_cut['Close'].iloc[-1]
            o = df_cut['Open'].iloc[-1]
            h = df_cut['High'].iloc[-1]
            l = df_cut['Low'].iloc[-1]
            v = int(df_cut['Volume'].iloc[-1])
            
            prev_c = df_cut['Close'].iloc[-2] if len(df_cut) > 1 else c
            diff = c - prev_c
            diff_pct = (diff / prev_c) * 100 if prev_c != 0 else 0
            
            sign = "▲" if diff > 0 else ("▼" if diff < 0 else "-")
            
            bg_color = '#131722'      
            grid_color = '#2A2E39'    
            text_main = '#FFFFFF'     
            text_sub = '#8A91A5'      
            color_up = '#FF3B69'      
            color_down = '#00B4D8'    
            
            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)

            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut['High'].max() - df_cut['Low'].min()) * 0.04 
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - y_offset
            ap = mpf.make_addplot(signal_marker, type='scatter', markersize=300, marker='^', color='#FFD700', alpha=1.0)

            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge='inherit', wick='inherit', volume='inherit')
            s = mpf.make_mpf_style(
                marketcolors=mc, facecolor=bg_color, edgecolor=bg_color, figcolor=bg_color, 
                gridcolor=grid_color, gridstyle='--', y_on_right=True,
                rc={'font.family': plt.rcParams['font.family'], 'text.color': text_main, 'axes.labelcolor': text_sub, 'xtick.color': text_sub, 'ytick.color': text_sub}
            )
            
            plt.close('all')
            fig, axes = mpf.plot(
                df_cut, type="candle", volume=show_volume, addplot=ap,
                style=s, figsize=(11, 6.5), tight_layout=False, returnfig=True
            )

            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            fig.text(0.05, 0.93, f"{code} | {name}", fontsize=22, fontweight='bold', color=text_main, ha='left')
            fig.text(0.05, 0.88, "1D / KRX", fontsize=12, color=text_sub, ha='left')

            right_text1 = f"Close: {c:,.0f} ({sign} {abs(diff):,.0f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, 0.93, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            right_text2 = f"Vol: {v:,}  |  O: {o:,.0f}  H: {h:,.0f}  L: {l:,.0f}"
            fig.text(0.95, 0.88, right_text2, fontsize=12, color=text_sub, ha='right')

            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

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
    
    # ⭐️ 날짜가 바뀌면 어제 보냈던 기록 리셋
    if today_str != last_run_date:
        sent_today.clear()
        last_run_date = today_str

    stock_list = get_krx_list_kind()
    if stock_list.empty: return

    print(f"\n⚡ [일봉 전용] 한국장 V(눌림목) 스캔 시작! (당일 중복 차단 🛡️)")

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
            if is_valid: hit, sig_type, df, dbg = compute_signal(df_raw)

            hit_rank = 0
            with console_lock:
                tracker['scanned'] += 1
                if is_valid: tracker['analyzed'] += 1 
                if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                    print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 당일 신규 포착: {tracker['hits']}개)")

                # ⭐️ 2차 차단: 오늘 이미 텔레그램으로 쏜 종목이면 패스!
                if hit:
                    if code in sent_today:
                        hit = False 
                    else:
                        tracker['hits'] += 1
                        hit_rank = tracker['hits']
                        sent_today.add(code) 
                    
          if hit:
                # 1️⃣ 본캐용 (거래량 포함된 다크 차트 1장 생성)
                main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True)
                
                if main_chart_path:
                    ai_fact_check = generate_kr_ai_report(code, name)
                    
                    # 본캐용 상세 캡션
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
                    telegram_queue.put((main_chart_path, main_caption)) # 텔레그램에 1번 담음

                    # 2️⃣ 쓰레드 홍보용 (거래량 뺀 다크 차트 1장 추가 생성)
                    threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False)
                    
                    if threads_chart_path:
                        # 홍보용 깔끔한 캡션
                        threads_caption = (
                            f"🏢 종목명: {name} ({code})\n"
                            f"💰 현재가: {dbg['last_close']:,.0f}원\n\n"
                            f"💡 시장의 주목을 받기 전, 검색기에 발굴된 차트 분석입니다. 투자의 참고 자료로 활용해 보세요!"
                        )
                        telegram_queue.put((threads_chart_path, threads_caption)) # 텔레그램에 2번 담음
                        
                    print(f"\n✅ [{name}] 본캐용 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")
                else:
                    print(f"\n⚠️ [{name}] 차트 생성 실패로 전송이 취소되었습니다.")
        except Exception as e:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()

    print(f"\n✅ [5번 봇: KRX V 스캔 완료] 신규 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

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
    run_scheduler()  # 💡 스케줄러를 잠시 끄고
