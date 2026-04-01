# Dante_Nulrim_1D_LS_AI_Pro_DualBot.py
import os, re, time, threading, queue, concurrent.futures
from datetime import datetime, timedelta
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import requests
import warnings, urllib3
from bs4 import BeautifulSoup
from io import StringIO
import FinanceDataReader as fdr

from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv() 
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("🚨 API 키를 찾을 수 없습니다! .env 파일을 확인해 주세요.")

client = genai.Client(api_key=GEMINI_API_KEY)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리)
TELEGRAM_TOKEN_MAIN  = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_TOKEN_PROMO = "7996581031:AAFou3HWYhIXzRtlW4ildx8tOitcQBVubPg"
TELEGRAM_CHAT_ID     = "6838834566"
SEND_TELEGRAM        = True

q_main = queue.Queue()
q_promo = queue.Queue()

sent_today = set()
last_run_date = ""

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

def telegram_sender_daemon(target_queue, token):
    while True:
        item = target_queue.get()
        if item is None: break
        img_path, caption = item
        safe_caption = caption[:1000] + "\n...(요약됨)" if len(caption) > 1000 else caption

        if SEND_TELEGRAM:
            for _ in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, files={"photo": f}, timeout=60, verify=False)
                    if res.status_code == 200: break
                    elif res.status_code == 429: time.sleep(3)
                except: time.sleep(2)
            time.sleep(1.5)
        target_queue.task_done()

threading.Thread(target=telegram_sender_daemon, args=(q_main, TELEGRAM_TOKEN_MAIN), daemon=True).start()
threading.Thread(target=telegram_sender_daemon, args=(q_promo, TELEGRAM_TOKEN_PROMO), daemon=True).start()

# 💡 [공통] 본캐 팩트 + 실시간 트렌드 해시태그 생성기
def generate_kr_ai_report(code: str, company_name: str):
    import re, time
    
    # 1. 팩트 데이터 추출
    try:
        if code.isdigit(): # 한국장
            res = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            soup = BeautifulSoup(res.text, 'html.parser')
            sector_kr = soup.select_one('h4.h_sub.sub_tit7 a').text.strip() if soup.select_one('h4.h_sub.sub_tit7 a') else '국내 증시'
        else: # 미국장
            tk = yf.Ticker(code)
            sector = tk.info.get('sector', '글로벌 산업')
            sector_kr_map = {"Technology": "테크/기술", "Healthcare": "헬스케어", "Financial Services": "금융", "Consumer Cyclical": "소비재", "Industrials": "산업재", "Energy": "에너지", "Basic Materials": "원자재"}
            sector_kr = sector_kr_map.get(sector, sector)
    except:
        sector_kr = '유망 섹터'

    # 비상용 기본 멘트
    fb_main = f"1. 섹터: {sector_kr}\n2. 실적: 데이터 분석 중\n3. 모멘텀: 수급 유입 및 차트 반등 포착"
    fb_tags = f"X: #{company_name.replace(' ','')} #주식투자\nThreads: #{sector_kr.replace('/','')} #주식스타그램"

    # 2. 구글 AI 호출 (속도 제한 방어 4초 쿨타임)
    for attempt in range(3):
        try:
            time.sleep(4) 
            
            prompt = f"""
            너는 주식 전문 마케터야. [{company_name} ({code})] 종목과 관련된 오늘자 최신 이슈나 테마를 검색해서 아래 양식에 맞게 딱 출력해.
            
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분해. 굵은 글씨(**) 금지.
            2. [해시태그]는 뜬금없는 단어 금지! 오늘 이 종목/섹터와 가장 연관성 높고 트래픽 터지는 실시간 인기 태그 1, 2위를 X와 Threads 특성에 맞게 2개씩만 작성해.

            [본캐]
            1. 섹터: (어떤 테마인지 한글로 1줄 요약)
            2. 실적: (팩트 수치 한글 1줄 요약)
            3. 모멘텀: (앞으로의 호재 한글 1줄 요약)
            
            [해시태그]
            X: #태그1 #태그2
            Threads: #태그1 #태그2
            """
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
            
            if not response or not response.text:
                continue
                
            report = response.text.replace('*', '').strip() 
            
            m_part = re.search(r'\[본캐\](.*?)(?=\[해시태그\])', report, re.DOTALL)
            tag_part = re.search(r'\[해시태그\](.*)', report, re.DOTALL)

            if not (m_part and tag_part): 
                raise ValueError("파싱오류")

            return m_part.group(1).strip(), tag_part.group(1).strip()
        except:
            pass 
            
    return final_report, sector

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

MIN_PRICE = 1000                  
MIN_TRANS_MONEY = 100_000_000  

def calculate_trust_score(c, e60, is_hit):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   
    return max(1, min(10, score))

# 💡 3. S4 별점 채점 알고리즘 추가
def calculate_star_score(o, h, l, c, prev_c, e10, e20, e30, e60):
    score = 0
    change_pct = ((c - prev_c) / prev_c) * 100 if prev_c > 0 else 0
    if 5.0 <= change_pct <= 8.5: score += 30
    elif 8.5 < change_pct <= 10.0: score += 25
    elif 10.0 < change_pct <= 15.0: score += 10
    elif change_pct > 15.0: score += 0           
    else: score += 20 
        
    embraced_count = 0
    if l <= e10 <= c: embraced_count += 1
    if l <= e20 <= c: embraced_count += 1
    if l <= e30 <= c: embraced_count += 1
    
    if embraced_count == 3: score += 40
    elif embraced_count == 2: score += 30
    elif embraced_count == 1: score += 20
    else:
        if l > e10 and l > e20 and l > e30: score += 5  
        else: score += 10
            
    if e10 > e20: score += 10
    if e20 > e30: score += 10
    if e30 > e60: score += 10
    
    if score >= 90: stars = "★★★★★"
    elif score >= 80: stars = "★★★★☆"
    elif score >= 65: stars = "★★★☆☆"
    elif score >= 50: stars = "★★☆☆☆"
    else: stars = "★☆☆☆☆"
    return stars, score

def compute_signal(df_raw: pd.DataFrame): # 💡 한국장 파일에선 함수명을 compute_signal 로 유지하세요!
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # 💡 조건 (각 파일 상단 변수에 맞게 자동 적용)
    moneyOk = (c * v) >= (5_000_000 if 'USD' in globals().get('MIN_MONEY_USD', '') else 100_000_000)
    priceOk = c >= (3.0 if 'USD' in globals().get('MIN_MONEY_USD', '') else 1000)
    isBullish = c > o

    # 150일 정배열 유지 판독 (S1/S4 실제, 참고 판별용)
    macroBull_for_s4 = (e112 > e224) & (e224 > e448)
    is_150_align = pd.Series(macroBull_for_s4).rolling(150).sum().values == 150

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

    # ⭐️ S1: 448 재정렬 시그널
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    
    # ⭐️ S4: 정배열 20선 눌림돌파
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    s4 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i

    # ⭐️ S6: 완전 바닥 탈출 (단기턴)
    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1); prev_shortBull[0] = False
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish

    # ⭐️ S7: 장기 역배열 + 나머지 10~112 정배열 전환 (중기턴)
    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    # 🚨 [버그 수정 1] S6 누적 로직 완벽 개선 (돌파 시그널 당일에도 누적치 보존)
    s6_counts = np.zeros(len(c), dtype=int)
    current_s6_count = 0
    for i in range(len(c)):
        if s6[i]: 
            current_s6_count += 1
            
        # 💡 리셋 전에 오늘(i)의 값을 먼저 기록해야 S4/S1 돌파 시 결과지에 과거 누적이 뜸!
        s6_counts[i] = current_s6_count
        
        # 기록한 후에 리셋을 줍니다. (다음 캔들부터 적용되도록)
        if s1[i] or s4[i] or s7[i]: 
            current_s6_count = 0

    cond_base = moneyOk & priceOk
    
    hit1 = s1[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1] 
    hit6 = s6[-1] and cond_base[-1]
    hit7 = s7[-1] and cond_base[-1]

    if not (hit1 or hit4 or hit6 or hit7): 
        return False, "", df, {}

    recommend = ""
    # 🚨 [버그 수정 2] 타점 우선순위 재배치! (더 중요한 S1, S7, S4가 S6에 씹히지 않게 위로 올림)
    if hit1: 
        stars, pt = calculate_star_score(o[-1], h[-1], l[-1], c[-1], prev_c[-1], e10[-1], e20[-1], e30[-1], e60[-1])
        usage_tag = "(실제용)" if is_150_align[-1] else "(참고용)"
        sig_type = f"S1 | {stars} ({pt}점) {usage_tag}"
        recommend = "스윙 / 종가배팅"
        
    elif hit7: 
        # 💡 S7 타점 확실한 강조 멘트 추가
        sig_type = "🔥 S7 (중기 정배열 턴 강조!)"
        recommend = "중장기 / 종가배팅"
        
    elif hit4: 
        stars, pt = calculate_star_score(o[-1], h[-1], l[-1], c[-1], prev_c[-1], e10[-1], e20[-1], e30[-1], e60[-1])
        usage_tag = "(실제용)" if is_150_align[-1] else "(참고용)"
        sig_type = f"S4 | {stars} ({pt}점) {usage_tag}"
        recommend = "스윙 / 종가배팅"
        
    elif hit6:
        # 💡 S6 단독 포착 시 누적 강조
        sig_type = f"🌱 S6 (누적 {s6_counts[-1]}회 바닥턴)" if s6_counts[-1] >= 2 else "🌱 S6 (신규 바닥턴)"
        recommend = "관심종목, 중장기 / 종가배팅"

    # 미국장/한국장 호환 트러스트 스코어
    try:
        trust_score = calculate_trust_score(c, e60)
    except:
        trust_score = calculate_trust_score(c, e60, True)

    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "s6_count": int(s6_counts[-1]), "recommend": recommend}

chart_lock = threading.Lock()

def get_daily_theme():
    # 💡 오늘 날짜 기준으로 5가지 테마 매일 자동 로테이션
    theme_idx = datetime.now().day % 5
    themes = [
        {'bg': '#131722', 'grid': '#2A2E39', 'text': '#FFFFFF', 'up': '#FF3B69', 'down': '#00B4D8'}, # 0: 프리미엄 다크
        {'bg': '#FFFFFF', 'grid': '#EAEAEA', 'text': '#111111', 'up': '#E63946', 'down': '#1D3557'}, # 1: 스노우 화이트 (가독성 극대화)
        {'bg': '#0B1021', 'grid': '#1B2236', 'text': '#E2E8F0', 'up': '#00FFA3', 'down': '#FF3366'}, # 2: 사이버펑크
        {'bg': '#1E1E1E', 'grid': '#333333', 'text': '#D4D4D4', 'up': '#4CAF50', 'down': '#F44336'}, # 3: 터미널 딥
        {'bg': '#F8F9FA', 'grid': '#DEE2E6', 'text': '#212529', 'up': '#FF4757', 'down': '#2ED573'}  # 4: 모던 라이트
    ]
    return themes[theme_idx]

chart_lock = threading.Lock()

def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, show_volume=False, is_promo=False) -> str:
    with chart_lock:
        try:
            plt.rcParams['font.family'] = 'NanumGothic'
            plt.rcParams['axes.unicode_minus'] = False
            
            timestamp_ms = int(time.time() * 1000)
            vol_suffix = "promo" if is_promo else ("wVol" if show_volume else "noVol")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{timestamp_ms}_{vol_suffix}.png")
            
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            df_cut.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
            if df_cut.empty or len(df_cut) < 5: return None

            c, o, h, l = df_cut['Close'].iloc[-1], df_cut['Open'].iloc[-1], df_cut['High'].iloc[-1], df_cut['Low'].iloc[-1]
            v = int(df_cut['Volume'].iloc[-1])
            prev_c = df_cut['Close'].iloc[-2] if len(df_cut) > 1 else c
            diff = c - prev_c
            diff_pct = (diff / prev_c) * 100 if prev_c != 0 else 0
            sign = "▲" if diff > 0 else ("▼" if diff < 0 else "-")
            
            # 💡 홍보용 vs 본캐용 테마 분기처리
            if is_promo:
                theme = get_daily_theme()
                bg_color, grid_color, text_main = theme['bg'], theme['grid'], theme['text']
                color_up, color_down = theme['up'], theme['down']
                text_sub = text_main # 홍보용은 서브 텍스트도 진하게
                custom_figsize = (9, 9) # 인스타/쓰레드 최적화 1:1 비율
            else:
                bg_color, grid_color, text_main, text_sub = '#131722', '#2A2E39', '#FFFFFF', '#8A91A5'
                color_up, color_down = '#FF3B69', '#00B4D8'
                custom_figsize = (11, 6.5) if show_volume else (9, 9)

            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)

            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut['High'].max() - df_cut['Low'].min()) * 0.04 
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - y_offset
            ap = mpf.make_addplot(signal_marker, type='scatter', markersize=400 if is_promo else 300, marker='^', color='#FFD700', alpha=1.0)

            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge='inherit', wick='inherit', volume='inherit')
            s = mpf.make_mpf_style(marketcolors=mc, facecolor=bg_color, edgecolor=bg_color, figcolor=bg_color, gridcolor=grid_color, gridstyle='--', y_on_right=True, rc={'font.family': plt.rcParams['font.family'], 'text.color': text_main, 'axes.labelcolor': text_sub, 'xtick.color': text_sub, 'ytick.color': text_sub})
            
            plt.close('all')
            fig, axes = mpf.plot(df_cut, type="candle", volume=show_volume, addplot=ap, style=s, figsize=custom_figsize, tight_layout=False, returnfig=True)

            title_y, sub_y = (0.94, 0.90) if not show_volume or is_promo else (0.93, 0.88)
            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            
            fig.text(0.05, title_y, f"{code} | {name}", fontsize=24 if is_promo else 22, fontweight='bold', color=text_main, ha='left')
            
            right_text1 = f"{sign} {abs(diff_pct):.2f}%" if is_promo else f"Close: {c:,.2f} ({sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=22 if is_promo else 18, fontweight='bold', color=color_diff, ha='right')

            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=250 if is_promo else 200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
            print(f"\n❌ [{name}] 차트 에러: {e}")
            return None

def scan_market_1d():
    global sent_today, last_run_date
    kr_tz = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(kr_tz).strftime('%Y-%m-%d')
    
    # 영구 기억장치용 파일
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_nulrim.txt")

    if today_str != last_run_date:
        sent_today.clear()
        last_run_date = today_str
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    lines = f.read().splitlines()
                    if lines and lines[0] == today_str:
                        sent_today = set(lines[1:])
            except: pass

    stock_list = get_krx_list_kind()
    if stock_list.empty: return

    print(f"\n⚡ [일봉 전용] 한국장 V(눌림목) 스캔 시작! (당일 중복 차단 🛡️)")
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
                hit, sig_type, df, dbg = compute_signal(df_raw)
        except Exception:
            pass

        hit_rank = 0
        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")
            if hit:
                # 💡 중복 발송 철통 방어
                if code in sent_today:
                    hit = False 
                else:
                    tracker['hits'] += 1
                    hit_rank = tracker['hits']
                    sent_today.add(code) 
                    try:
                        with open(log_file, "w") as f:
                            f.write(today_str + "\n")
                            for s_code in sent_today: f.write(s_code + "\n")
                    except: pass
                
        if hit:
    # 1️⃣ 본캐용 (차트 + 거래량 + 유료방용 상세 브리핑)
    main_chart_path = save_chart(df, code, name, tracker['hits'], dbg, show_volume=True, is_promo=False)
    if main_chart_path:
        ai_fact_check, sector_name = generate_ai_report(code, name) # (또는 generate_kr_ai_report)
                    
                    # 1️⃣ 본캐용 캡션 (유료방용 - 기존 멘트 유지)
                    main_caption = (
                        f"🎯 [{dbg.get('sig_type', '')}]\n"
                        f"🎯 추천: {dbg.get('recommend', '단타, 스윙 / 종가배팅')}\n\n"
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.2f}\n\n"
                        f"📉 [매수/손절 전략]\n"
                        f"- 양봉 길이만큼 분할매수\n"
                        f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                        f"⭐ 알고리즘 신뢰도: {dbg.get('score', 10)} / 10점\n\n"
                        f"💡 [AI 비즈니스 요약]\n"
                        f"{ai_main}\n\n"
                        f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                        f"⚠️ [면책 조항]\n"
                        f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다. 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                    )
                    telegram_queue.put((main_chart_path, main_caption)) # (듀얼봇 파일의 경우 q_main.put)

                    # 2️⃣ 홍보용 (거래량 제거 + 매일 바뀌는 5가지 테마 + 초심플 캡션)
        promo_chart_path = save_chart(df, code, name, tracker['hits'], dbg, show_volume=False, is_promo=True)
        if promo_chart_path:
            promo_caption = (
                f"🏢 종목명: {name}\n"
                f"📌 섹터: {sector_name}
            )
            telegram_queue.put((promo_chart_path, promo_caption)) # (듀얼봇 파일의 경우 q_promo.put)
            
        print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 완료!")

    # 💡 5. 일꾼들(스레드) 가동
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [한국장 V 스캔 완료] 신규 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

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
    scan_market_1d()
