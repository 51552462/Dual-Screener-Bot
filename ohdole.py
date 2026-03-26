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

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리)
TELEGRAM_TOKEN_MAIN  = "8004222500:AAFS9rPPtiQiNx4SxGgYOnODFGULqLTNO8M"
TELEGRAM_TOKEN_PROMO = "7996581031:AAFou3HWYhIXzRtlW4ildx8tOitcQBVubPg"
TELEGRAM_CHAT_ID     = "6838834566"
SEND_TELEGRAM        = True

q_main = queue.Queue()
q_promo = queue.Queue()

sent_today = set()
last_run_date = ""

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Pro_System')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

ai_request_lock = threading.Lock()

# 💡 2. 한국장 맞춤 팩트 수집 및 4가지 플랫폼 다중인격 생성 (스팸 완벽 차단)
def generate_kr_ai_report(code: str, company_name: str):
    import re # 정규식 모듈 필수
    sector = "정보 없음"
    headers = {'User-Agent': 'Mozilla/5.0'}
    fn_summary, naver_summary = [], []

    try:
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            soup = BeautifulSoup(res_naver.text, 'html.parser')
            tag = soup.select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
    except: pass
                
    try:
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if tags: summary_parts = [t.text.strip() for t in tags]
            else: summary_parts = []
    except: summary_parts = []

    performance = "실적 데이터 분석 중"
    if len(summary_parts) >= 2: performance = summary_parts[1].replace("동사는", f"{company_name}은(는)")
    elif len(summary_parts) == 1: performance = summary_parts[0].replace("동사는", f"{company_name}은(는)")

    # 1차 방어막: 비상용 멘트도 종목마다 다르게 팩트 기반으로 섞이도록 수정
    fb_main = f"1. 섹터: {sector}\n2. 실적: {performance[:50]}...\n3. 모멘텀: 기업 펀더멘탈 분석 중"
    fb_threads = f"👀 {company_name} 자리 체크 필수! {sector} 쪽에 최근 자금이 쏠리면서 차트 밸런스가 잡히고 있습니다. 비즈니스 흐름도 확인해보세요."
    fb_blog = f"📌 오늘 분석할 종목은 {company_name} ({code})입니다. 최근 {sector} 테마에서 유의미한 흐름을 보여주고 있으며, 바닥권 에너지가 응축되고 있습니다."
    fb_x = f"🔥 {company_name} 지금 무조건 봐야 함. {sector} 관련주 중 차트 제일 이쁨. 팩트체크 필수! #한국주식 #{company_name}"
    fb_blind = f"형들 {company_name} 차트 봄? {sector} 쪽인데 지금 완전 바닥 다지고 거래량 터지기 직전임. 워치리스트 ㄱㄱ"

    for attempt in range(3):
        try:
            prompt = f"""
            너는 한국 주식 전문 애널리스트야. [{company_name} ({code})]에 대해 구글 검색을 통해 최신 팩트를 찾아 5가지 버전의 글을 작성해.
            
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분할 것. 기호나 굵은 글씨(**) 절대 금지.
            2. 무조건 '팩트(매출/이익 수치 %, 구체적인 비즈니스 내용)'를 포함할 것. 추상적이고 뻔한 헛소리 절대 금지.
            3. 매번 똑같은 패턴 템플릿 쓰지 말고 생성할 때마다 문장 구조와 이모지를 완전히 다르게 쓸 것.

            [팩트 데이터]
            섹터/테마: {sector}
            실적: {performance}

            [출력 양식]
            [본캐]
            1. 섹터: (테마 1줄 요약)
            2. 실적: (팩트 수치 1줄 요약)
            3. 모멘텀: (앞으로의 호재 1줄 요약)
            
            [쓰레드]
            (트렌디한 말투, 이모지, 구체적 팩트 포함 2~3문장)
            
            [블로그]
            (전문가 말투, 구체적 팩트 기반 3~4문장)
            
            [X]
            (다급한 느낌, 팩트 위주, 해시태그 2~3개 필수)
            
            [블라인드]
            (블라인드 주식게시판 반말/형들체, 팩트 포함 2~3문장)
            """
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
            
            if not response or not response.text:
                time.sleep(2); continue
                
            # 💡 어떤 마크다운 기호가 붙어도 다 무시하고 찰떡같이 텍스트만 빼내는 무적의 정규식 파싱
            report = response.text.replace('*', '').strip() 
            
            m_part = re.search(r'\[본캐\](.*?)(?=\[쓰레드\])', report, re.DOTALL)
            th_part = re.search(r'\[쓰레드\](.*?)(?=\[블로그\])', report, re.DOTALL)
            bg_part = re.search(r'\[블로그\](.*?)(?=\[X\])', report, re.DOTALL)
            x_part = re.search(r'\[X\](.*?)(?=\[블라인드\])', report, re.DOTALL)
            bl_part = re.search(r'\[블라인드\](.*)', report, re.DOTALL)

            if not (m_part and th_part and bg_part and x_part and bl_part): 
                raise ValueError("파싱오류")

            return m_part.group(1).strip(), th_part.group(1).strip(), bg_part.group(1).strip(), x_part.group(1).strip(), bl_part.group(1).strip()
        except:
            time.sleep(3)
            
    return fb_main, fb_threads, fb_blog, fb_x, fb_blind

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

def telegram_sender_daemon(target_queue, token):
    while True:
        item = target_queue.get()
        if item is None: break
        img_path, caption = item
        safe_caption = caption[:1000] + "\n...(글자수 제한으로 요약됨)" if len(caption) > 1000 else caption
        
        if SEND_TELEGRAM:
            for attempt in range(3):
                try:
                    if img_path: 
                        with open(img_path, 'rb') as f:
                            res = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, files={"photo": f}, timeout=60, verify=False)
                    else:
                        res = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_caption}, timeout=60, verify=False)

                    if res.status_code == 200: break
                    elif res.status_code == 429: time.sleep(3)
                except requests.exceptions.ReadTimeout: break
                except: time.sleep(2)
            time.sleep(1.5)
        target_queue.task_done()

threading.Thread(target=telegram_sender_daemon, args=(q_main, TELEGRAM_TOKEN_MAIN), daemon=True).start()
threading.Thread(target=telegram_sender_daemon, args=(q_promo, TELEGRAM_TOKEN_PROMO), daemon=True).start()

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

# 💡 3. S1 별점 채점 알고리즘
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

def compute_ohdole_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [5, 10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    e5, e10, e20, e30 = df['EMA5'].values, df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    e60, e112, e224, e448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # 💡 주의: 한국장은 100_000_000 / 1000, 미국장은 5_000_000 / 3.0 으로 유지해 주세요!
    is_money_ok = (c * v) >= 100_000_000 
    is_price_ok = c >= 1000
    cond_base = is_money_ok & is_price_ok

    # 1. 양봉 조건
    isBullish = c > o

    # 2. 112, 224, 448 완벽 정배열
    macroBull = (e112 > e224) & (e224 > e448)
    
    # 💡 [신규 추가] 3. 현재 캔들이 224일선 및 448일선 위에 위치
    isAboveLongMA = (c > e224) & (c > e448)
    
    # 💡 [신규 추가] 4. 10일선 > 20일선 정배열 필수 (역배열 차단)
    isShortBull = (e10 > e20)
    
    # 150일 이상 장기 정배열 유지 여부 판독
    is_150_align = pd.Series(macroBull).rolling(150).sum().values == 150

    # 5/30 확실한 상향 돌파
    prev_e5 = np.roll(e5, 1); prev_e5[0] = np.inf
    prev_e30 = np.roll(e30, 1); prev_e30[0] = 0
    isStrictCrossUp30 = (prev_e5 < prev_e30) & (e5 > e30)

    # ⭐️ 최종 시그널 산출 (신규 필터 2개 추가 결합)
    signal1 = isStrictCrossUp30 & isBullish & macroBull & isAboveLongMA & isShortBull & cond_base

    if not signal1[-1]: return False, "", df, {}

    # 별점 및 추천 멘트 할당
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    stars, pt = calculate_star_score(o[-1], h[-1], l[-1], c[-1], prev_c[-1], e10[-1], e20[-1], e30[-1], e60[-1])
    
    usage_tag = "(실제용)" if is_150_align[-1] else "(참고용)"
    sig_type = f"S1 | {stars} ({pt}점) {usage_tag}"
    recommend = "단타, 스윙 / 종가배팅"

    trust_score = calculate_trust_score(c, e60) 
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "recommend": recommend}
    
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
    
    # 💡 당일 중복 발송용 영구 파일 로드
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_ohdole.txt")
    
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

    print(f"\n⚡ [일봉 전용] 한국장 1번(오돌이) 스캔 시작! (당일 중복 차단 🛡️)")
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
                        # 💡 파일에 기록 (영구 차단)
                        try:
                            with open(log_file, "w") as f:
                                f.write(today_str + "\n")
                                for s_code in sent_today: f.write(s_code + "\n")
                        except: pass
                    
            if hit:
                main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True)
                if main_chart_path:
                    ai_main, ai_threads, ai_blog, ai_x, ai_blind = generate_kr_ai_report(code, name)
                    
                    # 1️⃣ 본캐용 캡션
                    main_caption = (
                        f"🎯 [{dbg.get('sig_type', '')}]\n"
                        f"🎯 추천: {dbg.get('recommend', '단타, 스윙 / 종가배팅')}\n\n"
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
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
                    q_main.put((main_chart_path, main_caption))

                    # 2️⃣ 홍보용 캡션 (4개 플랫폼)
                    threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False) 
                    if threads_chart_path:
                        promo_caption = (
                            f"🏢 {name} ({code}) | 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                            f"📱 [Threads 용]\n{ai_threads}\n\n"
                            f"📝 [네이버 블로그 용]\n{ai_blog}\n\n"
                            f"🐦 [X (트위터) 용]\n{ai_x}\n\n"
                            f"🏢 [블라인드 용]\n{ai_blind}\n\n"
                            f"⚠️ [면책 조항] 본 정보는 기술적 분석일 뿐, 매수/매도 권유가 아닙니다. 책임은 투자자 본인에게 있습니다."
                        )
                        q_promo.put((threads_chart_path, promo_caption))

                    print(f"\n✅ [{name}] 한국장 오돌이 듀얼 발송 대기열 추가 완료!")
        except Exception as e:
            pass

    # 💡 5. 일꾼(스레드) 가동 및 대기
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [한국장 1번 오돌이 스캔 완료] 신규 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

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
