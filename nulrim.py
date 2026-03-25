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

def compute_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    # Low 데이터 포함
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    moneyOk = (c * v) >= 100_000_000  # 한국장 기준 1억
    priceOk = c >= 1000               # 한국장 기준 1000원
    isBullish = c > o

    # 💡 150일 정배열 유지 여부 판독 (S4 실제/참고용)
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

    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    s2 = align224 & (~prev_align224) & prev_longKeep224 & (e224 < e448) & isBullish
    s3 = align112 & (~prev_align112) & prev_longKeep112 & (e112 < e224) & isBullish
    
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish

    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1); prev_shortBull[0] = False
    
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish

    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    s4 = np.zeros_like(c, dtype=bool)
    last_pullback_bar = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pullback_bar > 5):
            s4[i] = True
            last_pullback_bar = i

    s5 = np.zeros_like(c, dtype=bool) 

    s6_counts = np.zeros(len(c), dtype=int)
    current_s6_count = 0
    for i in range(len(c)):
        if s1[i] or s2[i] or s3[i] or s4[i] or s5[i] or s7[i]: current_s6_count = 0
        if s6[i]: current_s6_count += 1
        s6_counts[i] = current_s6_count

    cond_base = moneyOk & priceOk
    
    hit2 = s2[-1] and cond_base[-1]
    hit4 = s4[-1] and cond_base[-1]
    hit6 = s6[-1] and cond_base[-1]
    hit7 = s7[-1] and cond_base[-1]

    if not (hit2 or hit4 or hit6 or hit7): 
        return False, "", df, {}

    recommend = ""
    # 💡 4. 불필요한 수식어 제거 및 추천 멘트 할당
    if hit6:
        sig_type = f"S6 (누적 {s6_counts[-1]}회)" if s6_counts[-1] >= 2 else "S6"
        recommend = "관심종목, 중장기 / 종가배팅"
    elif hit7:
        sig_type = "S7"
        recommend = "중장기 / 종가배팅"
    elif hit4:
        stars, pt = calculate_star_score(o[-1], h[-1], l[-1], c[-1], prev_c[-1], e10[-1], e20[-1], e30[-1], e60[-1])
        usage_tag = "(실제용)" if is_150_align[-1] else "(참고용)"
        sig_type = f"S4 | {stars} ({pt}점) {usage_tag}"
        recommend = "스윙 / 종가배팅"
    else:
        sig_type = "S2"
        recommend = "스윙 / 종가배팅"

    trust_score = calculate_trust_score(c, e60, True)
    
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "s6_count": int(s6_counts[-1]), "recommend": recommend}

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

            c, o, h, l = df_cut['Close'].iloc[-1], df_cut['Open'].iloc[-1], df_cut['High'].iloc[-1], df_cut['Low'].iloc[-1]
            v = int(df_cut['Volume'].iloc[-1])
            prev_c = df_cut['Close'].iloc[-2] if len(df_cut) > 1 else c
            diff = c - prev_c
            diff_pct = (diff / prev_c) * 100 if prev_c != 0 else 0
            
            sign = "▲" if diff > 0 else ("▼" if diff < 0 else "-")
            
            bg_color, grid_color, text_main, text_sub = '#131722', '#2A2E39', '#FFFFFF', '#8A91A5'
            color_up, color_down = '#FF3B69', '#00B4D8'
            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)

            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut['High'].max() - df_cut['Low'].min()) * 0.04 
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - y_offset
            ap = mpf.make_addplot(signal_marker, type='scatter', markersize=300, marker='^', color='#FFD700', alpha=1.0)

            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge='inherit', wick='inherit', volume='inherit')
            s = mpf.make_mpf_style(marketcolors=mc, facecolor=bg_color, edgecolor=bg_color, figcolor=bg_color, gridcolor=grid_color, gridstyle='--', y_on_right=True, rc={'font.family': plt.rcParams['font.family'], 'text.color': text_main, 'axes.labelcolor': text_sub, 'xtick.color': text_sub, 'ytick.color': text_sub})
            
            if show_volume:
                custom_figsize = (11, 6.5) 
                title_y, sub_y = 0.93, 0.88
            else:
                custom_figsize = (9, 9)    
                title_y, sub_y = 0.94, 0.90

            plt.close('all')
            fig, axes = mpf.plot(df_cut, type="candle", volume=show_volume, addplot=ap, style=s, figsize=custom_figsize, tight_layout=False, returnfig=True)

            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            fig.text(0.05, title_y, f"{code} | {name}", fontsize=22, fontweight='bold', color=text_main, ha='left')
            fig.text(0.05, sub_y, "1D / KRX", fontsize=12, color=text_sub, ha='left')

            right_text1 = f"Close: {c:,.0f} ({sign} {abs(diff):,.0f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            right_text2 = f"Vol: {v:,}  |  O: {o:,.0f}  H: {h:,.0f}  L: {l:,.0f}"
            fig.text(0.95, sub_y, right_text2, fontsize=12, color=text_sub, ha='right')
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
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
            main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True)
            if main_chart_path:
                ai_main, ai_threads, ai_blog, ai_x, ai_blind = generate_kr_ai_report(code, name)
                
                # 1️⃣ 본캐용 캡션 (추천 맨 위로!) [cite: 384-388]
                main_caption = (
                    f"🎯 [{dbg.get('sig_type', '')}]\n"
                    f"🎯 추천: {dbg.get('recommend', '스윙, 중장기 / 종가배팅')}\n\n"
                    f"🏢 {name} ({code})\n"
                    f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                    f"📉 [매수/손절 전략]\n"
                    f"- 양봉 길이만큼 분할매수\n"
                    f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                    f"🌟 사전 매집/바닥턴 누적: 별x{dbg.get('s6_count', 0)}\n"
                    f"⭐ 알고리즘 신뢰도: {dbg.get('score', 10)} / 10점\n\n"
                    f"💡 [AI 비즈니스 요약]\n"
                    f"{ai_main}\n\n"
                    f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                    f"⚠️ [면책 조항]\n"
                    f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다. 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                )
                q_main.put((main_chart_path, main_caption))

                # 2️⃣ 홍보용 캡션 (4가지 플랫폼 동시 발송)
                threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False)
                if threads_chart_path:
                    promo_caption = (
                        f"🏢 {name} ({code}) | 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                        f"📱 [Threads 용]\n{ai_threads}\n\n"
                        f"📝 [네이버 블로그 용]\n{ai_blog}\n\n"
                        f"🐦 [X (트위터) 용]\n{ai_x}\n\n"
                        f"🏢 [블라인드 용]\n{ai_blind}\n\n"
                        f"⚠️ [면책 조항] 본 정보는 알고리즘에 의한 기술적 분석일 뿐, 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                    )
                    q_promo.put((threads_chart_path, promo_caption))
                
                print(f"\n✅ [{name}] 본캐 1개 + 홍보 1개 듀얼 전송 완료!")

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
