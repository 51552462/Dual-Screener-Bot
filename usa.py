# Dante_US_Bowl_1D_AI_Pro.py
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
import yfinance as yf
import FinanceDataReader as fdr
import logging

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
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리)
TELEGRAM_TOKEN_MAIN  = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_TOKEN_PROMO = "7996581031:AAFou3HWYhIXzRtlW4ildx8tOitcQBVubPg"
TELEGRAM_CHAT_ID     = "6838834566"
SEND_TELEGRAM        = True

q_main = queue.Queue()
q_promo = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Bowl_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

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

# 💡 2. 100% 스팸 회피형 스핀택스(Spintax) + AI 쿨타임 방어막 탑재
def generate_ai_report(ticker_str: str, company_name: str):
    import re, random, time
    
    # 1. 팩트 데이터 추출
    try:
        tk = yf.Ticker(ticker_str)
        info = tk.info
        sector = info.get('sector', '글로벌 산업')
        industry = info.get('industry', '주요 섹터')
        
        # 유료방 영문 도배 방지용 한글 변환 맵 (대표적인 것만 처리, 나머진 그대로)
        sector_kr_map = {"Technology": "테크/기술", "Healthcare": "헬스케어", "Financial Services": "금융", "Consumer Cyclical": "소비재", "Industrials": "산업재", "Energy": "에너지", "Basic Materials": "원자재"}
        sector_kr = sector_kr_map.get(sector, sector)
    except:
        sector_kr, industry = '글로벌 산업', '유망 섹터'

    # 2. 🤖 무한 랜덤 문장 조합기 (Spintax) - 스팸 필터 100% 우회
    # 쓰레드용 랜덤 조합
    th_intro = random.choice([f"👀 {company_name} 자리 체크 필수!", f"🔥 {ticker_str} 수급 들어오는 거 보이시나요?", f"🚨 지금 {sector_kr} 관련해서 심상치 않은 종목 하나 뜹니다.", f"💡 {company_name} 차트가 아주 예쁘게 만들어지고 있네요."])
    th_body = random.choice([f"최근 {industry} 쪽으로 자금이 쏠리면서 완벽한 밸런스가 잡혔습니다.", "바닥 다지고 머리 드는 전형적인 턴어라운드 흐름입니다.", "비즈니스 펀더멘탈도 나쁘지 않고 기술적 타점도 예술이네요."])
    th_outro = random.choice(["킵해두고 지켜보세요!", "워치리스트에 당장 추가하세요.", "단기 시세 분출 기대해볼 만합니다."])
    fb_threads = f"{th_intro} {th_body} {th_outro}"

    # 블로그용 랜덤 조합
    bg_intro = random.choice([f"📌 오늘 분석해 볼 미국 주식은 {company_name} ({ticker_str})입니다.", f"📈 {sector_kr} 섹터에서 유의미한 흐름을 보여주는 {company_name}을(를) 살펴봅니다.", f"📊 주목해야 할 {industry} 관련주, {ticker_str} 차트 분석입니다."])
    bg_body = random.choice(["알고리즘 상 강한 매수 에너지가 응축되고 있는 것이 특징입니다.", "오랜 기간 바닥을 다진 후 추세 전환의 초입에 위치해 있습니다.", "시장 소외 구간을 지나 본격적인 거래량 유입이 기대되는 자리입니다."])
    fb_blog = f"{bg_intro} {bg_body} 기술적 반등 시나리오를 참고하시어 투자 전략을 세워보시길 바랍니다."

    # X(트위터)용 랜덤 조합
    x_intro = random.choice([f"🔥 {ticker_str} 지금 당장 봐야 함.", f"🚨 {company_name} 자리 폼 미쳤음.", f"👀 {sector_kr} 대장주급 차트 등장."])
    x_body = random.choice(["바닥 탈출 시그널 떴음.", "수급 쫙 빨아들이기 직전.", "알고리즘 타점 정확히 들어왔음."])
    fb_x = f"{x_intro} {x_body} 팩트체크 필수! #미국주식 #{ticker_str} #{sector_kr.replace('/', '')}"

    # 블라인드용 랜덤 조합
    bl_intro = random.choice([f"형들 {company_name} 차트 봄?", f"{ticker_str} 이거 지금 나만 보고 있는 거 아니지?", f"미장 {sector_kr} 쪽인데 지금 자리 개꿀임."])
    bl_body = random.choice(["완전 바닥 다지고 거래량 터지기 직전인 듯.", "알고리즘에 딱 걸림. 재무도 평타 이상.", "차트충 등판해봐 이거 무조건 반등 자리 아님?"])
    fb_blind = f"{bl_intro} {bl_body} 워치리스트 ㄱㄱ"

    # 유료방 본캐용 깔끔한 한글 대체 멘트 (더 이상 영문 덤프 안 됨)
    fb_main = f"1. 섹터: {sector_kr} ({industry})\n2. 실적: 최근 재무 데이터 및 시장 컨센서스 분석 중\n3. 모멘텀: 차트 상 유의미한 바닥권 탈출 및 수급 유입 패턴 포착"

    # 3. 구글 AI 호출 (속도 제한 방어 쿨타임 적용)
    for attempt in range(3):
        try:
            time.sleep(4) # 💡 핵심: 4초 대기! 이걸 넣어야 구글이 스팸으로 차단하지 않습니다.
            
            prompt = f"""
            너는 미국 주식 전문 애널리스트야. [{company_name} ({ticker_str})]에 대해 구글 검색을 통해 최신 팩트를 찾아 5가지 버전의 글을 작성해.
            
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분할 것. 기호나 굵은 글씨(**) 절대 금지.
            2. 무조건 '한글'로만 작성할 것. 영어 원문 그대로 출력 금지.
            3. 실적이나 파이프라인 등 구체적인 '팩트 수치/이름'을 포함할 것.
            4. 매번 문장 구조와 이모지를 완전히 다르게 창작할 것.

            [팩트 데이터]
            섹터/산업: {sector_kr} / {industry}

            [출력 양식]
            [본캐]
            1. 섹터: (어떤 테마인지 한글로 1줄 요약)
            2. 실적: (팩트 수치 한글 1줄 요약)
            3. 모멘텀: (앞으로의 호재 한글 1줄 요약)
            
            [쓰레드]
            (트렌디한 말투, 이모지, 구체적 팩트 포함 2~3문장)
            
            [블로그]
            (전문가 말투, 구체적 팩트 기반 3~4문장)
            
            [X]
            (다급한 느낌, 팩트 위주, 해시태그 2~3개)
            
            [블라인드]
            (블라인드 주식게시판 반말/형들체, 팩트 포함 2~3문장)
            """
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
            
            if not response or not response.text:
                continue
                
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
            pass # 에러 나면 다음 시도로 넘어가고, 3번 다 실패하면 밖으로 빠져나감
            
    # 💡 4. AI가 결국 실패하더라도 준비된 '무한 랜덤 문장'이 대신 나감
    return fb_main, fb_threads, fb_blog, fb_x, fb_blind

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

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
                    valid = False
                    break
            if valid: score += 2 
    return max(1, min(10, score))

def compute_bobgeureut_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    ema10, ema20, ema30, ema60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    ema112, ema224, ema448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    ma20 = pd.Series(c).rolling(20, min_periods=1).mean().values
    stddev = pd.Series(c).rolling(20, min_periods=1).std(ddof=1).values
    bb_upper = ma20 + (stddev * 2)

    tenkan = (pd.Series(h).rolling(9, min_periods=1).max() + pd.Series(l).rolling(9, min_periods=1).min()) / 2
    kijun = (pd.Series(h).rolling(26, min_periods=1).max() + pd.Series(l).rolling(26, min_periods=1).min()) / 2
    spanA = (tenkan + kijun) / 2
    spanB = (pd.Series(h).rolling(52, min_periods=1).max() + pd.Series(l).rolling(52, min_periods=1).min()) / 2
    senkou1 = np.roll(spanA, 25); senkou1[:25] = np.nan
    senkou2 = np.roll(spanB, 25); senkou2[:25] = np.nan
    cloud_top = np.fmax(senkou1, senkou2)

    volavg20 = pd.Series(v).rolling(20, min_periods=1).mean().values
    avgvol3 = pd.Series(v).shift(1).rolling(3, min_periods=1).mean().values

    mean120 = pd.Series(c).rolling(120, min_periods=1).mean().shift(5).values
    std120 = pd.Series(c).rolling(120, min_periods=1).std(ddof=1).shift(5).values
    mean60 = pd.Series(c).rolling(60, min_periods=1).mean().shift(5).values
    std60 = pd.Series(c).rolling(60, min_periods=1).std(ddof=1).shift(5).values
    
    with np.errstate(divide='ignore', invalid='ignore'):
        isCat2 = (std120 / mean120) < 0.20
        isCat1 = (~isCat2) & ((std60 / mean60) < 0.20)
    hasBox = isCat1 | isCat2

    isBullish = c > o
    prev_c = np.roll(c, 1); prev_c[0] = np.inf
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    
    condEma = (c > ema224) & (prev_c < prev_ema224 * 1.05)
    condCloud = (c > cloud_top) & (~np.isnan(cloud_top))
    condBb = c >= bb_upper * 0.98
    condVol = v > volavg20 * 2.0
    condNotOverheated = c <= ema224 * 1.15
    
    with np.errstate(invalid='ignore'): condVolSpike = v >= (np.nan_to_num(avgvol3, nan=1.0) * 5)
    
    condMoney = (c * v) >= 1_000_000
    condPrice = c >= 1.0

    signalBase = condPrice & condMoney & isBullish & condEma & condCloud & condBb & condVol & condNotOverheated & hasBox & condVolSpike
    
    if not signalBase[-1]: return False, "", df, {}

    signalCat2 = signalBase & isCat2
    signalCat1 = signalBase & isCat1
    isAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224)

    if (signalCat2 & isAligned)[-1] or (signalCat1 & isAligned)[-1]: sig_type = "B (J 강조)"
    else: sig_type = "B (일반)"

    # ⭐️ 바닥권(Cat2) 3봉 내 15% 상승 실패 시 누적, 성공 시 리셋 로직 ⭐️
    cat2_counts = np.zeros(len(c), dtype=int)
    current_cat2_count = 0
    wait_idx = -1

    for i in range(len(c)):
        if wait_idx != -1:
            if i <= wait_idx + 3:
                if h[i] >= c[wait_idx] * 1.15: 
                    current_cat2_count = 0
                    wait_idx = -1
            if i == wait_idx + 3 and wait_idx != -1:
                wait_idx = -1

        if signalCat2[i]:
            current_cat2_count += 1
            wait_idx = i
            
        cat2_counts[i] = current_cat2_count

    trust_score = calculate_trust_score(c, ema60, signalBase)

    # 에러 원인이었던 s67_count 버그 해결
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "cat2_count": int(cat2_counts[-1])}

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
            
            plt.close('all')
            fig, axes = mpf.plot(df_cut, type="candle", volume=show_volume, addplot=ap, style=s, figsize=(11, 6.5), tight_layout=False, returnfig=True)

            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            fig.text(0.05, 0.93, f"{code} | {name}", fontsize=22, fontweight='bold', color=text_main, ha='left')
            fig.text(0.05, 0.88, "1D / US", fontsize=12, color=text_sub, ha='left')

            right_text1 = f"Close: ${c:,.2f} ({sign} ${abs(diff):,.2f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, 0.93, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            right_text2 = f"Vol: {v:,}  |  O: ${o:,.2f}  H: ${h:,.2f}  L: ${l:,.2f}"
            fig.text(0.95, 0.88, right_text2, fontsize=12, color=text_sub, ha='right')
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
            return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 3번(밥그릇) 스캔 시작!")

    # 💡 당일 중복 발송 차단 로직
    ny_tz = pytz.timezone('America/New_York')
    today_str = datetime.now(ny_tz).strftime('%Y-%m-%d')
    log_file = os.path.join(TOP_FOLDER, "sent_log_us_b.txt")
    
    sent_today = set()
    if os.path.exists(log_file):
        try:
            with open(log_file, "r") as f:
                lines = f.read().splitlines()
                if lines and lines[0] == today_str:
                    sent_today = set(lines[1:])
        except: pass
    ticker_to_info = {row['Symbol']: {'code': row['Symbol'], 'name': row['Name']} for _, row in stock_list.iterrows()}
    tickers = list(ticker_to_info.keys())
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    chunk_size = 100 

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        df_batch = None
        fallback_dict = {}

        try:
            df_batch = yf.download(" ".join(chunk), interval="1d", period="3y", group_by="ticker", progress=False, threads=False)
        except:
            def fetch_single(tk):
                try:
                    df_s = yf.download(tk, interval="1d", period="3y", progress=False, threads=False)
                    if not df_s.empty: fallback_dict[tk] = df_s
                except: pass
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                executor.map(fetch_single, chunk)

        for tk in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(tk)
            if not info: continue
            name, code = info['name'], info['code']

            try:
                if df_batch is not None:
                    if len(chunk) == 1: df_ticker = df_batch.copy()
                    else: 
                        if tk not in df_batch.columns.get_level_values(0): continue
                        df_ticker = df_batch[tk].copy()
                else:
                    df_ticker = fallback_dict.get(tk)
                if df_ticker is None or df_ticker.empty: continue

                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                if df_ticker.index.tzinfo is not None: df_ticker.index = df_ticker.index.tz_convert('America/New_York').tz_localize(None)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    hit, sig_type, df, dbg = compute_bobgeureut_1d(df_ticker)
                    
                    if hit:
                        # 💡 1. 당일 중복 차단
                        if code in sent_today: continue
                        sent_today.add(code)
                        try:
                            with open(log_file, "w") as f:
                                f.write(today_str + "\n")
                                for s_code in sent_today: f.write(s_code + "\n")
                        except: pass

                        tracker['hits'] += 1
                        
                        main_chart_path = save_chart(df, code, name, tracker['hits'], dbg, show_volume=True)
                        if main_chart_path:
                            # 💡 2. 5가지 버전 멘트 가져오기
                            ai_main, ai_threads, ai_blog, ai_x, ai_blind = generate_ai_report(code, name)
                            
                            # 타점 누적 횟수 및 J강조에 따른 심플한 타이틀 및 추천 멘트
                            cat2_count = dbg.get('cat2_count', 0)
                            if cat2_count >= 3:
                                sig_type = f"B (누적 {cat2_count}회)"
                                recommend = "관심종목, 중장기 / 종가배팅"
                            elif "J 강조" in dbg.get('sig_type', ""):
                                sig_type = "B (J 강조)"
                                recommend = "스윙, 중장기 / 종가배팅"
                            else:
                                sig_type = "B"
                                recommend = "관심종목 / 관망"
                            
                            # ⭐️ 3. 유료방(본캐) 결과지: 추천 맨 위로, 불필요한 수식어 제거 완료!
                            main_caption = (
                                f"🎯 [{sig_type}]\n"
                                f"🎯 추천: {recommend}\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                                f"⚖️ [건강한 매매를 위한 가이드]\n"
                                f"• 여유로운 접근: 현재가부터 천천히 모아가며 마음의 여유를 가지세요.\n"
                                f"• 원칙 대응: 약속된 지지라인(-5%) 이탈 시에는 기계적으로 대응하여 소중한 자산을 보호합니다.\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_main}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                                f"⚠️ [면책 조항]\n"
                                f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다. 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                            )
                            q_main.put((main_chart_path, main_caption))

                            # ⭐️ 4. 홍보방 결과지: 4가지 플랫폼 맞춤형!
                            threads_chart_path = save_chart(df, code, name, tracker['hits'], dbg, show_volume=False)
                            if threads_chart_path:
                                promo_caption = (
                                    f"🏢 {name} ({code}) | 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                                    f"📱 [Threads 용]\n{ai_threads}\n\n"
                                    f"📝 [네이버 블로그 용]\n{ai_blog}\n\n"
                                    f"🐦 [X (트위터) 용]\n{ai_x}\n\n"
                                    f"🏢 [블라인드 용]\n{ai_blind}\n\n"
                                    f"⚠️ [면책 조항] 본 정보는 기술적 분석일 뿐, 매수/매도 권유가 아닙니다. 책임은 투자자 본인에게 있습니다."
                                )
                                q_promo.put((threads_chart_path, promo_caption))
                                
                            print(f"\n✅ [{name}] 미국장 밥그릇 듀얼 발송 대기열 추가 완료!")
            except Exception as e:
                pass
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    dt = time.time() - t0
    print(f"\n✅ [미국장 3번 B 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [3번 미국장 밥그릇 검색기] 10:30 / 12:30 / 14:30 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 10 and now_ny.minute == 30) or (now_ny.hour == 12 and now_ny.minute == 30) or (now_ny.hour == 14 and now_ny.minute == 30):
            print(f"🚀 [3번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d()
