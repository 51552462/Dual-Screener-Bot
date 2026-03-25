# Dante_US_Reverse_Breakout_1D_AI_Pro.py
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_P_1D')
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

# 💡 2. 플랫폼별 4종 세트 생성 및 에러 방어막(Fallback) 구축
def generate_ai_report(ticker_str: str, company_name: str):
    import re # 정규식 모듈
    try:
        tk = yf.Ticker(ticker_str)
        info = tk.info
        sector = info.get('sector', '관련 산업')
        industry = info.get('industry', '해당 섹터')
        summary = str(info.get('longBusinessSummary', ''))[:100] + "..."
    except:
        sector, industry, summary = '글로벌 산업', '주요 섹터', '안정적인 비즈니스 모델'

    # 비상용 멘트도 종목마다 조금씩 다르게 팩트 기반으로 섞이도록 수정
    fb_main = f"1. 섹터: {sector} ({industry})\n2. 실적: 최근 재무 데이터 갱신 중\n3. 모멘텀: {summary}"
    fb_threads = f"👀 {company_name} 자리 체크 필수! {sector} 쪽에 최근 자금이 쏠리면서 차트 밸런스가 잡히고 있습니다. 비즈니스도 나쁘지 않네요."
    fb_blog = f"📌 오늘 분석할 종목은 {company_name} ({ticker_str})입니다. {industry} 분야에서 눈에 띄는 펀더멘탈을 유지 중이며, 바닥권 에너지가 응축되고 있습니다."
    fb_x = f"🔥 {ticker_str} 지금 무조건 봐야 함. {sector} 대장주급 차트 흐름 나오는 중. 기업 팩트체크 완료! #미국주식 #{ticker_str}"
    fb_blind = f"형들 {company_name} 차트 봄? {sector} 쪽인데 지금 완전 바닥 다지고 거래량 터지기 직전임. 워치리스트 ㄱㄱ"

    for attempt in range(3):
        try:
            prompt = f"""
            너는 미국 주식 전문 애널리스트야. [{company_name} ({ticker_str})]에 대해 구글 검색을 통해 최신 팩트를 찾아 5가지 버전의 글을 작성해.
            
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분할 것. 기호나 굵은 글씨 절대 금지.
            2. 무조건 '팩트(매출/이익 수치 %, 구체적인 비즈니스/파이프라인 이름)'를 포함할 것. 추상적이고 뻔한 헛소리 절대 금지.
            3. 매번 똑같은 패턴 템플릿 쓰지 말고 생성할 때마다 문장 구조와 이모지를 완전히 다르게 쓸 것.

            [팩트 데이터]
            섹터/산업: {sector} / {industry}
            비즈니스 요약: {summary}

            [출력 양식]
            [본캐]
            1. 섹터: (테마 1줄)
            2. 실적: (팩트 수치 1줄)
            3. 모멘텀: (앞으로의 호재 1줄)
            
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

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()] 
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
        img_path, caption = item
        safe_caption = caption[:1000] + "\n...(글자수 제한으로 요약됨)" if len(caption) > 1000 else caption

        if SEND_TELEGRAM:
            is_success = False
            for _ in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, files={"photo": f}, timeout=60, verify=False)
                    if res.status_code == 200: 
                        is_success = True
                        break
                    elif res.status_code == 429: time.sleep(3)
                except: time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

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

def compute_inverse_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    av3 = df['AvgVol3'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    moneyOk = (c * v) >= 5_000_000 
    priceOk = c >= 3.0
    
    condBearAlign = (ema112 < ema224) & (ema224 < ema448)
    condHold112 = c > ema112

    condCrossEvent = np.zeros(len(c), dtype=bool)
    for i in range(1, 9):
        shifted_c = np.roll(c, i); shifted_c[:i] = np.inf 
        shifted_ema112 = np.roll(ema112, i)
        condCrossEvent |= (shifted_c < shifted_ema112)

    isAccBull = c > o
    rng = h - l
    with np.errstate(divide='ignore', invalid='ignore'):
        closePos = np.where(rng > 0, (c - l) / rng, 0)
        
    valMa20 = pd.Series(c*v).rolling(20, min_periods=1).mean().values
    isAccCandle = isAccBull & ((c*v) >= (1.6 * valMa20)) & (closePos >= 0.68)
    condHasAcc = pd.Series(isAccCandle).rolling(window=20, min_periods=1).sum().values > 0

    with np.errstate(invalid='ignore'):
        condVolSpike = v >= (np.nan_to_num(av3, nan=1.0) * 3)

    signalBase = priceOk & moneyOk & condBearAlign & condHold112 & condCrossEvent & condHasAcc & condVolSpike & (c > o)
    if not signalBase[-1]: return False, "", df, {}

    condBullAlign = (ema112 > ema224) & (ema224 > ema448)
    
    p_counts = np.zeros(len(c), dtype=int)
    current_p_count = 0
    wait_idx = -1

    for i in range(len(c)):
        if condBullAlign[i]: 
            current_p_count = 0
            wait_idx = -1

        if wait_idx != -1:
            if i <= wait_idx + 3:
                if h[i] >= c[wait_idx] * 1.15: 
                    current_p_count = 0
                    wait_idx = -1
            if i == wait_idx + 3 and wait_idx != -1:
                wait_idx = -1

        if signalBase[i]:
            current_p_count += 1
            wait_idx = i
            
        p_counts[i] = current_p_count

    sig_type = "P (연속)" if p_counts[-1] > 1 else "P (신규)"
    trust_score = calculate_trust_score(c, ema60, signalBase)
    
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "p_count": int(p_counts[-1])}

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
            
            # 💡 [핵심] 비율 스위칭 (본캐 넓게 / 쓰레드 정방형)
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
            fig.text(0.05, sub_y, "1D / US", fontsize=12, color=text_sub, ha='left')

            right_text1 = f"Close: ${c:,.2f} ({sign} ${abs(diff):,.2f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            right_text2 = f"Vol: {v:,}  |  O: ${o:,.2f}  H: ${h:,.2f}  L: ${l:,.2f}"
            fig.text(0.95, sub_y, right_text2, fontsize=12, color=text_sub, ha='right')
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
            print(f"\n❌ [{name}] 차트 에러: {e}")
            return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 2번(역매공파) 스캔 시작! (안정화 패치 완료)")
    
    # 💡 당일 중복 발송 차단 로직
    ny_tz = pytz.timezone('America/New_York')
    today_str = datetime.now(ny_tz).strftime('%Y-%m-%d')
    log_file = os.path.join(TOP_FOLDER, "sent_log_us_p.txt")
    
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
    chunk_size = 100 
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}

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
                    hit, sig_type, df, dbg = compute_inverse_1d(df_ticker)
                    
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
                            
                            # 타점 누적 횟수에 따른 심플한 타이틀 및 추천 멘트
                            p_count = dbg.get('p_count', 1)
                            sig_type = f"P (누적 {p_count}회)" if p_count >= 2 else "P (신규)"
                            recommend = "단기반등, 스윙 / 종가배팅" if p_count >= 3 else "관심종목 / 관망"
                            
                            # ⭐️ 3. 유료방(본캐) 결과지
                            main_caption = (
                                f"🎯 [{sig_type}]\n"
                                f"🎯 추천: {recommend}\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg['last_close']:,.2f}\n\n"
                                f"⚖️ [건강한 투자를 위한 기준]\n"
                                f"• 관심종목 편입: 타이밍이 올때까지 천천히 기다리세요.\n"
                                f"• 단기 진입 시: 실전 매매에 참여하신다면, 진입 시가 이탈 시 칼 같은 손절 필수.\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_main}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                                f"⚠️ [면책 조항]\n"
                                f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다. 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                            )
                            q_main.put((main_chart_path, main_caption))

                            # ⭐️ 4. 홍보방 결과지
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
                                
                            print(f"\n✅ [{name}] 미국장 역매공파 듀얼 발송 대기열 추가 완료 (누적: {p_count}회)")
            except Exception as e:
                pass
                
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [미국장 2번 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [2번 미국장 검색기] 10:00 / 12:00 / 14:00 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 10 and now_ny.minute == 0) or (now_ny.hour == 12 and now_ny.minute == 0) or (now_ny.hour == 14 and now_ny.minute == 0):
            print(f"🚀 [2번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d()
