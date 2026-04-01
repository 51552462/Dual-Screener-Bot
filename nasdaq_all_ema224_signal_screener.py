# Dante_US_Ohdole_1D_AI_Pro.py
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Ohdole_1D')
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

# 💡 [공통] 본캐 팩트 + 실시간 트렌드 해시태그 생성기
def generate_ai_report(code: str, company_name: str):
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
            
    return fb_main, fb_tags

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()] 
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

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

def calculate_trust_score(c, e60):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   
    return max(1, min(10, score))

# 💡 누락되었던 별점 채점기 추가
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
    is_money_ok = (c * v) >= 5_000_000
    is_price_ok = c >= 3.0
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
    print(f"\n🇺🇸 [일봉 전용] 미국장 1번(오돌이) 스캔 시작!")

    # 💡 당일 중복 발송 차단 로직
    ny_tz = pytz.timezone('America/New_York')
    today_str = datetime.now(ny_tz).strftime('%Y-%m-%d')
    log_file = os.path.join(TOP_FOLDER, "sent_log_us.txt")
    
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
                    hit, sig_type, df, dbg = compute_ohdole_1d(df_ticker)
                    
                    if hit:
                # 💡 본캐용 차트 생성 (is_promo=False)
                main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
                
                if main_chart_path:
                    # 💡 변경점: 이제 함수가 5개가 아니라 딱 2개(본캐 팩트, 해시태그)만 뱉어냅니다!
                    ai_main, ai_tags = generate_ai_report(code, name)
                    
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
                    q_main.put((main_chart_path, main_caption))

                    # 2️⃣ 홍보용 캡션 (쓸데없는 멘트 다 빼고 압축!)
                    # 💡 is_promo=True 로 차트 테마 자동 로테이션 적용
                    threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
                    
                    if threads_chart_path:
                        # 본캐 AI 결과에서 '섹터' 부분만 딱 뽑아오기
                        try:
                            sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                        except:
                            sector_info = "유망 섹터 포착"
                            
                        # AI가 뽑아준 해시태그 분리
                        try:
                            x_tags = re.search(r'X:\s*(.*)', ai_tags).group(1).strip()
                            th_tags = re.search(r'Threads:\s*(.*)', ai_tags).group(1).strip()
                        except:
                            x_tags = f"#{code} #주식"
                            th_tags = "#주식투자 #재테크"
                        
                        # 화폐 기호 자동 감지 (한국장 6자리 숫자는 원화 없음, 미국장은 $)
                        currency = "" if code.isdigit() and len(code) == 6 else "$"
                        price_fmt = f"{currency}{dbg.get('last_close', 0):,.0f}" if not currency else f"{currency}{dbg.get('last_close', 0):,.2f}"

                        # ⭐️ 멘트 싹 날리고 [차트+종목+섹터+현재가+해시태그]만!
                        promo_caption = (
                            f"📈 [알고리즘 차트 포착]\n\n"
                            f"🏢 종목: {name} ({code})\n"
                            f"🏷️ 섹터: {sector_info}\n"
                            f"💰 현재가: {price_fmt}\n\n"
                            f"🐦 X(트위터) 추천 태그:\n{x_tags}\n\n"
                            f"📱 Threads 추천 태그:\n{th_tags}\n\n"
                            f"⚠️ 본 정보는 기술적 분석일 뿐, 매수/매도 권유가 아닙니다."
                        )
                        q_promo.put((threads_chart_path, promo_caption))

                    print(f"\n✅ [{name}] 듀얼 발송 대기열 추가 완료!")
            except Exception as e:
                pass
                
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [미국장 1번 E 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [1번 미국장 오돌이 검색기] 09:30 / 12:00 / 14:30 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 9 and now_ny.minute == 30) or (now_ny.hour == 12 and now_ny.minute == 0) or (now_ny.hour == 14 and now_ny.minute == 30):
            print(f"🚀 [1번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d()
