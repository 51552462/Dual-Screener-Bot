# Dante_KRX_Reverse_Breakout_1D_AI_Pro.py
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
TELEGRAM_TOKEN_MAIN  = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
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

# 💡 2. 본캐 팩트 + 실시간 트렌드 해시태그 생성기 (스팸 차단 및 구글 검색 연동)
def generate_ai_report(ticker_str: str, company_name: str):
    import re, time
    
    # 1. 팩트 데이터 추출 시도
    try:
        if ticker_str.isdigit(): # 한국장
            res = requests.get(f"https://finance.naver.com/item/main.naver?code={ticker_str}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            soup = BeautifulSoup(res.text, 'html.parser')
            sector_kr = soup.select_one('h4.h_sub.sub_tit7 a').text.strip() if soup.select_one('h4.h_sub.sub_tit7 a') else '국내 증시'
        else: # 미국장
            tk = yf.Ticker(ticker_str)
            sector = tk.info.get('sector', '글로벌 산업')
            sector_kr_map = {"Technology": "테크/기술", "Healthcare": "헬스케어", "Financial Services": "금융", "Consumer Cyclical": "소비재", "Industrials": "산업재", "Energy": "에너지", "Basic Materials": "원자재"}
            sector_kr = sector_kr_map.get(sector, sector)
    except:
        sector_kr = '유망 섹터'

    # 비상용 기본 멘트 (AI 뻗었을 때)
    fb_main = f"1. 섹터: {sector_kr}\n2. 실적: 데이터 분석 중\n3. 모멘텀: 수급 유입 및 차트 반등 포착"
    fb_tags = f"X: #{company_name.replace(' ','')} #주식투자\nThreads: #{sector_kr.replace('/','')} #주식스타그램"

    # 3. 구글 AI 호출 (속도 제한 방어 4초 쿨타임)
    for attempt in range(3):
        try:
            time.sleep(4) 
            
            prompt = f"""
            너는 주식 전문 마케터야. [{company_name} ({ticker_str})] 종목과 관련된 오늘자 최신 이슈나 테마를 검색해서 아래 양식에 맞게 딱 출력해.
            
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분해. 굵은 글씨(**) 금지.
            2. [해시태그]는 뜬금없는 단어 금지! 오늘 이 종목/섹터와 가장 연관성 높고 트래픽 터지는 실시간 인기 태그 1, 2위를 X와 Threads 특성에 맞게 2개씩만 작성해. (예: #엔비디아 #AI대장주)

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
            
            # 본캐 부분과 해시태그 부분만 딱 잘라내기
            m_part = re.search(r'\[본캐\](.*?)(?=\[해시태그\])', report, re.DOTALL)
            tag_part = re.search(r'\[해시태그\](.*)', report, re.DOTALL)

            if not (m_part and tag_part): 
                raise ValueError("파싱오류")

            return m_part.group(1).strip(), tag_part.group(1).strip()
        except:
            pass 
            
    # AI 3번 다 실패 시 기본값 리턴
    return fb_main, fb_tags

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
                    valid = False; break
            if valid: score += 2 
    return max(1, min(10, score)) 

def compute_inverse_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    ema112, ema224, ema448, ema60 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values, df['EMA60'].values
    av3 = df['AvgVol3'].values

    moneyOk = (c * v) >= 100_000_000
    priceOk = c >= 1000
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
    
    # ⭐️ 3봉 내 15% 상승 실패 시 누적, 성공 시 리셋 로직 ⭐️
    p_counts = np.zeros(len(c), dtype=int)
    current_p_count = 0
    wait_idx = -1

    for i in range(len(c)):
        # 추세가 완전히 우상향(정배열)으로 바뀌면 카운트 초기화
        if condBullAlign[i]: 
            current_p_count = 0
            wait_idx = -1

        if wait_idx != -1:
            # 타점 발생 후 3봉 이내에 고가가 15% 이상 상승했는지 체크
            if i <= wait_idx + 3:
                if h[i] >= c[wait_idx] * 1.15: # 15% 달성 시 리셋 (시세 분출 완료)
                    current_p_count = 0
                    wait_idx = -1
            # 3봉이 지났는데도 15% 도달을 못했으면 누적 유지 (에너지 응축 중)
            if i == wait_idx + 3 and wait_idx != -1:
                wait_idx = -1

        # 타점 발생 시 카운트 올리고 대기열에 올림
        if signalBase[i]:
            current_p_count += 1
            wait_idx = i
            
        p_counts[i] = current_p_count

    sig_type = "P (연속)" if p_counts[-1] > 1 else "P (신규)"
    trust_score = calculate_trust_score(c, ema60, signalBase)
    
    # 💡 p_count(누적 횟수)를 텔레그램으로 넘겨줌
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "p_count": int(p_counts[-1])}

chart_lock = threading.Lock()

def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, show_volume=False, is_promo=False) -> str:
    with chart_lock:
        try:
            plt.rcParams['font.family'] = 'NanumGothic' # 맥/리눅스의 경우 폰트에 맞게 유지
            plt.rcParams['axes.unicode_minus'] = False
            
            timestamp_ms = int(time.time() * 1000)
            vol_suffix = "promo" if is_promo else "main"
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

            # 💡 [핵심] 홍보용(Promo)일 경우 매일 바뀌는 5가지 프리미엄 테마 적용
            if is_promo:
                # 매일 자정을 기준으로 0~4번 테마가 자동 로테이션
                style_idx = datetime.now(pytz.timezone('Asia/Seoul')).day % 5
                themes = [
                    # 0: 다크 네온 (사이버펑크 느낌의 트렌디 다크)
                    {'bg': '#131722', 'grid': '#2A2E39', 'text': '#FFFFFF', 'sub': '#8A91A5', 'up': '#FF3B69', 'down': '#00B4D8', 'name': 'Dark Neon Theme'},
                    # 1: 인스티튜셔널 화이트 (깔끔한 증권사 리포트 스타일)
                    {'bg': '#FFFFFF', 'grid': '#E0E0E0', 'text': '#000000', 'sub': '#757575', 'up': '#D32F2F', 'down': '#1976D2', 'name': 'Institutional White'},
                    # 2: 블룸버그 딥블랙 (월가 터미널 클래식 스타일)
                    {'bg': '#000000', 'grid': '#333333', 'text': '#FFA500', 'sub': '#A9A9A9', 'up': '#00FF00', 'down': '#FF0000', 'name': 'Terminal Classic'},
                    # 3: 미드나잇 네이비 (모던하고 신뢰감 있는 딥블루)
                    {'bg': '#0A192F', 'grid': '#172A45', 'text': '#E2E8F0', 'sub': '#8892B0', 'up': '#00E676', 'down': '#FF5252', 'name': 'Midnight Navy'},
                    # 4: 차콜 엘레강스 (고급스러운 무광 회색톤)
                    {'bg': '#2B2B2B', 'grid': '#424242', 'text': '#EEEEEE', 'sub': '#9E9E9E', 'up': '#FF8A65', 'down': '#4DD0E1', 'name': 'Charcoal Elegance'}
                ]
                t = themes[style_idx]
            else:
                # 본캐(유료방)는 익숙한 프리미엄 다크 네온으로 고정
                t = {'bg': '#131722', 'grid': '#2A2E39', 'text': '#FFFFFF', 'sub': '#8A91A5', 'up': '#FF3B69', 'down': '#00B4D8', 'name': 'Proprietary Algorithmic Signal'}

            bg_color, grid_color, text_main, text_sub = t['bg'], t['grid'], t['text'], t['sub']
            color_up, color_down = t['up'], t['down']
            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)

            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut['High'].max() - df_cut['Low'].min()) * 0.04 
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - y_offset
            ap = mpf.make_addplot(signal_marker, type='scatter', markersize=300, marker='^', color='#FFD700', alpha=1.0)

            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge='inherit', wick='inherit', volume='inherit')
            s = mpf.make_mpf_style(marketcolors=mc, facecolor=bg_color, edgecolor=bg_color, figcolor=bg_color, gridcolor=grid_color, gridstyle='--', y_on_right=True, rc={'font.family': plt.rcParams['font.family'], 'text.color': text_main, 'axes.labelcolor': text_sub, 'xtick.color': text_sub, 'ytick.color': text_sub})
            
            plt.close('all')
            
            # 본캐는 거래량 포함 길게, 홍보용은 1:1 정방형 썸네일 비율로 시선 집중
            custom_figsize = (11, 6.5) if show_volume else (9, 9)
            title_y, sub_y = (0.93, 0.88) if show_volume else (0.94, 0.90)

            fig, axes = mpf.plot(df_cut, type="candle", volume=show_volume, addplot=ap, style=s, figsize=custom_figsize, tight_layout=False, returnfig=True)
            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            
            fig.text(0.05, title_y, f"{code} | {name}", fontsize=22, fontweight='bold', color=text_main, ha='left')
            fig.text(0.05, sub_y, "1D Chart", fontsize=12, color=text_sub, ha='left')

            # 한/미장 화폐 기호 자동 감지 (코드가 숫자 6자리로만 되어있으면 한국장 원화, 아니면 미국장 달러)
            currency = "" if code.isdigit() and len(code) == 6 else "$"
            c_fmt = f"{currency}{c:,.0f}" if not currency else f"{currency}{c:,.2f}"
            diff_fmt = f"{currency}{abs(diff):,.0f}" if not currency else f"{currency}{abs(diff):,.2f}"
            o_fmt = f"{currency}{o:,.0f}" if not currency else f"{currency}{o:,.2f}"
            h_fmt = f"{currency}{h:,.0f}" if not currency else f"{currency}{h:,.2f}"
            l_fmt = f"{currency}{l:,.0f}" if not currency else f"{currency}{l:,.2f}"

            right_text1 = f"Close: {c_fmt} ({sign} {diff_fmt}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            right_text2 = f"Vol: {v:,}  |  O: {o_fmt}  H: {h_fmt}  L: {l_fmt}"
            fig.text(0.95, sub_y, right_text2, fontsize=12, color=text_sub, ha='right')
            
            # 워터마크에 테마명 인쇄
            fig.text(0.05, 0.03, t['name'], fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
            return None

def scan_market_1d():
    global sent_today, last_run_date
    kr_tz = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(kr_tz).strftime('%Y-%m-%d')
    
    # 💡 3. 당일 중복 발송용 영구 파일 로드
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_p.txt")
    
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

    print(f"\n⚡ [일봉 전용] 한국장 3번(역매공파) 스캔 시작! (당일 중복 차단 🛡️)")
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    
    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    
    def worker(row_tuple):
        try:
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
                    hit, sig_type, df, dbg = compute_inverse_1d(df_raw)
            except Exception:
                pass 
                
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
                        try:
                            with open(log_file, "w") as f:
                                f.write(today_str + "\n")
                                for s_code in sent_today: f.write(s_code + "\n")
                        except: pass
                    
           if hit:
                # 💡 본캐용 차트 생성
                main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
                
                if main_chart_path:
                    # AI 멘트 생성 (본캐용 멘트와 해시태그 뭉치 2개만 리턴받음)
                    ai_main, ai_tags = generate_ai_report(code, name) 
                    
                    # ----------------------------------------------------
                    # (본캐용 main_caption 로직은 기존 유지!)
                    # ----------------------------------------------------
                    
                    # 💡 홍보용 썸네일 차트 생성 (테마 로테이션)
                    threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True) 
                    
                    if threads_chart_path:
                        # 본캐 AI 결과에서 섹터 정보만 추출
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
                        
                        # 화폐 기호 자동 감지
                        currency = "" if code.isdigit() and len(code) == 6 else "$"
                        price_fmt = f"{currency}{dbg.get('last_close', 0):,.0f}" if not currency else f"{currency}{dbg.get('last_close', 0):,.2f}"

                        # ⭐️ 깔끔한 팩트 + 맞춤형 트렌드 해시태그 삽입!
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

    # 💡 5. 일꾼(스레드) 가동 및 대기
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()
        
    print(f"\n✅ [한국장 3번 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

# ⭐️ 3번 스케줄러 세팅 (10:00, 12:30, 15:00) ⭐️
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [3번 검색기] 10:00 / 12:30 / 15:00 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 10 and now_kr.minute == 0) or (now_kr.hour == 12 and now_kr.minute == 30) or (now_kr.hour == 15 and now_kr.minute == 0):
            print(f"🚀 [3번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d()  # ⭐️ 이 줄이 있으면 실행 즉시 1회 스캔을 시작합니다.
    # run_scheduler() # 스케줄러를 같이 돌리려면 이 줄의 주석을 해제하세요.
