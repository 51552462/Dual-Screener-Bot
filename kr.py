# Dante_KRX_Bowl_1D_AI_Pro.py
import os, re, time, threading, queue, concurrent.futures
from datetime import datetime, timedelta
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
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

# 💡 [공통] 본캐 팩트 + 실시간 트렌드 해시태그 파싱 제거
def generate_ai_report(code: str, company_name: str):
    import re, time
    import yfinance as yf
    
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

    # 2. 구글 AI 호출
    for attempt in range(3):
        try:
            time.sleep(4) 
            
            prompt = f"""
            너는 주식 전문 마케터야. [{company_name} ({code})] 종목과 관련된 오늘자 최신 이슈나 테마를 검색해서 아래 양식에 맞게 딱 출력해.
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분해. 굵은 글씨(**) 금지.

            [본캐]
            1. 섹터: (어떤 테마인지 한글로 1줄 요약)
            2. 실적: (팩트 수치 한글 1줄 요약)
            3. 모멘텀: (앞으로의 호재 한글 1줄 요약)
            """
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
            
            if not response or not response.text:
                continue
                
            report = response.text.replace('*', '').strip() 
            
            m_part = re.search(r'\[본캐\](.*)', report, re.DOTALL)

            if not m_part: 
                raise ValueError("파싱오류")

            return m_part.group(1).strip(), ""
        except:
            pass 
            
    return fb_main, ""

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
                    valid = False
                    break
            if valid: score += 2 
    return max(1, min(10, score)) 

def compute_bobgeureut(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    
    ma20 = pd.Series(c).rolling(20, min_periods=1).mean().values
    stddev = pd.Series(c).rolling(20, min_periods=1).std(ddof=1).values
    bbUpper = ma20 + (stddev * 2)

    tenkan = (pd.Series(h).rolling(9, min_periods=1).max() + pd.Series(l).rolling(9, min_periods=1).min()) / 2
    kijun = (pd.Series(h).rolling(26, min_periods=1).max() + pd.Series(l).rolling(26, min_periods=1).min()) / 2
    spanA = (tenkan + kijun) / 2
    spanB = (pd.Series(h).rolling(52, min_periods=1).max() + pd.Series(l).rolling(52, min_periods=1).min()) / 2
    senkou1 = np.roll(spanA, 25); senkou1[:25] = np.nan
    senkou2 = np.roll(spanB, 25); senkou2[:25] = np.nan
    cloudTop = np.fmax(senkou1, senkou2)

    mean120 = pd.Series(c).rolling(120, min_periods=1).mean().values
    std120 = pd.Series(c).rolling(120, min_periods=1).std(ddof=1).values
    mean120_s = np.roll(mean120, 5); mean120_s[:5] = np.nan
    std120_s = np.roll(std120, 5); std120_s[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox6m = (std120_s / mean120_s) < 0.20

    mean60 = pd.Series(c).rolling(60, min_periods=1).mean().values
    std60 = pd.Series(c).rolling(60, min_periods=1).std(ddof=1).values
    mean60_s = np.roll(mean60, 5); mean60_s[:5] = np.nan
    std60_s = np.roll(std60, 5); std60_s[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox3m = (std60_s / mean60_s) < 0.20

    isCat2 = condBox6m
    isCat1 = (~condBox6m) & condBox3m
    hasBox = isCat1 | isCat2

    ema10, ema20, ema30, ema60, ema112, ema224 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values, df['EMA112'].values, df['EMA224'].values
    condPrice = c >= 1000
    isBullish = c > o
    prev_c = np.roll(c, 1); prev_c[0] = np.inf
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    condEma = (c > ema224) & (prev_c < prev_ema224 * 1.05)
    condCloud = (c > cloudTop) & (~np.isnan(cloudTop))
    condBb = c >= bbUpper * 0.98
    volAvg = pd.Series(v).rolling(20, min_periods=1).mean().values
    condVol = v > volAvg * 2.0
    condNotOverheated = c <= ema224 * 1.15
    
    v_1 = np.roll(v, 1); v_1[0] = 0
    v_2 = np.roll(v, 2); v_2[:2] = 0
    v_3 = np.roll(v, 3); v_3[:3] = 0
    avgVol3 = (v_1 + v_2 + v_3) / 3
    condVolSpike = v >= (avgVol3 * 5)

    signalBase = condPrice & isBullish & condEma & condCloud & condBb & condVol & condNotOverheated & hasBox & condVolSpike
    if not signalBase[-1]: return False, "", df, {}

    signalCat2 = signalBase & isCat2
    signalCat1 = signalBase & isCat1
    isAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224)

    if (signalCat2 & isAligned)[-1] or (signalCat1 & isAligned)[-1]: sig_type = "B (J 강조)"
    else: sig_type = "B (일반)"

    # ⭐️ 밥그릇 Cat2(바닥권) 타점 기준: 3봉 내 15% 상승 실패 시 누적, 성공 시 리셋 로직 ⭐️
    cat2_counts = np.zeros(len(c), dtype=int)
    current_cat2_count = 0
    wait_idx = -1

    for i in range(len(c)):
        if wait_idx != -1:
            # 타점 발생 후 3봉 이내에 고가가 15% 이상 상승했는지 체크
            if i <= wait_idx + 3:
                if h[i] >= c[wait_idx] * 1.15: # 15% 달성 시 리셋 (시세 분출 완료)
                    current_cat2_count = 0
                    wait_idx = -1
         
            # 3봉이 지났는데도 15% 도달을 못했으면 누적 유지 (세력의 가격 통제 및 매집 지속)
            if i == wait_idx + 3 and wait_idx != -1:
                wait_idx = -1

        # Cat2 타점 발생 시 카운트 올리고 대기열에 등록
        if signalCat2[i]:
            current_cat2_count += 1
            wait_idx = i
            
        cat2_counts[i] = current_cat2_count

    trust_score = calculate_trust_score(c, ema60, signalBase)

    # 💡 cat2_count(누적 횟수)를 텔레그램으로 넘겨줌
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "cat2_count": int(cat2_counts[-1])}

# 💡 매일 로테이션되는 5가지 프리미엄 차트 테마
def get_daily_theme():
    theme_idx = datetime.now().day % 5
    themes = [
        {'bg': '#0B0E14', 'grid': '#1A202C', 'text': '#FFFFFF', 'up': '#F6465D', 'down': '#0ECB81'}, # 0: Binance Dark
        {'bg': '#FFFFFF', 'grid': '#F0F0F0', 'text': '#131722', 'up': '#E0294A', 'down': '#2EBD85'}, # 1: Institutional White
        {'bg': '#131722', 'grid': '#2A2E39', 'text': '#D1D4DC', 'up': '#26A69A', 'down': '#EF5350'}, # 2: TradingView Premium
        {'bg': '#000000', 'grid': '#111111', 'text': '#00FFA3', 'up': '#00FFA3', 'down': '#FF3366'}, # 3: Cyberpunk
        {'bg': '#F8F9FA', 'grid': '#E9ECEF', 'text': '#212529', 'up': '#FF4757', 'down': '#2ED573'}  # 4: Modern Light
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
            
            # 💡 홍보용 vs 본캐용 분기
            if is_promo:
                theme = get_daily_theme()
                bg_color, grid_color, text_main = theme['bg'], theme['grid'], theme['text']
                color_up, color_down = theme['up'], theme['down']
                text_sub = text_main
                custom_figsize = (9, 9) 
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
            
            right_text1 = f"{sign} {abs(diff_pct):.2f}%" if is_promo else f"Close: {c:,.0f} ({sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=22 if is_promo else 18, fontweight='bold', color=color_diff, ha='right')

            if not is_promo:
                right_text2 = f"Vol: {v:,}  | O: {o:,.0f}  H: {h:,.0f}  L: {l:,.0f}"
                fig.text(0.95, sub_y, right_text2, fontsize=12, color=text_sub, ha='right')
                
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
    
    # 💡 3. 당일 중복 발송용 영구 파일 로드
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_b.txt")
    
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

    print(f"\n⚡ [일봉 전용] 한국장 4번(밥그릇) 스캔 시작! (당일 중복 차단 🛡️)")
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
                    hit, sig_type, df, dbg = compute_bobgeureut(df_raw)
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
                # 💡 본캐용 차트 및 홍보용 차트 각각 생성
                main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
                threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
                
                if main_chart_path and threads_chart_path:
                    # 💡 안전해진 AI 로직으로 팩트 호출
                    ai_main, _ = generate_ai_report(code, name)
                    
                    # 1️⃣ 본캐용 캡션 (유료방용 - 기존 멘트 유지)
                    main_caption = (
                        f"🎯 [{dbg.get('sig_type', '')}]\n"
                        f"🎯 추천: {dbg.get('recommend', '스윙, 중장기 / 종가배팅')}\n\n"
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

                    # 2️⃣ 홍보용 캡션 (쓸데없는 멘트 다 빼고 압축!)
                    try:
                        sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                    except:
                        sector_info = "유망 섹터 포착"
                        
                    # ⭐️ 멘트 싹 날리고 [차트+종목+섹터+현재가]만!
                    promo_caption = (
                        f"📈 [알고리즘 차트 포착]\n\n"
                        f"🏢 종목: {name} ({code})\n"
                        f"🏷️ 섹터: {sector_info}\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원"
                    )
                    q_promo.put((threads_chart_path, promo_caption))

                    print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")
        except Exception as e:
            pass

    # 💡 5. 일꾼(스레드) 가동 및 대기
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()
        
    print(f"\n✅ [한국장 4번 밥그릇 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")
    
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [4번 검색기] 10:30 / 13:00 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 10 and now_kr.minute == 30) or (now_kr.hour == 13 and now_kr.minute == 0):
            print(f"🚀 [4번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
