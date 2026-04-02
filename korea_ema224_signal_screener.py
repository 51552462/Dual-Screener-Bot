# Dante_KRX_Danta_1D30m_AI_Pro.py
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
                except: time.sleep(2)
            time.sleep(1.5)
        target_queue.task_done()

threading.Thread(target=telegram_sender_daemon, args=(q_main, TELEGRAM_TOKEN_MAIN), daemon=True).start()
threading.Thread(target=telegram_sender_daemon, args=(q_promo, TELEGRAM_TOKEN_PROMO), daemon=True).start()

def generate_ai_report(code: str, company_name: str):
    try:
        if code.isdigit(): 
            res = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            soup = BeautifulSoup(res.text, 'html.parser')
            sector_kr = soup.select_one('h4.h_sub.sub_tit7 a').text.strip() if soup.select_one('h4.h_sub.sub_tit7 a') else '국내 증시'
        else: 
            tk = yf.Ticker(code)
            sector = tk.info.get('sector', '글로벌 산업')
            sector_kr_map = {"Technology": "테크/기술", "Healthcare": "헬스케어", "Financial Services": "금융", "Consumer Cyclical": "소비재", "Industrials": "산업재", "Energy": "에너지", "Basic Materials": "원자재"}
            sector_kr = sector_kr_map.get(sector, sector)
    except:
        sector_kr = '유망 섹터'

    fb_main = f"1. 섹터: {sector_kr}\n2. 실적: 데이터 분석 중\n3. 모멘텀: 수급 유입 및 차트 반등 포착"

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
            
            if not response or not response.text: continue
            report = response.text.replace('*', '').strip() 
            m_part = re.search(r'\[본캐\](.*)', report, re.DOTALL)

            if not m_part: raise ValueError("파싱오류")
            return m_part.group(1).strip(), ""
        except: pass 
            
    return fb_main, ""

# 💡 잡주 필터 (스팩, ETN, ETF, 우선주, 리츠 등 차단)
def get_krx_list_kind():
    try:
        df_ks = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt", verify=False, timeout=10).text), header=0)[0]
        df_ks['Market'] = 'KOSPI'
        df_kq = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt", verify=False, timeout=10).text), header=0)[0]
        df_kq['Market'] = 'KOSDAQ'
        df = pd.concat([df_ks, df_kq])
        df['Code'] = df['종목코드'].astype(str).str.zfill(6)
        df = df.rename(columns={'회사명': 'Name'})
        
        junk_pattern = '스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권'
        clean_df = df[~df['Name'].str.contains(junk_pattern, regex=True)].copy()
        return clean_df[['Code', 'Name', 'Market']].dropna()
    except: return pd.DataFrame()

# 💡 단타 타점 시그널만 남긴 계산기
def compute_danta_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 450: return False, "", df_raw, {}
    df = df_raw.copy()
    
    c = df['Close'].values
    o = df['Open'].values
    v = df['Volume'].values
    
    sma5 = df['Close'].rolling(5).mean().values
    sma20 = df['Close'].rolling(20).mean().values
    sma60 = df['Close'].rolling(60).mean().values
    ema224 = df['Close'].ewm(span=224, adjust=False).mean().values
    ema448 = df['Close'].ewm(span=448, adjust=False).mean().values
    
    sma40 = df['Close'].rolling(40).mean().values
    std40 = df['Close'].rolling(40).std(ddof=1).values
    bbUpper = sma40 + (std40 * 2)
    
    prev_v = df['Volume'].shift(1).values
    avgVol5_prev = pd.Series(prev_v).rolling(5).mean().values
    val = c * v
    avgVal5_prev = pd.Series(val).shift(1).rolling(5).mean().values
    
    with np.errstate(invalid='ignore'):
        condValMin = avgVal5_prev >= 500_000_000 # 5일 평균 거래대금 5억
        condVolMin = avgVol5_prev >= 70_000      # 5일 평균 거래량 7만 주
        condVolSurge = v >= prev_v               
        
        prev_c = df['Close'].shift(1).values
        condGap = (o >= prev_c * 1.02) & (o <= prev_c * 1.15) 
        condPriceUp = c >= prev_c * 1.025
        
        condBBUpper = c >= bbUpper * 0.96 
        condTrend = (c > sma5) & (c > sma20) & (c > sma60) 
        
        condNotOverheated = (c <= ema224 * 1.30) & (c <= ema448 * 1.30)
        
        isBullish = c > o
        
        signal = condVolSurge & condValMin & condVolMin & condGap & (condPriceUp | condBBUpper) & condTrend & condNotOverheated & isBullish
    
    if not signal[-1]:
        return False, "", df, {}
        
    sig_type = "🔥일봉+30m 완벽 교집합"
    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": "오전장 단타 / 데이트레이딩"
    }

def get_daily_theme():
    theme_idx = datetime.now().day % 5
    themes = [
        {'bg': '#0B0E14', 'grid': '#1A202C', 'text': '#FFFFFF', 'up': '#F6465D', 'down': '#0ECB81'}, 
        {'bg': '#FFFFFF', 'grid': '#F0F0F0', 'text': '#131722', 'up': '#E0294A', 'down': '#2EBD85'}, 
        {'bg': '#131722', 'grid': '#2A2E39', 'text': '#D1D4DC', 'up': '#26A69A', 'down': '#EF5350'}, 
        {'bg': '#000000', 'grid': '#111111', 'text': '#00FFA3', 'up': '#00FFA3', 'down': '#FF3366'}, 
        {'bg': '#F8F9FA', 'grid': '#E9ECEF', 'text': '#212529', 'up': '#FF4757', 'down': '#2ED573'}  
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
            return None

def scan_market_danta():
    global sent_today, last_run_date
    kr_tz = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(kr_tz).strftime('%Y-%m-%d')
    
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_danta.txt")
    
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

    print(f"\n⚡ [단타 전용] 한국장 오전 무한루프 스캔 진행 중... (1D + 30m 완벽 교집합 검증)")
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    def worker(row_tuple):
        try:
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            
            # 1단계: 일봉 (1D) 데이터 검증
            df_1d = fdr.DataReader(code, start_date)
            hit_1d = False
            df_to_plot = None
            dbg_info = {}

            if df_1d is not None and not df_1d.empty and len(df_1d) >= 65:
                df_1d = df_1d[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                hit_1d, sig_type, df_to_plot, dbg_info = compute_danta_signal(df_1d)
            
            final_hit = False
            
            # 2단계: 30분봉(30m) 교집합 검증
            if hit_1d:
                yf_suffix = ".KS" if row["Market"] == "KOSPI" else ".KQ"
                yf_ticker = f"{code}{yf_suffix}"
                
                try:
                    df_30m = yf.download(yf_ticker, interval="30m", period="60d", progress=False, threads=False)
                    if isinstance(df_30m.columns, pd.MultiIndex):
                        df_30m.columns = df_30m.columns.get_level_values(0)
                    
                    df_30m = df_30m[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                    
                    if len(df_30m) >= 65:
                        hit_30m, _, _, _ = compute_danta_signal(df_30m)
                        if hit_30m:
                            final_hit = True # 일봉과 30분봉 시그널이 정확히 겹침!
                except:
                    pass

            hit_rank = 0
            with console_lock:
                tracker['scanned'] += 1
                if df_1d is not None and len(df_1d) >= 65: tracker['analyzed'] += 1 

                if final_hit:
                    if code in sent_today:
                        final_hit = False 
                    else:
                        tracker['hits'] += 1
                        hit_rank = tracker['hits']
                        sent_today.add(code) 
                        try:
                            with open(log_file, "w") as f:
                                f.write(today_str + "\n")
                                for s_code in sent_today: f.write(s_code + "\n")
                        except: pass
                    
            if final_hit:
                main_chart_path = save_chart(df_to_plot, code, name, hit_rank, dbg_info, show_volume=True, is_promo=False)
                promo_chart_path = save_chart(df_to_plot, code, name, hit_rank, dbg_info, show_volume=False, is_promo=True)
                
                if main_chart_path and promo_chart_path:
                    ai_main, _ = generate_ai_report(code, name)
                    
                    main_caption = (
                        f"🎯 [{dbg_info.get('sig_type', '')}]\n"
                        f"🎯 추천: {dbg_info.get('recommend', '오전장 단타 / 데이트레이딩')}\n\n"
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg_info.get('last_close', 0):,.0f}원\n\n"
                        f"📉 [스마트 매수/손절 전략]\n"
                        f"- 1D, 30m 강한 수급 유입 동시 포착\n"
                        f"- 매수 후 20일선을 이탈할 경우 즉시 칼손절 대응\n\n"
                        f"💡 [AI 비즈니스 요약]\n"
                        f"{ai_main}\n\n"
                        f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                        f"⚠️ [면책 조항]\n"
                        f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 매수/매도 권유가 아닙니다."
                    )
                    q_main.put((main_chart_path, main_caption))

                    try:
                        sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                    except:
                        sector_info = "유망 섹터 포착"
                            
                    promo_caption = (
                        f"📈 [오전장 단타 알고리즘 포착]\n\n"
                        f"🏢 종목: {name} ({code})\n"
                        f"🏷️ 섹터: {sector_info}\n"
                        f"💰 현재가: {dbg_info.get('last_close', 0):,.0f}원"
                    )
                    q_promo.put((promo_chart_path, promo_caption))

                    print(f"\n✅ [{name}] 단타 포착! 듀얼 발송 대기열 추가 완료!")
        except Exception as e:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))

# 💡 핵심: 9시부터 9시 30분까지 "무한 반복" 로직
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [단타 검색기] 09:00 ~ 09:30 무한 연속 스캔 대기 중...")
    
    while True:
        now_kr = datetime.now(kr_tz)
        
        # 9시 0분부터 9시 30분 사이일 경우 쉬지 않고 연속 스캔 실행
        if now_kr.hour == 9 and 0 <= now_kr.minute <= 30:
            print(f"🚀 [단타 무한 스캔 사이클 시작] {now_kr.strftime('%H:%M:%S')}")
            scan_market_danta()
            time.sleep(2) # 1사이클 끝나면 2초 대기 후 즉시 재시작
        else:
            time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
