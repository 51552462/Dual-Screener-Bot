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

TELEGRAM_TOKEN    = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_P_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

def generate_ai_report(ticker_str: str, company_name: str) -> str:
    for attempt in range(3):
        try:
            tk = yf.Ticker(ticker_str)
            info = tk.info
            sector = info.get('sector', '정보 없음')
            industry = info.get('industry', '정보 없음')
            market_cap = info.get('marketCap', '정보 없음')
            if isinstance(market_cap, int): market_cap = f"${market_cap / 1_000_000_000:.2f}B"
            eps = info.get('trailingEps', '정보 없음')
            revenue_growth = info.get('revenueGrowth', '정보 없음')
            business_summary = info.get('longBusinessSummary', '정보 없음')[:800] 
            financials = f"EPS: {eps}, 매출성장률: {revenue_growth}"

            today_date = datetime.now().strftime('%Y년 %m월 %d일')
            prompt = f"""
            너는 월스트리트의 냉철하고 전문적인 탑 애널리스트야.
            오늘 날짜는 {today_date}이야. 반드시 최신 구글 검색 결과를 바탕으로 팩트 중심의 투자 메모를 작성해.
            추상적이거나 감정적인 표현은 철저히 배제하고, 기관 보고서처럼 간결하고 명확하게 써.
            
            [종목 정보]
            - 종목명: {company_name} ({ticker_str})
            - 섹터: {sector} / 산업군: {industry}
            - 시가총액: {market_cap}
            - 실적 및 재무: {financials}
            - 비즈니스 요약: {business_summary}

            [출력 양식]
            1. 섹터 종류: (간단한 설명)
            2. 업계 점유율/규모: (시총 규모 및 지위)
            3. 최근 실적: (흑자/적자 여부, 핵심 지표)
            4. 미래 모멘텀: (파이프라인, 최신 호재/악재 등)
            5. 기업 전망: (짧고 굵은 전망)
            """
            response = client.models.generate_content(
                model='gemini-2.5-flash', 
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
            return response.text.strip()
        except Exception as e: 
            time.sleep(3)
            
    return f"⚠️ AI 요약 실패\n(진짜 에러 원인: {e})"

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
    
    # ⭐️ 한국장과 동일한 3봉 내 15% 상승 실패 시 누적, 성공 시 리셋 로직 ⭐️
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
    print(f"\n🇺🇸 [일봉 전용] 미국장 2번(역매공파) 스캔 시작! (안정화 패치 완료)")
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
                        tracker['hits'] += 1
                        
                        # 1️⃣ 본캐용 다크 차트 생성
                        main_chart_path = save_chart(df, code, name, tracker['hits'], dbg, show_volume=True)
                        if main_chart_path:
                            ai_fact_check = generate_ai_report(code, name)
                            
                            # ⭐️ 누적 횟수에 따른 강조 카피라이팅
                            p_count = dbg.get('p_count', 1)
                            if p_count >= 3:
                                intro_title = "🌟 [진입타점]"
                                intro_desc = "기준을 잡고 거기에 맞춰서 대응하고 매매하기."
                            else:
                                intro_title = "💎 [관심종목]"
                                intro_desc = "무관심할때 조금씩 관심 가져주기."
                            
                            main_caption = (
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg['last_close']:.2f}\n\n"
                                f"{intro_title}\n"
                                f"{intro_desc}\n\n"
                                f"⚖️ [건강한 투자를 위한 기준]\n"
                                f"• 관심종목 편입: 타이밍이 올때까지 천천히 기다리세요.\n"
                                f"• 단기 진입 시: 실전 매매에 참여하신다면, 진입 시가 이탈 시 칼 같은 손절 필수.\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_fact_check}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요."
                            )
                            telegram_queue.put((main_chart_path, main_caption))

                            # 2️⃣ 쓰레드 홍보용 다크 차트 생성
                            threads_chart_path = save_chart(df, code, name, tracker['hits'], dbg, show_volume=False)
                            if threads_chart_path:
                                threads_caption = (
                                    f"🏢 종목명: {name} ({code})\n"
                                    f"💰 현재가: ${dbg['last_close']:.2f}\n\n"
                                    f"💡 시장의 주목을 받기 전, 기본기에 충실한 차트 분석입니다. 투자의 참고 자료로 활용해 보세요!"
                                )
                                telegram_queue.put((threads_chart_path, threads_caption))
                                
                            print(f"\n✅ [{name}] 미국장 역매공파 본캐 1개 + 홍보용 1개 (총 2개) 전송 완료! (누적: {p_count}회)")
            except Exception as e:
                pass
                
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()

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
