# Dante_US_Top1_Master_1D_AI_Pro.py (미국장 Top 1% 마스터 SIG 1,2,3 전용)
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

# 💡 1. 듀얼 텔레그램 봇 세팅
TELEGRAM_TOKEN_MAIN  = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
TELEGRAM_TOKEN_PROMO = "7996581031:AAFou3HWYhIXzRtlW4ildx8tOitcQBVubPg"
TELEGRAM_CHAT_ID     = "6838834566"
SEND_TELEGRAM        = True

q_main = queue.Queue()
q_promo = queue.Queue()

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Top1_Master')
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

# 💡 2. 본캐 팩트 리포트 (해시태그 파싱 오류 제거)
def generate_ai_report(code: str, company_name: str):
    import re, time
    try:
        if code.isdigit(): 
            res = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            from bs4 import BeautifulSoup
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
            
            if not response or not response.text:
                continue
                
            report = response.text.replace('*', '').strip() 
            m_part = re.search(r'\[본캐\](.*)', report, re.DOTALL)

            if not m_part: raise ValueError("파싱오류")

            return m_part.group(1).strip(), ""
        except:
            pass 
            
    return fb_main, ""

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()] 
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

# 💡 3. Top 1% 마스터 (미국장 호환) 타점 로직
def compute_top1_master_signal(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    c = df['Close'].values
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    v = df['Volume'].values
    
    # 7중 EMA 계산
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
        
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # 변동성(ATR 20) 계산
    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    atr = pd.Series(tr).ewm(alpha=1/20, adjust=False, min_periods=0).mean().values

    # 배열 상태
    is_aligned_30 = (e10 > e20) & (e20 > e30)
    is_aligned_112 = is_aligned_30 & (e30 > e60) & (e60 > e112)
    is_aligned_224 = is_aligned_112 & (e112 > e224)
    is_aligned_448 = is_aligned_224 & (e224 > e448)
    
    is_bullish = c > o
    show_values = is_aligned_112 & is_bullish

    # 이격도 및 모멘텀 계산
    with np.errstate(divide='ignore', invalid='ignore'):
        spread_10_20 = np.where(show_values, ((e10 - e20) / atr) * 100, 0)
        spread_10_30 = np.where(show_values, ((e10 - e30) / atr) * 100, 0)
        spread_112_224 = np.where(show_values, ((e112 - e224) / atr) * 100, 0)

        # 상관계수 및 ROC
        idx = np.arange(len(c))
        r_val = pd.Series(e10).rolling(10).corr(pd.Series(idx)).fillna(0).values
        r_squared = r_val * r_val
        
        e10_3 = np.roll(e10, 3)
        e10_3[:3] = e10[:3]
        ema_roc = np.where(e10_3 != 0, ((e10 - e10_3) / e10_3) * 5000, 0)

    true_momentum_line = np.where(is_aligned_30, ema_roc * (r_squared ** 2), 0)
    prev_tml = np.roll(true_momentum_line, 1)
    prev_tml[0] = 0

    # 조건 판별
    cond_rising = true_momentum_line > prev_tml
    cond_blue_30 = spread_112_224 >= 30
    
    val_angle = true_momentum_line
    cond_highest_angle = (val_angle > spread_10_20) & (val_angle > spread_10_30) & (val_angle > spread_112_224)

    cond_val_sig1 = (spread_10_30 >= 100) & (spread_10_20 >= 50) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle
    cond_val_sig2_3 = (spread_10_30 >= 150) & (spread_10_20 >= 100) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle

    raw_sig1 = is_aligned_112 & cond_val_sig1 & cond_rising
    raw_sig2 = is_aligned_224 & cond_val_sig2_3 & cond_rising
    raw_sig3 = is_aligned_448 & cond_val_sig2_3 & cond_rising

    # 중복 방지 (우선순위: sig3 > sig2 > sig1)
    signal_3 = raw_sig3
    signal_2 = raw_sig2 & ~signal_3
    signal_1 = raw_sig1 & ~signal_2 & ~signal_3

    # 💡 미국장 특화 잡주 차단 (500만 달러 & 3달러 이상)
    moneyOk = (c * v) >= 5_000_000
    priceOk = c >= 3.0

    hit_1 = signal_1 & moneyOk & priceOk
    hit_2 = signal_2 & moneyOk & priceOk
    hit_3 = signal_3 & moneyOk & priceOk
    
    final_hit = hit_1 | hit_2 | hit_3

    if not final_hit[-1]:
        return False, "", df, {}
        
    # ⭐️ 3봉 이내 중복 필터 (한국장과 동일하게 적용) ⭐️
    if final_hit[-4:-1].any():
        return False, "", df, {}
        
    if hit_3[-1]:
        sig_type = "🔥 SIG 3 (Top 1% 초강력)"
    elif hit_2[-1]:
        sig_type = "🔥 SIG 2 (Top 1% 강력)"
    else:
        sig_type = "🔥 SIG 1 (Top 1% 돌파)"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": "스윙 / 중장기"
    }

# 💡 매일 로테이션되는 5가지 프리미엄 차트 테마
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
            
            right_text1 = f"{sign} {abs(diff_pct):.2f}%" if is_promo else f"Close: ${c:,.2f} ({sign} ${abs(diff):,.2f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=22 if is_promo else 18, fontweight='bold', color=color_diff, ha='right')

            if not is_promo:
                right_text2 = f"Vol: {v:,}  | O: ${o:,.2f}  H: ${h:,.2f}  L: ${l:,.2f}"
                fig.text(0.95, sub_y, right_text2, fontsize=12, color=text_sub, ha='right')
                
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=250 if is_promo else 200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
            return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 Top 1% 마스터 스캔 시작! (연속 타점 필터링 🛡️)")

    # 💡 당일 중복 발송 차단 로직
    ny_tz = pytz.timezone('America/New_York')
    today_str = datetime.now(ny_tz).strftime('%Y-%m-%d')
    log_file = os.path.join(TOP_FOLDER, "sent_log_us_top1.txt")
    
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
                    hit, sig_type, df, dbg = compute_top1_master_signal(df_ticker)
                    
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
                        main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
                        threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
                        
                        if main_chart_path and threads_chart_path:
                            ai_main, _ = generate_ai_report(code, name)
                            
                            # 1️⃣ 본캐용 캡션 (유료방용)
                            main_caption = (
                                f"🎯 [{dbg.get('sig_type', '')}]\n"
                                f"🎯 추천: {dbg.get('recommend', '스윙, 중장기 / 종가배팅')}\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                                f"📉 [스마트 매수/손절 전략]\n"
                                f"- 1~3단계 정배열 이격도 및 강력 모멘텀 결합 타점 (신규 진입)\n"
                                f"- 단기선 이탈 및 데드크로스 발생 시 기계적 손절 대응\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_main}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                                f"⚠️ [면책 조항]\n"
                                f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 매수/매도 권유가 아닙니다."
                            )
                            q_main.put((main_chart_path, main_caption))

                            # 2️⃣ 홍보용 캡션 (쓸데없는 멘트 다 빼고 초심플 압축)
                            try:
                                sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                            except:
                                sector_info = "유망 섹터 포착"
                                
                            # ⭐️ 멘트 싹 날리고 [차트+종목+섹터+현재가]만! (미국장이므로 $ 유지)
                            promo_caption = (
                                f"📈 [Top 1% 마스터 알고리즘 포착]\n\n"
                                f"🏢 종목: {name} ({code})\n"
                                f"🏷️ 섹터: {sector_info}\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}"
                            )
                            q_promo.put((threads_chart_path, promo_caption))

                            print(f"\n✅ [{name}] 미국장 Top 1% 포착! 듀얼 발송 대기열 추가 완료!")
            except Exception as e:
                pass
                
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [미국장 Top 1% 마스터 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [미국장 Top 1% 마스터 검색기] 09:30 / 12:00 / 14:30 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 9 and now_ny.minute == 30) or (now_ny.hour == 12 and now_ny.minute == 0) or (now_ny.hour == 14 and now_ny.minute == 30):
            print(f"🚀 [미국장 Top 1% 마스터 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d()
