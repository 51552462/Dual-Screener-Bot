# Dante_US_Nulrim_1D_AI_Pro_DualBot.py
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_Nulrim_1D')
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

# 💡 3. AI 리포트 완벽 분리 (본캐용 3단 요약 vs 홍보용 사람 말투)
def generate_ai_report(ticker_str: str, company_name: str):
    for attempt in range(3):
        try:
            prompt = f"""
            [{company_name} ({ticker_str})]에 대한 최신 팩트를 구글 검색으로 찾아줘.
            반드시 아래의 형식(구분선 포함)을 100% 똑같이 지켜서 답변해. 다른 인사말은 절대 금지.

            ===본캐===
            1. 섹터: (어떤 사업인지 1줄 요약)
            2. 실적: (매출/이익 등 최근 실적 1줄 요약)
            3. 모멘텀: (앞으로의 호재나 기대감 1줄 요약)
            
            ===홍보===
            (위 팩트를 바탕으로, 주식 투자자가 개인 SNS에 올리는 듯한 아주 자연스러운 사람 말투로 1~2줄짜리 코멘트를 작성해. 매번 말투와 강조점(어떤 날은 실적 강조, 어떤 날은 섹터 전망 강조 등)을 다르게 변주해줘. 이모지도 자연스럽게 1개 정도 써줘. 절대 로봇처럼 보이면 안 돼.)
            """
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(tools=[{"google_search": {}}])
            )
            report = response.text.strip()
            
            # 💡 안전한 파싱 (구분선이 없거나 꼬여도 에러 안 나게 철통 방어)
            if "===홍보===" in report:
                parts = report.split("===홍보===")
                main_part = parts[0].replace("===본캐===", "").strip()
                promo_part = parts[1].strip()
            else:
                main_part = report
                promo_part = "최근 시장에서 유의미한 비즈니스 흐름을 보여주고 있는 기업입니다. 차트와 함께 기본기를 체크해 보세요!"
                
            # 만약 promo_part가 텅 비었거나 '...'만 있다면 팩트 기반 대체 문구 삽입
            if not promo_part or promo_part == "..." or "데이터 분석 중" in promo_part:
                promo_part = f"{company_name}의 최근 시장 흐름이 심상치 않네요. 관심 종목에 두고 지켜볼 만한 자리입니다."

            return main_part, promo_part
        except:
            time.sleep(3)
            
    return "⚠️ AI 데이터 수집 지연 (수동 확인 필요)", f"{company_name} 차트에서 흥미로운 패턴이 포착되었습니다. 투자의 참고 자료로 활용해 보세요!"

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

MIN_PRICE_USD = 3.0               
MIN_MONEY_USD = 5_000_000         

def calculate_trust_score(c, e60):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   
    return max(1, min(10, score))

def compute_nulrim_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, v = df['Close'].values, df['Open'].values, df['High'].values, df['Volume'].values
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    moneyOk = (c * v) >= MIN_MONEY_USD
    priceOk = c >= MIN_PRICE_USD
    isBullish = c > o

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
    
    prev_c = np.roll(c, 1); prev_c[0] = 0
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

    if hit6:
        if s6_counts[-1] >= 2: sig_type = f"💥 S6 (바닥 다지기 누적 {s6_counts[-1]}회 포착!)"
        else: sig_type = "🌱 S6 (바닥 다지기 첫 진입)"
    elif hit7: sig_type = "🚀 S7 (추세 전환 돌파)"
    elif hit4: sig_type = "🎯 S4 (정배열 눌림 돌파)" 
    else: sig_type = "✨ S2 (224 재정렬)"

    trust_score = calculate_trust_score(c, e60)
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "s6_count": int(s6_counts[-1])}

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
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - ((df_cut['High'].max() - df_cut['Low'].min()) * 0.04)
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
            fig.text(0.05, sub_y, "1D / US", fontsize=12, color=text_sub, ha='left')

            right_text1 = f"Close: ${c:,.2f} ({sign} ${abs(diff):,.2f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            right_text2 = f"Vol: {v:,}  |  O: ${o:,.2f}  H: ${h:,.2f}  L: ${l:,.2f}"
            fig.text(0.95, sub_y, right_text2, fontsize=12, color=text_sub, ha='right')
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except: return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 4번(눌림목) 스캔 시작!")

    # 💡 3. 당일 중복 발송 원천 차단 (하루 몇 번을 돌려도 중복 방지)
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
                    hit, sig_type, df, dbg = compute_nulrim_1d(df_ticker)
                    
                    if hit:
                        # 💡 이미 당일에 발송된 종목이면 깔끔하게 패스 (중복 컷)
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
                            # 💡 본캐용 3단 요약, 홍보용 팩트 티저 각각 받아오기
                            ai_main_report, ai_promo_teaser = generate_ai_report(code, name)
                            
                            # 기존 본캐용 캡션 (유료방용 디테일)
                            main_caption = (
                                f"🎯 [{dbg.get('sig_type', '')}]\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n"
                                f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                                f"📉 [매수/손절 전략]\n"
                                f"- 양봉 길이만큼 분할매수\n"
                                f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                                f"🌟 사전 매집/바닥턴 누적: 별x{dbg.get('s6_count', 0)}\n"
                                f"⭐ 알고리즘 신뢰도: {dbg.get('score', 10)} / 10점\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_main_report}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                                f"⚠️ [면책 조항]\n"
                                f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다. 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                            )
                            q_main.put((main_chart_path, main_caption))

                            # 홍보용 캡션 (스팸 방지용 동적 텍스트 적용 & 새 홍보 봇으로 발송)
                            threads_chart_path = save_chart(df, code, name, tracker['hits'], dbg, show_volume=False)
                            if threads_chart_path:
                                threads_caption = (
                                    f"🏢 종목명: {name} ({code})\n"
                                    f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                                    f"💬 {ai_promo_teaser}\n\n"
                                    f"💡 시장의 주목을 받기 전, 알고리즘에 포착된 차트 분석입니다. 투자의 참고 자료로 활용해 보세요!\n\n"
                                    f"⚠️ [면책 조항] 본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다. 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                                )
                                q_promo.put((threads_chart_path, threads_caption))
                                
                            print(f"\n✅ [{name}] 미국장 눌림목 듀얼 발송 대기열 추가 (바닥 누적: {dbg.get('s6_count', 0)}회)")
            except Exception as e:
                pass
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 듀얼 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    dt = time.time() - t0
    print(f"\n✅ [미국장 4번 V 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [4번 미국장 검색기] 11:00 / 13:00 / 15:00 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 11 and now_ny.minute == 0) or (now_ny.hour == 13 and now_ny.minute == 0) or (now_ny.hour == 15 and now_ny.minute == 0):
            print(f"🚀 [4번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d()  # ⭐️ 이 줄이 있으면 실행 즉시 1회 스캔을 시작합니다.
    # run_scheduler() # 스케줄러를 같이 돌리려면 이 줄의 주석을 해제하세요.
