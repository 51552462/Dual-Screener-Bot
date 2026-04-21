# Dante_US_5EMA_AI_Pro_DualBot.py
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

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_5EMA_1D')
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

def get_us_ticker_list():
    try:
        # 💡 각 종목이 어느 시장 소속인지 'Market' 컬럼을 생성하여 합칩니다.
        df_nasdaq = fdr.StockListing('NASDAQ').assign(Market='NASDAQ')
        df_nyse = fdr.StockListing('NYSE').assign(Market='NYSE')
        df_amex = fdr.StockListing('AMEX').assign(Market='AMEX')
        df = pd.concat([df_nasdaq, df_nyse, df_amex])
        df = df[df['Symbol'].str.isalpha()]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name', 'Market']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

MIN_PRICE_USD = 3.0               
MIN_MONEY_USD = 5_000_000         

# 💡 스케일링 함수
def scale_score(val, best, worst):
    if best > worst:
        if val >= best: return 10.0
        if val <= worst: return 1.0
        return 1.0 + 9.0 * (val - worst) / (best - worst)
    else:
        if val <= best: return 10.0
        if val >= worst: return 1.0
        return 1.0 + 9.0 * (worst - val) / (worst - best)

# 💡 3. [교체됨] 미국장 5일선 관통 전용 마스터 시그널 엔진 (US V7.0 적용)
def compute_us_5ema_signal(df_raw: pd.DataFrame, idx_close: pd.Series, vix_close: pd.Series):
    if df_raw is None or len(df_raw) < 250: return False, "", df_raw, {}
    df = df_raw.copy()
    
    idx_close_aligned = idx_close.reindex(df.index).ffill()
    df['Idx_Close'] = idx_close_aligned
    df['VIX_Close'] = vix_close.reindex(df.index).ffill()
    for n in [5, 10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    e5, e10, e20, e30 = df['EMA5'].values, df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    e60, e112, e224, e448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # =========================================================================
    # 👑 [1단계] 4대 핵심 변수 수식 (US V7.0)
    # =========================================================================
    cpv = np.where(h != l, (c - o) / (h - l), 0.5)
    v_ma20 = pd.Series(v).rolling(20).mean().values
    vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
    tb_index = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)

    bb_mid = pd.Series(c).rolling(20).mean().values
    bb_std = pd.Series(c).rolling(20).std().values
    bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
    bb_energy = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)

    c_20 = pd.Series(c).shift(20).values
    idx_20 = df['Idx_Close'].shift(20).values
    with np.errstate(divide='ignore', invalid='ignore'):
        stock_ret = np.where(c_20 > 0, (c - c_20) / c_20, 0.0)
        idx_ret = np.where(idx_20 > 0, (df['Idx_Close'].values - idx_20) / idx_20, 0.0001)
        idx_ret = np.where(idx_ret == 0, 0.0001, idx_ret) 
        rs = (stock_ret / idx_ret) * 100
    rs = np.nan_to_num(rs, nan=0.0)

    # =========================================================================
    # 👑 [2단계] 5일선 관통(S1 대세 추세) 단독 포착 로직
    # =========================================================================
    moneyOk = (c * v) >= MIN_MONEY_USD
    priceOk = c >= MIN_PRICE_USD
    
    # 1. 448일 완전 정배열 조건 필수
    alignFullBull = (e5 > e10) & (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112) & (e112 > e224) & (e224 > e448)
    
    # 2. 양봉 및 5선 몸통 관통
    isBullish = c > o
    isBodyCross5 = (o < e5) & (c > e5)
    
    # ⚠️ S2, S3, S4 배제 및 미국장 특성에 맞춰 거래량 조건은 응축에너지로 검증 (condVol 패스)
    finalSignal = alignFullBull & isBullish & isBodyCross5 & moneyOk & priceOk

    if not finalSignal[-1]: 
        return False, "", df, {}

    # =========================================================================
    # 👑 [3단계] S1 스코어링 매핑 (미국장 V9.0 팩트 대입)
    # =========================================================================
    recent_hits = finalSignal[-252:-1].sum() if len(c) > 252 else finalSignal[:-1].sum()
    freq_count = int(recent_hits)

    ema_stat_str = "승률 27.7% / 손익비 2.81 (대세 상승장, 448 완전정배열 100% 타겟팅)"

    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    cur_vix = df['VIX_Close'].iloc[-1] if not pd.isna(df['VIX_Close'].iloc[-1]) else 15.0
    
    sig_type = "🔥 US S1 (5선 관통 / 448 완전정배열)"
    
    score_rs   = scale_score(cur_rs, 1005.50, -39.00)  
    score_ema  = 10.0                                  
    score_cpv  = scale_score(cur_cpv, 0.23, 0.94)      
    score_bbe  = scale_score(cur_bbe, 39.50, 2.30)     
    score_tb   = scale_score(cur_tb, 5.20, 0.70)       
    score_freq = 10.0 if 1 <= freq_count <= 5 else (2.0 if freq_count >= 14 else 6.0)

    total_score = (score_rs*10 + score_ema*9 + score_cpv*8 + score_bbe*7 + score_tb*6 + score_freq*5) / 450 * 100
    
    trap_warning = ""

    # =========================================================================
    # 👑 [4단계] US V9.0 디테일: 데스콤보, VIX 공포지수 매핑, 뱃지 시스템
    # =========================================================================
    weekday = df.index[-1].weekday()
    if weekday == 4: total_score *= 1.05 
    elif weekday == 0: total_score *= 0.95 

    is_death_combo = (cur_cpv > 0.94) and (cur_rs < -39.00)
    if is_death_combo: 
        total_score *= 0.70
        trap_warning += "⚠️ [데스 콤보 발동] 거래량 없이 만든 꽉 찬 양봉 + 소외주 (점수 30% 삭감)\n"
        
    if freq_count >= 14 and (score_rs < 8.0 or score_cpv < 8.0):
        total_score *= 0.50
        trap_warning += "🚫 [고빈도 잡주 경고] 알고리즘 단타 놀이터! 강제 패스 권장 (-50% 삭감)\n"

    if trap_warning != "" and not is_death_combo and "고빈도" not in trap_warning: 
        total_score *= 0.70 

    total_score = min(max(total_score, 0), 100)

    # 💡 [V9.0 종목 맞춤형 동적 청산 전략]
    if cur_cpv >= 0.31:
        cpv_stat = f"양봉/꽉찬 캔들 (CPV {cur_cpv:.2f})"
        action = "월가 알고리즘의 단기 설거지(휩소) 타겟이 될 확률이 높습니다. 진입 후 3~4일 내로 꺾이는 모습이 나오면 'ZLEMA 이탈' 시 즉각 칼손절하여 계좌를 방어하십시오."
    else:
        cpv_stat = f"꼬리가 길게 달린 캔들 (CPV {cur_cpv:.2f})"
        action = f"숏 스퀴즈를 유발하는 진짜 대장주 패턴입니다. 잔파도에 털리지 말고 '단기데드(EMA 20)' 이탈 전까지 약 10일간 추세를 발라먹으십시오."

    if total_score >= 80:
        tier_stat = f"총점 {total_score:.1f}점(1티어). 수학적으로 방어력이 입증되었으므로 메인 비중 진입 권장."
    else:
        tier_stat = f"총점 {total_score:.1f}점 하위권. 가짜 휩소 리스크가 크므로 철저히 비중 축소 요망."

    exit_strategy = f"[{cpv_stat}]\n{action}\n\n💡 비중 조언: {tier_stat}"

    # 💡 [V9.0 VIX(공포지수) 기반 비중 조절 로직]
    vix_strategy = ""
    if cur_vix >= 30:
        vix_strategy = f"🌋 [극단적 공포장 | VIX {cur_vix:.1f}] 승률 34.2%, 평균수익 40.6% 터지는 초거대 대박 구간! 진입 비중 1.5배 상향 및 적극 매수."
    elif cur_vix >= 20:
        vix_strategy = f"🌪️ [조정장 | VIX {cur_vix:.1f}] 평균수익 27.4% 급증 구간! 진입 비중 1.2배 상향."
    else:
        vix_strategy = f"🌊 [평온장 | VIX {cur_vix:.1f}] 시스템 기본 비중(1배수) 기계적 매매."

    # 💡 [V9.0 뱃지 시스템 및 밈 주식 예외 로직]
    badge_str = ""
    if total_score >= 80.0:
        badge_str = "🔥 [1티어 뱃지] 가산점 부여 대상 (평균수익 29.4%, 승률 45.1%. UI 상단 노출 및 비중 1.5배 확대)"
        sig_type = "👑 [1티어] " + sig_type
    elif total_score <= 50.0 and cur_rs > 500 and cur_cpv <= 0.3:
        badge_str = "💎 [특급 모멘텀 예외] 점수 무시 텐배거 (월가/WSB 돌발 밈 주식 펌핑 가능성. 비중 최소화 로또 진입)"
        sig_type = "💎 [로또] " + sig_type
    else:
        badge_str = "⚠️ [비중 축소] 80점 미만은 가짜 휩소 리스크가 크므로 철저히 비중 축소 요망"

    v9_comment = (
        f"📊 [System B US 5선 관통 V9.0 마스터 리포트]\n"
        f"🔹 시스템 총점: {total_score:.1f} / 100점\n"
        f"🎖️ {badge_str}\n"
        f"{vix_strategy}\n\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.2f} ({score_cpv:.1f}점)\n"
        f"▪️ 진짜양봉지수: {cur_tb:.1f} ({score_tb:.1f}점)\n"
        f"▪️ 응축에너지: {cur_bbe:.1f} ({score_bbe:.1f}점)\n"
        f"▪️ 시장상대강도: {cur_rs:.1f}% ({score_rs:.1f}점)\n"
        f"▪️ 과거 매매빈도: {freq_count}회 ({score_freq:.1f}점)\n"
        f"▪️ 이평선국면점수: {score_ema:.1f}점\n\n"
        f"💡 [이평선 국면 팩트 데이터]\n{ema_stat_str}\n"
    )
    
    if trap_warning != "": v9_comment += f"\n{trap_warning}"
    if weekday == 4: v9_comment += f"✨ 금요일 주말 리스크를 이겨낸 진짜 주도주 프리미엄 (+5% 가산)\n"
    elif weekday == 0: v9_comment += f"⚠️ 월요일 고점 털기 리스크 반영 (-5% 삭감)\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": f"{exit_strategy}",
        "v9_comment": v9_comment,
        "score": total_score,
        "v_cpv": cur_cpv,
        "v_yang": cur_tb,
        "v_energy": cur_bbe,
        "v_rs": cur_rs
    }

# 💡 매일 로테이션되는 5가지 프리미엄 차트 테마
def get_daily_theme():
    theme_idx = datetime.now().day % 5
    themes = [
        {'bg': '#0B0E14', 'grid': '#1A202C', 'text': '#FFFFFF', 'up': '#F6465D', 'down': '#0ECB81'}, # 0: Binance Premium
        {'bg': '#FFFFFF', 'grid': '#F0F0F0', 'text': '#131722', 'up': '#E0294A', 'down': '#2EBD85'}, # 1: Institutional White
        {'bg': '#131722', 'grid': '#2A2E39', 'text': '#D1D4DC', 'up': '#26A69A', 'down': '#EF5350'}, # 2: TradingView Classic
        {'bg': '#000000', 'grid': '#111111', 'text': '#00FFA3', 'up': '#00FFA3', 'down': '#FF3366'}, # 3: Cyberpunk Terminal
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
        except: return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 5일선 관통(S1 스나이퍼) 스캔 시작!")

    # 💡 [V9.0 핵심] 벤치마크 지수(SPY, QQQ) 및 VIX(공포지수) 데이터 동시 로드
    print("📊 벤치마크 지수 및 VIX(공포지수) 데이터 안전하게 로드 중...")
    try:
        idx_df = yf.download("SPY QQQ ^VIX", interval="1d", period="3y", group_by="ticker", progress=False, threads=False)
        if not idx_df.empty:
            spy_idx = idx_df['SPY']['Close'] if 'SPY' in idx_df.columns.levels[0] else pd.Series(dtype=float)
            qqq_idx = idx_df['QQQ']['Close'] if 'QQQ' in idx_df.columns.levels[0] else pd.Series(dtype=float)
            vix_idx = idx_df['^VIX']['Close'] if '^VIX' in idx_df.columns.levels[0] else pd.Series(dtype=float)
            
            if spy_idx.index.tzinfo is not None: spy_idx.index = spy_idx.index.tz_convert('America/New_York').tz_localize(None)
            if qqq_idx.index.tzinfo is not None: qqq_idx.index = qqq_idx.index.tz_convert('America/New_York').tz_localize(None)
            if vix_idx.index.tzinfo is not None: vix_idx.index = vix_idx.index.tz_convert('America/New_York').tz_localize(None)
            
            spy_idx = spy_idx[~spy_idx.index.duplicated(keep='last')]
            qqq_idx = qqq_idx[~qqq_idx.index.duplicated(keep='last')]
            vix_idx = vix_idx[~vix_idx.index.duplicated(keep='last')]
        else:
            spy_idx, qqq_idx, vix_idx = pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)
    except:
        spy_idx, qqq_idx, vix_idx = pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)

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

    # 💡 'Market' 정보를 딕셔너리에 함께 저장합니다.
    ticker_to_info = {row['Symbol']: {'code': row['Symbol'], 'name': row['Name'], 'market': row['Market']} for _, row in stock_list.iterrows()}

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
                    
                    # 💡 [V9.0 벤치마크 및 VIX 매핑]
                    market_type = info['market']
                    target_idx = qqq_idx if market_type == 'NASDAQ' else spy_idx
                    
                    # 🚨 [버그 픽스] 잘못된 중복 함수 호출 제거, VIX 데이터 추가하여 정상 호출
                    hit, sig_type, df, dbg = compute_us_5ema_signal(df_ticker, target_idx, vix_idx)
                    
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
                        promo_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
                        
                        if main_chart_path and promo_chart_path:
                            ai_main, _ = generate_ai_report(code, name)
                            
                            # 1️⃣ 본캐용 캡션 (유료방용 - V9.0 뱃지, VIX 및 점수 브리핑 출력)
                            main_caption = (
                                f"🎯 [{dbg.get('sig_type', '')}]\n"
                                f"🎯 추천: 스윙, 추세 홀딩 / 종가배팅\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                                f"{dbg.get('v9_comment', '')}\n"
                                f"📉 [스마트 매수/청산 전략]\n"
                                f"- {dbg.get('recommend', '')}\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_main}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                                f"⚠️ [면책 조항]\n"
                                f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다.\n투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                            )
                            q_main.put((main_chart_path, main_caption))

                    # 💡 [오토 포워드 테스팅 시스템에 종목 편입 시도 및 버그 픽스]
                    try:
                        import auto_forward_tester as aft # 상단에 임포트 안 해도 여기서 동적 로드
                        
                        market_type = 'US' # 미국장 검색기에는 'US'로 변경!!
                        entry_facts = {
                            'v_cpv': dbg.get('v_cpv', 0),
                            'v_yang': dbg.get('v_yang', 0),
                            'v_energy': dbg.get('v_energy', 0),
                            'v_rs': dbg.get('v_rs', 0)
                        }
                        
                        success, fwd_msg = aft.try_add_virtual_position(
                            market=market_type,
                            code=code,
                            name=name,
                            sig_type=dbg.get('sig_type', ''),
                            score=dbg.get('score', 0), 
                            ep=dbg.get('last_close', c[-1]),
                            facts=entry_facts
                        )
                        print(f"   ↳ [포워드 장부 기록]: {fwd_msg}")
                    except Exception as e:
                        print(f"   ↳ [포워드 장부 에러]: {e}")
                            # 2️⃣ 홍보용 캡션 (초심플 압축)
                    try:
                                sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                    except:
                                sector_info = "유망 섹터 포착"
                                
                    promo_caption = (
                                f"📈 [알고리즘 차트 포착]\n\n"
                                f"🏢 종목: {name} ({code})\n"
                                f"🏷️ 섹터: {sector_info}\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}"
                            )
                    q_promo.put((promo_chart_path, promo_caption))

                    print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 전송 대기열 추가 완료!")
            except Exception as e:
                pass
        
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 듀얼 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    dt = time.time() - t0
    print(f"\n✅ [미국장 5일선 관통 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {dt/60:.1f}분\n")

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [미국장 5일선 검색기] 11:30 / 14:00 / 16:30 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 11 and now_ny.minute == 0) or (now_ny.hour == 13 and now_ny.minute == 0) or (now_ny.hour == 15 and now_ny.minute == 0):
            print(f"🚀 [미국장 5일선 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    scan_market_1d()
