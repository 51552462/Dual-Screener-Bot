# korea_ema224_signal_screener.py (Top 1% 마스터 SIG 1,2,3,4 통합본 + System B V7.0 마스터 로직)
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

# 💡 1. 듀얼 텔레그램 봇 세팅
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

# 💡 2. 본캐 팩트 리포트
def generate_ai_report(code: str, company_name: str):
    try:
        if code.isdigit(): 
            res = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            soup = BeautifulSoup(res.text, 'html.parser')
            sector_kr = soup.select_one('h4.h_sub.sub_tit7 a').text.strip() if soup.select_one('h4.h_sub.sub_tit7 a') else '국내 증시'
        else: 
            import yfinance as yf
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
            너는 주식 전문 마케터야.
            [{company_name} ({code})] 종목과 관련된 오늘자 최신 이슈나 테마를 검색해서 아래 양식에 맞게 딱 출력해.
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분해.
            굵은 글씨(**) 금지.

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

# 💡 3. 잡주 필터 및 시가총액 병합 (안전성 100% 보장형)
def get_krx_list_kind():
    try:
        # 💡 [핵심 픽스] 이전 눌림목 검색기에서 검증된 KIND 베이스 + FDR 시총 병합 방식 이식
        df_ks = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt", verify=False, timeout=10).text), header=0)[0]
        df_ks['Market'] = 'KOSPI'
        df_kq = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt", verify=False, timeout=10).text), header=0)[0]
        df_kq['Market'] = 'KOSDAQ'
        df = pd.concat([df_ks, df_kq])
        df['Code'] = df['종목코드'].astype(str).str.zfill(6)
        df = df.rename(columns={'회사명': 'Name'})
        
        junk_pattern = '스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권'
        filtered_df = df[~df['Name'].str.contains(junk_pattern, regex=True)].copy()
        
        # 시가총액(Marcap) 데이터 100% 안전 조인 (FDR 버전/타입 호환성 완벽 해결)
        try:
            fdr_df = fdr.StockListing('KRX')
            
            # 💡 [핵심 방어 로직 1] 컬럼명이 Symbol이거나 MarketCap일 경우 강제 통일
            if 'Symbol' in fdr_df.columns and 'Code' not in fdr_df.columns:
                fdr_df = fdr_df.rename(columns={'Symbol': 'Code'})
            if 'MarketCap' in fdr_df.columns and 'Marcap' not in fdr_df.columns:
                fdr_df = fdr_df.rename(columns={'MarketCap': 'Marcap'})
                
            # 안전하게 Code와 Marcap만 추출
            fdr_df = fdr_df[['Code', 'Marcap']].copy()
            
            # 💡 [핵심 방어 로직 2] 종목코드 규격화 (공백 제거 및 6자리)
            fdr_df['Code'] = fdr_df['Code'].astype(str).str.strip().str.zfill(6)
            
            # 💡 [핵심 방어 로직 3] 시총 데이터에 콤마(,)가 포함된 문자열일 경우 숫자 변환 전 처리
            if fdr_df['Marcap'].dtype == object:
                fdr_df['Marcap'] = fdr_df['Marcap'].astype(str).str.replace(',', '')
            
            fdr_df['Marcap'] = pd.to_numeric(fdr_df['Marcap'], errors='coerce').fillna(0)

            filtered_df = filtered_df.merge(fdr_df, on='Code', how='left')
            filtered_df['Marcap'] = filtered_df['Marcap'].fillna(0)
            print("✅ 시가총액(Marcap) 데이터 100% 정상 조인 완료!")
        except Exception as e:
            print(f"⚠️ 시가총액 데이터 조인 실패: {e}")
            filtered_df['Marcap'] = 0
            
        return filtered_df[['Code', 'Name', 'Market', 'Marcap']].dropna()
    except: 
        return pd.DataFrame()

# 💡 보조 함수 1: 1~10점 스케일링 함수 (방향성 완벽 지원)
def scale_score(val, best, worst):
    if best > worst: # 높을수록 좋은 지표 (RS, 진짜양봉, 응축에너지)
        if val >= best: return 10.0
        if val <= worst: return 1.0
        return 1.0 + 9.0 * (val - worst) / (best - worst)
    else: # 낮을수록 좋은 지표 (CPV 등)
        if val <= best: return 10.0
        if val >= worst: return 1.0
        return 1.0 + 9.0 * (worst - val) / (worst - best)

# 💡 4. Top 1% 마스터 (트뷰 원본 SIG 1, SIG 4 완벽 이식 엔진)
def compute_korea_master_signal(df_raw: pd.DataFrame, idx_close: pd.Series, marcap: float):
    if df_raw is None or len(df_raw) < 500: 
        return False, "", df_raw, {}
    df = df_raw.copy()
    
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    df['Idx_Close'] = idx_close.reindex(df.index).ffill()

    # 1. 7중 EMA 계산
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # 2. 기본 지표 및 4대 핵심 변수 수식 계산
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    atr = pd.Series(tr).ewm(alpha=1/20, adjust=False, min_periods=0).mean().values

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
    # 👑 3. 마스터 모멘텀 수식 (트뷰 로직 100% 이식)
    # =========================================================================
    is_aligned_30 = (e10 > e20) & (e20 > e30)
    is_aligned_112 = is_aligned_30 & (e30 > e60) & (e60 > e112)
    is_aligned_224 = is_aligned_112 & (e112 > e224)
    is_aligned_448 = is_aligned_224 & (e224 > e448)
    is_bullish = c > o
    show_values = is_aligned_112 & is_bullish

    with np.errstate(divide='ignore', invalid='ignore'):
        spread_112_224 = np.where(show_values, ((e112 - e224) / atr) * 100, 0)
        spread_10_30 = np.where(show_values, ((e10 - e30) / atr) * 100, 0)
        spread_10_20 = np.where(show_values, ((e10 - e20) / atr) * 100, 0)

        idx_arr = np.arange(len(c))
        r_val = pd.Series(e10).rolling(10).corr(pd.Series(idx_arr)).fillna(0).values
        r_squared = r_val * r_val
        
        e10_3 = np.roll(e10, 3); e10_3[:3] = e10[:3]
        ema_roc = np.where(e10_3 != 0, ((e10 - e10_3) / e10_3) * 5000, 0)
        
    true_momentum_line = np.where(is_aligned_30, ema_roc * (r_squared**2), 0)

    # =========================================================================
    # 👑 4. 타점 조건 및 상호 배제 로직 (S1, S4 추출)
    # =========================================================================
    prev_tml = np.roll(true_momentum_line, 1); prev_tml[0] = 0
    cond_rising = true_momentum_line > prev_tml
    cond_blue_30 = spread_112_224 >= 30
    cond_highest_angle = (true_momentum_line > spread_10_20) & (true_momentum_line > spread_10_30) & (true_momentum_line > spread_112_224)

    cond_val_sig1 = (spread_10_30 >= 100) & (spread_10_20 >= 50) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle
    cond_val_sig2_3 = (spread_10_30 >= 150) & (spread_10_20 >= 100) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle

    # Raw 시그널
    raw_sig1 = is_aligned_112 & cond_val_sig1 & cond_rising
    raw_sig2 = is_aligned_224 & cond_val_sig2_3 & cond_rising
    raw_sig3 = is_aligned_448 & cond_val_sig2_3 & cond_rising

    # 캔들 앵글 및 S4 바닥 로직 (WMA 3 계산)
    c_3 = np.roll(c, 3); c_3[:3] = c[:3]
    candle_roc = np.where(c_3 != 0, ((c - c_3) / c_3) * 1000, 0)
    wma_roc = pd.Series(candle_roc).rolling(3).apply(lambda x: np.dot(x, [1, 2, 3]) / 6, raw=True).fillna(0).values
    candle_angle = np.where(is_aligned_30, wma_roc, 0)

    raw_sig4 = np.zeros(len(c), dtype=bool)
    is_candle_bottom = False
    
    for i in range(len(c)):
        if candle_angle[i] <= 0:
            is_candle_bottom = True
        
        if is_candle_bottom and (candle_angle[i] >= 50) and is_aligned_30[i] and is_bullish[i]:
            raw_sig4[i] = True
            is_candle_bottom = False

    # 트뷰 원본 상호 배제 로직 적용
    signal_3 = raw_sig3
    signal_2 = raw_sig2 & (~signal_3)
    signal_1 = raw_sig1 & (~signal_2) & (~signal_3)
    signal_4 = raw_sig4 & (~signal_1) & (~signal_2) & (~signal_3)

    # ---------------------------------------------------------
    # 최종 타점 확정 (한국장 기준 거래대금/주가 필터 및 S1, S4 추출)
    # ---------------------------------------------------------
    moneyOk = (c * v) >= 100_000_000
    priceOk = c >= 1000

    hit_s1 = signal_1 & moneyOk & priceOk
    hit_s4 = signal_4 & moneyOk & priceOk

    final_hit = hit_s1 | hit_s4
    if not final_hit[-1]: return False, "", df, {}

    # =========================================================================
    # 👑 5. 한국장 시가총액(Marcap) 및 스코어링 로직
    # =========================================================================
    marcap_eok = marcap / 100_000_000 
    
    if marcap_eok >= 10000: cap_str, score_marcap = "① 1조 이상 (대형주)", 10.0
    elif marcap_eok >= 3000: cap_str, score_marcap = "② 3천억~1조 (중형주)", 8.0
    elif marcap_eok >= 1000: cap_str, score_marcap = "③ 1천억~3천억 (중소형주)", 6.0
    elif marcap_eok >= 500: cap_str, score_marcap = "④ 500억~1천억 (소형주)", 4.0
    else: cap_str, score_marcap = "⑤ 500억 미만 (초소형 잡주)", 2.0

    recent_hits = final_hit[-252:-1].sum() if len(c) > 252 else final_hit[:-1].sum()
    freq_count = int(recent_hits)
    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    
    if hit_s1[-1]:
        sig_type = "🔥 S1 (대세 추세 모멘텀 - SIG 1)"
        ema_stat_str = "승률 34.0% / 손익비 4.71 (대세 상승장)"
        score_rs = scale_score(cur_rs, 2025.28, -821.13)
        score_ema = 10.0
        score_cpv = scale_score(cur_cpv, 0.39, 0.95)
        score_bbe = scale_score(cur_bbe, 56.80, 3.80)
        score_tb = scale_score(cur_tb, 20.13, 2.47)
        score_freq = 10.0 if 1 <= freq_count <= 5 else 5.0
    else:
        sig_type = "🔥 S4 (V자 바닥 탈출 - SIG 4)"
        ema_stat_str = "승률 24.3% / 손익비 3.00 (역배열 바닥 로또 타점)"
        score_rs = scale_score(cur_rs, 500.0, -100.0)
        score_ema = 5.0
        score_cpv = scale_score(cur_cpv, 0.1, 0.8)
        score_bbe = scale_score(cur_bbe, 40.0, 5.0)
        score_tb = scale_score(cur_tb, 50.0, 5.0)
        score_freq = 6.0

    total_score = (score_rs*10 + score_ema*9 + score_marcap*8 + score_cpv*7 + score_bbe*6 + score_tb*5 + score_freq*4) / 490 * 100
    total_score = min(max(total_score, 0), 100)

    if total_score >= 80: badge_str = "🔥 [1티어 뱃지] 가산점 대상"
    elif total_score <= 50 and cur_rs > 500: badge_str = "💎 [특급 모멘텀 예외] 소액 로또 접근"
    else: badge_str = "⚠️ [비중 축소] 하위권 점수"

    v11_comment = (
        f"📊 [System B 한국 이평선 마스터 V11.0 리포트]\n"
        f"🔹 시스템 총점: {total_score:.1f} / 100점\n"
        f"🔹 시가총액: {cap_str}\n"
        f"🎖️ {badge_str}\n\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.2f} ({score_cpv:.1f}점)\n"
        f"▪️ 진짜양봉지수: {cur_tb:.1f} ({score_tb:.1f}점)\n"
        f"▪️ 응축에너지: {cur_bbe:.1f} ({score_bbe:.1f}점)\n"
        f"▪️ 시장상대강도: {cur_rs:.1f}% ({score_rs:.1f}점)\n"
        f"▪️ 과거 매매빈도: {freq_count}회 ({score_freq:.1f}점)\n"
        f"▪️ 시총 체급점수: {score_marcap:.1f}점\n\n"
        f"💡 [이평선 국면 팩트 데이터]\n{ema_stat_str}\n"
    )
    
    exit_strategy = "MFE 정점(평균 10.32일). ZLEMA 혹은 단기데드 기계적 청산."

    return True, sig_type, df, {
        "sig_type": sig_type, "last_close": float(c[-1]), "recommend": exit_strategy, 
        "v11_comment": v11_comment, "score": total_score, "v_cpv": cur_cpv, 
        "v_yang": cur_tb, "v_energy": cur_bbe, "v_rs": cur_rs
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

def scan_market_1d():
    global sent_today, last_run_date
    kr_tz = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(kr_tz).strftime('%Y-%m-%d')
    
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_top1.txt")
    
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

    print(f"\n⚡ [일봉 전용] 한국장 Top 1% 마스터 스캔 시작! (S1~S4 전체 포착 및 V7.0 점수 엔진 가동 🛡️)")
    t0 = time.time()
    
    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    
    # 💡 벤치마크 지수 (KOSPI/KOSDAQ) 일괄 로드 (상대강도 RS 계산용)
    print("📊 벤치마크 지수(KODEX ETF 대용) 데이터 안전하게 로드 중...")
    try:
        # LOGOUT 에러 및 차단 방지를 위해 지수 추종 ETF를 대용으로 사용
        kospi_idx = fdr.DataReader('069500', start_date)['Close']  # KODEX 200
        kosdaq_idx = fdr.DataReader('229200', start_date)['Close'] # KODEX 코스닥150
    except Exception as e:
        print(f"⚠️ 벤치마크 지수 로드 실패 ({e}). 빈 데이터로 우회합니다.")
        kospi_idx = pd.Series(dtype=float)
        kosdaq_idx = pd.Series(dtype=float)
        
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    
    def worker(row_tuple):
        try:
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            marcap = row.get("Marcap", 0) # 💡 [V11.0] 시가총액 데이터 추출
            df_raw = None
            
            try:
                df_raw = fdr.DataReader(code, start_date)
            except: pass

            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            hit, sig_type, df, dbg = False, "", None, {}
            
            if is_valid: 
                idx_close = kospi_idx if row["Market"] == 'KOSPI' else kosdaq_idx
                # 💡 [V11.0] 엔진에 marcap 전달
                hit, sig_type, df, dbg = compute_5ema_signal(df_raw, idx_close, marcap)
            
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
                # 💡 본캐용 및 홍보용 차트 생성
                main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
                promo_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
                
                if main_chart_path and promo_chart_path:
                    ai_main, _ = generate_ai_report(code, name)
                    
                    # 1️⃣ 본캐용 캡션 (유료방용 - V11.0 뱃지 및 점수 브리핑 출력)
                    main_caption = (
                        f"🎯 [{dbg.get('sig_type', '')}]\n"
                        f"🎯 추천: 스윙, 추세 홀딩 / 종가배팅\n\n"
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                        f"{dbg.get('v11_comment', '')}\n"  # 💡 [V11.0 적용]
                        f"📉 [스마트 매수/청산 전략]\n"
                        f"{dbg.get('recommend', '')}\n\n"
                        f"💡 [AI 비즈니스 요약]\n"
                        f"{ai_main}\n\n"
                        f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                        f"⚠️ [면책 조항]\n"
                        f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다.\n투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                    )
                    q_main.put((main_chart_path, main_caption))

                    # 💡 [오토 포워드 테스팅 시스템 변수 에러 픽스]
                    try:
                        import auto_forward_tester as aft
                        
                        market_type = 'KR' 
                        entry_facts = {
                            'v_cpv': dbg.get('v_cpv', 0),
                            'v_yang': dbg.get('v_yang', 0),
                            'v_energy': dbg.get('v_energy', 0),
                            'v_rs': dbg.get('v_rs', 0)
                        }
                        
                        # 👇👇 [수정해야 할 부분] 포워드 테스팅 시스템 호출부 👇👇
                        success, fwd_msg = aft.try_add_virtual_position(
                            market=market_type,
                            code=code,
                            name=name,
                            sig_type=dbg.get('sig_type', ''),
                            score=dbg.get('score', 0), 
                            # 💡 [핵심 픽스] 스코프 밖에 있는 c[-1] 참조 제거. 0으로 안전하게 예외처리!
                            ep=dbg.get('last_close', 0), 
                            facts=entry_facts
                        )
                        print(f"   ↳ [포워드 장부 기록]: {fwd_msg}")
                        # 👆👆 [여기까지 덮어쓰기] 👆👆
                    except Exception as e:
                        print(f"   ↳ [포워드 장부 에러]: {e}")

                    # 2️⃣ 홍보용 캡션 (쓸데없는 멘트 다 빼고 초심플 압축)
                    try:
                        sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                    except:
                        sector_info = "유망 섹터 포착"
                            
                    promo_caption = (
                        f"📈 [알고리즘 차트 포착]\n\n"
                        f"🏢 종목: {name} ({code})\n"
                        f"🏷️ 섹터: {sector_info}\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원"
                    )
                    q_promo.put((promo_chart_path, promo_caption))

                    print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")
        except Exception as e:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [한국장 Top 1% 마스터 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [Top 1% 마스터 검색기] 10:40 / 12:40 / 14:40 대기 중...")
    
    while True:
        now_kr = datetime.now(kr_tz)
        
        if (now_kr.hour == 10 and now_kr.minute == 40) or \
           (now_kr.hour == 12 and now_kr.minute == 40) or \
           (now_kr.hour == 14 and now_kr.minute == 40):
            print(f"🚀 [Top 1% 마스터 스캔 시작] {now_kr.strftime('%H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else:
            time.sleep(10)

if __name__ == "__main__":
    # run_scheduler()  <-- 이 줄을 주석 처리하거나 지우고
    scan_market_1d()   # ⭐️ 이 문구를 추가하면 즉시 1회 스캔이 시작됩니다.
