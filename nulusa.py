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
import json

# 💡 [자율 관제탑 연결] 조율된 파라미터 수신
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')

def load_system_config():
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r') as f: return json.load(f)
    except: pass
    return {} # 에러 시 빈 데이터 반환 (하드코딩된 기본값으로 자동 우회)

SYS_CONFIG = load_system_config()

# 💡 [DB 경로 세팅] 로컬 데이터베이스 위치
DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')

# 💡 [Next Level 1] 하이브리드 데이터 로더 (한국장 전용)
def get_safe_data(code, start_date):
    table_name = f"KR_{code}"
    try:
        # 1. 내 컴퓨터(DB)에서 과거 데이터 광속 로드
        conn = sqlite3.connect(DB_PATH)
        df_db = pd.read_sql(f"SELECT * FROM {table_name}", conn, index_col='Date')
        conn.close()
        df_db.index = pd.to_datetime(df_db.index)

        # 2. 오늘 실시간 캔들 딱 1개만 가져오기
        df_live = fdr.DataReader(code, datetime.now().strftime('%Y-%m-%d'))
        
        if not df_live.empty:
            df_combined = pd.concat([df_db, df_live])
            return df_combined[~df_combined.index.duplicated(keep='last')]
        return df_db
    except:
        # DB가 없거나 에러 시 즉시 기존 방식으로 우회 (절대 멈추지 않음)
        return fdr.DataReader(code, start_date)

# 💡 [Next Level 2] 동적 백분위 스코어링 함수
def get_dynamic_score(series_data, higher_is_better=True, window=252):
    if len(series_data) < 20: return 5.0
    pct_rank = pd.Series(series_data).rolling(window, min_periods=20).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    ).fillna(0.5).values[-1]
    
    if higher_is_better: return 1.0 + (pct_rank * 9.0)
    else: return 1.0 + ((1.0 - pct_rank) * 9.0)

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
TELEGRAM_TOKEN_MAIN  = "7791873924:AAElFAy66rBbM-cgC3QW_jJ1hnBOu8bZV-I"
TELEGRAM_TOKEN_PROMO = "7996581031:AAHbZ3wZcsWgXk3xITTzBntI5YzlC6unhI0"
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
                        res = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption, "parse_mode": "HTML"}, files={"photo": f}, timeout=60, verify=False)
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
    
    # 1. 팩트 데이터 추출
    try:
        if code.isdigit(): # 한국장
            res = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(res.text, 'html.parser')
            sector_kr = soup.select_one('h4.h_sub.sub_tit7 a').text.strip() if soup.select_one('h4.h_sub.sub_tit7 a') else '국내 증시'
        else: # 미국장
            tk = yf.Ticker(code)
            sector = tk.info.get('sector', '글로벌 산업')
            sector_kr_map = {"Technology": "테크/기술", "Healthcare": "헬스케어", "Financial Services": "금융", "Consumer Cyclical": "소비재", "Industrials": "산업재", "Energy": "에너지", "Basic Materials": "원자재"}
            sector_kr = sector_kr_map.get(sector, sector)
    except:
        sector_kr = '유망 섹터'

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

            if not m_part: raise ValueError("파싱오류")

            return m_part.group(1).strip(), ""
        except:
            pass 
            
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

MIN_PRICE_USD = 3.0               
MIN_MONEY_USD = 5_000_000         

# 💡 [추가] 1~10점 스케일링 함수 (방향성 완벽 지원)
def scale_score(val, best, worst):
    if best > worst: # 높을수록 좋은 지표 (RS, 진짜양봉, 응축에너지)
        if val >= best: return 10.0
        if val <= worst: return 1.0
        return 1.0 + 9.0 * (val - worst) / (best - worst)
    else: # 낮을수록 좋은 지표 (CPV 등)
        if val <= best: return 10.0
        if val >= worst: return 1.0
        return 1.0 + 9.0 * (worst - val) / (worst - best)

# 💡 [교체] 미국장 V7.0 마스터 시그널 엔진 (169,021건 팩트 데이터 완벽 적용)
def compute_nulrim_1d(df_raw: pd.DataFrame, idx_close: pd.Series, vix_close: pd.Series): 
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    df['Idx_Close'] = idx_close
    df['Idx_Close'] = df['Idx_Close'].ffill()

    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

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

    # 상대강도(RS) 계산
    c_20 = pd.Series(c).shift(20).values
    idx_20 = df['Idx_Close'].shift(20).values
    with np.errstate(divide='ignore', invalid='ignore'):
        stock_ret = np.where(c_20 > 0, (c - c_20) / c_20, 0.0)
        idx_ret = np.where(idx_20 > 0, (df['Idx_Close'].values - idx_20) / idx_20, 0.0001)
        idx_ret = np.where(idx_ret == 0, 0.0001, idx_ret) 
        rs = (stock_ret / idx_ret) * 100
    rs = np.nan_to_num(rs, nan=0.0)

  # =========================================================================
    # 👑 [2단계] 눌림목 타점 발생 로직 (S1, S2, S4, S6 4가지만 명확히 분리 포착)
    # =========================================================================
    moneyOk = (c * v) >= 5_000_000
    priceOk = c >= 3.0
    isBullish = c > o

    align112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    align224 = align112 & (e112 > e224)
    align448 = align224 & (e224 > e448)

    longKeep448 = e224 > e448 
    longKeep224 = e112 > e224

    prev_align448 = np.roll(align448, 1); prev_align448[0] = False
    prev_align224 = np.roll(align224, 1); prev_align224[0] = False

    prev_longKeep448 = np.roll(longKeep448, 1); prev_longKeep448[0] = False
    prev_longKeep224 = np.roll(longKeep224, 1); prev_longKeep224[0] = False

    # 1. S1 (448일 완전정배열 재정렬)
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    
    # 2. S2 (224일 정배열 재정렬)
    s2 = align224 & (~prev_align224) & prev_longKeep224 & (e224 < e448) & isBullish
    
    # 3. S4 (20일선 눌림돌파)
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    
    s4 = np.zeros_like(c, dtype=bool)
    last_pb = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pb > 5):
            s4[i] = True
            last_pb = i

    # 4. S6 (바닥 탈출 단기 정배열)
    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1); prev_shortBull[0] = False
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish

    cond_base = moneyOk & priceOk

    cond_base = moneyOk & priceOk

    # 💡 [V9.0 업데이트] S3, S6, S7 배제! 오직 S1(대세추세), S2(급등눌림), S4(바닥탈출)만 타점으로 잡음
    df['VIX_Close'] = vix_close.reindex(df.index).ffill() # VIX 지수 매핑
    
    # 💡 [버그 픽스] Pandas 인덱싱 에러(KeyError) 방지를 위해 Numpy 배열로 값 추출
    cond_base_arr = cond_base.values if isinstance(cond_base, pd.Series) else cond_base
    s1_arr = s1.values if isinstance(s1, pd.Series) else s1
    s2_arr = s2.values if isinstance(s2, pd.Series) else s2
    s4_arr = s4.values if isinstance(s4, pd.Series) else s4

    hit_s1 = s1_arr[-1] and cond_base_arr[-1]
    hit_s2 = s2_arr[-1] and cond_base_arr[-1]
    hit_s4 = s4_arr[-1] and cond_base_arr[-1]

    if not (hit_s1 or hit_s2 or hit_s4): 
        return False, "", df, {}

    # =========================================================================
    # 👑 [3단계] S1, S2, S4 스코어링 매핑 (미국장 V9.0 팩트 대입)
    # =========================================================================
    recent_hits = (s1 | s2 | s4)[-252:-1].sum() if len(c) > 252 else (s1 | s2 | s4)[:-1].sum()
    freq_count = int(recent_hits)

    if align448[-1]: ema_stat_str = "승률 29.3% / 손익비 2.67 (가장 튼튼한 대세 상승 주도주)"
    elif align224[-1]: ema_stat_str = "승률 29.5% (불장 트렌드 서퍼, 단기 급등)"
    else: ema_stat_str = "완전 역배열 바닥 탈출 (밈 주식 로또성 타점)"

    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    cur_vix = df['VIX_Close'].iloc[-1] if not pd.isna(df['VIX_Close'].iloc[-1]) else 15.0

    score_cpv, score_tb, score_bbe, score_rs, score_ema, score_freq = 0, 0, 0, 0, 0, 0
    total_score = 0
    trap_warning = ""
    
    # 💡 [V9.0 청산 전략 가이드 (월가 알고리즘 의도 파악)]
    exit_strategy = "MFE 정점(평균 16.58일). 지저분한 꼬리(CPV 0.22)로 올리면 단기데드로 끝까지 홀딩. 진입 후 꽉찬 예쁜 양봉(CPV 0.30 이상)만 나오면 월가 알고리즘의 설거지 패턴이므로 4일 내 ZLEMA 즉각 칼손절."

    if hit_s2: # [S2 단기 급등 돌파 매핑]
        sig_type = "🔥 [눌림] S2 (224 재정렬)"
        score_rs   = scale_score(cur_rs, 1432.14, -80.50)  
        score_ema  = 10.0 if align224[-1] else 5.0         
        score_cpv  = scale_score(cur_cpv, 0.12, 0.86)      
        score_bbe  = scale_score(cur_bbe, 30.80, 1.80)     
        score_tb   = scale_score(cur_tb, 11.10, 0.90)      
        score_freq = 6.0
        total_score = (score_rs*10 + score_ema*9 + score_cpv*8 + score_bbe*7 + score_tb*6 + score_freq*5) / 450 * 100
        regime_weight = SYS_CONFIG.get("WEIGHT_US_NULRIM_S2", 1.0)
        
    elif hit_s1: # [S1 448 대세 추세 전용 블록]
        sig_type = "🔥 [눌림] S1 (448 대세 추세)"
        score_rs   = scale_score(cur_rs, SYS_CONFIG.get("US_NULRIM_S1_RS_BEST", 990.40), SYS_CONFIG.get("US_NULRIM_S1_RS_WORST", -102.75))
        score_ema  = 10.0 if align448[-1] else 1.0
        score_cpv  = scale_score(cur_cpv, SYS_CONFIG.get("US_NULRIM_S1_CPV_BEST", 0.12), SYS_CONFIG.get("US_NULRIM_S1_CPV_WORST", 0.87))
        score_bbe  = scale_score(cur_bbe, SYS_CONFIG.get("US_NULRIM_S1_BBE_BEST", 31.60), SYS_CONFIG.get("US_NULRIM_S1_BBE_WORST", 2.50))
        score_tb   = scale_score(cur_tb, SYS_CONFIG.get("US_NULRIM_S1_TB_BEST", 11.10), SYS_CONFIG.get("US_NULRIM_S1_TB_WORST", 0.90))
        score_freq = 10.0 if 1 <= freq_count <= 5 else 5.0
        
        total_score = (score_rs*10 + score_ema*9 + score_cpv*8 + score_bbe*7 + score_tb*6 + score_freq*5) / 450 * 100
        
        regime_weight = SYS_CONFIG.get("WEIGHT_US_NULRIM_S1", 1.0) # 👈 관제탑 S1 자본 배분율 로드

    elif hit_s4: # [S4 바닥 탈출 전용 블록]
        sig_type = "🔥 [눌림] S4 (바닥 탈출 밈 주식 로또 타점)"
        score_rs   = scale_score(cur_rs, SYS_CONFIG.get("US_NULRIM_S4_RS_BEST", 990.40), SYS_CONFIG.get("US_NULRIM_S4_RS_WORST", -102.75))
        score_ema  = 10.0 if align448[-1] else 1.0
        score_cpv  = scale_score(cur_cpv, SYS_CONFIG.get("US_NULRIM_S4_CPV_BEST", 0.12), SYS_CONFIG.get("US_NULRIM_S4_CPV_WORST", 0.87))
        score_bbe  = scale_score(cur_bbe, SYS_CONFIG.get("US_NULRIM_S4_BBE_BEST", 31.60), SYS_CONFIG.get("US_NULRIM_S4_BBE_WORST", 2.50))
        score_tb   = scale_score(cur_tb, SYS_CONFIG.get("US_NULRIM_S4_TB_BEST", 11.10), SYS_CONFIG.get("US_NULRIM_S4_TB_WORST", 0.90))
        score_freq = 10.0 if 1 <= freq_count <= 5 else 5.0
        
        total_score = (score_rs*10 + score_ema*9 + score_cpv*8 + score_bbe*7 + score_tb*6 + score_freq*5) / 450 * 100
        
        regime_weight = SYS_CONFIG.get("WEIGHT_US_NULRIM_S4", 1.0) # 👈 관제탑 S4 자본 배분율 로드
        
    # =========================================================================
    # 👑 [4단계] 미국장 V9.0 디테일: VIX 매핑, 요일 효과, 데스콤보, 뱃지 시스템
    # =========================================================================
    weekday = df.index[-1].weekday()
    if weekday == 4: total_score *= 1.05 
    elif weekday == 0: total_score *= 0.95 

    # V9.0 데스콤보
    is_death_combo = (cur_cpv > 0.95) and (total_score < 40.0)
    if is_death_combo: 
        total_score *= 0.70
        trap_warning += "⚠️ [데스 콤보 발동] 세력 단기 차익 실현 후 폭락 패턴 (점수 30% 삭감)\n"
        
    total_score = min(max(total_score, 0), 100)

    # =========================================================================
    # 👑 [비선형 의사결정 나무 (Decision Tree) 필터] - 선형 덧셈의 오류 차단
    # =========================================================================
    # 💡 [버그 픽스] 글로벌 0.85 하드코딩 삭제! 각 전략방(Namespace)에 맞는 자율 학습치 로드
    live_params = SYS_CONFIG.get(f"{ns_prefix}_LIVE_PARAMS", SYS_CONFIG)
    tree_fatal_cpv = live_params.get("TREE_FATAL_CPV", 0.95) # 기본 허들을 0.95로 넓혀서 정상 종목 억울한 기각 방지
    
    is_tree_rejected = False
    tree_reason = ""

    # [Node 1]: 꽉 찬 양봉(월가 설거지 패턴) 한계치 초과 시 기각 (Death Combo)
    if cur_cpv > tree_fatal_cpv:
        is_tree_rejected = True
        # 💡 [텍스트 교정] "악성 매물"은 윗꼬리를 뜻하므로 잘못된 텍스트. "가짜 펌핑(꽉 찬 양봉)"으로 용어 교정
        tree_reason = f"월가 가짜 펌핑(꽉 찬 양봉) 한계치 초과 (CPV {cur_cpv:.2f} > {tree_fatal_cpv})"
        
    # [Node 2]: VIX가 25 이상(공포장)인데, 모멘텀(RS)이 0 이하면 기각 (약한 놈부터 죽음)
    elif cur_vix >= 25.0 and cur_rs < 0:
        is_tree_rejected = True
        tree_reason = f"공포장(VIX {cur_vix:.1f}) 속 모멘텀 붕괴 (RS {cur_rs:.1f})"

    # 비선형 필터에 걸렸다면 총점을 강제로 0점 처리하고 사형 선고
    if is_tree_rejected:
        total_score = 0.0
        trap_warning += f"🚫 <b>[Decision Tree 기각]</b>: 선형 점수는 높을 수 있으나, 비선형 팩트에 의해 차단되었습니다. (사유: {tree_reason})\n"
        badge_str = "💀 [비선형 필터 기각] 매수 절대 금지"
    # =========================================================================

    # =========================================================================
    # 👑 [종목 맞춤형 동적 청산 전략 (관제탑 지시 기반)]
    # =========================================================================
    if cur_cpv >= 0.30:
        cpv_stat = f"예쁜 꽉 찬 양봉 (CPV {cur_cpv:.2f})"
    elif cur_cpv <= 0.24:
        cpv_stat = f"지저분한 꼬리 캔들 (CPV {cur_cpv:.2f})"
    else:
        cpv_stat = f"표준적인 캔들 (CPV {cur_cpv:.2f})"

    # 👇 타점에 따른 동적 네임스페이스 분리 (S1, S2, S4)
    if hit_s1: ns_prefix = "US_NULRIM_S1"
    elif hit_s2: ns_prefix = "US_NULRIM_S2"
    else: ns_prefix = "US_NULRIM_S4"

    active_exit_mode = SYS_CONFIG.get("ACTIVE_EXIT_MODE", "HYBRID")
    opt_time_stop    = SYS_CONFIG.get(f"{ns_prefix}_TIME_STOP", 10)
    opt_sl_atr       = SYS_CONFIG.get(f"{ns_prefix}_ATR_SL", 2.0)

    if active_exit_mode == "TECH":
        action = "📈 <b>[TECH 추세 모드 가동]</b>\n대세 상승장 판독 완료. 통계적 숏컷을 무시하고, '단기데드' 및 'ZLEMA 이탈' 전까지 차트 추세를 끝까지 발라먹으십시오."
    elif active_exit_mode == "STAT":
        action = (f"🎯 <b>[STAT 통계 모드 가동]</b>\n변동성/휩소 장세 판독 완료. 차트 무시!\n"
                  f"▪️ 진입 후 <b>{opt_time_stop}일 차 종가</b>에 무조건 타임스탑(기계적 청산) 하십시오.\n"
                  f"▪️ 진입가 대비 <b>ATR {opt_sl_atr}배</b> 이탈 시 즉각 칼손절하십시오.")
    else: # HYBRID
        action = (f"⚖️ <b>[HYBRID 공수겸장 가동]</b>\n"
                  f"추세를 타되(ZLEMA 익절), 최대 <b>{opt_time_stop}일</b> 내에 승부를 보고, 폭락 시 <b>ATR {opt_sl_atr}배</b>에서 즉각 손절 차단하십시오.")

    tier_stat = ""

    if (hit_s4 or hit_s2) and cur_rs <= -1000:
        tier_stat = f"💡 [특급 로또 타점] 현재 완벽한 소외주(RS {cur_rs:.1f})입니다. 평소엔 승률이 낮아 패스해야 하지만..."

    regime_msg = f"🚨 <b>[관제탑 자본통제]: 현재 국면 판단에 따라 진입 비중을 {regime_weight}배로 강제 제한합니다.</b>"
    exit_strategy = f"[{cpv_stat}]\n{action}\n\n{tier_stat}\n{regime_msg}"

    # =========================================================================
    # 👑 [다중 클러스터 도플갱어 매칭] 다중 시계열 워핑(DTW) 렌즈 적용 (30~210일 확장)
    # =========================================================================
    match_result = "NONE"
    match_similarity = 0.0
    matched_name = ""
    best_window = 150
    max_sn_similarity = 0.0

    try:
        # 💡 [핵심] 30, 60, 90, 120, 150, 180, 210일 7가지 렌즈로 시계열 압축/팽창 탐색
        for window in [30, 60, 90, 120, 150, 180, 210]:
            if len(c) >= window:
                cur_cpv_w = np.nanmean(cpv[-window:])
                cur_tb_w = np.nanmean(tb_index[-window:])
                cur_bbe_w = np.nanmax(bb_energy[-20:]) # 에너지 응축은 항상 최근 20일 기준 고정
                cur_rs_w = ((c[-1] - c[-window]) / c[-window]) * 100 if c[-window] != 0 else 0
                
                tr_w = np.maximum(h[-window:] - l[-window:], np.maximum(abs(h[-window:] - np.roll(c[-window:], 1)), abs(l[-window:] - np.roll(c[-window:], 1))))
                cur_vcp_w = np.mean(tr_w[-20:]) / np.mean(tr_w) if np.mean(tr_w) > 0 else 1.0
                
                up_v = np.sum(np.where(c[-window:] > o[-window:], v[-window:], 0))
                dn_v = np.sum(np.where(c[-window:] < o[-window:], v[-window:], 0))
                cur_vol_w = up_v / dn_v if dn_v > 0 else 1.0
                
                cur_emas = [e10[-1], e20[-1], e60[-1], e112[-1], e224[-1]]
                cur_ma_w = (max(cur_emas) - min(cur_emas)) / min(cur_emas) * 100 if min(cur_emas) > 0 else 0

                # 7차원 유클리디안 거리(유사도) 연산 함수 (오류 방지 0.001 보정)
                def calc_similarity(dna):
                    if not dna: return 0.0
                    err = (
                        min(abs(cur_cpv_w - dna.get('cpv', 0.5)) / (dna.get('cpv', 0.5) + 0.001), 1.0) * 0.15 +
                        min(abs(cur_tb_w - dna.get('tb', 1.0)) / (dna.get('tb', 1.0) + 0.001), 1.0) * 0.15 +
                        min(abs(cur_bbe_w - dna.get('bbe', 0.1)) / (dna.get('bbe', 0.1) + 0.001), 1.0) * 0.15 +
                        min(abs(cur_rs_w - dna.get('rs', 0)) / (abs(dna.get('rs', 0)) + 0.001), 1.0) * 0.15 +
                        min(abs(cur_vcp_w - dna.get('vcp', 1.0)) / (dna.get('vcp', 1.0) + 0.001), 1.0) * 0.15 +
                        min(abs(cur_vol_w - dna.get('vol', 1.0)) / (dna.get('vol', 1.0) + 0.001), 1.0) * 0.10 +
                        min(abs(cur_ma_w - dna.get('ma', 5.0)) / (dna.get('ma', 5.0) + 0.001), 1.0) * 0.15
                    )
                    return max(0, 100.0 - (err * 100))

                # 대장주 1~3위 & 참사주 1~3위 템플릿과 비교하여 가장 높은 유사도 찾기
                for i in [1, 2, 3]:
                    # Alpha 매칭
                    a_dna = SYS_CONFIG.get(f"DNA_ALPHA_RANK{i}")
                    if a_dna:
                        sim = calc_similarity(a_dna)
                        if sim > match_similarity: 
                            match_similarity, match_result, matched_name, best_window = sim, f"ALPHA_{i}", a_dna.get('name', '대장주'), window
                    
                    # Trap 매칭
                    t_dna = SYS_CONFIG.get(f"DNA_TRAP_RANK{i}")
                    if t_dna:
                        sim = calc_similarity(t_dna)
                        if sim > match_similarity: 
                            match_similarity, match_result, matched_name, best_window = sim, f"TRAP_{i}", t_dna.get('name', '참사주'), window

        # 👇👇 [여기에 V53.0 초신성(Supernova) 타임머신 매칭 로직을 추가합니다!] 👇👇
                market_str = "US" if "US" in ns_prefix else "KR"
                sn_dna = SYS_CONFIG.get(f"DNA_SUPERNOVA_{market_str}")
                if sn_dna:
                    sn_sim = calc_similarity(sn_dna)
                    if sn_sim > max_sn_similarity: 
                        max_sn_similarity = sn_sim
                # 👆👆 [초신성 매칭 끝] 👆👆
        
        # 4. 80% 이상 매칭 시 전략(exit_strategy) 문구 삽입 및 점수(total_score) 보정
        if match_similarity >= 80.0:
            if "ALPHA" in match_result:
                rank = match_result.split('_')[1]
                doppel_msg = (
                    f"\n\n🌌 <b>[대장주 {rank}순위 궤적 매칭! - 유사도 {match_similarity:.1f}%]</b>\n"
                    f"이 종목의 최근 <b>{best_window}일</b> 궤적이 역대 최고 알파 종목이었던 <b>[{matched_name}]</b>의 매집 흐름과 사실상 동일합니다. 세력의 빌드업이 끝났으니 비중을 높이십시오."
                )
                exit_strategy += doppel_msg
                total_score = min(total_score + 10, 100.0) # 대장주 일치 시 강제 가산점 부여
            
            elif "TRAP" in match_result:
                rank = match_result.split('_')[1]
                doppel_msg = (
                    f"\n\n💀 <b>[참사/지옥행 {rank}순위 궤적 매칭! - 유사도 {match_similarity:.1f}%]</b>\n"
                    f"🚨 <b>치명적 경고:</b> 최근 <b>{best_window}일</b> 궤적이 과거 계좌를 녹였던 <b>[{matched_name}]</b>의 가짜 돌파(설거지) 궤적과 똑같습니다. 점수를 무시하고 매수 취소!"
                )
                exit_strategy += doppel_msg
                total_score = max(total_score - 30, 0.0) # 참사주 일치 시 치명적 감점
    except Exception as e:
        pass # 에러 시 시스템 정지 방지용 조용히 패스
    # =========================================================================

    # =========================================================================
    # 👑 [변동성 타겟팅 정밀 매수 가이드 (리스크 패리티)]
    # =========================================================================
    try:
        TOTAL_CAPITAL = 35_000  # 👈 (수정완료) 약 5천만원에 해당하는 3만5천 '달러'로 입력
        BASE_RISK = 0.015           # 👈 1종목당 기본 허용 리스크 1.5%

        # 1. 14일 평균 변동폭(ATR) 계산 (결측치 방어)
        tr = np.maximum(h[-14:] - l[-14:], np.maximum(abs(h[-14:] - np.roll(c[-14:], 1)), abs(l[-14:] - np.roll(c[-14:], 1))))
        cur_atr = np.mean(tr) if np.mean(tr) > 0 else c[-1] * 0.03
        
        # 2. 관제탑 손절 승수 로드 (파일별 ns_prefix 사용)
        opt_sl_atr = SYS_CONFIG.get(f"{ns_prefix}_ATR_SL", 2.0)
        
        # 3. 1주당 손실 예상 금액 (리스크 폭)
        risk_per_share = cur_atr * opt_sl_atr
        
        # 4. KNN 도플갱어 매칭 결과에 따른 비중 승수 조작
        final_weight = regime_weight
        if "ALPHA" in match_result and match_similarity >= 80.0:
            final_weight *= 1.5  # 대장주 패턴이면 비중 1.5배 확대
        elif "TRAP" in match_result and match_similarity >= 80.0:
            final_weight *= 0.0  # 참사 패턴이면 매수 금지 (0배)
            
        # 5. 최종 매수 수량 및 투입 금액 계산
        target_risk_amount = TOTAL_CAPITAL * BASE_RISK * final_weight
        target_shares = int(target_risk_amount / risk_per_share) if risk_per_share > 0 else 0
        recommended_investment = target_shares * c[-1]

        if final_weight > 0 and target_shares > 0:
            currency = "달러"
            sizing_msg = (
                f"\n\n💰 <b>[변동성 타겟팅 정밀 매수 지시]</b>\n"
                f"▪️ 권장 매수 수량: <b>{target_shares:,}주</b>\n"
                f"▪️ 총 투입 금액: <b>{recommended_investment:,.0f}{currency}</b>\n"
                f"<i>(💡 팩트: 손절선을 터치해도 계좌 총손실은 안전하게 방어됩니다.)</i>"
            )
            exit_strategy += sizing_msg
    except Exception as e:
        pass
    # =========================================================================

    # 💡 [V9.0 VIX(공포지수) 기반 비중 조절 로직]
    vix_strategy = ""
    if cur_vix >= 30:
        vix_strategy = f"🌋 [극단적 공포장 | VIX {cur_vix:.1f}] 승률 43%, 평균수익 40.6% 터지는 초거대 대박 구간! 진입 비중 1.5배 상향 및 적극 매수."
    elif cur_vix >= 20:
        vix_strategy = f"🌪️ [조정장 | VIX {cur_vix:.1f}] 손익비 3.16 상승 구간! 진입 비중 1.2배 상향."
    else:
        vix_strategy = f"🌊 [평온장 | VIX {cur_vix:.1f}] 시스템 기본 비중(1배수) 기계적 매매."
    # =========================================================================
    # 👑 [Next Level 2] 듀얼 트랙 상대 평가 (미국장 백분위 랭크 추가)
    # =========================================================================
    dyn_rs_score = get_dynamic_score(rs, higher_is_better=True)
    dyn_tb_score = get_dynamic_score(tb_index, higher_is_better=True)
    dyn_cpv_score = get_dynamic_score(cpv, higher_is_better=False)

    # 💡 텔레그램 결과지에 출력될 브리핑 데이터 조립 (V9.0 적용)
    v9_comment = (
        f"📊 [System B US 시그널 V9.0 마스터 리포트]\n"
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
        # 👇 추가된 백분위 팩트 데이터
        f"💡 [상대평가] RS 상위 {(10 - dyn_rs_score) * 11.1:.1f}% / 찐양봉 상위 {(10 - dyn_tb_score) * 11.1:.1f}%\n"
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
        "v_rs": cur_rs,
        # 👇 장부 기록을 위해 3줄 추가
        "dyn_rs_score": dyn_rs_score,
        "dyn_cpv_score": dyn_cpv_score,
        "dyn_tb_score": dyn_tb_score,
        "sn_score": max_sn_similarity
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
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - ((df_cut['High'].max() - df_cut['Low'].min()) * 0.04)
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
    print(f"\n🇺🇸 [일봉 전용] 미국장 4번(눌림목) 스캔 시작!")

    # 💡 [V9.0 핵심] 벤치마크 지수(SPY, QQQ) 및 VIX(공포지수) 데이터 동시 로드
    print("📊 벤치마크 지수 및 VIX(공포지수) 데이터 로드 중...")
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
            with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
                executor.map(fetch_single, chunk)
        
        for tk in chunk:
            tracker['scanned'] += 1
            info = ticker_to_info.get(tk)
            if not info: continue
            name, code = info['name'], info['code']

            # 👇👇 [기존 코드 지우고 여기서부터] 👇👇
            try:
                if df_batch is not None:
                    if len(chunk) == 1: 
                        df_ticker = df_batch.copy()
                    else: 
                        # 💡 [핵심 픽스 1 이식] yfinance 최신/구버전 완벽 호환 무적 방어 로직
                        if isinstance(df_batch.columns, pd.MultiIndex):
                            if tk in df_batch.columns.get_level_values(0):
                                df_ticker = df_batch[tk].copy()
                            elif tk in df_batch.columns.get_level_values(1):
                                df_ticker = df_batch.xs(tk, level=1, axis=1).copy()
                            else:
                                continue
                        else:
                            df_ticker = df_batch.copy()
                else:
                    # 💡 [핵심 픽스 2 이식] batch 다운로드가 실패했을 때만 fallback_dict를 쓰도록 else 처리
                    df_ticker = fallback_dict.get(tk)

                if df_ticker is None or df_ticker.empty: continue

                # 1. 계산에 필요한 컬럼만 추출 후 결측치 제거
                df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
                # 2. 타임존 제거 (비교를 위해 통일)
                if df_ticker.index.tzinfo is not None: 
                    df_ticker.index = df_ticker.index.tz_convert('America/New_York').tz_localize(None)
                
                # 3. 중복 날짜 제거 (최신 데이터 유지)
                df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

                # 4. 모든 데이터를 계산 가능한 실수형(float)으로 강제 변환
                df_ticker = df_ticker.astype(float)
                
                # 5. 판다스 내부에서 날짜순(오름차순)으로 강력 정렬 (가장 중요)
                df_ticker.sort_index(inplace=True)
            # 👆👆 [여기까지 통째로 덮어쓰기 하십시오] 👆👆

                if len(df_ticker) >= 500:
                    tracker['analyzed'] += 1
                    
                    market_type = info['market']
                    target_idx = qqq_idx if market_type == 'NASDAQ' else spy_idx
                    hit, sig_type, df, dbg = compute_nulrim_1d(df_ticker, target_idx, vix_idx)
                    
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
                            
                            try:
                                sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                            except:
                                sector_info = "유망 섹터 포착"
                            
                            main_caption = (
                                f"🎯 [{dbg.get('sig_type', '')}]\n"
                                f"🎯 추천: 단타, 스윙 / 종가배팅\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                                f"{dbg.get('v9_comment', '')}\n"
                                f"📉 [스마트 매수/청산 전략]\n"
                                f"{dbg.get('recommend', '')}\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_main}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                                f"⚠️ [면책 조항]\n"
                                f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 매수/매도 권유가 아닙니다."
                            )
                            q_main.put((main_chart_path, main_caption))

                            try:
                                import auto_forward_tester as aft
                                market_type = 'US' if 'US' in dbg.get('sig_type', '') else 'KR'
                                entry_facts = {
                                    'v_rs': dbg.get('v_rs', 0),
                                    'v_cpv': dbg.get('v_cpv', 0),
                                    'v_yang': dbg.get('v_yang', 0),
                                    'v_energy': dbg.get('v_energy', 0),
                                    'marcap_eok': 0,       
                                    'score_marcap': 0,     
                                    'freq_count': dbg.get('freq_count', 0),
                                    'dyn_rs': dbg.get('dyn_rs_score', 0),
                                    'dyn_cpv': dbg.get('dyn_cpv_score', 0),
                                    'dyn_tb': dbg.get('dyn_tb_score', 0),
                                    'is_tenbagger': 0, 
                                    'is_top_dna': 0,   
                                    'is_worst_dna': 0, 
                                    'is_death_combo': 1 if dbg.get('is_death_combo') else 0
                                }
                                
                                success, fwd_msg = aft.try_add_virtual_position(
                                    market=market_type, code=code, name=name,
                                    sig_type=dbg.get('sig_type', ''), score=dbg.get('score', 0), 
                                    ep=dbg.get('last_close', 0), facts=entry_facts, sector=sector_info,
                                    trade_source="STANDARD"
                                )
                                print(f"   ↳ [오리지널 장부]: {fwd_msg}")
                                
                                sn_score = dbg.get('sn_score', 0.0)
                                if sn_score >= 50.0:
                                    _, sn_msg = aft.try_add_virtual_position(
                                        market=market_type, code=code, name=name,
                                        sig_type=dbg.get('sig_type', ''), score=max(dbg.get('score', 0), 50.0), 
                                        ep=dbg.get('last_close', 0), facts=entry_facts, sector=sector_info,
                                        trade_source="SUPERNOVA"
                                    )
                                    print(f"   ↳ [초신성 장부]: {sn_msg}")
                                    
                            except Exception as e:
                                print(f"   ↳ [포워드 장부 에러]: {e}")

                            promo_caption = (
                                f"📈 [알고리즘 차트 포착]\n\n"
                                f"🏢 종목: {name} ({code})\n"
                                f"🏷️ 섹터: {sector_info}\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}"
                            )
                            q_promo.put((promo_chart_path, promo_caption))

                        print(f"\n✅ [{name}] 미국장 포착! 듀얼 발송 대기열 추가 완료!")
            except Exception as e:
                err_name = name if 'name' in locals() else tk
                err_text = f"⚠️ Worker 구동 중 에러 발생 [{err_name}]: {e}"
                print(err_text)
                # 불필요한 텔레그램 에러 메시지 전송은 주석 처리하여 깔끔하게 만듭니다.
                # q_main.put((None, f"🚨 <b>[미국장 검색기 워커 에러]</b>\n{err_text}"))
            # 👆👆 [수정 완료] 👆👆
        
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
    scan_market_1d()
