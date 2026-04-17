# Dante_US_Top1_Master_1D_AI_Pro.py (미국장 Top 1% 마스터 SIG 1,2,3,4 + System B US V7.0 완전판)
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

# 💡 2. 본캐 팩트 리포트
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

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()] 
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

# 💡 보조 함수: 1~10점 스케일링 (10점 기준값과 1점 기준값을 정확히 매핑)
def scale_score(val, pt10_val, pt1_val):
    if pt10_val > pt1_val: # 높을수록 좋은 지표 (RS, 찐양봉, 응축에너지)
        if val >= pt10_val: return 10.0
        if val <= pt1_val: return 1.0
        return 1.0 + 9.0 * (val - pt1_val) / (pt10_val - pt1_val)
    else: # 낮을수록 좋은 지표 (CPV 등)
        if val <= pt10_val: return 10.0
        if val >= pt1_val: return 1.0
        return 1.0 + 9.0 * (pt1_val - val) / (pt1_val - pt10_val)

# 💡 3. Top 1% 마스터 (미국장 US V7.0 완전판 로직)
def compute_top1_master_signal(df_raw: pd.DataFrame, idx_close: pd.Series):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    
    df['Idx_Close'] = idx_close
    df['Idx_Close'] = df['Idx_Close'].ffill()

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

    with np.errstate(divide='ignore', invalid='ignore'):
        spread_10_20 = np.where(show_values, ((e10 - e20) / atr) * 100, 0)
        spread_10_30 = np.where(show_values, ((e10 - e30) / atr) * 100, 0)
        spread_112_224 = np.where(show_values, ((e112 - e224) / atr) * 100, 0)

        idx = np.arange(len(c))
        r_val = pd.Series(e10).rolling(10).corr(pd.Series(idx)).fillna(0).values
        r_squared = r_val * r_val
        
        e10_3 = np.roll(e10, 3)
        e10_3[:3] = e10[:3]
        ema_roc = np.where(e10_3 != 0, ((e10 - e10_3) / e10_3) * 5000, 0)

    true_momentum_line = np.where(is_aligned_30, ema_roc * (r_squared ** 2), 0)
    prev_tml = np.roll(true_momentum_line, 1)
    prev_tml[0] = 0

    cond_rising = true_momentum_line > prev_tml
    cond_blue_30 = spread_112_224 >= 30
    
    val_angle = true_momentum_line
    cond_highest_angle = (val_angle > spread_10_20) & (val_angle > spread_10_30) & (val_angle > spread_112_224)

    cond_val_sig1 = (spread_10_30 >= 100) & (spread_10_20 >= 50) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle
    cond_val_sig2_3 = (spread_10_30 >= 150) & (spread_10_20 >= 100) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle

    # =========================================================================
    # 👑 [1단계] 4대 핵심 파생 변수 수학적 정의 적용
    # =========================================================================
    cpv = np.where(h != l, (c - o) / (h - l), 0.5)
    cpv_scaled = cpv * 10.0 # 파일의 2.0, 6.0 컷오프 기준에 맞추기 위한 스케일 변환

    v_ma20 = pd.Series(v).rolling(20).mean().values
    vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
    tb_index = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)

    bb_mid = pd.Series(c).rolling(20).mean().values
    bb_std = pd.Series(c).rolling(20).std().values
    bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
    bb_energy = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)

    # 💡 시장 상대강도(RS): (종목 수익률 / 지수 수익률) * 100 나눗셈 비율 수식 적용
    c_20 = pd.Series(c).shift(20).values
    idx_20 = df['Idx_Close'].shift(20).values
    with np.errstate(divide='ignore', invalid='ignore'):
        stock_ret = np.where(c_20 > 0, (c - c_20) / c_20, 0.0)
        idx_ret = np.where(idx_20 > 0, (df['Idx_Close'].values - idx_20) / idx_20, 0.0001)
        # 지수 수익률이 0이 되는 것을 방지하기 위해 0.0001 보정
        idx_ret = np.where(idx_ret == 0, 0.0001, idx_ret)
        rs = (stock_ret / idx_ret) * 100
    rs = np.nan_to_num(rs, nan=0.0)

    # 시그널 판별
    raw_sig1 = is_aligned_112 & cond_val_sig1 & cond_rising
    raw_sig2 = is_aligned_224 & cond_val_sig2_3 & cond_rising
    raw_sig3 = is_aligned_448 & cond_val_sig2_3 & cond_rising
    raw_sig4 = (~is_aligned_112) & (tb_index >= 15.0) & (vol_mult >= 2.0) & is_bullish

    signal_3 = raw_sig3
    signal_2 = raw_sig2 & ~signal_3
    signal_1 = raw_sig1 & ~signal_2 & ~signal_3
    signal_4 = raw_sig4 & ~signal_1 & ~signal_2 & ~signal_3

    moneyOk = (c * v) >= 5_000_000
    priceOk = c >= 3.0

    hit_1 = signal_1 & moneyOk & priceOk # S3 과열권
    hit_2 = signal_2 & moneyOk & priceOk # S2 단기급등
    hit_3 = signal_3 & moneyOk & priceOk # S1 추세
    hit_4 = signal_4 & moneyOk & priceOk # S4 바닥
    
    final_hit = hit_1 | hit_2 | hit_3 | hit_4

    if not final_hit[-1]: return False, "", df, {}
    if final_hit[-4:-1].any(): return False, "", df, {}

    # =========================================================================
    # 👑 [2단계] 매매 빈도 측정 및 이평선 데이터
    # =========================================================================
    recent_hits = final_hit[-252:-1].sum() if len(final_hit) > 252 else final_hit[:-1].sum()
    freq_count = int(recent_hits)

    if is_aligned_448[-1]: ema_stat_str = "승률 31.2% / 손익비 2.50 (수익폭은 크나 고점 설거지 주의)"
    elif is_aligned_224[-1]: ema_stat_str = "승률 31.0% / 손익비 2.52 (장기 매물 소화 완료)"
    elif is_aligned_30[-1]: ema_stat_str = "승률 28.6% / 손익비 2.35 (저항대 돌파 시도)"
    else: ema_stat_str = "승률 28.4% / 손익비 1.94 (찐바닥 탈출 구간으로 승률 1위)"

    # =========================================================================
    # 👑 [3단계] S1~S4 1~10점 컷오프 파일 100% 개별 대입
    # =========================================================================
    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv_scaled[-1], tb_index[-1], bb_energy[-1], rs[-1]
    
    score_cpv, score_tb, score_bbe, score_rs, score_ema, score_freq = 0, 0, 0, 0, 0, 0
    total_score = 0
    trap_warning = ""
    exit_strategy = ""

    if hit_4[-1]: # S4 바닥 탈출
        sig_type = "🔥 S4 (바닥 탈출/역배열 돌파)"
        score_rs   = scale_score(cur_rs, 10.0, 2.0)   # 1위
        score_cpv  = scale_score(cur_cpv, 2.0, 6.0)   # 2위
        score_bbe  = scale_score(cur_bbe, 10.0, 2.0)  # 3위
        score_ema  = 10.0 if not is_aligned_30[-1] else 5.0 # 4위 (역배열/혼조 우대)
        score_tb   = scale_score(cur_tb, 10.0, 2.0)   # 5위
        if 6 <= freq_count <= 15: score_freq = 10.0
        elif freq_count >= 16: score_freq = 2.0
        else: score_freq = 6.0                        # 6위
        
        total_score = (score_rs*10 + score_cpv*9 + score_bbe*8 + score_ema*7 + score_tb*6 + score_freq*5) / 450 * 100
        
        # S4 함정 검증
        if score_tb <= 3.0 and score_bbe <= 3.0: trap_warning += "🚨 [기회비용 늪] 바닥인 척 튀었으나 돈과 에너지 부재!\n"
        if score_cpv <= 3.0 and freq_count >= 16: trap_warning += "💀 [참사의 늪] 세력 단타 놀이터! 즉각 갭하락 주의!\n"
        exit_strategy = "MFE 정점은 평균 16.5일 차. 진입 다음날부터 4.08일 내 시가 갭하락 등 반등 실패 시 즉각 칼손절. 횡보는 10일 후 타임컷."

    elif hit_1[-1]: # S3 과열권 모멘텀
        sig_type = "🔥 S3 (과열권 모멘텀 - 112 정배열)"
        score_cpv  = scale_score(cur_cpv, 2.0, 6.0)   # 1위
        if 1 <= freq_count <= 5: score_freq = 10.0
        elif freq_count >= 16: score_freq = 2.0
        else: score_freq = 6.0                        # 2위
        score_ema  = 10.0 if is_aligned_448[-1] else 5.0 # 3위
        score_tb   = scale_score(cur_tb, 10.0, 2.0)   # 4위
        score_rs   = scale_score(cur_rs, 6.0, 2.0)    # 5위
        score_bbe  = scale_score(cur_bbe, 10.0, 2.0)  # 6위

        total_score = (score_cpv*10 + score_freq*9 + score_ema*8 + score_tb*7 + score_rs*6 + score_bbe*5) / 450 * 100

        # S3 함정 검증
        if 4.0 <= score_cpv <= 6.0 and score_rs <= 3.0: trap_warning += "🚨 [기회비용 늪] 애매한 캔들에 시장 소외주 조합. 횡보 주의!\n"
        if score_cpv <= 3.0: trap_warning += "💀 [참사의 늪] 고점 거래량 터진 꽉 찬 양봉은 100% 설거지 폭락!\n"
        exit_strategy = "MFE 정점은 평균 16.5일 차. 단기데드 로직(트레일링 스탑)으로 전환하여 끝까지 추세 홀딩."

    elif hit_2[-1]: # S2 단기 급등 돌파
        sig_type = "🔥 S2 (단기 급등 돌파 - 224 정배열)"
        score_rs   = scale_score(cur_rs, 10.0, 2.0)   # 1위
        score_cpv  = scale_score(cur_cpv, 2.0, 6.0)   # 2위
        score_ema  = 10.0 if is_aligned_224[-1] else 5.0 # 3위
        score_tb   = scale_score(cur_tb, 10.0, 2.0)   # 4위
        score_freq = 10.0                             # 5위 무관 (가중치 6)
        score_bbe  = scale_score(cur_bbe, 10.0, 2.0)  # 6위

        total_score = (score_rs*10 + score_cpv*9 + score_ema*8 + score_tb*7 + score_freq*6 + score_bbe*5) / 450 * 100

        # S2 함정 검증
        if 4.0 <= score_cpv <= 6.0 and score_rs <= 3.0: trap_warning += "🚨 [기회비용 늪] 애매한 캔들에 시장 소외주 조합. 자본 묶임 주의\n"
        if score_rs <= 3.0: trap_warning += "💀 [참사의 늪] 지수 이기지 못하는 소외 잡주 단독 급등! 갭하락 참사 주의!\n"
        exit_strategy = "MFE 정점은 평균 16.5일 차. 4.08일 내 하락 반전 시 즉각 칼손절. 단기데드 로직으로 끝까지 홀딩."

    else: # hit_3[-1] S1 대세 추세 추종
        sig_type = "🔥 S1 (대세 추세 추종 - 단기정배열 이상)"
        score_rs   = scale_score(cur_rs, 10.0, 2.0)      # 1위
        score_cpv  = scale_score(cur_cpv, 2.0, 6.0)      # 2위
        score_ema  = 10.0 if is_aligned_30[-1] else 1.0  # 3위 (단기정배열 10점, 역배열 1점)
        if 1 <= freq_count <= 5: score_freq = 10.0
        else: score_freq = 5.0                           # 4위
        score_tb   = scale_score(cur_tb, 10.0, 2.0)      # 5위
        score_bbe  = 5.0                                 # 6위 미반영 (가중치 0 처리)

        total_score = (score_rs*10 + score_cpv*9 + score_ema*8 + score_freq*7 + score_tb*6) / 400 * 100

        # S1 함정 검증
        if score_rs <= 3.0: trap_warning += "🚨 [기회비용 늪] 정배열이어도 지수를 못 이겨 박스권 갇힘!\n"
        if not is_aligned_30[-1]: trap_warning += "💀 [참사의 늪] 단기 추세조차 없는 역배열/혼조 구간 진입!\n"
        exit_strategy = "MFE 정점은 평균 16.5일 차. ZLEMA 이탈 로직으로 추세가 깨질 때까지 장기 홀딩."

    # =========================================================================
    # 👑 [4단계] 데스콤보, 고빈도 필터, 텐배거, DNA(Top/Worst) 검증
    # =========================================================================
    weekday = df.index[-1].weekday()
    if weekday == 4: total_score *= 1.05 # 금요일 가산
    elif weekday == 0: total_score *= 0.95 # 월요일 차감

    # 1. 미국장 공통 데스콤보
    is_death_combo = (score_cpv <= 2.0) and (score_rs <= 3.0)
    if is_death_combo: 
        total_score *= 0.70
        trap_warning += "⚠️ [데스 콤보 발동] 거래량 없이 억지로 만든 꽉 찬 양봉+소외주 (점수 30% 삭감)\n"
        
    # 2. 고빈도 잡주 강제 필터링
    if freq_count >= 16 and (score_rs < 8.0 or score_cpv < 8.0):
        total_score *= 0.50
        trap_warning += "🚫 [고빈도 알고리즘 놀이터] RS/CPV 기준 미달! 강제 패스 권장 (-50% 삭감)\n"

    # 3. 늪/참사 함정 발생 시 점수 삭감 페널티 (중복 삭감 방지)
    if trap_warning != "" and not is_death_combo and "고빈도" not in trap_warning: 
        total_score *= 0.70 

    # 4. 미국장 초격차 텐배거 조건 (MFE > 50%)
    is_tenbagger = False
    if hit_4[-1] and score_rs >= 8.0 and score_cpv >= 8.0 and not is_aligned_30[-1]: is_tenbagger = True
    if hit_1[-1] and score_cpv >= 8.0 and is_aligned_448[-1]: is_tenbagger = True
    if hit_2[-1] and score_rs >= 8.0 and is_aligned_224[-1]: is_tenbagger = True
    if hit_3[-1] and score_rs >= 8.0 and is_aligned_30[-1]: is_tenbagger = True

    # 5. 미국장 DNA 팩트 검증
    is_top_dna = (score_cpv >= 4.27) and (score_tb >= 2.67) and (score_rs >= 7.87) and (score_bbe >= 2.93)
    is_worst_dna = (score_cpv <= 2.00)

    total_score = min(max(total_score, 0), 100) # 0~100점 보정

    v7_comment = (
        f"📊 [System B US V7.0 종합 진단 리포트]\n"
        f"🔹 시스템 총점: {total_score:.1f} / 100점\n\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.2f} ({score_cpv:.1f}점) -> 위꼬리 매물 소화\n"
        f"▪️ 진짜양봉지수: {cur_tb:.1f} ({score_tb:.1f}점) -> 돈의 매집 밀도\n"
        f"▪️ 응축에너지: {cur_bbe:.1f} ({score_bbe:.1f}점) -> 볼린저 바닥 탄성\n"
        f"▪️ 시장상대강도: {cur_rs:.1f}% ({score_rs:.1f}점) -> 지수 대비 주도력\n"
        f"▪️ 과거 매매빈도: {freq_count}회 ({score_freq:.1f}점)\n"
        f"▪️ 이평선국면점수: {score_ema:.1f}점\n\n"
        f"💡 [이평선 국면 팩트 데이터]\n{ema_stat_str}\n"
    )
    
    if trap_warning != "": v7_comment += f"\n{trap_warning}"
    if is_top_dna: v7_comment += f"\n💎 [미국장 승리 DNA 검증] 승률 100% 종목 평균 돌파!\n"
    elif is_worst_dna: v7_comment += f"\n💀 [Worst 30 지옥행 DNA 일치] 극단적 꽉찬양봉! 잦은 손절 패턴입니다.\n"
    if is_tenbagger: v7_comment += f"\n🚀 [초격차 텐배거 포착] 계좌 퀀텀점프 대박주 필수 조합 충족!\n"
    if weekday == 4: v7_comment += f"✨ 금요일 주말 리스크를 이겨낸 주도주 프리미엄\n"
    elif weekday == 0: v7_comment += f"⚠️ 월요일 호재를 이용한 고점 털기 리스크\n"

    # 타점이 맞으면 100% 무조건 반환
    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": f"{exit_strategy}",
        "v7_comment": v7_comment
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
    print(f"\n🇺🇸 [일봉 전용] 미국장 Top 1% 마스터 스캔 시작! (US V7.0 무타협 엔진 🛡️)")

    print("📊 벤치마크 지수(QQQ ETF 대용) 데이터 안전하게 로드 중...")
    try:
        idx_df = yf.download("QQQ", interval="1d", period="3y", progress=False, threads=False)
        if not idx_df.empty:
            kospi_idx = idx_df['Close']['QQQ'] if isinstance(idx_df.columns, pd.MultiIndex) else idx_df['Close']
            if kospi_idx.index.tzinfo is not None: 
                kospi_idx.index = kospi_idx.index.tz_convert('America/New_York').tz_localize(None)
            kospi_idx = kospi_idx[~kospi_idx.index.duplicated(keep='last')]
        else:
            kospi_idx = pd.Series(dtype=float)
    except:
        print("⚠️ 벤치마크 지수 로드 실패. 빈 데이터로 우회합니다.")
        kospi_idx = pd.Series(dtype=float)

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
                    # 시그널 판별 (100% 무조건 반환)
                    hit, sig_type, df, dbg = compute_top1_master_signal(df_ticker, kospi_idx)
                    
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
                            
                            main_caption = (
                                f"🎯 [{dbg.get('sig_type', '')}]\n"
                                f"🎯 추천: 스윙, 중장기 / 종가배팅\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                                f"{dbg.get('v7_comment', '')}\n"
                                f"📉 [스마트 매수/손절 전략]\n"
                                f"- {dbg.get('recommend', '')}\n\n"
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
