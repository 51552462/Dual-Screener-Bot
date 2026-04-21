# Dante_Nulrim_1D_LS_AI_Pro_DualBot.py
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

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리)
TELEGRAM_TOKEN_MAIN  = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_TOKEN_PROMO = "7996581031:AAFou3HWYhIXzRtlW4ildx8tOitcQBVubPg"
TELEGRAM_CHAT_ID     = "6838834566"
SEND_TELEGRAM        = True

q_main = queue.Queue()
q_promo = queue.Queue()

sent_today = set()
last_run_date = ""

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Nulrim_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

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

MIN_PRICE = 1000                  
MIN_TRANS_MONEY = 100_000_000  

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

# 💡 [교체] V7.0 마스터 시그널 엔진 (55,571건 팩트 데이터 완벽 적용)
def compute_signal(df_raw: pd.DataFrame, idx_close: pd.Series): 
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
    # 👑 [1단계] 4대 핵심 변수 수식 (V7.0)
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
    # 👑 [2단계] 눌림목 타점 발생 로직 (S1, S4, S6, S7 4가지만 명확히 분리 포착)
    # =========================================================================
    moneyOk = (c * v) >= 100_000_000
    priceOk = c >= 1000
    isBullish = c > o

    align112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    align224 = align112 & (e112 > e224)
    align448 = align224 & (e224 > e448)

    longKeep448 = e224 > e448 
    longKeep112 = e60 > e112

    prev_align448 = np.roll(align448, 1); prev_align448[0] = False
    prev_align112 = np.roll(align112, 1); prev_align112[0] = False

    prev_longKeep448 = np.roll(longKeep448, 1); prev_longKeep448[0] = False
    prev_longKeep112 = np.roll(longKeep112, 1); prev_longKeep112[0] = False

    # 1. S1 (448일 완전정배열 재정렬)
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    
    # 2. S4 (20일선 눌림돌파)
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    
    s4 = np.zeros_like(c, dtype=bool)
    last_pb = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pb > 5):
            s4[i] = True
            last_pb = i

    # 3. S6 (바닥 탈출 단기 정배열)
    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1); prev_shortBull[0] = False
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish

    # 4. S7 (112 중기 정배열 턴)
    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    cond_base = moneyOk & priceOk

    # 💡 [버그 픽스] Pandas 인덱싱 에러(KeyError) 방지를 위해 Numpy 배열로 값 추출
    cond_base_arr = cond_base.values if isinstance(cond_base, pd.Series) else cond_base
    s1_arr = s1.values if isinstance(s1, pd.Series) else s1
    s4_arr = s4.values if isinstance(s4, pd.Series) else s4
    s6_arr = s6.values if isinstance(s6, pd.Series) else s6
    s7_arr = s7.values if isinstance(s7, pd.Series) else s7

    # 💡 S2, S3 배제! 오직 S1, S4, S6, S7만 명시적 분리 포착
    hit_s1 = s1_arr[-1] and cond_base_arr[-1]
    hit_s4 = s4_arr[-1] and cond_base_arr[-1]
    hit_s6 = s6_arr[-1] and cond_base_arr[-1]
    hit_s7 = s7_arr[-1] and cond_base_arr[-1]

    if not (hit_s1 or hit_s4 or hit_s6 or hit_s7): 
        return False, "", df, {}

    # =========================================================================
    # 👑 [3단계] S1, S4, S6, S7 스코어링 매핑 (V8.0 최신 팩트 데이터 완벽 대입)
    # =========================================================================
    recent_hits = (s1 | s4 | s6 | s7)[-252:-1].sum() if len(c) > 252 else (s1 | s4 | s6 | s7)[:-1].sum()
    freq_count = int(recent_hits)

    if align448[-1]: ema_stat_str = "승률 28.3% / 손익비 3.27 (대세 상승장, 승률 1위)"
    elif align112[-1]: ema_stat_str = "승률 25.1% / 손익비 3.20 (저항대 돌파 시도)"
    else: ema_stat_str = "승률 25.1% / 손익비 3.02 (바닥 탈출 구간)"

    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    score_cpv, score_tb, score_bbe, score_rs, score_ema, score_freq = 0, 0, 0, 0, 0, 0
    total_score = 0
    trap_warning = ""

    # 💡 [버그 픽스] 삭제되었던 S6, S7 로직 완벽 복구
    if hit_s6: 
        sig_type = "🌱 [눌림] S6 (바닥턴 단기 정배열)"
        score_rs   = scale_score(cur_rs, 770.60, -65.50)   
        score_tb   = scale_score(cur_tb, 24.60, 0.90)      
        score_cpv  = scale_score(cur_cpv, 0.14, 0.83)      
        score_bbe  = scale_score(cur_bbe, 53.40, 1.80)     
        score_ema  = 10.0 if not align112[-1] else 5.0     
        if 6 <= freq_count <= 15: score_freq = 10.0
        elif freq_count >= 30: score_freq = 2.0 
        else: score_freq = 6.0                      
        
        total_score = (score_rs*10 + score_tb*9 + score_cpv*8 + score_bbe*7 + score_ema*6 + score_freq*5) / 450 * 100
        
        if cur_tb < 0.90 and cur_bbe < 1.80: trap_warning += "🚨 [기회비용 늪] 바닥인 척 튀었으나 돈과 에너지가 없음!\n"
        if cur_cpv > 0.83 and freq_count >= 30: trap_warning += "💀 [참사의 늪] 세력 단타 놀이터! 즉각 갭하락 지옥행 주의!\n"

    elif hit_s7: 
        sig_type = "🔥 [눌림] S7 (112 중기 정배열 턴)"
        score_cpv  = scale_score(cur_cpv, 0.14, 0.86)      
        if 1 <= freq_count <= 5: score_freq = 10.0
        elif freq_count >= 30: score_freq = 2.0
        else: score_freq = 6.0  
        score_bbe  = scale_score(cur_bbe, 44.70, 1.30)     
        score_tb   = scale_score(cur_tb, 19.00, 0.90)      
        score_rs   = scale_score(cur_rs, 1189.53, -358.20) 
        score_ema  = 10.0 if align112[-1] else 5.0 

        total_score = (score_cpv*10 + score_freq*9 + score_bbe*8 + score_tb*7 + score_rs*6 + score_ema*5) / 450 * 100

        if cur_cpv > 0.85 and cur_rs < -358.20: trap_warning += "🚨 [기회비용 늪] 애매한 캔들에 시장 소외주. 자본 묶임 주의!\n"
        if cur_cpv > 0.86: trap_warning += "💀 [참사의 늪] 고점에서 거래량 터진 꽉 찬 양봉은 100% 설거지 폭락!\n"

    elif hit_s1 or hit_s4: 
        sig_type = "🔥 [눌림] S4 (바닥 탈출 텐배거 로또 타점)" if hit_s4 else "🔥 [눌림] S1 (448 대세 추세)"
        score_rs   = scale_score(cur_rs, 1563.0, -745.10) 
        score_ema  = 10.0 if align448[-1] else 5.0    
        score_cpv  = scale_score(cur_cpv, 0.23, 0.85)      
        score_bbe  = scale_score(cur_bbe, 5400.0, 10.0)    
        score_tb   = scale_score(cur_tb, 20.0, 5.0)      
        score_freq = 10.0 if 1 <= freq_count <= 5 else 5.0 

        total_score = (score_rs*10 + score_ema*9 + score_cpv*8 + score_bbe*7 + score_tb*6 + score_freq*5) / 450 * 100

        if hit_s4: trap_warning += "⚠️ [S4 바닥 탈출] 승률 방어를 위해 반드시 서브 비중으로만 접근 요망!\n"
        if cur_rs < -745.10: trap_warning += "🚨 [기회비용 늪] 정배열이어도 지수를 이기지 못해 박스권 갇힘!\n"
        if not align112[-1]: trap_warning += "💀 [참사의 늪] 장기 추세가 없는 역배열/혼조 구간 진입 페이크 상승!\n"

    # =========================================================================
    # 👑 [4단계] 한국장 V8.0 디테일: 요일 효과, 데스콤보, 뱃지 시스템
    # =========================================================================
    weekday = df.index[-1].weekday()
    if weekday == 4: total_score *= 1.05 # 금요일 가산
    elif weekday == 0: total_score *= 0.95 # 월요일 삭감

    is_death_combo = (cur_cpv > 0.85) and (cur_rs < 0)
    if is_death_combo: 
        total_score *= 0.70
        trap_warning += "⚠️ [데스 콤보 발동] 거래량 없이 만든 가짜 양봉 + 시장 소외주 (점수 30% 삭감)\n"

    total_score = min(max(total_score, 0), 100) # 0~100점 보정
    
    # 💡 [버그 픽스] 누락되었던 텐배거 조건 및 DNA 검증 로직 복구
    is_tenbagger = False
    if hit_s6 and cur_rs >= 207.60 and cur_cpv <= 0.46 and (not align112[-1]): is_tenbagger = True
    if hit_s7 and cur_rs >= 293.80 and cur_cpv <= 0.56 and align112[-1]: is_tenbagger = True
    if hit_s1 and cur_rs >= 239.30 and cur_cpv <= 0.55 and align448[-1]: is_tenbagger = True

    is_top_dna = (cur_cpv <= 0.56) and (cur_tb >= 10.83) and (cur_bbe >= 16.12)
    is_worst_dna = (cur_cpv >= 0.56) and (cur_tb <= 10.36) and (cur_bbe <= 5.20) 

    # =========================================================================
    # 👑 [종목 맞춤형 동적 청산 전략 (스마트 매수/손절)]
    # =========================================================================
    if cur_cpv >= 0.70:
        cpv_stat = f"현재 꽉 찬 양봉 (CPV {cur_cpv:.2f})"
        action = "💡 [한국장 특급 주의] 세력 특성상, 진입 직후 상대강도(RS)가 비정상적으로 급등하며 꽉 찬 양봉을 유지한다면 100% 설거지 꼬시기 패턴입니다. 조금이라도 꺾이면 'ZLEMA 이탈' 시 즉각 칼손절하십시오."
    elif cur_cpv <= 0.40:
        cpv_stat = f"꼬리가 길게 달린 매물 소화 캔들 (CPV {cur_cpv:.2f})"
        action = f"세력이 개미를 흔들면서 올라가는 진짜 대장주 패턴입니다. 휩소에 털리지 말고 '단기데드(EMA 20)' 이탈 전까지 추세를 끝까지 발라먹으십시오."
    else:
        cpv_stat = f"표준적인 캔들 (CPV {cur_cpv:.2f})"
        action = f"상승 시 '단기데드'로 수익 극대화, 하락 시 'ZLEMA 이탈'로 짧게 끊어내는 기계적 대응을 권장합니다."

    if hit_s4 and cur_rs <= -1000:
        tier_stat = f"💡 [특급 로또 타점] 현재 완벽한 소외주(RS {cur_rs:.1f}) 바닥권입니다. 진입 직후 거래량이 폭발하면 손익비 4.0~5.0 이상 터지는 텐배거 자리입니다. 로또용 소액 접근 요망."
    elif total_score >= 80:
        tier_stat = f"총점 {total_score:.1f}점(1티어)으로 방어력이 수학적으로 완벽히 입증되었습니다. 메인 비중 진입을 권장합니다."
    else:
        tier_stat = f"총점 {total_score:.1f}점 하위권 타점입니다. 가짜 돌파 휩소 리스크를 피하기 위해 반드시 비중을 축소하십시오."

    exit_strategy = f"[{cpv_stat}]\n{action}\n\n{tier_stat}"

    badge_str = ""
    if total_score >= 80.0:
        badge_str = "🔥 [1티어 뱃지] 가산점 부여 대상 (대박 비율 상승 / 최우선 매매)"
        sig_type = "👑 [1티어] " + sig_type
    elif total_score <= 50.0 and cur_rs <= -1000 and cur_cpv <= 0.3:
        badge_str = "💎 [특급 모멘텀 예외] 점수 무시 텐배거 (매물 소화 완벽, 극단적 소외주 돌발 펌핑)"
        sig_type = "💎 [로또] " + sig_type
    else:
        badge_str = "⚠️ [비중 축소] 80점 미만은 가짜 휩소 확률이 높으므로 철저히 비중 축소 요망"

    v8_comment = (
        f"📊 [System B 한국 눌림목 V8.0 마스터 리포트]\n"
        f"🔹 시스템 총점: {total_score:.1f} / 100점\n"
        f"🎖️ {badge_str}\n\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.2f} ({score_cpv:.1f}점)\n"
        f"▪️ 진짜양봉지수: {cur_tb:.1f} ({score_tb:.1f}점)\n"
        f"▪️ 응축에너지: {cur_bbe:.1f} ({score_bbe:.1f}점)\n"
        f"▪️ 시장상대강도: {cur_rs:.1f}% ({score_rs:.1f}점)\n"
        f"▪️ 과거 매매빈도: {freq_count}회 ({score_freq:.1f}점)\n"
        f"▪️ 이평선국면점수: {score_ema:.1f}점\n\n"
        f"💡 [이평선 국면 팩트 데이터]\n{ema_stat_str}\n"
    )
    
    if trap_warning != "": v8_comment += f"\n{trap_warning}"
    if is_top_dna: v8_comment += f"\n💎 [Top 30 우량 DNA 검증 완료] 대박 확률 대폭 상승!\n"
    elif is_worst_dna: v8_comment += f"\n💀 [Worst 30 지옥행 DNA 일치] 꼬리는 달렸으나 에너지가 죽은 종목. 진입 주의!\n"
    if is_tenbagger: v8_comment += f"\n🚀 [초격차 텐배거 포착] 대세 상승 퀀텀점프 필수 조합 충족!\n"
    if weekday == 4: v8_comment += f"✨ 금요일 주말 리스크를 이겨낸 진짜 주도주 프리미엄 (+5% 가산)\n"
    elif weekday == 0: v8_comment += f"⚠️ 월요일 주말 호재 고점 털기 리스크 반영 (-5% 삭감)\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": f"{exit_strategy}",
        "v8_comment": v8_comment,        
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
    
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_nulrim.txt")

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

    print(f"\n⚡ [일봉 전용] 한국장 V(눌림목) 스캔 시작!\n(당일 중복 차단 🛡️)")
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()  # 💡 <--- 이 줄이 빠져있을 겁니다. 여기에 꼭 추가해 주세요!
    
    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    
    # 💡 [추가] 벤치마크 지수(KODEX ETF 대용) 로드 (상대강도 계산용)
    print("📊 벤치마크 지수 데이터 안전하게 로드 중...")
    try:
        kospi_idx = fdr.DataReader('069500', start_date)['Close']
        kosdaq_idx = fdr.DataReader('229200', start_date)['Close']
    except:
        kospi_idx, kosdaq_idx = pd.Series(dtype=float), pd.Series(dtype=float)

    def worker(row_tuple):
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
                # 💡 [변경] 시장에 맞는 지수를 같이 넘겨줌
                idx_close = kospi_idx if row["Market"] == 'KOSPI' else kosdaq_idx
                hit, sig_type, df, dbg = compute_signal(df_raw, idx_close)
        except Exception:
            pass

        hit_rank = 0
        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")
            if hit:
                # 💡 중복 발송 철통 방어
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
            # 1️⃣ 본캐용 / 홍보용 차트 생성
            main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
            promo_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
            
            if main_chart_path and promo_chart_path:
                ai_main, _ = generate_ai_report(code, name) 
                        
                # 1️⃣ 본캐용 캡션 (유료방용 - 동적 전략 및 V8.0 브리핑 출력)
                main_caption = (
                    f"🎯 [{dbg.get('sig_type', '')}]\n"
                    f"🎯 추천: 단타, 스윙 / 종가배팅\n\n"
                    f"🏢 {name} ({code})\n"
                    f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                    f"{dbg.get('v8_comment', '')}\n"  # 💡 V8.0 데이터 정상 로드
                    f"📉 [스마트 매수/청산 전략]\n"
                    f"{dbg.get('recommend', '')}\n\n" # 💡 종목 맞춤형 전략 정상 로드
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
                # 2️⃣ 홍보용 캡션 (쓸데없는 멘트 다 빼고 초심플 압축)
                try:
                    sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                except:
                    sector_info = "유망 섹터 포착"

                # ⭐️ 멘트 싹 날리고 [차트+종목+섹터+현재가]만!
                promo_caption = (
                    f"📈 [알고리즘 차트 포착]\n\n"
                    f"🏢 종목: {name} ({code})\n"
                    f"🏷️ 섹터: {sector_info}\n"
                    f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                )
                q_promo.put((promo_chart_path, promo_caption))
                
                print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")

    # 💡 5. 일꾼들(스레드) 가동
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [한국장 5번 V 눌림목 스캔 완료] 신규 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [5번 검색기] 10:30 / 13:30 / 15:10 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 10 and now_kr.minute == 30) or (now_kr.hour == 13 and now_kr.minute == 30) or (now_kr.hour == 15 and now_kr.minute == 10):
            print(f"🚀 [5번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    # run_scheduler()  <-- 이 줄을 주석 처리하거나 지우고
    scan_market_1d()   # ⭐️ 이 문구를 추가하면 즉시 1회 스캔이 시작됩니다.
