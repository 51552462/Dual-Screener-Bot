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

# 💡 3. 잡주 필터
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

# 💡 4. Top 1% 마스터 (System B V7.0 한국장 무타협 완전판 엔진)
def compute_top1_master_signal(df_raw: pd.DataFrame, idx_close: pd.Series):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    
    df['Idx_Close'] = idx_close
    df['Idx_Close'] = df['Idx_Close'].ffill()

    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
        
    e10, e20, e30, e60 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values
    e112, e224, e448 = df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    atr = pd.Series(tr).ewm(alpha=1/20, adjust=False, min_periods=0).mean().values

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
    # 👑 [1단계] 4대 핵심 변수 수식
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

    # ---------------------------------------------------------
    # 💡 [S4 하이브리드 엔진] 트레이딩뷰 V반등각도 100% 동기화
    # ---------------------------------------------------------
    c_3 = np.roll(c, 3)
    c_3[:3] = c[:3]
    candle_roc = np.where(c_3 != 0, ((c - c_3) / c_3) * 1000, 0)
    
    weights = np.array([1, 2, 3])
    candle_angle = np.zeros(len(c))
    for i in range(2, len(c)):
        candle_angle[i] = (candle_roc[i-2]*weights[0] + candle_roc[i-1]*weights[1] + candle_roc[i]*weights[2]) / 6.0
    candle_angle = np.where(is_aligned_30, candle_angle, 0)
    
    raw_sig4_arr = np.zeros(len(c), dtype=bool)
    is_candle_bottom = False
    for i in range(len(c)):
        if candle_angle[i] <= 0:
            is_candle_bottom = True
        
        # 🚨 [버그 픽스 완료] 너무 빡빡했던 과거 좀비 필터 제거, 트뷰 원본 로직 완벽 복원
        if is_candle_bottom and candle_angle[i] >= 50 and is_aligned_30[i] and is_bullish[i]:
            raw_sig4_arr[i] = True
            is_candle_bottom = False

    # =========================================================================
    # 👑 [2단계] 트뷰 원본 시그널 확정 및 상호 배제 로직 (100% 동일 매핑)
    # =========================================================================
    # 1. 트레이딩뷰 원본 그대로 raw_sig 계산 (112, 224, 448, V반등)
    tv_raw_sig1 = is_aligned_112 & cond_val_sig1 & cond_rising
    tv_raw_sig2 = is_aligned_224 & cond_val_sig2_3 & cond_rising
    tv_raw_sig3 = is_aligned_448 & cond_val_sig2_3 & cond_rising
    tv_raw_sig4 = raw_sig4_arr 

    # 2. 트레이딩뷰 상호 배제 로직 (겹침 방지: S3 > S2 > S1 > S4)
    tv_signal_3 = tv_raw_sig3
    tv_signal_2 = tv_raw_sig2 & ~tv_signal_3
    tv_signal_1 = tv_raw_sig1 & ~tv_signal_2 & ~tv_signal_3
    tv_signal_4 = tv_raw_sig4 & ~tv_signal_1 & ~tv_signal_2 & ~tv_signal_3

    moneyOk = (c * v) >= 100_000_000
    priceOk = c >= 1000

    # 3. 파이썬 직관적 변수명 매핑 및 컷오프 (Numpy 최적화 및 에러 완벽 차단)
    # 💡 [팩트체크]: 트뷰의 signal_3(448선) = 기획서의 S1(대세추세) 이므로 연결합니다!
    hit_1 = np.zeros(len(c), dtype=bool) # S3(112선) 참사 휩소 방지용 강제 차단
    hit_2 = np.zeros(len(c), dtype=bool) # S2(224선) 참사 휩소 방지용 강제 차단
    hit_3 = tv_signal_3 & moneyOk & priceOk  # S1 대세추세 (트뷰 signal_3 맵핑)
    hit_4 = tv_signal_4 & moneyOk & priceOk  # S4 바닥탈출 (트뷰 signal_4 맵핑)

    final_hit = hit_1 | hit_2 | hit_3 | hit_4

    if not final_hit[-1]: return False, "", df, {}
    if np.any(final_hit[-4:-1]): return False, "", df, {}

    # =========================================================================
    # 👑 [3단계] S1, S4 스코어링 매핑 (V8.0 기준)
    # =========================================================================
    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    
    score_cpv, score_tb, score_bbe, score_rs, score_ema, score_freq = 0, 0, 0, 0, 0, 0
    total_score = 0
    trap_warning = ""
    
    # 💡 [V8.0 청산 전략 가이드 (진입 이후 캔들 흐름 대응)]
    exit_strategy = "MFE 정점(평균 7.39일). 예쁜 양봉(CPV 0.36 이상) 연속 출현 시 한국형 설거지이므로 3일 내 ZLEMA 즉각 칼손절. 지저분한 꼬리(CPV 0.23 부근) 달며 오르면 진짜 대장주이므로 단기데드로 끝까지 추세 홀딩."

    if hit_4[-1]: # [S4 바닥 탈출]
        sig_type = "🔥 [돌파] S4 (이평 바닥 탈출/역배열 돌파)"
        score_bbe  = scale_score(cur_bbe, 90.70, 11.60) 
        score_tb   = scale_score(cur_tb, 61.80, 16.20)  
        score_ema  = 10.0 if not is_aligned_112[-1] else 5.0 
        score_cpv  = scale_score(cur_cpv, 0.10, 0.89)   
        if 6 <= freq_count <= 15: score_freq = 10.0
        elif freq_count >= 16: score_freq = 2.0 
        else: score_freq = 6.0                          
        score_rs   = scale_score(cur_rs, 1510.50, 0.0)  
        
        total_score = (score_bbe*10 + score_tb*9 + score_ema*8 + score_cpv*7 + score_freq*6 + score_rs*5) / 450 * 100
        
        if cur_tb < 16.20 and cur_bbe < 11.60: trap_warning += "🚨 [기회비용 늪] 바닥인 척 튀었으나 돈과 에너지가 없음!\n"
        if cur_cpv > 0.89 and freq_count >= 16: trap_warning += "💀 [참사의 늪] 세력 단타 놀이터! 즉각 갭하락 지옥행 주의!\n"

    elif hit_3[-1]: # [S1 대세 추세 추종]
        sig_type = "🔥 [돌파] S1 (448 완전정배열 돌파)"
        score_ema  = 10.0 if is_aligned_448[-1] else 1.0 
        score_rs   = scale_score(cur_rs, 4037.80, 0.0)   
        if 1 <= freq_count <= 5: score_freq = 10.0
        else: score_freq = 5.0                           
        score_cpv  = scale_score(cur_cpv, 0.15, 0.89)    
        score_tb   = scale_score(cur_tb, 15.00, 0.0)     
        total_score = (score_ema*10 + score_rs*9 + score_freq*8 + score_cpv*7 + score_tb*6) / 400 * 100

        if cur_rs < -1934.70: trap_warning += "🚨 [기회비용 늪] 정배열이어도 지수를 이기지 못해 박스권 갇힘!\n"
        if not is_aligned_112[-1]: trap_warning += "💀 [참사의 늪] 장기 추세가 없는 역배열/혼조 구간 진입 페이크 상승!\n"

    # =========================================================================
    # 👑 [4단계] V8.0 디테일: 요일 효과, 데스콤보, 뱃지 시스템
    # =========================================================================
    weekday = df.index[-1].weekday()
    if weekday == 4: total_score *= 1.05 # 금요일 가산
    elif weekday == 0: total_score *= 0.95 # 월요일 차감

    # 💡 [V8.0 데스 콤보 방어]
    is_death_combo = (cur_cpv > 0.85) and (cur_rs < 0)
    if is_death_combo: 
        total_score *= 0.70
        trap_warning += "⚠️ [데스 콤보 발동] 거래량 없이 만든 가짜 양봉 + 소외주 (점수 30% 삭감)\n"

    total_score = min(max(total_score, 0), 100)

    # =========================================================================
    # 👑 [종목 맞춤형 동적 청산 전략 (스마트 매수/손절)]
    # 기획서 특급비밀3 (한국형 RS 급등 설거지 패턴) 및 극단적 소외주 로또 타점 완벽 반영
    # =========================================================================
    # 1. 캔들(CPV) 및 한국장 특유의 RS 델타 설거지 패턴 경고
    if cur_cpv >= 0.70:
        cpv_stat = f"현재 꽉 찬 양봉 (CPV {cur_cpv:.2f})"
        action = "💡 [한국장 특급 주의] 세력 특성상, 진입 이후 꽉 찬 양봉을 유지한 채 상대강도(RS)가 300점 이상 비정상적으로 급등한다면 100% 설거지 꼬시기 패턴입니다. 조금이라도 꺾이면 'ZLEMA 이탈' 시 3일 내 즉각 칼손절하여 계좌를 방어하십시오."
    elif cur_cpv <= 0.40:
        cpv_stat = f"꼬리가 길게 달린 매물 소화 캔들 (CPV {cur_cpv:.2f})"
        action = f"세력이 단타 개미를 흔들면서 올라가는 진짜 대장주 패턴입니다. 가짜 휩소에 털리지 말고 '단기데드(EMA 20)' 이탈 전까지 1~2주간 추세를 끝까지 발라먹으십시오."
    else:
        cpv_stat = f"표준적인 캔들 (CPV {cur_cpv:.2f})"
        action = f"상승 시 '단기데드'로 수익 극대화, 하락 시 'ZLEMA 이탈'로 짧게 끊어내는 기계적 대응을 권장합니다."

    # 2. 점수 티어 및 S4 극단적 소외주(로또) 타점 판별
    if hit_4[-1] and cur_rs <= -1000:
        tier_stat = f"💡 [특급 로또 타점] 현재 완벽한 소외주(RS {cur_rs:.1f}) 바닥권입니다. 평소 승률은 낮지만, 진입 직후 비정상적인 거래량 폭발이 동반되면 손익비 4.0~5.0 이상 터지는 텐배거 자리입니다. 비중을 대폭 줄여 로또용으로만 접근하십시오."
    elif total_score >= 80:
        tier_stat = f"총점 {total_score:.1f}점(1티어)으로 방어력이 수학적으로 완벽히 입증되었습니다. 메인 비중 진입을 권장합니다."
    else:
        tier_stat = f"총점 {total_score:.1f}점의 하위권 타점입니다. 한국형 가짜 돌파 휩소 리스크를 피하기 위해 반드시 비중을 대폭 축소하십시오."

    exit_strategy = f"[{cpv_stat}]\n{action}\n\n{tier_stat}"

    # 💡 [V8.0 뱃지 및 특급 예외 시스템 로직]
    badge_str = ""
    if total_score >= 80.0:
        badge_str = "🔥 [1티어 뱃지] 가산점 부여 대상 (대박 비율 84~85% / 참사 2~3% 최우선 매매)"
        sig_type = "👑 [1티어] " + sig_type
    elif total_score <= 50.0 and cur_rs <= -1000 and cur_cpv <= 0.3:
        badge_str = "💎 [특급 모멘텀 예외] 점수 무시 텐배거 (매물 소화 완벽, 극단적 소외주 돌발 펌핑. 소액 로또 접근)"
        sig_type = "💎 [로또] " + sig_type
    else:
        badge_str = "⚠️ [비중 축소] 80점 미만은 가짜 휩소 확률이 높으므로 철저히 비중 축소 요망"

    v8_comment = (
        f"📊 [System B 한국 이평선 돌파 V8.0 마스터 리포트]\n"
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
    if weekday == 4: v8_comment += f"✨ 금요일 주말 리스크를 이겨낸 주도주 프리미엄 (+5% 가산)\n"
    elif weekday == 0: v8_comment += f"⚠️ 월요일 고점 털기 리스크 반영 (-5% 삭감)\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": f"{exit_strategy}", # 👈 종목 맞춤형 동적 전략 저장!
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
            name, code, market_type = row["Name"], row["Code"], row["Market"]
            
            df_raw = fdr.DataReader(code, start_date)
            hit = False
            df_to_plot = None
            dbg_info = {}

            if df_raw is not None and not df_raw.empty and len(df_raw) >= 500:
                df_raw = df_raw[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                idx_close = kospi_idx if market_type == 'KOSPI' else kosdaq_idx
                
                # 시그널 판별 엔진 호출 (모든 시그널 통과 및 점수화)
                hit, sig_type, df_to_plot, dbg_info = compute_top1_master_signal(df_raw, idx_close)

            hit_rank = 0
            with console_lock:
                tracker['scanned'] += 1
                if df_raw is not None and len(df_raw) >= 500: tracker['analyzed'] += 1 
 
                if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                    print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

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
                main_chart_path = save_chart(df_to_plot, code, name, hit_rank, dbg_info, show_volume=True, is_promo=False)
                promo_chart_path = save_chart(df_to_plot, code, name, hit_rank, dbg_info, show_volume=False, is_promo=True)
                
                if main_chart_path and promo_chart_path:
                    ai_main, _ = generate_ai_report(code, name)
                    
                    # 1️⃣ 본캐용 캡션 (유료방용 - 동적 전략 및 V8.0 브리핑 출력)
                    main_caption = (
                        f"🎯 [{dbg_info.get('sig_type', '')}]\n"
                        f"🎯 추천: 단타, 스윙 / 종가배팅\n\n"
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg_info.get('last_close', 0):,.0f}원\n\n"
                        f"{dbg_info.get('v8_comment', '')}\n"
                        f"📉 [스마트 매수/청산 전략]\n"
                        f"{dbg_info.get('recommend', '')}\n\n"  # 💡 종목 맞춤형 전략 송출!
                        f"💡 [AI 비즈니스 요약]\n"
                        f"{ai_main}\n\n"
                        f"⚠️ [면책 조항]\n"
                        f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 매수/매도 권유가 아닙니다."
                    )
                    q_main.put((main_chart_path, main_caption))

                    # 💡 [오토 포워드 테스팅 시스템 변수 에러 픽스]
                    try:
                        import auto_forward_tester as aft
                        
                        market_type = 'KR' 
                        entry_facts = {
                            'v_cpv': dbg_info.get('v_cpv', 0),
                            'v_yang': dbg_info.get('v_yang', 0),
                            'v_energy': dbg_info.get('v_energy', 0),
                            'v_rs': dbg_info.get('v_rs', 0)
                        }
                        
                        success, fwd_msg = aft.try_add_virtual_position(
                            market=market_type,
                            code=code,
                            name=name,
                            sig_type=dbg_info.get('sig_type', ''),
                            score=dbg_info.get('score', 0), 
                            ep=dbg_info.get('last_close', c[-1]),
                            facts=entry_facts
                        )
                        print(f"   ↳ [포워드 장부 기록]: {fwd_msg}")
                    except Exception as e:
                        print(f"   ↳ [포워드 장부 에러]: {e}")
                        
                    try:
                        sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                    except:
                        sector_info = "유망 섹터 포착"
                            
                    promo_caption = (
                        f"📈 [Top 1% 마스터 알고리즘 포착]\n\n"
                        f"🏢 종목: {name} ({code})\n"
                        f"🏷️ 섹터: {sector_info}\n"
                        f"💰 현재가: {dbg_info.get('last_close', 0):,.0f}원"
                    )
                    q_promo.put((promo_chart_path, promo_caption))

                    print(f"\n✅ [{name}] Top 1% 시그널 포착! 듀얼 발송 대기열 추가 완료!")
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
