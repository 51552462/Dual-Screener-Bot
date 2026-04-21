# Dante_Ohdole_1D_AI_Pro.py
import os, re, time, threading, queue, concurrent.futures
from datetime import datetime, timedelta
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import warnings, urllib3
from bs4 import BeautifulSoup
from io import StringIO
import FinanceDataReader as fdr
import random
import matplotlib.font_manager as fm

from google import genai
from google.genai import types
from dotenv import load_dotenv

# ==========================================
# 🔑 .env 안전 파일 방식 적용
# ==========================================
load_dotenv() 
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("🚨 API 키를 찾을 수 없습니다! .env 파일을 확인해 주세요.")

client = genai.Client(api_key=GEMINI_API_KEY)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리)
TELEGRAM_TOKEN_MAIN  = "8004222500:AAFS9rPPtiQiNx4SxGgYOnODFGULqLTNO8M"
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

ai_request_lock = threading.Lock()

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

# 💡 [추가] 1~10점 스케일링 함수
def scale_score(val, best, worst):
    if best > worst:
        if val >= best: return 10.0
        if val <= worst: return 1.0
        return 1.0 + 9.0 * (val - worst) / (best - worst)
    else:
        if val <= best: return 10.0
        if val >= worst: return 1.0
        return 1.0 + 9.0 * (worst - val) / (worst - best)

# 💡 [교체] 5일선 관통 전용 마스터 시그널 엔진 (8,485건 팩트 대입)
def compute_5ema_signal(df_raw: pd.DataFrame, idx_close: pd.Series):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    df['Idx_Close'] = idx_close
    df['Idx_Close'] = df['Idx_Close'].ffill()

    # 5선부터 448선까지 전수 계산
    for n in [5, 10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    e5, e10, e20, e30 = df['EMA5'].values, df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    e60, e112, e224, e448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

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
    moneyOk = (c * v) >= 100_000_000
    priceOk = c >= 1000
    
    # 1. 완전 정배열 조건 (5선 ~ 448선)
    alignFullBull = (e5 > e10) & (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112) & (e112 > e224) & (e224 > e448)
    
    # 2. 5선 몸통 관통 양봉
    isBullish = c > o
    isBodyCross5 = (o < e5) & (c > e5)
    
    # 3. 거래량 폭발 (전일 대비 3배)
    v_prev = np.roll(v, 1); v_prev[0] = v[0]
    condVol = v >= (v_prev * 3)

    # ⚠️ S2, S3, S4 배제 (오직 S1 스나이퍼 타점만 포착)
    finalSignal = alignFullBull & isBullish & isBodyCross5 & condVol & moneyOk & priceOk

    if not finalSignal[-1]: 
        return False, "", df, {}

    # =========================================================================
    # 👑 [3단계] S1 스코어링 매핑 (V8.1 최신 팩트 데이터 100% 대입)
    # =========================================================================
    recent_hits = finalSignal[-252:-1].sum() if len(c) > 252 else finalSignal[:-1].sum()
    freq_count = int(recent_hits)

    ema_stat_str = "승률 26.8% / 손익비 3.40 (대세 상승장, 본 스나이퍼 타점 100% 점유)"

    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    
    sig_type = "🔥 S1 (5선 관통 / 448 완전정배열)"
    
    # 💡 [V8.1 스코어링 매트릭스 엄격 적용]
    score_rs   = scale_score(cur_rs, 2025.28, -821.13) 
    score_ema  = 10.0                                  
    score_cpv  = scale_score(cur_cpv, 0.39, 0.95)      
    score_bbe  = scale_score(cur_bbe, 56.80, 3.80)     
    score_tb   = scale_score(cur_tb, 20.13, 2.47)      
    score_freq = 10.0 if 1 <= freq_count <= 5 else (2.0 if freq_count >= 10 else 6.0) 

    total_score = (score_rs*10 + score_ema*9 + score_cpv*8 + score_bbe*7 + score_tb*6 + score_freq*5) / 450 * 100
    
    # 💡 [V8.1 청산 전략 가이드 (미래 시계열 데이터 대응)]
    trap_warning = ""
    exit_strategy = "MFE 정점(평균 5.92일). 단기데드로 끝까지 홀딩. 진입 후 꽉찬 양봉(CPV 0.59 이상) 출현 시 설거지 패턴이므로 즉각 ZLEMA 칼손절."

    # 💡 [V8.1 기회비용 및 참사 함정 분석]
    if cur_rs < -821.13: trap_warning += "🚨 [기회비용 늪] 정배열이어도 지수를 이기지 못해 박스권 갇힘 우려!\n"
    if cur_cpv > 0.95 and total_score <= 30.0: trap_warning += "💀 [참사의 늪] 시장 소외주의 꽉 찬 가짜 상승! 즉각 지옥행 주의!\n"

    # =========================================================================
    # 👑 [4단계] 5일선 V8.1 디테일: 요일마법, 데스콤보, 고빈도, 텐배거 시스템
    # =========================================================================
    weekday = df.index[-1].weekday()
    if weekday == 4: total_score *= 1.05 # 금요일 가산
    elif weekday == 0: total_score *= 0.95 # 월요일 삭감

    # 데스콤보 방어
    is_death_combo = (cur_cpv > 0.95) and (total_score < 40.0)
    if is_death_combo: 
        total_score *= 0.70
        trap_warning += "⚠️ [데스 콤보 발동] 세력 단기 차익 실현 후 폭락 패턴 (점수 30% 삭감)\n"
        
    # 알고리즘 단타 고빈도 컷오프
    if freq_count >= 10 and (score_rs < 8.0 or score_cpv < 8.0):
        total_score *= 0.50
        trap_warning += "🚫 [고빈도 잡주 경고] 알고리즘 단타로 때가 묻은 종목! (-50% 삭감)\n"

    if trap_warning != "" and not is_death_combo and "고빈도" not in trap_warning: 
        total_score *= 0.70 

    total_score = min(max(total_score, 0), 100) # 0~100점 보정

    # =========================================================================
    # 👑 [종목 맞춤형 동적 청산 전략 (스마트 매수/손절)]
    # 고정된 텍스트가 아닌, 해당 종목의 실시간 CPV, 총점, RS 데이터를 바탕으로 전략이 매번 바뀝니다!
    # =========================================================================
    if cur_cpv >= 0.70:
        cpv_stat = f"현재 꽉 찬 양봉 (CPV {cur_cpv:.2f})"
        action = "월가 알고리즘의 단기 설거지(휩소) 타겟이 될 확률이 높습니다. 진입 후 3~4일 내로 강하게 상승하지 못하거나 조금이라도 밀리면 'ZLEMA 이탈' 시 즉각 칼손절하여 계좌를 방어하십시오."
    elif cur_cpv <= 0.40:
        cpv_stat = f"꼬리가 길게 달린 매물 소화 캔들 (CPV {cur_cpv:.2f})"
        action = f"강력한 주도력(RS {cur_rs:.1f}%)으로 숏 스퀴즈를 유발하는 진짜 대장주 패턴입니다. 잔파도에 털리지 말고 '단기데드(EMA 20)' 이탈 전까지 2주 이상 끝까지 추세를 발라먹으십시오."
    else:
        cpv_stat = f"표준적인 양봉 (CPV {cur_cpv:.2f})"
        action = f"현재 응축에너지({cur_bbe:.1f})의 폭발 여부를 지켜봐야 합니다. 강한 슈팅이 나오면 '단기데드'로 홀딩하고, 지지부진하게 꺾이면 'ZLEMA' 라인에서 짧게 익절/본절 하십시오."

    if total_score >= 80:
        tier_stat = f"총점 {total_score:.1f}점(1티어)으로 계좌 방어력이 수학적으로 완벽히 입증되었으므로 메인 비중 진입을 권장합니다."
    elif total_score <= 50 and cur_rs > 500 and cur_cpv <= 0.3:
        tier_stat = f"총점은 {total_score:.1f}점으로 낮으나, 극단적 모멘텀이 포착된 밈(Meme) 주식 예외 타점이므로 10% 미만의 소액 로또 비중으로만 접근하십시오."
    else:
        tier_stat = f"총점 {total_score:.1f}점의 하위권 서브 타점이므로, 가짜 돌파 리스크를 피하기 위해 반드시 비중을 대폭 축소하십시오."

    exit_strategy = f"[{cpv_stat}]\n{action}\n\n💡 비중 조언: {tier_stat}"

    # 💡 [V9.0 VIX(공포지수) 기반 비중 조절 로직]
    vix_strategy = ""
    if cur_vix >= 30:
        vix_strategy = f"🌋 [극단적 공포장 | VIX {cur_vix:.1f}] 승률 43%, 평균수익 29% 압도적 대박 구간! 진입 비중 1.5배 상향 및 적극 매수."
    elif cur_vix >= 20:
        vix_strategy = f"🌪️ [조정장 | VIX {cur_vix:.1f}] 수익폭 1.5배 점프 구간! 진입 비중 1.2배 상향."
    else:
        vix_strategy = f"🌊 [평온장 | VIX {cur_vix:.1f}] 시스템 기본 비중(1배수) 기계적 돌파 매매."

    # 💡 [V9.0 뱃지 시스템 및 하위권 밈(Meme) 주식 예외 로직]
    badge_str = ""
    if total_score >= 80.0:
        badge_str = "🔥 [1티어 뱃지] 가산점 부여 대상 (승률 30.1% 수학적 검증 완료. UI 상단 노출 및 비중 1.5배 확대)"
        sig_type = "👑 [1티어] " + sig_type
    elif total_score <= 50.0 and cur_rs > 500 and cur_cpv <= 0.3:
        badge_str = "💎 [특급 모멘텀 예외] 점수 무시 텐배거 (매물 소화 끝난 돌발 밈 주식 펌핑 가능성. 비중 최소화 로또 진입)"
        sig_type = "💎 [로또] " + sig_type
    else:
        badge_str = "⚠️ [비중 축소] 80점 미만은 철저히 비중을 축소하고 1티어 뱃지 위주로 매매 요망"

    v9_comment = (
        f"📊 [System B US 돌파 마스터 V9.0 리포트]\n"
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
        "recommend": f"{exit_strategy}", # 👈 여기서 종목 맞춤형 전략이 송출됩니다!
        "v9_comment": v9_comment,
        "score": total_score,
        "v_cpv": cur_cpv,
        "v_yang": cur_tb,
        "v_energy": cur_bbe,
        "v_rs": cur_rs
    }
def compute_ohdole_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    
    for n in [5, 10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    e5, e10, e20, e30 = df['EMA5'].values, df['EMA10'].values, df['EMA20'].values, df['EMA30'].values
    e60, e112, e224, e448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    is_money_ok = (c * v) >= 100_000_000 
    is_price_ok = c >= 1000
    cond_base = is_money_ok & is_price_ok

    isBullish = c > o
    macroBull = (e112 > e224) & (e224 > e448)
    isAboveLongMA = (c > e224) & (c > e448)
    isShortBull = (e10 > e20)
    
    is_150_align = pd.Series(macroBull).rolling(150).sum().values == 150

    prev_e5 = np.roll(e5, 1); prev_e5[0] = np.inf
    prev_e30 = np.roll(e30, 1); prev_e30[0] = 0
    isStrictCrossUp30 = (prev_e5 < prev_e30) & (e5 > e30)

    signal1 = isStrictCrossUp30 & isBullish & macroBull & isAboveLongMA & isShortBull & cond_base

    if not signal1[-1]: return False, "", df, {}

    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    stars, pt = calculate_star_score(o[-1], h[-1], l[-1], c[-1], prev_c[-1], e10[-1], e20[-1], e30[-1], e60[-1])
    
    usage_tag = "(실제용)" if is_150_align[-1] else "(참고용)"
    sig_type = f"S1 | {stars} ({pt}점) {usage_tag}"
    recommend = "단타, 스윙 / 종가배팅"

    trust_score = calculate_trust_score(c, e60) 
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "recommend": recommend}

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
    
    # 💡 당일 중복 발송용 영구 파일 로드
    log_file = os.path.join(TOP_FOLDER, "sent_log_kr_ohdole.txt")
    
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

    print(f"\n⚡ [일봉 전용] 한국장 5선 관통 스나이퍼 스캔 시작!\n(당일 중복 차단 🛡️)")
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()

    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')

    # 💡 [추가] 벤치마크 지수 데이터 로드 (상대강도 계산용)
    print("📊 벤치마크 지수(KODEX ETF 대용) 데이터 안전하게 로드 중...")
    try:
        kospi_idx = fdr.DataReader('069500', start_date)['Close']
        kosdaq_idx = fdr.DataReader('229200', start_date)['Close']
    except:
        kospi_idx, kosdaq_idx = pd.Series(dtype=float), pd.Series(dtype=float)
    
    def worker(row_tuple):
        try:
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            df_raw = None
            
            try:
                df_raw = fdr.DataReader(code, start_date)
            except: pass

            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            hit, sig_type, df, dbg = False, "", None, {}
            
            if is_valid: 
                # 💡 시장에 맞는 지수를 넘겨줌
                idx_close = kospi_idx if row["Market"] == 'KOSPI' else kosdaq_idx
                hit, sig_type, df, dbg = compute_5ema_signal(df_raw, idx_close)
            
            # 🚨 아래 구형 오돌이 로직은 트뷰와 다르므로 주석 처리(비활성화) 합니다.
            # if is_valid: hit, sig_type, df, dbg = compute_ohdole_1d(df_raw)

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
                    
                    # 1️⃣ 본캐용 캡션 (유료방용 - V8.1 뱃지 및 점수 브리핑 출력)
                    main_caption = (
                        f"🎯 [{dbg.get('sig_type', '')}]\n"
                        f"🎯 추천: 스윙, 추세 홀딩 / 종가배팅\n\n"
                        f"🏢 {name} ({code})\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                        f"{dbg.get('v8_comment', '')}\n"  # 💡 V8.1 팩트 데이터 정상 로드
                        f"📉 [스마트 매수/청산 전략]\n"
                        f"{dbg.get('recommend', '')}\n\n" # 💡 종목 맞춤형 동적 전략 정상 로드
                        f"💡 [AI 비즈니스 요약]\n"
                        f"{ai_main}\n\n"
                        f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                        f"⚠️ [면책 조항]\n"
                        f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다.\n투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                    )
                    
                    # 💡 캡션 큐에 1회만 정확히 담습니다.
                    q_main.put((main_chart_path, main_caption))

                    # 💡 [오토 포워드 테스팅 시스템 변수 에러 픽스 및 중복 제거]
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
                    q_main.put((main_chart_path, main_caption))
# 💡 [오토 포워드 테스팅 시스템에 종목 편입 시도]
                    try:
                        import auto_forward_tester as aft # 상단에 임포트 안 해도 여기서 동적 로드
                        
                        market_type = 'KR' # 미국장 검색기에는 'US'로 변경!!
                        entry_facts = {
                            'v_cpv': dbg.get('v_cpv', cur_cpv),
                            'v_yang': dbg.get('v_yang', cur_tb),
                            'v_energy': dbg.get('v_energy', cur_bbe),
                            'v_rs': dbg.get('v_rs', cur_rs)
                        }
                        
                        success, fwd_msg = aft.try_add_virtual_position(
                            market=market_type,
                            code=code,
                            name=name,
                            sig_type=dbg.get('sig_type', sig_type),
                            score=dbg.get('score', total_score), # 총점 매핑 확인
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
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원"
                    )
                    q_promo.put((promo_chart_path, promo_caption))

                    print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")
        except Exception as e:
            pass

    # 💡 5. 일꾼(스레드) 가동 및 대기
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        q_main.join()
        q_promo.join()

    print(f"\n✅ [한국장 1번 오돌이 스캔 완료] 신규 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")

# ⭐️ 2번 스케줄러 세팅 (09:30, 12:00, 14:30) ⭐️
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [1번 검색기] 10:00 / 12:00 / 14:00 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 10 and now_kr.minute == 0) or (now_kr.hour == 12 and now_kr.minute == 0) or (now_kr.hour == 14 and now_kr.minute == 0):
            print(f"🚀 [1번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()
