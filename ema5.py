# Dante_Ohdole_1D_AI_Pro.py
import os, re, time, threading, concurrent.futures
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
import FinanceDataReader as fdr
import random
import matplotlib.font_manager as fm
import sqlite3
import json

# 💡 [자율 관제탑 연결] 조율된 파라미터 수신
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')


def load_config(max_retries=5):
    """
    [장갑차 로직] JSONDecodeError 및 파일 잠금(Lock) 방어막 적용
    """
    if not os.path.exists(CONFIG_PATH):
        return {}

    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] 관제탑 뇌(JSON) 읽기 최종 실패 (동시 쓰기 과부하): {e}")
                return {}
    return {}


def load_system_config():
    return load_config()

SYS_CONFIG = load_system_config()

# 💡 [DB 경로 세팅] 로컬 데이터베이스 위치
DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')

# 💡 [Next Level 1] 시세 로드 — market_data_fetcher 단일 파이프라인
def get_safe_data(code, start_date):
    from market_data_fetcher import fetch_market_data

    end_date = datetime.now().strftime("%Y-%m-%d")
    return fetch_market_data(str(code).strip(), "KR", start_date, end_date)

# 💡 [Next Level 2] 동적 백분위 스코어링 함수
def get_dynamic_score(series_data, higher_is_better=True, window=252):
    if len(series_data) < 20: return 5.0
    pct_rank = pd.Series(series_data).rolling(window, min_periods=20).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    ).fillna(0.5).values[-1]
    
    if higher_is_better: return 1.0 + (pct_rank * 9.0)
    else: return 1.0 + ((1.0 - pct_rank) * 9.0)
        
# ==========================================
# 🔑 리포트: gemini_report_cache 파사드 (import 시 google.generativeai 비로드)
# ==========================================
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리) — .env → telegram_env
import telegram_env

TELEGRAM_TOKEN_MAIN = telegram_env.get_equity_kr_main_token()
TELEGRAM_TOKEN_PROMO = telegram_env.get_equity_kr_promo_token()
TELEGRAM_CHAT_ID = telegram_env.get_equity_kr_factory_chat_id()
SEND_TELEGRAM = bool(TELEGRAM_TOKEN_MAIN and TELEGRAM_CHAT_ID)

from telegram_message_queue import (
    enqueue_telegram,
    start_telegram_queue_daemons,
    wait_telegram_queue_drained,
)

start_telegram_queue_daemons(
    TELEGRAM_TOKEN_MAIN,
    TELEGRAM_TOKEN_PROMO or TELEGRAM_TOKEN_MAIN,
    TELEGRAM_CHAT_ID,
    SEND_TELEGRAM,
)

sent_today = set()
last_run_date = ""

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Pro_System')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

ai_request_lock = threading.Lock()

# 💡 2. 본캐 팩트 리포트 (해시태그 파싱 오류 제거) — Gemini 캐시·다중키: gemini_report_cache
def generate_ai_report(code: str, company_name: str):
    from gemini_report_cache import get_report_provider

    return get_report_provider().generate("stock", code=code, company_name=company_name)

# 💡 3. 잡주 필터 및 시가총액 (FDR → krx_list_cache.csv → sqlite KR_* 3단계 생존)
def get_krx_list_kind():
    from krx_list_survival import collect_krx_list_survival

    try:
        df, _src = collect_krx_list_survival(
            db_path=DB_PATH,
            fdr_module=fdr,
        )
    except Exception:
        df = pd.DataFrame()

    return df if df is not None else pd.DataFrame()

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
# 💡 [버그 픽스] current_marcap과 code를 받을 수 있도록 인자(Parameter) 수정
def compute_5ema_signal(df_raw: pd.DataFrame, idx_close: pd.Series, current_marcap: float = 0.0, code: str = ""):
    if df_raw is None or len(df_raw) < 500: 
        return False, "", df_raw, {}
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

    # 💡 [버그 픽스] Pandas 인덱싱 에러(KeyError) 방지를 위해 Numpy로 안전하게 변환
    finalSignal_arr = finalSignal.values if isinstance(finalSignal, pd.Series) else finalSignal

    if not finalSignal_arr[-1]: 
        return False, "", df, {}

    # 💡 [무적 방어 로직] KRX 서버 차단으로 시가총액이 0일 경우, 타점이 포착된 종목만 네이버에서 실시간 팩트 체크!
    # 💡 [무적 방어 로직] KRX 서버 차단으로 시가총액이 0일 경우, 타점이 포착된 종목만 네이버에서 실시간 팩트 체크!
    try:
        if isinstance(current_marcap, str): 
            marcap_val = float(current_marcap.replace(',', '').replace('조', '0000').replace('억', ''))
        else: 
            marcap_val = float(current_marcap)
        if np.isnan(marcap_val): marcap_val = 0.0
    except:
        marcap_val = 0.0

    if marcap_val == 0 and code != "":
        try:
            import requests, re
            res = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=False)
            m = re.search(r'<em id="_market_sum"[^>]*>\s*([\d,]+)\s*</em>', res.text, re.DOTALL)
            if m:
                marcap_val = float(m.group(1).replace(',', '')) * 100_000_000
        except:
            pass

    # =========================================================================
    # 👑 [3단계] S1 스코어링 매핑 시작 부분에 시총 계산 로직 추가
    # =========================================================================
    recent_hits = finalSignal[-252:-1].sum() if len(c) > 252 else finalSignal[:-1].sum()
    freq_count = int(recent_hits)

    # 👇👇 [여기서부터 추가] 동적 시가총액 분류 및 점수화 👇👇
    marcap_eok = marcap_val / 100_000_000  # 억원 단위 변환
    if marcap_eok >= 100000:
        marcap_str = "⭐ 10조 이상 (초대형주)"
        score_marcap = 10.0
    elif marcap_eok >= 10000:
        marcap_str = "① 1조~10조 (대형주)"
        score_marcap = 9.0
    elif marcap_eok >= 5000:
        marcap_str = "② 5천억~1조 (중견주)"
        score_marcap = 7.0
    elif marcap_eok >= 1000:
        marcap_str = "③ 1천억~5천억 (중소형주)"
        score_marcap = 5.0
    else:
        marcap_str = "④ 1천억 미만 (초소형주/잡주)"
        score_marcap = 2.0

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
    regime_weight = SYS_CONFIG.get("WEIGHT_KR_EMA5_S1", 1.0)
    
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
    # 👑 [비선형 의사결정 나무 (Decision Tree) 필터] - 선형 덧셈의 오류 차단
    # =========================================================================
    tree_fatal_cpv = SYS_CONFIG.get("TREE_FATAL_CPV", 0.85) # 관제탑이 학습한 한계치 로드
    is_tree_rejected = False
    tree_reason = ""

    # [Node 1]: CPV가 한계치를 넘으면 RS 점수가 아무리 높아도 무조건 기각 (Death Combo)
    if cur_cpv > tree_fatal_cpv:
        is_tree_rejected = True
        tree_reason = f"악성 매물 캔들 한계치 초과 (CPV {cur_cpv:.2f} > {tree_fatal_cpv})"

    # 비선형 필터에 걸렸다면 총점을 강제로 0점 처리하고 사형 선고
    if is_tree_rejected:
        total_score = 0.0
        trap_warning += f"🚫 <b>[Decision Tree 기각]</b>: 선형 점수는 높을 수 있으나, 비선형 팩트에 의해 차단되었습니다. (사유: {tree_reason})\n"
        badge_str = "💀 [비선형 필터 기각] 매수 절대 금지"
    # =========================================================================

    # =========================================================================
    # 👑 [종목 맞춤형 동적 청산 전략 (스마트 매수/손절)]
    # 고정된 텍스트가 아닌, 해당 종목의 실시간 CPV, 총점, RS 데이터를 바탕으로 전략이 매번 바뀝니다!
    # =========================================================================
    # =========================================================================
    # 👑 [종목 맞춤형 동적 청산 전략 (관제탑 지시 기반)]
    # =========================================================================
    if cur_cpv >= 0.70:
        cpv_stat = f"현재 꽉 찬 양봉 (CPV {cur_cpv:.2f})"
    elif cur_cpv <= 0.40:
        cpv_stat = f"꼬리가 길게 달린 매물 소화 캔들 (CPV {cur_cpv:.2f})"
    else:
        cpv_stat = f"표준적인 양봉 (CPV {cur_cpv:.2f})"

    # 👇 관제탑 청산 모드 로드 (KR_5EMA_S1 단일 네임스페이스)
    active_exit_mode = SYS_CONFIG.get("ACTIVE_EXIT_MODE", "HYBRID")
    opt_time_stop    = SYS_CONFIG.get("KR_5EMA_S1_TIME_STOP", 10)
    opt_sl_atr       = SYS_CONFIG.get("KR_5EMA_S1_ATR_SL", 2.0)

    if active_exit_mode == "TECH":
        action = "📈 <b>[TECH 추세 모드 가동]</b>\n대세 상승장 판독 완료. 통계적 숏컷을 무시하고, '단기데드' 및 'ZLEMA 이탈' 전까지 차트 추세를 끝까지 발라먹으십시오."
    elif active_exit_mode == "STAT":
        action = (f"🎯 <b>[STAT 통계 모드 가동]</b>\n변동성/휩소 장세 판독 완료. 차트 무시!\n"
                  f"▪️ 진입 후 <b>{opt_time_stop}일 차 종가</b>에 무조건 타임스탑(기계적 청산) 하십시오.\n"
                  f"▪️ 진입가 대비 <b>ATR {opt_sl_atr}배</b> 이탈 시 즉각 칼손절하십시오.")
    else: # HYBRID
        action = (f"⚖️ <b>[HYBRID 공수겸장 가동]</b>\n"
                  f"추세를 타되(ZLEMA 익절), 최대 <b>{opt_time_stop}일</b> 내에 승부를 보고, 폭락 시 <b>ATR {opt_sl_atr}배</b>에서 즉각 손절 차단하십시오.")

    # 💡 [버그 픽스] 80점 미만일 때도 tier_stat 변수가 무조건 생성되도록 else 분기 추가
    if total_score >= 80:
        tier_stat = f"총점 {total_score:.1f}점(1티어)으로 계좌 방어력이 수학적으로 완벽히 입증되었으므로 메인 비중 진입을 권장합니다."
    elif total_score <= 50 and cur_rs > 500 and cur_cpv <= 0.3:
        tier_stat = f"💡 [특급 모멘텀 예외] 총점은 낮으나 시장 주도력(RS {cur_rs:.1f})이 압도적입니다. 소액 진입을 허용합니다."
    else:
        tier_stat = f"총점 {total_score:.1f}점의 하위/일반 타점입니다. 가짜 휩소 리스크를 피하기 위해 반드시 비중을 대폭 축소하십시오."

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
        TOTAL_CAPITAL = 50_000_000  # 👈 본인의 실제 투자 원금으로 수정하세요 (예: 5천만원)
        BASE_RISK = 0.015           # 👈 1종목당 기본 허용 리스크 1.5%

        # 1. 14일 평균 변동폭(ATR) 계산 (결측치 방어)
        tr = np.maximum(h[-14:] - l[-14:], np.maximum(abs(h[-14:] - np.roll(c[-14:], 1)), abs(l[-14:] - np.roll(c[-14:], 1))))
        cur_atr = np.mean(tr) if np.mean(tr) > 0 else c[-1] * 0.03

        ns_prefix = "KR_5EMA_S1"
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
            currency = "원"
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

    # =========================================================================
    # 👑 [Next Level 2] 듀얼 트랙 상대 평가 (기존 수치 유지 + 상대 랭크 추가)
    # =========================================================================
    dyn_rs_score = get_dynamic_score(rs, higher_is_better=True)
    dyn_tb_score = get_dynamic_score(tb_index, higher_is_better=True)
    dyn_cpv_score = get_dynamic_score(cpv, higher_is_better=False)

    is_hybrid_trap = (dyn_rs_score <= 3.0) and (dyn_cpv_score <= 2.0)
    if is_hybrid_trap:
        total_score *= 0.60
        badge_str += "\n⚠️ [NextLevel 경고] 1년 내 최하위 소외주의 억지 캔들! (가짜상승 주의, 점수 40% 삭감)"
        
    # 💡 텔레그램 결과지에 출력될 브리핑 데이터 조립 (VIX 블록 완전 제거)
    v11_comment = (
        f"📊 [System B 5일선 스나이퍼 V11.0 리포트]\n"
        f"🔹 시스템 총점: {total_score:.1f} / 100점\n"
        f"🔹 시가총액: {marcap_str}\n"
        f"🎖️ {badge_str}\n\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.2f} ({score_cpv:.1f}점)\n"
        f"▪️ 진짜양봉지수: {cur_tb:.1f} ({score_tb:.1f}점)\n"
        f"▪️ 응축에너지: {cur_bbe:.1f} ({score_bbe:.1f}점)\n"
        f"▪️ 시장상대강도: {cur_rs:.1f}% ({score_rs:.1f}점)\n"
        f"▪️ 과거 매매빈도: {freq_count}회 ({score_freq:.1f}점)\n"
        f"▪️ 시총 체급점수: {score_marcap:.1f}점\n\n"
        f"💡 [이평선 국면 팩트 데이터]\n{ema_stat_str}\n"
        f"💡 [상대평가] RS 상위 {(10 - dyn_rs_score) * 11.1:.1f}% / 찐양봉 상위 {(10 - dyn_tb_score) * 11.1:.1f}%\n"
    )
    
    if trap_warning != "": v11_comment += f"\n{trap_warning}"
    if weekday == 4: v11_comment += f"✨ 금요일 주말 리스크를 이겨낸 진짜 주도주 프리미엄 (+5% 가산)\n"
    elif weekday == 0: v11_comment += f"⚠️ 월요일 고점 털기 리스크 반영 (-5% 삭감)\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": f"{exit_strategy}", 
        "v11_comment": v11_comment,
        "score": total_score,
        "v_cpv": cur_cpv,
        "v_yang": cur_tb,
        "v_energy": cur_bbe,
        "v_rs": cur_rs,
        "dyn_rs_score": dyn_rs_score,
        "dyn_cpv_score": dyn_cpv_score,
        "dyn_tb_score": dyn_tb_score,
        "sn_score": max_sn_similarity,
        "marcap_eok": marcap_eok,          # 👈 포워드 장부 에러 픽스용 추가
        "score_marcap": score_marcap,      # 👈 포워드 장부 에러 픽스용 추가
        "freq_count": freq_count           # 👈 포워드 장부 에러 픽스용 추가
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

    # 💡 벤치마크 지수 (KOSPI/KOSDAQ) 일괄 로드 (하이브리드 엔진 적용)
    print("📊 벤치마크 지수(KODEX ETF 대용) 데이터 안전하게 로드 중...")
    try:
        # 먼저 로컬 DB에서 불러오기 시도
        conn = sqlite3.connect(DB_PATH)
        kospi_idx = pd.read_sql("SELECT * FROM KR_KOSPI_IDX", conn, index_col='Date')['Close']
        kosdaq_idx = pd.read_sql("SELECT * FROM KR_KOSDAQ_IDX", conn, index_col='Date')['Close']
        conn.close()
        kospi_idx.index = pd.to_datetime(kospi_idx.index)
        kosdaq_idx.index = pd.to_datetime(kosdaq_idx.index)
        kospi_idx = kospi_idx.loc[~kospi_idx.index.duplicated(keep='last')]
        kosdaq_idx = kosdaq_idx.loc[~kosdaq_idx.index.duplicated(keep='last')]
    except Exception as e:
        print(f"⚠️ DB 지수 로드 실패, 실시간 API로 대체합니다: {e}")
        try:
            kospi_idx = fdr.DataReader('069500', start_date)['Close'] 
            kosdaq_idx = fdr.DataReader('229200', start_date)['Close']
            kospi_idx = kospi_idx.loc[~kospi_idx.index.duplicated(keep='last')]
            kosdaq_idx = kosdaq_idx.loc[~kosdaq_idx.index.duplicated(keep='last')]
        except:
            kospi_idx, kosdaq_idx = pd.Series(dtype=float), pd.Series(dtype=float)
            
    def worker(row_tuple):
        try:
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            df_raw = None
            
            try:
                df_raw = get_safe_data(code, start_date)
            except: pass

            # 거래정지·단일가(Static Quote) — 최근 3일 동일 종가 + 거래량 극소 시 매집 착시 방지 (한국장)
            if df_raw is not None and not df_raw.empty and len(df_raw) >= 3:
                try:
                    if all(c in df_raw.columns for c in ("Close", "Volume")):
                        t3 = df_raw[["Close", "Volume"]].tail(3).dropna()
                        if len(t3) >= 3 and int(t3["Close"].nunique()) == 1 and float(t3["Volume"].sum()) < 10000:
                            df_raw = None
                except Exception:
                    pass

            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            hit, sig_type, df, dbg = False, "", None, {}
            
            # 기존 is_valid 조건문 안의 내용을 아래와 같이 교체하세요.
            if is_valid: 
                # 💡 시장에 맞는 지수 및 시가총액을 넘겨줌 (current_marcap 추가)
                idx_close = kospi_idx if row["Market"] == 'KOSPI' else kosdaq_idx
                current_marcap = row.get("Marcap", 0) 
                
                # 💡 code를 파라미터로 넘겨 네이버 실시간 스크래핑이 가능하게 함
                hit, sig_type, df, dbg = compute_5ema_signal(df_raw, idx_close, current_marcap, code)
            
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
                        main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
                        # 💡 [수정] threads_chart_path -> promo_chart_path 로 이름 통일
                        promo_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
                        
                        if main_chart_path and promo_chart_path:
                            ai_main, _ = generate_ai_report(code, name)
                            
                            # 💡 [버그 픽스] 섹터 추출을 장부 기록보다 '먼저' 실행
                            try:
                                sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                            except:
                                sector_info = "유망 섹터 포착"
                    
                            # 1️⃣ 본캐용 캡션
                            main_caption = (
                                f"🎯 [{dbg.get('sig_type', '')}]\n"
                                f"🎯 추천: 스윙, 추세 홀딩 / 종가배팅\n\n"
                                f"🏢 {name} ({code})\n"
                                f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                                f"{dbg.get('v11_comment', '')}\n"
                                f"📉 [스마트 매수/청산 전략]\n"
                                f"{dbg.get('recommend', '')}\n\n"
                                f"💡 [AI 비즈니스 요약]\n"
                                f"{ai_main}\n\n"
                                f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                                f"⚠️ [면책 조항]\n"
                                f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다.\n투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                            )
                            enqueue_telegram(
                                "MAIN",
                                main_chart_path,
                                main_caption,
                                enabled=SEND_TELEGRAM,
                                send_profile="html_ro",
                            )

                            # 💡 [오토 포워드 장부 기록] - 한국장 전용 (모든 필터 포함)
                            try:
                                import auto_forward_tester as aft
                                market_type = 'US' if 'US' in dbg.get('sig_type', '') else 'KR' # 파일에 맞게 자동 인식
                                entry_facts = {
                                 'v_rs': dbg.get('v_rs', 0),
                                 'v_cpv': dbg.get('v_cpv', 0),
                                 'v_yang': dbg.get('v_yang', 0),
                                 'v_energy': dbg.get('v_energy', 0),
                                   # 🚨 [버그 픽스] 존재하지 않는 오타 변수 대신 dbg에서 안전하게 로드
                                 'marcap_eok': dbg.get('marcap_eok', 0),    
                                 'score_marcap': dbg.get('score_marcap', 0),
                        
                                 'dyn_rs': dbg.get('dyn_rs_score', 0),
                                 'dyn_cpv': dbg.get('dyn_cpv_score', 0),
                                 'dyn_tb': dbg.get('dyn_tb_score', 0),
                        
                                 'is_tenbagger': 1 if dbg.get('is_tenbagger') else 0, # 👈 한국장만 있음
                                 'is_top_dna': 1 if dbg.get('is_top_dna') else 0,     # 👈 한국장만 있음
                                 'is_worst_dna': 1 if dbg.get('is_worst_dna') else 0, # 👈 한국장만 있음
                                 'is_death_combo': 1 if dbg.get('is_death_combo') else 0
                                }
                    
                                # 1. 오리지널 로직 장부 기록 (STANDARD 진영)
                                success, fwd_msg = aft.try_add_virtual_position(
                                    market=market_type, code=code, name=name,
                                    sig_type=dbg.get('sig_type', ''), score=dbg.get('score', 0), 
                                    ep=dbg.get('last_close', 0), facts=entry_facts, sector=sector_info,
                                    trade_source="STANDARD"
                                )
                                print(f"   ↳ [오리지널 장부]: {fwd_msg}")
                                
                                # 2. 초신성 공통점 매칭 합격 시 추가 진입 (SUPERNOVA 진영 선취매)
                                sn_score = dbg.get('sn_score', 0.0)
                                if sn_score >= 50.0: # 💡 [V53.1 하향] 데이터 수집을 위해 커트라인 50%로 대폭 개방!
                                    _, sn_msg = aft.try_add_virtual_position(
                                        market=market_type, code=code, name=name,
                                        sig_type=dbg.get('sig_type', ''), score=max(dbg.get('score', 0), 50.0), # 💡 점수도 50으로 보정
                                        ep=dbg.get('last_close', 0), facts=entry_facts, sector=sector_info,
                                        trade_source="SUPERNOVA"
                                    )
                                    print(f"   ↳ [초신성 장부]: {sn_msg}")
                                    
                            except Exception as e:
                                print(f"   ↳ [포워드 장부 에러]: {e}")
                            # 👆👆 [듀얼 리그 진입 끝] 👆👆

                            # 💡 4. 홍보용 캡션 (한국장에 맞게 원화로 픽스)
                            promo_caption = (
                                f"📈 [알고리즘 차트 포착]\n\n"
                                f"🏢 종목: {name} ({code})\n"
                                f"🏷️ 섹터: {sector_info}\n"
                                f"💰 현재가: {dbg.get('last_close', 0):,.0f}원"
                            )
                            enqueue_telegram(
                                "PROMO",
                                promo_chart_path,
                                promo_caption,
                                enabled=SEND_TELEGRAM,
                                send_profile="html_ro",
                            )

                            print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")
        except Exception as e:
            err_name = row.get("Name", "Unknown") if 'row' in locals() else "Unknown"
            err_text = f"⚠️ Worker 구동 중 에러 발생 [{err_name}]: {e}"
            print(err_text)
            enqueue_telegram(
                "MAIN",
                None,
                f"🚨 <b>[한국장 검색기 워커 에러]</b>\n{err_text}",
                enabled=SEND_TELEGRAM,
                send_profile="html_ro",
            )

    # 💡 5. 일꾼(스레드) 가동 및 대기
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        wait_telegram_queue_drained(("MAIN", "PROMO"), timeout_sec=7200.0)

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
        try:
            import ops_logger
            ops_logger.record_heartbeat("scanner.ema5")
        except Exception:
            pass

if __name__ == "__main__":
    # run_scheduler()  <-- 이 줄을 주석 처리하거나 지우고
    scan_market_1d()   # ⭐️ 이 문구를 추가하면 즉시 1회 스캔이 시작됩니다.
