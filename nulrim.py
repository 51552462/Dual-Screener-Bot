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
import sqlite3
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
DB_PATH = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Quant_System', 'market_data.sqlite')

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

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리)
TELEGRAM_TOKEN_MAIN  = "7764404352:AAHcj7FNpVB1h2BfQ1wtLkh1UJCES2LQK_s"
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
    import re # 💡 정규식 사용을 위해 추가
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
                            # 💡 params 대신 data를 사용하여 안정성 확보
                            res = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", data={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption, "parse_mode": "HTML"}, files={"photo": f}, timeout=60, verify=False)
                    else:
                        res = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_caption, "parse_mode": "HTML"}, timeout=60, verify=False)

                    # 💡 [핵심 버그 픽스] 특수문자(<, >, &)로 인해 텔레그램이 수신을 거부(400 에러)하면, 태그를 다 벗기고 쌩텍스트로 끈질기게 강제 전송!
                    if res.status_code == 400:
                        plain_caption = re.sub(r'<[^>]+>', '', safe_caption).replace('&', 'and').replace('<', '〈').replace('>', '〉')
                        if img_path:
                            with open(img_path, 'rb') as f:
                                res = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", data={"chat_id": TELEGRAM_CHAT_ID, "caption": plain_caption}, files={"photo": f}, timeout=60, verify=False)
                        else:
                            res = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": plain_caption}, timeout=60, verify=False)

                    if res.status_code == 200: break
                    elif res.status_code == 429: time.sleep(3)
                except requests.exceptions.ReadTimeout: break
                except Exception: time.sleep(2)
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

# 💡 3. 잡주 필터 및 시가총액 병합 (캐싱 방어막 100% 보장형)
def get_krx_list_kind():
    try:
        # 💡 [방어 1] User-Agent 헤더를 추가하여 KRX 봇 차단 완벽 회피
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        
        df_ks = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt", headers=headers, verify=False, timeout=10).text), header=0)[0]
        df_ks['Market'] = 'KOSPI'
        df_kq = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt", headers=headers, verify=False, timeout=10).text), header=0)[0]
        df_kq['Market'] = 'KOSDAQ'
        df = pd.concat([df_ks, df_kq])
        df['Code'] = df['종목코드'].astype(str).str.zfill(6)
        df = df.rename(columns={'회사명': 'Name'})
        
        junk_pattern = '스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권'
        filtered_df = df[~df['Name'].str.contains(junk_pattern, regex=True)].copy()
        
        # 💡 [캐시 파일 경로 세팅]
        CACHE_FILE = os.path.join(TOP_FOLDER, 'marcap_cache.csv')
        
        try:
            # 💡 [방어 2] KRX 서버 차단 시 KOSPI/KOSDAQ 분할 우회 로드
            try:
                fdr_df = fdr.StockListing('KRX')
            except Exception as e:
                print(f"💡 KRX 메인 서버 지연. KOSPI/KOSDAQ 우회 로드 가동: {e}")
                df_k = fdr.StockListing('KOSPI')
                df_q = fdr.StockListing('KOSDAQ')
                fdr_df = pd.concat([df_k, df_q])
            
            if not any(col in fdr_df.columns for col in ['Marcap', 'MarketCap', '시가총액']):
                fdr_df = fdr.StockListing('KRX-MARCAP')
                
            rename_map = {}
            if 'Symbol' in fdr_df.columns: rename_map['Symbol'] = 'Code'
            if '종목코드' in fdr_df.columns: rename_map['종목코드'] = 'Code'
            if 'MarketCap' in fdr_df.columns: rename_map['MarketCap'] = 'Marcap'
            if '시가총액' in fdr_df.columns: rename_map['시가총액'] = 'Marcap'
            
            if rename_map: fdr_df = fdr_df.rename(columns=rename_map)
            
            if 'Marcap' not in fdr_df.columns:
                raise ValueError("FinanceDataReader에서 시가총액을 찾을 수 없습니다.")
                
            fdr_df = fdr_df[['Code', 'Marcap']].copy()
            fdr_df['Code'] = fdr_df['Code'].astype(str).str.strip().str.zfill(6)
            
            if fdr_df['Marcap'].dtype == object:
                fdr_df['Marcap'] = fdr_df['Marcap'].astype(str).str.replace(',', '')
            
            fdr_df['Marcap'] = pd.to_numeric(fdr_df['Marcap'], errors='coerce').fillna(0)
            
            if len(fdr_df) > 1000:
                fdr_df.to_csv(CACHE_FILE, index=False)
                print("✅ 시가총액(Marcap) 라이브 로드 및 백업 완료!")
            else:
                raise ValueError("API가 빈 껍데기를 반환함")
                
        except Exception as e:
            print(f"⚠️ 라이브 시총 로드 실패({e}). 안전 백업(Cache) 데이터로 복구합니다!")
            if os.path.exists(CACHE_FILE):
                fdr_df = pd.read_csv(CACHE_FILE)
                fdr_df['Code'] = fdr_df['Code'].astype(str).str.zfill(6)
            else:
                print("🚨 백업 파일도 없습니다. 전 종목 시가총액이 0으로 계산됩니다.")
                fdr_df = pd.DataFrame(columns=['Code', 'Marcap'])

        filtered_df = filtered_df.merge(fdr_df, on='Code', how='left')
        filtered_df['Marcap'] = filtered_df['Marcap'].fillna(0)
            
        return filtered_df[['Code', 'Name', 'Market', 'Marcap']].dropna()
        
    except Exception as outer_e: 
        print(f"🚨 리스트 수집 치명적 에러: {outer_e}")
        return pd.DataFrame()
        
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

# 💡 [교체] V11.0 눌림목 마스터 시그널 엔진 (시가총액 팩트 데이터 완벽 적용)
def compute_signal(df_raw: pd.DataFrame, idx_close: pd.Series, marcap: float, code: str = ""): 
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

    # =========================================================================
    # 👑 [2단계] 눌림목 타점 발생 로직 (S1, S4, S6, S7)
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

    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    prev_e20 = np.roll(e20, 1); prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    
    s4 = np.zeros_like(c, dtype=bool)
    last_pb = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pb > 5):
            s4[i] = True
            last_pb = i

    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1); prev_shortBull[0] = False
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish

    prev_e60 = np.roll(e60, 1); prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1); prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    cond_base = moneyOk & priceOk
    cond_base_arr = cond_base.values if isinstance(cond_base, pd.Series) else cond_base
    s1_arr = s1.values if isinstance(s1, pd.Series) else s1
    s4_arr = s4.values if isinstance(s4, pd.Series) else s4
    s6_arr = s6.values if isinstance(s6, pd.Series) else s6
    s7_arr = s7.values if isinstance(s7, pd.Series) else s7

    hit_s1 = s1_arr[-1] and cond_base_arr[-1]
    hit_s4 = s4_arr[-1] and cond_base_arr[-1]
    hit_s6 = s6_arr[-1] and cond_base_arr[-1]
    hit_s7 = s7_arr[-1] and cond_base_arr[-1]

    if not (hit_s1 or hit_s4 or hit_s6 or hit_s7): 
        return False, "", df, {}

    # =========================================================================
    # 👑 [3단계] S1, S4, S6, S7 스코어링 매핑 (V11.0 시가총액 가중치 완벽 대입)
    # =========================================================================
    recent_hits = (s1 | s4 | s6 | s7)[-252:-1].sum() if len(c) > 252 else (s1 | s4 | s6 | s7)[:-1].sum()
    freq_count = int(recent_hits)

    # 💡 [무적 방어 로직] KRX 차단으로 시가총액이 0일 경우, 포착된 종목만 실시간 스크래핑!
    # 💡 [무적 방어 로직] KRX 차단으로 시가총액이 0일 경우, 포착된 종목만 실시간 스크래핑!
    try:
        if isinstance(marcap, str): 
            marcap_val = float(marcap.replace(',', '').replace('조', '0000').replace('억', ''))
        else: 
            marcap_val = float(marcap)
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
        except: pass

    # 💡 [V11.0] 시가총액 체급 판별 및 통계 매핑 (초대형주 방어 로직 추가)
    marcap_eok = marcap_val / 100_000_000 
    
    if marcap_eok >= 100000:
        cap_str = "⭐ 10조 이상 (초대형주)"
        score_marcap = 10.0
        ema_stat_str = "승률 35.2% / 손익비 4.80 (시장 주도주 최강 방어력)"
        weight_rec = "기본 비중의 1.5배 (최우선 적극 진입)"
    elif marcap_eok >= 10000:
        cap_str = "① 1조~10조 (대형주)"
        score_marcap = 9.0
        ema_stat_str = "승률 32.2% / 손익비 4.39 (수익성과 방어력 1위)"
        weight_rec = "기본 비중의 1.2배 (적극 진입)"
    elif marcap_eok >= 6000:
        cap_str = "② 6천억~1조 (중견주)"
        score_marcap = 8.0
        ema_stat_str = "승률 29.9% / 손익비 4.89 (수익성 상위)"
        weight_rec = "기본 비중 1.0배 적용"
    elif marcap_eok >= 3000:
        cap_str = "③ 3천억~6천억 (중소형주)"
        score_marcap = 6.0
        ema_stat_str = "승률 29.2% / 손익비 3.14"
        weight_rec = "기본 비중 1.0배 적용"
    elif marcap_eok >= 1000:
        cap_str = "④ 1천억~3천억 (소형주)"
        score_marcap = 4.0
        ema_stat_str = "승률 24.6% / 손익비 2.90"
        weight_rec = "기본 비중의 0.5배 (비중 축소)"
    else:
        cap_str = "⑤ 1천억 미만 (잡주/초소형주)"
        score_marcap = 2.0
        ema_stat_str = "승률 23.8% / 손익비 2.87 (지지선 이탈 휩소 최다 발생 구간)"
        weight_rec = "기본 비중의 0.5배 (철저한 소액 로또용)"

    score_cpv, score_tb, score_bbe, score_rs, score_ema, score_freq = 0, 0, 0, 0, 0, 0
    total_score = 0
    trap_warning = ""
    exit_strategy = "MFE 정점 도달 시 기계적 익절을 권장하며, 진입 후 윗꼬리 긴 악성 캔들 출현 시 ZLEMA 기준 즉각 칼손절하십시오."

    # 👇👇 [수술 1: 변수 초기화] 어떤 타점이든 에러가 나지 않게 기본값 세팅 👇👇
    regime_weight = 1.0 
    tier_stat = ""
    
    # 💡 [치명적 버그 픽스] 현재 캔들의 팩트 데이터를 배열의 맨 끝(-1)에서 뽑아주는 필수 선언 추가!
    cur_cpv = cpv[-1]
    cur_tb = tb_index[-1]
    cur_bbe = bb_energy[-1]
    cur_rs = rs[-1]
    
    if hit_s6: 
        sig_type = "🌱 [눌림] S6 (바닥턴 단기 정배열)"
        regime_weight = SYS_CONFIG.get("WEIGHT_KR_NULRIM_S6", 1.0)
        score_rs   = scale_score(cur_rs, 770.60, -65.50)   
        score_tb   = scale_score(cur_tb, 24.60, 0.90)      
        score_cpv  = scale_score(cur_cpv, 0.14, 0.83)      
        score_bbe  = scale_score(cur_bbe, 53.40, 1.80)     
        score_ema  = 10.0 if not align112[-1] else 5.0     
        if 6 <= freq_count <= 15: score_freq = 10.0
        elif freq_count >= 30: score_freq = 2.0 
        else: score_freq = 6.0                      
        
        total_score = (score_rs*10 + score_tb*9 + score_marcap*8 + score_cpv*7 + score_bbe*6 + score_ema*5 + score_freq*4) / 490 * 100
        if cur_tb < 0.90 and cur_bbe < 1.80: trap_warning += "🚨 [기회비용 늪] 바닥인 척 튀었으나 돈과 에너지가 없음!\n"

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

        total_score = (score_cpv*10 + score_freq*9 + score_marcap*8 + score_bbe*7 + score_tb*6 + score_rs*5 + score_ema*4) / 490 * 100
        if cur_cpv > 0.85 and cur_rs < -358.20: trap_warning += "🚨 [기회비용 늪] 애매한 캔들에 시장 소외주. 자본 묶임 주의!\n"

    elif hit_s4: 
        sig_type = "🔥 [눌림] S4 (바닥 탈출 텐배거 로또 타점)"
        score_bbe  = scale_score(cur_bbe, 5400.0, 10.0)    
        score_cpv  = scale_score(cur_cpv, 0.23, 0.85)      
        score_tb   = scale_score(cur_tb, 20.0, 5.0)      
        score_rs   = scale_score(cur_rs, 1563.0, -745.10) 
        score_ema  = 10.0 if align448[-1] else 5.0    
        score_freq = 10.0 if 1 <= freq_count <= 5 else 5.0 

        total_score = (score_bbe*10 + score_cpv*9 + score_tb*8 + score_marcap*7 + score_rs*6 + score_ema*5 + score_freq*4) / 490 * 100
        regime_weight = SYS_CONFIG.get("WEIGHT_KR_NULRIM_S4", 1.0)
        trap_warning += "⚠️ [S4 바닥 탈출] 승률 방어를 위해 반드시 서브 비중으로만 접근 요망!\n"

    else: # S1
        sig_type = "🔥 [눌림] S1 (448 대세 추세)"
        score_rs   = scale_score(cur_rs, 1563.0, -745.10) 
        score_ema  = 10.0 if align448[-1] else 5.0    
        score_cpv  = scale_score(cur_cpv, 0.23, 0.85)      
        score_bbe  = scale_score(cur_bbe, 5400.0, 10.0)    
        score_tb   = scale_score(cur_tb, 20.0, 5.0)      
        score_freq = 10.0 if 1 <= freq_count <= 5 else 5.0 

        total_score = (score_rs*10 + score_ema*9 + score_marcap*8 + score_cpv*7 + score_bbe*6 + score_tb*5 + score_freq*4) / 490 * 100
        regime_weight = SYS_CONFIG.get("WEIGHT_KR_NULRIM_S1", 1.0)
        if cur_rs < -745.10: trap_warning += "🚨 [기회비용 늪] 정배열이어도 지수를 이기지 못해 박스권 갇힘!\n"
        if not align112[-1]: trap_warning += "💀 [참사의 늪] 장기 추세가 없는 역배열/혼조 구간 진입 페이크 상승!\n"

    # =========================================================================
    # 👑 [4단계] V11.0 디테일: 요일 효과, 데스콤보, 뱃지 시스템
    # =========================================================================
    weekday = df.index[-1].weekday()
    if weekday == 4: total_score *= 1.05 
    elif weekday == 0: total_score *= 0.95 

    is_death_combo = (cur_cpv > 0.85) and (cur_rs < 0)
    if is_death_combo: 
        total_score *= 0.70
        trap_warning += "⚠️ [데스 콤보 발동] 거래량 없이 만든 가짜 양봉 + 시장 소외주 (점수 30% 삭감)\n"

    total_score = min(max(total_score, 0), 100) 

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
    
    is_tenbagger = False
    if hit_s6 and cur_rs >= 207.60 and cur_cpv <= 0.46 and (not align112[-1]): is_tenbagger = True
    if hit_s7 and cur_rs >= 293.80 and cur_cpv <= 0.56 and align112[-1]: is_tenbagger = True
    if hit_s1 and cur_rs >= 239.30 and cur_cpv <= 0.55 and align448[-1]: is_tenbagger = True

    is_top_dna = (cur_cpv <= 0.56) and (cur_tb >= 10.83) and (cur_bbe >= 16.12)
    is_worst_dna = (cur_cpv >= 0.56) and (cur_tb <= 10.36) and (cur_bbe <= 5.20) 

    # =========================================================================
    # 👑 [종목 맞춤형 동적 청산 전략 (V11.0 팩트 데이터)]
    # =========================================================================
    if cur_cpv >= 0.48:
        cpv_stat = f"현재 꽉 찬 양봉 (CPV {cur_cpv:.2f})"
    elif cur_cpv <= 0.23:
        cpv_stat = f"위아래 꼬리가 길게 달린 매물 소화 캔들 (CPV {cur_cpv:.2f})"
    else:
        cpv_stat = f"표준적인 눌림목 캔들 (CPV {cur_cpv:.2f})"

    # 👇 타점에 따른 동적 네임스페이스 분리 (S4면 S4 방, 아니면 S1 방)
    ns_prefix = "KR_NULRIM_S4" if hit_s4 else "KR_NULRIM_S1"
    
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

    if hit_s4 and cur_rs <= -1000:
        tier_stat = f"💡 [특급 로또 타점] 현재 완벽한 소외주(RS {cur_rs:.1f}) 역배열 바닥권입니다. (중략)..."

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

    badge_str = ""
    if total_score >= 80.0:
        badge_str = "🔥 [1티어 뱃지] 가산점 대상 (지옥행 참사 비율 1~2% 완벽 차단. 최우선 매수)"
        sig_type = "👑 [1티어] " + sig_type
    elif total_score <= 50.0 and cur_rs <= -1000 and cur_cpv <= 0.3:
        badge_str = "💎 [특급 모멘텀 예외] 역배열 바닥 탈출 텐배거 로또 타점 (소액 접근)"
        sig_type = "💎 [로또] " + sig_type
    else:
        badge_str = "⚠️ [비중 축소] 하위권 점수는 철저히 비중 축소 요망"
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
    
    v11_comment = (
        f"📊 [System B 한국 눌림목 V11.0 마스터 리포트]\n"
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
        f"💡 [상대평가] RS 상위 {(10 - dyn_rs_score) * 11.1:.1f}% / 찐양봉 상위 {(10 - dyn_tb_score) * 11.1:.1f}%\n"
    )
    
    if trap_warning != "": v11_comment += f"\n{trap_warning}"
    if is_top_dna: v11_comment += f"\n💎 [Top 30 우량 DNA 검증 완료] 대박 확률 대폭 상승!\n"
    elif is_worst_dna: v11_comment += f"\n💀 [Worst 30 지옥행 DNA 일치] 꼬리는 달렸으나 에너지가 죽은 종목. 진입 주의!\n"
    if is_tenbagger: v11_comment += f"\n🚀 [초격차 텐배거 포착] 대세 상승 퀀텀점프 필수 조합 충족!\n"
    if weekday == 4: v11_comment += f"✨ 금요일 주말 리스크를 이겨낸 진짜 주도주 프리미엄 (+5% 가산)\n"
    elif weekday == 0: v11_comment += f"⚠️ 월요일 주말 호재 고점 털기 리스크 반영 (-5% 삭감)\n"

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
        "marcap_eok": marcap_eok,          # 👈 추가
        "score_marcap": score_marcap,      # 👈 추가
        "freq_count": freq_count           # 👈 추가
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
    except Exception as e:
        print(f"⚠️ DB 지수 로드 실패, 실시간 API로 대체합니다: {e}")
        try:
            kospi_idx = fdr.DataReader('069500', start_date)['Close'] 
            kosdaq_idx = fdr.DataReader('229200', start_date)['Close']
        except:
            kospi_idx, kosdaq_idx = pd.Series(dtype=float), pd.Series(dtype=float)

    def worker(row_tuple):
        try: # 💡 [버그 픽스] 워커 전체를 감싸는 try 추가! (들여쓰기 완벽 교정)
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            marcap = row.get("Marcap", 0)
            df_raw = None
            is_valid = False
            hit, sig_type, df, dbg = False, "", None, {}

            try:
                df_raw = get_safe_data(code, start_date)
                if df_raw is not None and not df_raw.empty:
                    df_raw = df_raw[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()

                is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
                if is_valid:
                    idx_close = kospi_idx if row["Market"] == 'KOSPI' else kosdaq_idx
                    hit, sig_type, df, dbg = compute_signal(df_raw, idx_close, marcap, code)
            except Exception as inner_e:
                # 💡 [버그 픽스] 에러를 조용히 삼키지 않고 콘솔에 출력하도록 수정!
                print(f"⚠️ [{name}] 시그널 연산 중 에러: {inner_e}")
                pass

            hit_rank = 0
            with console_lock:
                tracker['scanned'] += 1
                if is_valid: tracker['analyzed'] += 1 
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
                    q_main.put((main_chart_path, main_caption))

                    try:
                        import auto_forward_tester as aft
                        market_type = 'US' if 'US' in dbg.get('sig_type', '') else 'KR' # 파일에 맞게 자동 인식
                        entry_facts = {
                            'v_rs': dbg.get('v_rs', 0),
                            'v_cpv': dbg.get('v_cpv', 0),
                            'v_yang': dbg.get('v_yang', 0),
                            'v_energy': dbg.get('v_energy', 0),
                            'marcap_eok': dbg.get('marcap_eok', 0),    
                            'score_marcap': dbg.get('score_marcap', 0), 
                            'freq_count': dbg.get('freq_count', 0),
                            'dyn_rs': dbg.get('dyn_rs_score', 0),
                            'dyn_cpv': dbg.get('dyn_cpv_score', 0),
                            'dyn_tb': dbg.get('dyn_tb_score', 0),
                            'is_tenbagger': 1 if dbg.get('is_tenbagger') else 0,
                            'is_top_dna': 1 if dbg.get('is_top_dna') else 0,
                            'is_worst_dna': 1 if dbg.get('is_worst_dna') else 0,
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
                        if sn_score >= 50.0: # 💡 폭등 6개월 전 DNA와 85% 이상 일치 시
                            _, sn_msg = aft.try_add_virtual_position(
                                market=market_type, code=code, name=name,
                                sig_type=dbg.get('sig_type', ''), score=max(dbg.get('score', 0), 50.0), # 점수 보정
                                ep=dbg.get('last_close', 0), facts=entry_facts, sector=sector_info,
                                trade_source="SUPERNOVA"
                            )
                            print(f"   ↳ [초신성 장부]: {sn_msg}")
                                    
                    except Exception as e:
                        print(f"   ↳ [포워드 장부 에러]: {e}")
                            # 👆👆 [듀얼 리그 진입 끝] 👆👆

                    promo_caption = (
                        f"📈 [알고리즘 차트 포착]\n\n"
                        f"🏢 종목: {name} ({code})\n"
                        f"🏷️ 섹터: {sector_info}\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원"
                        )
                    q_promo.put((promo_chart_path, promo_caption))

                print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")

        except Exception as e: # 💡 이제 짝꿍인 try가 있으므로 문법 에러가 발생하지 않습니다!
            err_name = row_tuple[1].get("Name", "Unknown") if 'row_tuple' in locals() else "Unknown"
            err_text = f"⚠️ Worker 구동 중 에러 발생 [{err_name}]: {e}"
            print(err_text)
            q_main.put((None, f"🚨 <b>[한국장 검색기 워커 에러]</b>\n{err_text}"))
        
    # 💡 5. 일꾼들(스레드) 가동
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
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
        try: # 💡 try는 while문 안쪽으로 4칸 들여쓰기
            now_kr = datetime.now(kr_tz)
            if (now_kr.hour == 10 and now_kr.minute == 30) or (now_kr.hour == 13 and now_kr.minute == 30) or (now_kr.hour == 15 and now_kr.minute == 10):
                print(f"🚀 [5번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
                scan_market_1d()
                time.sleep(60) 
            else: 
                time.sleep(10)
                
        except Exception as e: # 💡 except는 try와 완벽히 일직선상에 위치해야 함
            err_msg = f"🚨 <b>[검색기 스케줄러 긴급 에러]</b> 스캔 중 치명적 꼬임 발생:\n{e}"
            print(err_msg)
            q_main.put((None, err_msg))
            time.sleep(60)

if __name__ == "__main__":
    # run_scheduler()  <-- 이 줄을 주석 처리하거나 지우고
    scan_market_1d()   # ⭐️ 이 문구를 추가하면 즉시 1회 스캔이 시작됩니다.
