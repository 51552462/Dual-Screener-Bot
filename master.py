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

# 💡 1. 듀얼 텔레그램 봇 세팅
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

    # 배열 전체에 대해 AND 연산 후 마지막 캔들 확인
    hit_s1_arr = signal_1 & moneyOk & priceOk
    hit_s4_arr = signal_4 & moneyOk & priceOk

    final_hit = hit_s1_arr | hit_s4_arr
    if not final_hit[-1]: return False, "", df, {}

    # =========================================================================
    # 👑 5. 한국장 시가총액(Marcap) 필터 및 V11.0 종목 맞춤형 스코어링 로직
    # =========================================================================
    try:
        marcap_val = float(marcap)
        if np.isnan(marcap_val): marcap_val = 0.0
    except:
        marcap_val = 0.0
        
    marcap_eok = marcap_val / 100_000_000 
    
    # 💡 [정리파일 1 반영] 시가총액 규모별 극명한 체질 차이 세팅
    if marcap_eok >= 10000:
        cap_str, score_marcap = "① 1조 이상 (대형주)", 10.0
        cap_stat = "승률 33.0% / 손익비 4.51 (안정성 최강)"
        weight_rec = "기본 비중의 1.5배 (최우선 적극 진입)"
    elif marcap_eok >= 6000:
        cap_str, score_marcap = "② 6천억~1조 (중견주)", 8.0
        cap_stat = "승률 28.5% / 손익비 4.19"
        weight_rec = "기본 비중 1.0배 적용"
    elif marcap_eok >= 3000:
        cap_str, score_marcap = "③ 3천억~6천억 (중소형주)", 6.0
        cap_stat = "승률 28.2% / 손익비 3.40"
        weight_rec = "기본 비중 1.0배 적용"
    elif marcap_eok >= 1000:
        cap_str, score_marcap = "④ 1천억~3천억 (소형주)", 4.0
        cap_stat = "승률 24.0% / 손익비 2.77"
        weight_rec = "기본 비중의 0.5배 (비중 축소)"
    else:
        cap_str, score_marcap = "⑤ 1천억 미만 (잡주/초소형주)", 2.0
        cap_stat = "승률 21.4% / 손익비 2.64 (가짜 돌파 휩소 최다 발생 구간)"
        weight_rec = "기본 비중의 0.5배 (철저한 로또용 소액 매매)"

    recent_hits = final_hit[-252:-1].sum() if len(c) > 252 else final_hit[:-1].sum()
    freq_count = int(recent_hits)
    
    # 💡 [누락 지표 복구] 이평선 마스터 고유 변수 추출
    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    cur_momentum = true_momentum_line[-1] if not np.isnan(true_momentum_line[-1]) else 0.0
    cur_spread   = spread_112_224[-1] if not np.isnan(spread_112_224[-1]) else 0.0
    
    # =========================================================================
    # 👑 6. 시그널(S1/S4)과 시가총액 체급을 크로스 체킹한 [맞춤형 동적 전략]
    # =========================================================================
    # =========================================================================
    # 👑 6. 시그널(S1/S4)과 시가총액 체급을 크로스 체킹한 [맞춤형 동적 전략]
    # =========================================================================
    regime_weight = 1.0 # 💡 기본 자본 배분율
    
    if hit_s1_arr[-1]:
        ema_stat_str = "승률 26.5% / 손익비 3.27 (수익성과 방어력 1위)"
        # 👇 고정 숫자를 SYS_CONFIG.get() 으로 감싸서 관제탑의 통제를 받게 함
        score_rs   = scale_score(cur_rs, SYS_CONFIG.get("KR_S1_RS_BEST", 2025.28), SYS_CONFIG.get("KR_S1_RS_WORST", -821.13))
        score_ema  = 10.0 if is_aligned_448[-1] else 5.0
        score_cpv  = scale_score(cur_cpv, SYS_CONFIG.get("KR_S1_CPV_BEST", 0.39), SYS_CONFIG.get("KR_S1_CPV_WORST", 0.95))
        score_bbe  = scale_score(cur_bbe, SYS_CONFIG.get("KR_S1_BBE_BEST", 56.80), SYS_CONFIG.get("KR_S1_BBE_WORST", 3.80))
        score_tb   = scale_score(cur_tb, SYS_CONFIG.get("KR_S1_TB_BEST", 20.13), SYS_CONFIG.get("KR_S1_TB_WORST", 2.47))
        score_freq = 10.0 if 1 <= freq_count <= 5 else 5.0
        
        total_score = (score_rs*10 + score_ema*9 + score_marcap*8 + score_cpv*7 + score_bbe*6 + score_tb*5 + score_freq*4) / 490 * 100
        regime_weight = SYS_CONFIG.get("WEIGHT_KR_MASTER_S1", 1.0)
        
        # [교차 검증] S1 시그널이 어느 체급에 떴는가?
        if marcap_eok >= 3000:
            sig_type = "🔥 S1 (대세 추세 돌파 - 우량/중견주)"
            exit_strategy = "📈 [우량주 대세 추세 추종 전략]\n대형/중견주의 안정적인 448일 정배열 돌파입니다. 승률이 높으므로 비중을 싣고, MFE(최대수익) +15% 이상 도달 시 기계적 익절을 시작하며 '단기데드(EMA20)'로 끝까지 발라먹으십시오. (이탈 시 ZLEMA 방어)"
        else:
            sig_type = "🔥 S1 (대세 추세 돌파 - 소형/테마주)"
            exit_strategy = "📈 [소형주 단기 추세 스윙 전략]\n정배열이긴 하나 체급이 3천억 미만이라 휩소(가짜돌파) 위험이 큽니다. 비중을 절반으로 줄이고, 하락 시 며칠 못 버티고 밀리면 즉각 ZLEMA로 칼손절하여 기회비용을 살리십시오."

    else: # S4
        ema_stat_str = "승률 21.8% / 손익비 2.77 (승률은 낮으나 한방이 큼)"
        score_rs   = scale_score(cur_rs, 500.0, -100.0)
        score_ema  = 5.0
        score_cpv  = scale_score(cur_cpv, 0.1, 0.8)
        score_bbe  = scale_score(cur_bbe, 40.0, 5.0)
        score_tb   = scale_score(cur_tb, 50.0, 5.0)
        score_freq = 6.0
        total_score = (score_bbe*10 + score_cpv*9 + score_tb*8 + score_marcap*7 + score_rs*6 + score_ema*5 + score_freq*4) / 490 * 100
        regime_weight = SYS_CONFIG.get("WEIGHT_KR_MASTER_S4", 1.0)
        
        # [교차 검증] S4 시그널이 어느 체급에 떴는가?
        if marcap_eok >= 3000:
            sig_type = "🔥 S4 (역배열 바닥 탈출 - 우량/중견주)"
            exit_strategy = "🚀 [우량주 바닥턴 묵직한 스윙 전략]\n체급이 큰 종목의 역배열 바닥 매집 타점입니다. 초소형 잡주 로또와 달리 펀더멘탈이 있으므로, 기본 비중으로 진입 후 MFE +20% 이상을 스윙 목표로 삼고 홀딩하십시오."
        else:
            sig_type = "🔥 S4 (역배열 바닥 탈출 - 초소형 텐배거)"
            exit_strategy = "🚀 [초소형 텐배거 로또 전략]\n승률이 20%대로 깡통 위험이 크므로 진입 금액 자체를 대형주의 1/3로 철저히 축소하십시오. 잦은 손절을 만회하려면 한 번 터질 때 MFE +40% 이상 크게 잡고 단기데드로 끝까지 버텨야 합니다."

    total_score = min(max(total_score, 0), 100)

    # 👇👇 [수정] 관제탑 청산 모드 로드 및 문자열 이어붙이기
    ns_prefix = "KR_MASTER_S1" if hit_s1_arr[-1] else "KR_MASTER_S4"
    active_exit_mode = SYS_CONFIG.get("ACTIVE_EXIT_MODE", "HYBRID")
    opt_time_stop    = SYS_CONFIG.get(f"{ns_prefix}_TIME_STOP", 10)
    opt_sl_atr       = SYS_CONFIG.get(f"{ns_prefix}_ATR_SL", 2.0)

    if active_exit_mode == "TECH":
        action_msg = "\n\n📈 <b>[TECH 추세 모드 가동]</b>: 대세 상승장이므로 기계적 타임스탑을 해제하고 차트 추세(ZLEMA/단기데드)를 끝까지 추종하십시오."
    elif active_exit_mode == "STAT":
        action_msg = (f"\n\n🎯 <b>[STAT 통계 모드 가동]</b>: 변동성 장세이므로 차트를 무시하십시오!\n"
                      f"▪️ 진입 후 <b>{opt_time_stop}일 차 종가</b>에 무조건 타임스탑(청산) 하십시오.\n"
                      f"▪️ 진입가 대비 <b>ATR {opt_sl_atr}배</b> 이탈 시 즉각 칼손절하십시오.")
    else:
        action_msg = (f"\n\n⚖️ <b>[HYBRID 공수겸장 가동]</b>: 추세를 타되(ZLEMA 익절),\n"
                      f"▪️ 최대 <b>{opt_time_stop}일</b> 내에 승부를 보십시오.\n"
                      f"▪️ 폭락 시 <b>ATR {opt_sl_atr}배</b>에서 즉각 손절 차단하십시오.")

    regime_msg = f"\n🚨 <b>[관제탑 자본 통제]: 현재 국면 판단에 따라 진입 비중이 기본값의 {regime_weight}배로 강제 조율됩니다.</b>"
    
    # 최종 문자열 합체
    exit_strategy += action_msg + regime_msg

    # =========================================================================
    # 👑 [다중 클러스터 도플갱어 매칭] 5개월(150일) 7차원 궤적 KNN 대조
    # =========================================================================
    match_result = "NONE"
    match_similarity = 0.0
    matched_name = ""

    try:
        if len(c) >= 150:
            # 1. 현재 종목의 150일(5개월) 7D 팩터 연산
            cur_cpv_150 = np.nanmean(cpv[-150:])
            cur_tb_150 = np.nanmean(tb_index[-150:])
            cur_bbe_max = np.nanmax(bb_energy[-20:])
            cur_rs_150 = ((c[-1] - c[-150]) / c[-150]) * 100 if c[-150] != 0 else 0
            
            tr_150 = np.maximum(h[-150:] - l[-150:], np.maximum(abs(h[-150:] - np.roll(c[-150:], 1)), abs(l[-150:] - np.roll(c[-150:], 1))))
            cur_vcp = np.mean(tr_150[-20:]) / np.mean(tr_150) if np.mean(tr_150) > 0 else 1.0
            
            up_v = np.sum(np.where(c[-150:] > o[-150:], v[-150:], 0))
            dn_v = np.sum(np.where(c[-150:] < o[-150:], v[-150:], 0))
            cur_vol = up_v / dn_v if dn_v > 0 else 1.0
            
            cur_emas = [e10[-1], e20[-1], e60[-1], e112[-1], e224[-1]]
            cur_ma = (max(cur_emas) - min(cur_emas)) / min(cur_emas) * 100 if min(cur_emas) > 0 else 0

            # 2. 7차원 유클리디안 거리(유사도) 연산 함수 (오류 방지 0.001 보정)
            def calc_similarity(dna):
                if not dna: return 0.0
                err = (
                    min(abs(cur_cpv_150 - dna.get('cpv', 0.5)) / (dna.get('cpv', 0.5) + 0.001), 1.0) * 0.15 +
                    min(abs(cur_tb_150 - dna.get('tb', 1.0)) / (dna.get('tb', 1.0) + 0.001), 1.0) * 0.15 +
                    min(abs(cur_bbe_max - dna.get('bbe', 0.1)) / (dna.get('bbe', 0.1) + 0.001), 1.0) * 0.15 +
                    min(abs(cur_rs_150 - dna.get('rs', 0)) / (abs(dna.get('rs', 0)) + 0.001), 1.0) * 0.15 +
                    min(abs(cur_vcp - dna.get('vcp', 1.0)) / (dna.get('vcp', 1.0) + 0.001), 1.0) * 0.15 +
                    min(abs(cur_vol - dna.get('vol', 1.0)) / (dna.get('vol', 1.0) + 0.001), 1.0) * 0.10 +
                    min(abs(cur_ma - dna.get('ma', 5.0)) / (dna.get('ma', 5.0) + 0.001), 1.0) * 0.15
                )
                return max(0, 100.0 - (err * 100))

            # 3. 대장주 1~3위 & 참사주 1~3위와 대조하여 가장 닮은 놈(Centroid) 찾기
            best_sim = 0.0
            for i in [1, 2, 3]:
                # Alpha(대박주) 매칭
                a_dna = SYS_CONFIG.get(f"DNA_ALPHA_RANK{i}")
                if a_dna:
                    sim = calc_similarity(a_dna)
                    if sim > best_sim: best_sim, match_result, matched_name = sim, f"ALPHA_{i}", a_dna.get('name', '대장주')
                
                # Trap(참사주) 매칭
                t_dna = SYS_CONFIG.get(f"DNA_TRAP_RANK{i}")
                if t_dna:
                    sim = calc_similarity(t_dna)
                    if sim > best_sim: best_sim, match_result, matched_name = sim, f"TRAP_{i}", t_dna.get('name', '참사주')

            match_similarity = best_sim

            # 4. 80% 이상 매칭 시 전략(exit_strategy) 문구 삽입 및 점수(total_score) 보정
            if match_similarity >= 80.0:
                if "ALPHA" in match_result:
                    rank = match_result.split('_')[1]
                    doppel_msg = (
                        f"\n\n🌌 <b>[대장주 {rank}순위 궤적 매칭! - 유사도 {match_similarity:.1f}%]</b>\n"
                        f"이 종목의 5개월 궤적이 역대 최고 알파 종목이었던 <b>[{matched_name}]</b>의 폭등 전 매집 흐름과 사실상 동일합니다. 세력의 빌드업이 끝났으니 비중을 높이십시오."
                    )
                    exit_strategy += doppel_msg
                    total_score = min(total_score + 10, 100.0) # 대장주 일치 시 강제 가산점 부여
                
                elif "TRAP" in match_result:
                    rank = match_result.split('_')[1]
                    doppel_msg = (
                        f"\n\n💀 <b>[참사/지옥행 {rank}순위 궤적 매칭! - 유사도 {match_similarity:.1f}%]</b>\n"
                        f"🚨 <b>치명적 경고:</b> 과거 계좌를 녹였던 <b>[{matched_name}]</b>의 가짜 돌파(설거지) 궤적과 똑같습니다. 점수를 무시하고 매수 취소!"
                    )
                    exit_strategy += doppel_msg
                    total_score = max(total_score - 30, 0.0) # 참사주 일치 시 치명적 감점
    except Exception as e:
        pass # 에러 시 시스템 정지 방지용 조용히 패스
    # =========================================================================

    # 💡 [정리파일 2 반영] 뱃지 및 CPV 평가 로직
    badge_str = ""
    
    if total_score >= 80: 
        badge_str = "🔥 [1티어 뱃지] 최상위 타점 (평균 손실 -6.6% 철통방어 검증)"
    elif total_score <= 50 and cur_rs > 500: 
        badge_str = "💎 [특급 모멘텀 예외] 소액 로또 접근 허용"
    else: 
        badge_str = "⚠️ [비중 축소] 하위권 점수 타점 (리스크 관리 요망)"

    cpv_comment = ""
    if cur_cpv >= 0.50:
        cpv_comment = "🚨 [한국형 설거지 주의] 돌파 직후 꽉 찬 양봉만 연속으로 그리면 100% 설거지 확률 상승!"
    elif cur_cpv <= 0.23:
        cpv_comment = "💎 [진짜 대장주 캔들] 위아래 꼬리를 지저분하게 달며 악성 매물을 완벽히 소화했습니다."
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
        f"📊 [System B 한국 이평선 마스터 V11.0 완전체 리포트]\n"
        f"🔹 시스템 총점: {total_score:.1f} / 100점\n"
        f"🔹 시가총액 체급: {cap_str}\n"
        f"👉 [체급별 비중 조언]: {weight_rec}\n"
        f"🎖️ {badge_str}\n\n"
        f"💡 [이평선 팩트] {ema_stat_str}\n"
        f"💡 [체급별 팩트] {cap_stat}\n\n"
        f"🔍 [이평선 정밀 분석 지표]\n"
        f"▪️ 진모멘텀(TML): {cur_momentum:,.1f}\n"
        f"▪️ 중기선 이격도(Spread): {cur_spread:.1f}%\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.2f}\n"
        f"▪️ 응축에너지(BB): {cur_bbe:.1f}\n"
        f"▪️ 시장상대강도(RS): {cur_rs:.1f}%\n"
        f"💡 [상대평가] RS 상위 {(10 - dyn_rs_score) * 11.1:.1f}% / 찐양봉 상위 {(10 - dyn_tb_score) * 11.1:.1f}%\n"
    )
    
    if cpv_comment != "":
        v11_comment += f"\n{cpv_comment}\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "last_close": float(c[-1]),
        "recommend": exit_strategy,
        "v11_comment": v11_comment,
        "score": total_score,
        "v_cpv": cur_cpv,
        "v_yang": cur_tb,
        "v_energy": cur_bbe,
        "v_rs": cur_rs,
        "dyn_rs_score": dyn_rs_score,
        "dyn_cpv_score": dyn_cpv_score,
        "dyn_tb_score": dyn_tb_score
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
        
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()
    
    def worker(row_tuple):
        try:
            _, row = row_tuple
            name, code = row["Name"], row["Code"]
            marcap = row.get("Marcap", 0)
            df_raw = None
            
            try:
                df_raw = get_safe_data(code, start_date)
            except: 
                pass

            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            hit, sig_type, df, dbg = False, "", None, {}
            
            if is_valid: 
                idx_close = kospi_idx if row["Market"] == 'KOSPI' else kosdaq_idx
                # 💡 [수정 완료] 없는 함수인 compute_5ema_signal 대신 올바른 마스터 함수로 변경했습니다.
                hit, sig_type, df, dbg = compute_korea_master_signal(df_raw, idx_close, marcap)
            
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
                        except: 
                            pass
                    
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
                            q_main.put((main_chart_path, main_caption))

                            # 💡 [오토 포워드 장부 기록] - 한국장 전용 (모든 필터 포함)
                            try:
                                import auto_forward_tester as aft
                                market_type = 'KR'
                                entry_facts = {
                                   'v_rs': dbg.get('v_rs', 0),
                                   'v_cpv': dbg.get('v_cpv', 0),
                                   'v_yang': dbg.get('v_yang', 0),
                                   'v_energy': dbg.get('v_energy', 0),
                                   'marcap_eok': marcap / 100000000,    # 👈 한국장만 있음
                                   'score_marcap': dbg.get('score_marcap', 0), # 👈 한국장만 있음
                                   'freq_count': dbg.get('freq_count', 0),
                        
                                   'dyn_rs': dbg.get('dyn_rs_score', 0),
                                   'dyn_cpv': dbg.get('dyn_cpv_score', 0),
                                   'dyn_tb': dbg.get('dyn_tb_score', 0),
                        
                                   'is_tenbagger': 1 if dbg.get('is_tenbagger') else 0, # 👈 한국장만 있음
                                   'is_top_dna': 1 if dbg.get('is_top_dna') else 0,     # 👈 한국장만 있음
                                   'is_worst_dna': 1 if dbg.get('is_worst_dna') else 0, # 👈 한국장만 있음
                                   'is_death_combo': 1 if dbg.get('is_death_combo') else 0
                                }
                    
                                success, fwd_msg = aft.try_add_virtual_position(
                                   market=market_type, 
                                   code=code, name=name,
                                   sig_type=dbg.get('sig_type', ''), 
                                   score=dbg.get('score', 0), 
                                   ep=dbg.get('last_close', 0), 
                                   facts=entry_facts, 
                                   sector=sector_info
                                )
                                print(f"   ↳ [포워드 장부 기록]: {fwd_msg}")
                            except Exception as e:
                                print(f"   ↳ [포워드 장부 에러]: {e}")
 
                            # 💡 4. 홍보용 캡션 (한국장에 맞게 원화로 픽스)
                            promo_caption = (
                                f"📈 [알고리즘 차트 포착]\n\n"
                                f"🏢 종목: {name} ({code})\n"
                                f"🏷️ 섹터: {sector_info}\n"
                                f"💰 현재가: {dbg.get('last_close', 0):,.0f}원"
                            )
                            q_promo.put((promo_chart_path, promo_caption))

                        print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")
        except Exception as e:
            # 💡 [에러 추적용] 나중에 또 이유 없이 포착이 안될 때 원인을 알 수 있도록 출력문을 추가했습니다.
            print(f"⚠️ Worker 구동 중 에러 발생 [{row_tuple[1].get('Name', 'Unknown')}]: {e}")

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
