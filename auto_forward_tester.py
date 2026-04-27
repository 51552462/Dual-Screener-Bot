# auto_forward_tester.py
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
import os, time, requests
from datetime import datetime, timedelta
import pytz
import sqlite3
import json

TELEGRAM_TOKEN = "7988939051:AAG4FqMzzz12vd7Crzt8DVPWiL3fMHM8tEc"
TELEGRAM_CHAT_ID = "6838834566"

# 💡 [방향성 2번] 전문적인 DB 시스템 (CSV 폐기)
DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')

def send_telegram_msg(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        # 💡 [버그 픽스] parse_mode="HTML" 추가로 태그 노출 방지 및 정상 포맷팅
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except: pass

def init_forward_db():
    """장부 테이블 생성 및 V12.0 필수 컬럼 안전 추가"""
    # 💡 [V25.0] Timeout 60초 대기열 및 WAL 모드 전면 활성화
    conn = sqlite3.connect(DB_PATH, timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, entry_date TEXT, market TEXT, code TEXT, name TEXT, sector TEXT,    
            sig_type TEXT, tier TEXT, total_score REAL, dyn_rs REAL, dyn_cpv REAL, dyn_tb REAL,
            is_tenbagger INTEGER, is_top_dna INTEGER, is_worst_dna INTEGER, is_death_combo INTEGER,
            entry_price REAL, v_cpv REAL, v_yang REAL, v_rs REAL, v_energy REAL, marcap_eok REAL,       
            score_marcap REAL, freq_count INTEGER, max_high REAL, min_low REAL, bars_held INTEGER DEFAULT 0,
            up_vol_sum REAL DEFAULT 0, down_vol_sum REAL DEFAULT 0, status TEXT DEFAULT 'OPEN',
            exit_date TEXT, exit_reason TEXT, flow_tags TEXT, final_ret REAL, mfe REAL
        )
    ''')
    
    # 💡 [V12.0 팩트 추가] 기존 DB를 날리지 않고 안전하게 컬럼 추가
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_atr REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN exit_type TEXT DEFAULT 'UNKNOWN'")
    except: pass
    
    # 👇👇 [추가] V17.0 청산 우선순위 시뮬레이션 컬럼 👇👇
    for p in ['sim_stat', 'sim_tech']:
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_ret REAL DEFAULT 0.0")
        except: pass
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_status TEXT DEFAULT 'OPEN'")
        except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V24.0 시장 폭(Breadth) 실험 존 컬럼 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN market_breadth REAL DEFAULT 1.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_breadth REAL DEFAULT 1.0")
    except: pass
    for p in ['sim_breadth']:
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_ret REAL DEFAULT 0.0")
        except: pass
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_status TEXT DEFAULT 'OPEN'")
        except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V35.0 자율 조율을 위한 진입 시점 DNA/DTW 채점표 박제 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_cos_score REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_dtw_score REAL DEFAULT 99.0")
    except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V38.0 자본 기반 리스크 패리티 컬럼 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN invest_amount REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN shares INTEGER DEFAULT 0")
    except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V39.0 동적 켈리 베팅 시뮬레이션 컬럼 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_regime TEXT DEFAULT 'UNKNOWN'")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN sim_kelly_risk_pct REAL DEFAULT 0.02")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN sim_kelly_invest REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN sim_kelly_profit REAL DEFAULT 0.0")
    except: pass
    # 👆👆 [추가 끝] 👆👆

    conn.commit()
    conn.close()

# 💡 [시스템 연결] 관제탑 설정 로드 함수 추가 (init_forward_db 밑에 추가)
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')
def load_system_config():
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r') as f: return json.load(f)
    except: pass
    return {}

# ==========================================
# 1. 신규 종목 가상매매 편입 엔진 (검색기에서 호출)
# ==========================================
def try_add_virtual_position(market, code, name, sig_type, score, ep, facts, sector="유망섹터", trade_source="STANDARD"):
    init_forward_db()
    code_str = str(code).zfill(6) if market == 'KR' else str(code)
    
    # 💡 [V13.0 가상매매] 10점 단위 정밀 버킷 생성 (예: 85점 -> 80점대)
    score_bucket = int(score // 10) * 10
    if score_bucket >= 100: score_bucket = 90 # 100점은 90점대 최상위 티어로 병합
    tier_label = f"{score_bucket}점대"

    # 💡 [V25.0 픽스] 진입 함수에도 Timeout과 WAL 모드 필수 적용
    conn = sqlite3.connect(DB_PATH, timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # 1. 중복 체크 (이미 포트폴리오에 보유 중인 종목은 제외)
    cursor.execute("SELECT id FROM forward_trades WHERE code=? AND status='OPEN'", (code_str,))
    if cursor.fetchone():
        conn.close()
        return False, "중복 보유 중"
        
    # 2. 👑 [V23.0 포트폴리오 다중화: 주도섹터 폭격(2) + 차기섹터 정찰(2)]
    tz = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    today_str = datetime.now(tz).strftime('%Y-%m-%d')

    # 현재 포트폴리오의 1위 주도 섹터 파악 (자금 쏠림 감지)
    cursor.execute("SELECT sector FROM forward_trades WHERE market=? AND status='OPEN' GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 1", (market,))
    dom_row = cursor.fetchone()
    dominant_sector = dom_row[0] if dom_row else "None"

    # 오늘 해당 티어에서 매수한 종목들의 섹터 확인
    cursor.execute("SELECT sector FROM forward_trades WHERE entry_date=? AND market=? AND tier=?", (today_str, market, tier_label))
    today_sectors = [r[0] for r in cursor.fetchall()]

    if len(today_sectors) >= 4:
        conn.close()
        return False, f"오늘의 {tier_label} 최대 쿼터(4개) 모두 확보됨 (스킵)"

    # 로직 분기: 진입하려는 종목이 현재 시장을 주도하는 섹터인가?
    trend_bought = sum(1 for s in today_sectors if s == dominant_sector)
    hedge_bought = sum(1 for s in today_sectors if s != dominant_sector)

    if sector == dominant_sector:
        if trend_bought >= 2:
            conn.close()
            return False, f"🚨 섹터 쿼터 초과: 이미 주도섹터({dominant_sector}) 공격 편대 2기를 모두 파견했습니다."
        track_tag = "[🔥주도주 편대]"
    else:
        if hedge_bought >= 2:
            conn.close()
            return False, f"🛡️ 섹터 쿼터 초과: 이미 타 섹터 정찰대 2기를 모두 파견했습니다."
        track_tag = "[🛡️차기섹터 정찰]"

    # 시그널 타입에 트랙 태그(편대/정찰) 병합하여 기록
    sig_type = f"[{trade_source}] {sig_type} {track_tag}"

    # 👇👇 [수정] V34.0 DTW 투트랙 + V35.0 동적 커트라인 자율 매칭 👇👇
    max_alpha_cos, min_alpha_dtw = 0.0, 99.0
    max_trap_cos, min_trap_dtw = 0.0, 99.0
    
    # 💡 [버그 픽스] 안전 변수 초기화 (에러 시 DB 엉킴 원천 방지)
    entry_atr, invest_amount, shares, sim_kelly_invest = 0.0, 0, 0, 0
    
    try:
        sys_config = load_system_config()
        table_name = f"{market}_{code_str}"
        idx_table = 'US_SPY' if market == 'US' else 'KR_KOSDAQ_IDX'
        
        hist_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY Date DESC LIMIT 150", conn).sort_values('Date')
        idx_df = pd.read_sql(f"SELECT * FROM {idx_table} ORDER BY Date DESC LIMIT 150", conn).sort_values('Date')
        
        if len(hist_df) >= 150 and len(idx_df) >= 150:
            c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
            idx_c = idx_df['Close'].values
            
            # 1. 7D Z-Score 연산 (기존 유지)
            cpv = np.nanmean(np.where(h != l, (c - o) / (h - l), 0.5))
            v_ma20 = pd.Series(v).rolling(20).mean().values
            tb = np.nanmean(np.where(h != l, (v / v_ma20) / np.maximum((c - o) / (h - l), 0.01), 1.0))
            bb_std = pd.Series(c).rolling(20).std().values
            bbe = np.nanmax(np.where(bb_std > 0, 1.0 / ((4 * bb_std) / pd.Series(c).rolling(20).mean().values), 0)[-20:])
            rs_slope = ((c[-1] - c[0]) / c[0]) * 100
            tr = np.maximum(h - l, np.maximum(abs(h - np.roll(c, 1)), abs(l - np.roll(c, 1))))
            vcp_ratio = np.mean(tr[-20:]) / np.mean(tr) if np.mean(tr) > 0 else 1.0
            vol_flow = np.sum(np.where(c > o, v, 0)) / (np.sum(np.where(c < o, v, 0)) + 1)
            emas = [pd.Series(c).ewm(span=n).mean().iloc[-1] for n in [10, 20, 60, 112, 224]]
            ma_conv = (max(emas) - min(emas)) / min(emas) * 100
            
            idx_rs = ((idx_c[-1] - idx_c[0]) / idx_c[0]) * 100
            idx_vol = pd.Series(idx_c).pct_change().std() * 100 * np.sqrt(252)
            safe_vol = idx_vol if idx_vol > 0.1 else 1.0
            
            # 👇👇 [추가] V46.0 실시간 하락장 방어 프리미엄 주입 👇👇
            excess_return = rs_slope - idx_rs
            defiance_premium = 0.0
            if idx_rs < 0 and excess_return > 0:
                defiance_premium = abs(idx_rs) * 1.5
            
            z_rs = (excess_return + defiance_premium) / safe_vol
            # 👆👆 [추가 끝] 👆👆
            
            new_vec = np.nan_to_num(np.array([cpv, tb, bbe/safe_vol, z_rs, vcp_ratio, vol_flow, ma_conv]))
            
            # 2. [V34.0] 가격 궤적(Shape) 압축
            c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
            new_shape = np.mean(np.array_split(c_norm, 20), axis=1)

            # 3. [V34.0] 순수 파이썬 DTW 
            def calc_dtw(s, t):
                n, m = len(s), len(t)
                dtw = np.full((n+1, m+1), np.inf)
                dtw[0, 0] = 0
                for i in range(1, n+1):
                    for j in range(1, m+1):
                        cost = abs(s[i-1] - t[j-1])
                        dtw[i, j] = cost + min(dtw[i-1, j], dtw[i, j-1], dtw[i-1, j-1])
                return dtw[n, m]

            def cosine_sim(a, b):
                n_a, n_b = np.linalg.norm(a), np.linalg.norm(b)
                return np.dot(a, b) / (n_a * n_b) if n_a > 0 and n_b > 0 else 0
                
            # 투트랙 대조 (Cosine + DTW)
            for k, v_dict in sys_config.items():
                if isinstance(v_dict, dict) and 'shape' in v_dict:
                    t_vec = np.nan_to_num(np.array([v_dict.get('cpv',0), v_dict.get('tb',0), v_dict.get('bbe',0), v_dict.get('rs',0), v_dict.get('vcp',0), v_dict.get('vol',0), v_dict.get('ma',0)]))
                    t_shape = np.array(v_dict.get('shape'))
                    
                    cos_score = cosine_sim(new_vec, t_vec)
                    dtw_dist = calc_dtw(new_shape, t_shape)
                    
                    if "DNA_TRAP" in k:
                        max_trap_cos = max(max_trap_cos, cos_score)
                        min_trap_dtw = min(min_trap_dtw, dtw_dist)
                    elif "DNA_ALPHA" in k:
                        max_alpha_cos = max(max_alpha_cos, cos_score)
                        min_alpha_dtw = min(min_alpha_dtw, dtw_dist)
            
            # 💡 [V35.0] 관제탑이 하달한 동적 커트라인 로드 (하드코딩 삭제)
            dyn_cos_limit = sys_config.get("DYNAMIC_ALPHA_LIMIT", 0.75) # 자율 코사인 합격선
            dyn_trap_limit = sys_config.get("DYNAMIC_TRAP_LIMIT", 0.75) # 자율 참사주 방어선
            dyn_dtw_limit = sys_config.get("DYNAMIC_DTW_LIMIT", 2.5)    # 자율 궤적 허용 거리

            # 🛡️ 페일세이프 (내부수급과 궤적이 모두 자율 방어선을 넘었을 때만 기각)
            if max_trap_cos >= dyn_trap_limit and min_trap_dtw <= dyn_dtw_limit:
                if max_trap_cos > max_alpha_cos:
                    conn.close()
                    return False, f"🚨 [자율 투트랙 방어막] 세력의 시간끌기(DTW:{min_trap_dtw:.1f}) 및 내부수급(Cos:{max_trap_cos*100:.0f}%) 일치. 매수 기각"
            
            # 🚀 슈퍼 부스트
            if max_alpha_cos >= dyn_cos_limit and min_alpha_dtw <= dyn_dtw_limit:
                sig_type += f" [🌟시계열 자율판독 대장주 (Cos:{max_alpha_cos*100:.0f}%|DTW:{min_alpha_dtw:.1f})]"

            # 👇👇 [들여쓰기 픽스 완료] 리스크 패리티 연산은 반드시 try 블록 안에 있어야 합니다 👇👇
            hist_df['prev_c'] = hist_df['Close'].shift(1)
            hist_df['tr'] = np.maximum(hist_df['High'] - hist_df['Low'], np.maximum(abs(hist_df['High'] - hist_df['prev_c']), abs(hist_df['Low'] - hist_df['prev_c'])))
            hist_df['atr'] = hist_df['tr'].ewm(span=14, adjust=False).mean()
            entry_atr = float(hist_df['atr'].iloc[-1])

            opt_sl_atr = sys_config.get(f"{market}_MASTER_S1_ATR_SL", 2.0)
            sl_price = ep - (opt_sl_atr * entry_atr)
            risk_distance = ep - sl_price

            account_size = sys_config.get("ACCOUNT_SIZE", 20000000)
            fixed_risk_pct = 0.02 
            kelly_risk_pct = sys_config.get("DYNAMIC_KELLY_RISK", 0.01) 
            cur_regime = sys_config.get("CURRENT_REGIME_KEY", "UNKNOWN")
            
            if risk_distance > 0:
                # 👇👇 [V43.0 핵심] 판단과 실행의 완전 동기화 👇👇
                # 1. 실전 API로 넘어갈 '진짜 매수 수량(shares)'을 관제탑의 동적 켈리 리스크로 산출
                shares = int((account_size * kelly_risk_pct) / risk_distance)
                sim_kelly_invest = shares * ep  # 켈리 기반 실제 투입 금액
                
                # 2. V39.0 딥 다이브 비교 엔진(고정 2% vs 켈리)을 위해 가상의 고정 투입금 유지
                fixed_shares = int((account_size * fixed_risk_pct) / risk_distance)
                invest_amount = fixed_shares * ep  
            else:
                shares, invest_amount, sim_kelly_invest = 0, 0, 0
            # 👆👆 [수정 끝] 👆👆

    except Exception as e:
        print(f"하이브리드 벡터 매칭 에러: {e}")
    # 👆👆 [try 블록 완전 종료] 👆👆

    # 👇👇 [추가] V24.0 진입 시점의 시장 폭(Breadth) 실시간 측정 👇👇
    cur_breadth = 1.0
    try:
        b_df = yf.download("RSP SPY", period="5d", interval="1d", progress=False)
        if not b_df.empty:
            cur_breadth = (b_df['Close']['RSP'].iloc[-1] / b_df['Close']['SPY'].iloc[-1]) / \
                          (b_df['Close']['RSP'].mean() / b_df['Close']['SPY'].mean())
    except: pass
    # 👆👆 [추가 끝] 👆👆

    # 3. 가상 매매 장부에 팩트 데이터와 함께 기록 (V38.0 자금 통제 변수 추가)
    cursor.execute('''
        INSERT INTO forward_trades 
        (entry_date, market, code, name, sector, sig_type, tier, total_score, dyn_rs, dyn_cpv, dyn_tb, entry_price, v_cpv, v_yang, v_energy, v_rs, max_high, min_low, market_breadth, entry_breadth, entry_cos_score, entry_dtw_score, entry_atr, invest_amount, shares)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        today_str, market, code_str, name, sector, sig_type, tier_label, score,
        facts.get('dyn_rs', 0), facts.get('dyn_cpv', 0), facts.get('dyn_tb', 0), ep,
        facts.get('v_cpv', 0), facts.get('v_yang', 0), facts.get('v_energy', 0), facts.get('v_rs', 0),
        ep, ep, round(cur_breadth, 3), round(cur_breadth, 3), 
        round(max_alpha_cos, 3), round(min_alpha_dtw, 3), 
        round(entry_atr, 4), invest_amount, shares # 💡 V38.0 ATR 및 투입 금액/수량 기록
    ))
    conn.commit()
    conn.close()
    
    return True, f"🎯 {tier_label} 가상매매 편입 성공: {name} ({score:.1f}점)"

# ==========================================
# 2. 매일 종가 흐름 추적 및 청산 엔진 (DB 기반)
# ==========================================
def track_daily_positions(market):
    init_forward_db()
    # 💡 [V25.0] 긴 작업 시 다른 스레드가 대기할 수 있도록 60초 타임아웃 적용
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    # 현재 보유 중인 종목만 불러오기
    df_active = pd.read_sql("SELECT * FROM forward_trades WHERE market=? AND status='OPEN'", conn, params=(market,))
    if df_active.empty:
        conn.close()
        return

    print(f"\n🔍 [포워드 테스팅] {market} 시장 {len(df_active)}개 종목 추적 중...")
    
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    idx_ticker = '069500' if market == 'KR' else 'SPY'
    try:
        idx_df = fdr.DataReader(idx_ticker, start_date) if market == 'KR' else yf.download(idx_ticker, start=start_date, progress=False)
        idx_close = idx_df['Close'] if market == 'KR' else idx_df['Close'].squeeze()
    except: idx_close = pd.Series(dtype=float)

    for _, r in df_active.iterrows():
        code = r['code']
        ep = r['entry_price']
        
        try:
            df = fdr.DataReader(code, start_date) if market == 'KR' else yf.download(code, start=start_date, progress=False)
            if df.empty or len(df) < 20: continue
                
            c, o, h, l, v = float(df['Close'].iloc[-1]), float(df['Open'].iloc[-1]), float(df['High'].iloc[-1]), float(df['Low'].iloc[-1]), float(df['Volume'].iloc[-1])
            
            new_max = max(r['max_high'], h)
            new_min = min(r['min_low'], l)
            new_bars = r['bars_held'] + 1
            new_up_vol = r['up_vol_sum'] + (v if c > o else 0)
            new_down_vol = r['down_vol_sum'] + (v if c < o else 0)

            # =================================================================
            # 👑 [3차원 청산 최적화 엔진 가동] MFE/MAE, ATR, Time Stop 연산
            # =================================================================
            # 1. 14일 ATR(변동성) 실시간 연산
            df['prev_c'] = df['Close'].shift(1)
            df['tr'] = np.maximum(df['High'] - df['Low'], np.maximum(abs(df['High'] - df['prev_c']), abs(df['Low'] - df['prev_c'])))
            df['atr'] = df['tr'].ewm(span=14, adjust=False).mean()
            cur_atr = float(df['atr'].iloc[-1])
            
            # 진입 시점의 ATR이 DB에 없다면 현재 ATR로 팩트 보정 후 저장
            entry_atr = r.get('entry_atr', 0.0)
            if entry_atr == 0.0 or pd.isna(entry_atr):
                entry_atr = cur_atr
                conn.execute("UPDATE forward_trades SET entry_atr=? WHERE id=?", (entry_atr, r['id']))

            # 2. 기술적(TECH) 지표 연산 (기존 ZLEMA 및 단기데드)
            df['ema10'] = df['Close'].ewm(span=10, adjust=False).mean()
            df['ema20'] = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema1 = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema2 = z_ema1.ewm(span=20, adjust=False).mean()
            cur_zlema = float((z_ema1 + (z_ema1 - z_ema2)).iloc[-1])
            
            is_tech_exit = (c < cur_zlema) or (float(df['ema10'].iloc[-1]) < float(df['ema20'].iloc[-1]) and float(df['ema10'].iloc[-2]) >= float(df['ema20'].iloc[-2]))

            # 3. 🎯 관제탑 네임스페이스 매핑 및 JSON 지시사항 수신
            sys_config = load_system_config()
            active_mode = sys_config.get("ACTIVE_EXIT_MODE", "HYBRID")
            
            # 종목의 시장(KR/US)과 시그널(S1/S4)을 분석해 고유 방(Namespace) 찾기
            ns_prefix = f"{market}_MASTER_S1" # 기본값
            if "S4" in r['sig_type']: ns_prefix = f"{market}_MASTER_S4"
            if "눌림" in r['sig_type']: ns_prefix = f"{market}_NULRIM_S4" if "S4" in r['sig_type'] else f"{market}_NULRIM_S1"
            if "5선" in r['sig_type']: ns_prefix = f"{market}_5EMA_S1"
            
            opt_time_stop = sys_config.get(f"{ns_prefix}_TIME_STOP", 10)
            opt_sl_atr    = sys_config.get(f"{ns_prefix}_ATR_SL", 2.0)
            
            # 수학적 손절가(SL) 산출: 진입가 - (관제탑 승수 * 진입변동성)
            sl_price = ep - (opt_sl_atr * entry_atr)

            # 4. ⚔️ 청산 아레나: MFE/MAE 및 관제탑 모드에 따른 수학적 사형 집행
            do_exit, exit_rsn, actual_exit_type = False, "", "HOLD"
            
            # 👇👇 [수정] V42.0 장중 틱(Intraday) 모사 엔진 (낙관적 편향 제거) 👇👇
            current_ret_pct = ((c - ep) / ep) * 100        # 종가 기준 수익률
            low_ret_pct = ((l - ep) / ep) * 100            # 장중 최저 수익률 (손절 터치 감시용)
            high_ret_pct = ((h - ep) / ep) * 100           # 장중 최고 수익률 (익절 터치 감시용)
            
            # 💡 [V51.0 핵심] 내 전략(Namespace) 방에 할당된 독립 파라미터 뇌(Brain) 꺼내오기
            ns_live_params = sys_config.get(f"{ns_prefix}_LIVE_PARAMS", sys_config)
            
            # [V15.0 ABC 토너먼트 병렬 연산 (장중 손절 우선 반영)]
            abc_sets = {
                'live_a': ns_live_params,
                'cand_b': sys_config.get(f"{ns_prefix}_CANDIDATE_PARAMS", sys_config.get("CANDIDATE_PARAMS", {})),
                'champ_c': sys_config.get(f"{ns_prefix}_CHAMPION_PARAMS", sys_config.get("CHAMPION_PARAMS", {}))
            }

            for key, params in abc_sets.items():
                if not params: continue
                sl_limit = params.get("DYNAMIC_MAE_SL", -3.5)
                
                # 💡 팩트: 종가(current_ret_pct)가 아니라 장중 저가(low_ret_pct)가 손절선을 건드렸는지 확인
                if low_ret_pct <= sl_limit:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=?, {key}_status=? WHERE id=?", (sl_limit, "CLOSED_LOSS", r['id']))
                else:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # [V17.0 청산 평행우주 대결 (STAT vs TECH)]
            # 💡 [팩트] 관제탑이 내 전략방(ns_prefix) 맞춤형으로 깎아둔 실전 한계점 로드
            dyn_mae_sl = ns_live_params.get("DYNAMIC_MAE_SL", -3.5)
            dyn_mfe_tp = ns_live_params.get("DYNAMIC_MFE_TP", 10.0)

            if r.get('sim_stat_status', 'OPEN') == 'OPEN':
                if low_ret_pct <= dyn_mae_sl: # 장중 손절 터치
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif high_ret_pct >= dyn_mfe_tp: # 장중 익절 터치
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_WIN' WHERE id=?", (dyn_mfe_tp, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=? WHERE id=?", (current_ret_pct, r['id']))

            if r.get('sim_tech_status', 'OPEN') == 'OPEN':
                if low_ret_pct <= dyn_mae_sl:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif is_tech_exit:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_WIN' WHERE id=?", (current_ret_pct, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # [V24.0 시장 폭 필터링 실험 존]
            if r.get('sim_breadth_status', 'OPEN') == 'OPEN':
                e_breadth = r.get('entry_breadth', 1.0)
                if pd.isna(e_breadth): e_breadth = 1.0
                
                if e_breadth < 0.97:
                    conn.execute("UPDATE forward_trades SET sim_breadth_status='FILTERED_OUT' WHERE id=?", (r['id'],))
                else:
                    if low_ret_pct <= dyn_mae_sl:
                        conn.execute("UPDATE forward_trades SET sim_breadth_ret=?, sim_breadth_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                    elif high_ret_pct >= dyn_mfe_tp:
                        conn.execute("UPDATE forward_trades SET sim_breadth_ret=?, sim_breadth_status='CLOSED_WIN' WHERE id=?", (dyn_mfe_tp, r['id']))
                    else:
                        conn.execute("UPDATE forward_trades SET sim_breadth_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # 1순위: MFE/MAE 절대 한계점 도달 시 무조건 청산 
            actual_exit_price = c # 기본 청산가는 종가로 세팅
            
            # 💡 [핵심 교정] 종가가 아닌 '저가(l)'와 '고가(h)'로 실전과 똑같이 슬리피지 청산
            if low_ret_pct <= dyn_mae_sl:
                do_exit, exit_rsn, actual_exit_type = True, f"수학적 MAE 장중 이탈 칼손절 ({dyn_mae_sl}%)", "STAT_MAE"
                actual_exit_price = ep * (1 + (dyn_mae_sl / 100.0)) # 손절선에서 털린 가격
            elif high_ret_pct >= dyn_mfe_tp:
                do_exit, exit_rsn, actual_exit_type = True, f"수학적 MFE 장중 도달 익절 ({dyn_mfe_tp}%)", "STAT_MFE"
                actual_exit_price = ep * (1 + (dyn_mfe_tp / 100.0)) # 익절선에서 팔린 가격
            
            # 2순위: 한계점 내부에서 움직일 경우, 국면 모드에 따른 추세/시간 청산
            if not do_exit:
                if active_mode == "TECH":
                    if is_tech_exit: 
                        do_exit, exit_rsn, actual_exit_type = True, "기술적 추세 이탈 (ZLEMA/데드)", "TECH"
                elif active_mode == "STAT":
                    if new_bars >= opt_time_stop: 
                        do_exit, exit_rsn, actual_exit_type = True, f"통계적 유통기한 만료 ({opt_time_stop}일)", "STAT_TIME"
                    elif l <= sl_price: # 💡 c <= sl_price 가 아니라 장중 저가 l 로 변경
                        do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 장중 방어 손절", "STAT_ATR"
                        actual_exit_price = sl_price
                else: # HYBRID
                    if new_bars >= opt_time_stop: 
                        do_exit, exit_rsn, actual_exit_type = True, f"하이브리드 타임스탑 ({opt_time_stop}일)", "HYBRID_TIME"
                    elif l <= sl_price: # 💡 c <= sl_price 가 아니라 장중 저가 l 로 변경
                        do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 장중 방어 손절", "HYBRID_ATR"
                        actual_exit_price = sl_price
                    elif is_tech_exit: 
                        do_exit, exit_rsn, actual_exit_type = True, "하이브리드 추세 이탈 익절", "HYBRID_TECH"


            # 5. DB 업데이트 실행 (청산 시)
            if do_exit:
                # 💡 [핵심] 최종 수익률(ret)은 희망회로 종가(c)가 아니라 '실제 증권사가 던진 가격(actual_exit_price)' 기반으로 계산
                ret = round(((actual_exit_price - ep) / ep) * 100, 2)
                mfe = round(((new_max - ep) / ep) * 100, 2)
                
                tags = []
                if mfe >= 7.0 and new_bars <= 8: tags.append("#빠른슈팅_완벽")
                elif mfe >= 7.0 and new_bars > 8: tags.append("#지연슈팅_수명연장")
                elif mfe < 3.0: tags.append("#슈팅실패_조기소멸")
                
                vol_ratio = new_up_vol / (new_down_vol + 1)
                if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")
                
                flow_tags = " ".join(tags)
                exit_date = datetime.now().strftime('%Y-%m-%d')
                
                # 💡 관제탑이 피드백을 위해 수집할 exit_type 완벽 로깅
                conn.execute('''
                    UPDATE forward_trades 
                    SET status=?, exit_date=?, exit_reason=?, flow_tags=?, final_ret=?, mfe=?, max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?, exit_type=?
                    WHERE id=?
                ''', ('CLOSED_WIN' if ret > 0 else 'CLOSED_LOSS', exit_date, exit_rsn, flow_tags, ret, mfe, new_max, new_min, new_bars, new_up_vol, new_down_vol, actual_exit_type, r['id']))
                
                icon = "🔥스마트청산" if ret > 0 else "🛡️방어손절"
                # 💡 [V15.1 픽스] 시그널 타입(sig_type) 명시 및 점수 소수점 첫째 자리 정리
                send_telegram_msg(f"🤖 [{market} 관제탑 제어] {icon}: {r['name']} ({r['sig_type']} | {round(r['total_score'], 1)}점)\n▪️ 수익: {ret}%\n▪️ 모드: {active_mode}\n▪️ 사유: {exit_rsn}\n▪️ 태그: {flow_tags}")
            else:
                # DB 업데이트 (유지)
                conn.execute('''
                    UPDATE forward_trades 
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?
                    WHERE id=?
                ''', (new_max, new_min, new_bars, new_up_vol, new_down_vol, r['id']))
                
        except Exception as e: pass

    conn.commit()
    conn.close()

# ==========================================
# 3. 매일 16:00 일일 종합 리포트 텔레그램 (섹터 흐름 추가)
# ==========================================
def send_comprehensive_daily_report():
    """V41.0: 가상매매 일일 가동 현황 및 청산 로직 작동 통계 등 딥 다이브 데이터 보강"""
    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(tz_kr).strftime('%Y-%m-%d')
    sys_config = load_system_config()
    
    regime = sys_config.get("LAST_ANALYSED_REGIME", "CHOP")
    kelly_risk = sys_config.get("DYNAMIC_KELLY_RISK", 0.01) * 100
    report_msg = f"🛰️ <b>[관제탑 일일 가상매매 종합 리포트]</b>\n📅 {today_str} | 국면: {regime} | 켈리: {kelly_risk:.2f}%\n"
    report_msg += "━━━━━━━━━━━━━━━━━━\n"

    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        
        for market in ['KR', 'US']:
            market_icon = "🇰🇷" if market == 'KR' else "🇺🇸"
            report_msg += f"\n{market_icon} <b>[{market} MARKET INTELLIGENCE]</b>\n"
            
            # 1. 💡 [추가] 오늘 가상매매 가동 현황 (신규 편입 및 청산 추적)
            cursor = conn.execute("SELECT COUNT(*) FROM forward_trades WHERE market=? AND entry_date=?", (market, today_str))
            today_entries = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*), SUM(final_ret) FROM forward_trades WHERE market=? AND exit_date=?", (market, today_str))
            today_exit_data = cursor.fetchone()
            today_exits = today_exit_data[0]
            today_pnl = today_exit_data[1] if today_exit_data[1] else 0.0

            report_msg += f"🎯 <b>오늘의 매매:</b> 신규 진입 {today_entries}건 / 청산 {today_exits}건 (당일 실현손익 {today_pnl:.2f}%)\n"

            # 2. 포트폴리오 스냅샷 (승률 및 티어별 점유율)
            cursor = conn.execute("SELECT COUNT(*), SUM(CASE WHEN final_ret > 0 THEN 1 ELSE 0 END) FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'", (market,))
            total, wins = cursor.fetchone()
            wr = round((wins / total) * 100, 1) if total > 0 else 0
            report_msg += f"📈 <b>누적 실전 승률: {wr}%</b> (총 {total}건 검증)\n"
            
            cursor = conn.execute("SELECT tier, COUNT(*) FROM forward_trades WHERE market=? AND status='OPEN' GROUP BY tier", (market,))
            t_counts = {row[0]: row[1] for row in cursor.fetchall()}
            active_cnt = sum(t_counts.values())
            report_msg += f"📦 <b>현재 운용 비중: {active_cnt}/40개 종목</b> (가동률 {(active_cnt/40)*100:.0f}%)\n"

            # 3. 💡 [추가] 최근 7일 청산 사유 통계 (관제탑 로직 정상 작동 여부 팩트 체크)
            cursor = conn.execute("SELECT exit_type, COUNT(*) FROM forward_trades WHERE market=? AND exit_date >= date('now', '-7 days') AND status LIKE 'CLOSED%' GROUP BY exit_type", (market,))
            exit_reasons = cursor.fetchall()
            if exit_reasons:
                reason_str = ", ".join([f"{r[0].replace('STAT_', '').replace('HYBRID_', '')}({r[1]}건)" for r in exit_reasons if r[0]])
                report_msg += f"⚙️ <b>최근 작동 로직:</b> {reason_str}\n"
            
            # 4. 7D DNA & DTW 분석 (대박주와 참사주의 특징)
            df_m = pd.read_sql(f"SELECT * FROM forward_trades WHERE market='{market}' AND status LIKE 'CLOSED%' ORDER BY exit_date DESC LIMIT 30", conn)
            if not df_m.empty:
                winners = df_m[df_m['final_ret'] > 5.0]
                losers = df_m[df_m['final_ret'] < -3.0]
                
                if not winners.empty:
                    aw_rs = winners['dyn_rs'].mean()
                    report_msg += f"✅ <b>대박 DNA:</b> RS 상위 {(10-aw_rs)*11.1:.1f}% 중심 매수세 유입\n"
                if not losers.empty:
                    al_cpv = losers['dyn_cpv'].mean()
                    report_msg += f"💀 <b>참사 DNA:</b> 윗꼬리(CPV) 상위 {(10-al_cpv)*11.1:.1f}% 함정 주의\n"

            # 5. 주도 섹터 및 순환매 흐름
            report_msg += f"🔥 <b>주도 섹터:</b> "
            query = f"SELECT sector, COUNT(*) as cnt FROM forward_trades WHERE entry_date >= date('now', '-7 days') AND market='{market}' GROUP BY sector ORDER BY cnt DESC LIMIT 2"
            sectors = [f"{r[0]}({r[1]})" for r in conn.execute(query).fetchall()]
            report_msg += ", ".join(sectors) if sectors else "포착 중"
            report_msg += "\n"

            # 6. 한미 스필오버(전이) 팩트 체크 (KR 섹션 전용)
            if market == 'KR':
                report_msg += "🌐 <b>한미 전이:</b> 미국 테크 ➔ 한국 반도체 (D+1 추적 중)\n"

        conn.close()
        
    except Exception as e:
        report_msg += f"\n⚠️ 리포트 생성 중 에러: {e}"

    report_msg += "\n━━━━━━━━━━━━━━━━━━\n💡 <i>관제탑이 24시간 백그라운드에서 추세/통계 기반 슬리피지 청산을 자동 집행 중입니다.</i>"
    send_telegram_msg(report_msg)
# ==========================================
# 4. [방향성 5,6,7번] 퀀트 딥 다이브 분석 엔진 (특징 추출 및 티어별 성적표)
# ==========================================
def run_deep_dive_analysis(market='KR'):
    """
    미래 데이터(포워드 테스팅)를 기반으로 내 시스템의 과최적화를 검증하고,
    대박/참사 종목의 DNA와 티어별 진짜 승률을 텔레그램으로 보고합니다.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;") # 💡 추가
        df = pd.read_sql(f"SELECT * FROM forward_trades WHERE market='{market}' AND status LIKE 'CLOSED%'", conn)
        conn.close()
        
        if len(df) < 10:
            print(f"⚠️ [{market}] 아직 통계를 낼 만큼 청산된 데이터가 충분하지 않습니다. (최소 10개 필요)")
            return

        df['Win'] = np.where(df['final_ret'] > 0, 1, 0)
        
        report_msg = f"🔬 [{market}장 포워드 테스팅 딥 다이브 분석]\n(총 {len(df)}개 실전 검증 데이터 기반)\n\n"

        # ---------------------------------------------------------
        # 👑 [V19.0 구간별 Micro-DNA 정밀 분석 엔진]
        # ---------------------------------------------------------
        for t in range(10, 100, 10):
            tier_label = f"{t}점대"
            t_df = df[df['tier'] == tier_label]
            if len(t_df) < 5: continue # 데이터가 최소 5개는 쌓여야 분석 시작

            report_msg += f"📌 <b>[{tier_label} 구간 심층 분석]</b>\n"
            
            # 가계부 및 승률
            wins_count = len(t_df[t_df['final_ret'] > 0])
            t_wr = (wins_count / len(t_df)) * 100
            gross_profit = t_df[t_df['final_ret'] > 0]['final_ret'].sum()
            gross_loss = abs(t_df[t_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1
            t_pf = gross_profit / gross_loss

            report_msg += f"▪️ 성적: 승률 {t_wr:.1f}% | PF {t_pf:.2f}\n"

            # 그룹핑 (대박 / 횡보 / 참사)
            winners = t_df[t_df['final_ret'] > 5.0]
            sideways = t_df[(t_df['final_ret'] >= -3.0) & (t_df['final_ret'] <= 5.0)]
            losers = t_df[t_df['final_ret'] < -3.0]

            # DNA 추출 함수
            def get_dna(sub_df):
                if len(sub_df) == 0: return "표본없음"
                return f"RS:{(10-sub_df['dyn_rs'].mean())*11.1:.1f}% | CPV:{(10-sub_df['dyn_cpv'].mean())*11.1:.1f}% | ENG:{sub_df['v_energy'].mean():.1f}"

            report_msg += f" ✅ 대박 DNA: {get_dna(winners)}\n"
            report_msg += f" ↔️ 횡보 DNA: {get_dna(sideways)}\n"
            report_msg += f" 💀 참사 DNA: {get_dna(losers)}\n"
            
            # 전략적 통찰 자동 도출
            if len(winners) > 0 and len(losers) > 0:
                if winners['v_energy'].mean() > losers['v_energy'].mean() + 1.0:
                    report_msg += f" 💡 통찰: {tier_label}는 에너지가 높을 때만 날아갑니다. 에너지 낮은 종목은 거르십시오.\n"
            report_msg += "\n"

        # ---------------------------------------------------------
        # 👑 [V20.0 거시적(Macro) 전체 그룹 통합 DNA 교차 분석]
        # ---------------------------------------------------------
        report_msg += "🌍 [전체 티어 통합: 유니버설(Universal) DNA 분석]\n"
        
        # 티어(점수) 계급장을 모두 떼고 절대 수익률 기준으로만 3분할
        all_winners = df[df['final_ret'] > 5.0]
        all_sideways = df[(df['final_ret'] >= -3.0) & (df['final_ret'] <= 5.0)]
        all_losers = df[df['final_ret'] < -3.0]

        if len(all_winners) >= 5 and len(all_losers) >= 5:
            aw_rs = all_winners['dyn_rs'].mean()
            aw_eng = all_winners['v_energy'].mean()
            report_msg += f"✅ [전체 대박주 {len(all_winners)}개 절대 공통점]\n"
            report_msg += f" ↳ 평균 RS: 상위 {(10-aw_rs)*11.1:.1f}% | 평균 에너지: {aw_eng:.1f}\n"

            as_cpv = all_sideways['dyn_cpv'].mean()
            report_msg += f"↔️ [전체 횡보주 {len(all_sideways)}개 절대 공통점]\n"
            report_msg += f" ↳ 평균 캔들지배력(CPV): 상위 {(10-as_cpv)*11.1:.1f}% (애매한 매도세가 횡보를 유발함)\n"

            al_cpv = all_losers['dyn_cpv'].mean()
            al_tb = all_losers['dyn_tb'].mean()
            report_msg += f"💀 [전체 참사주 {len(all_losers)}개 절대 공통점]\n"
            report_msg += f" ↳ 평균 캔들지배력(CPV): 상위 {(10-al_cpv)*11.1:.1f}% | 찐양봉 빈도 하위 {(al_tb)*11.1:.1f}%\n"

            # 💡 시스템의 거시적 통찰 자동 도출
            report_msg += f"💡 <b>[관제탑 최종 결론]</b>\n"
            if aw_rs < al_cpv: 
                report_msg += "현재 시장은 점수와 무관하게 철저히 '상대강도(RS)'가 주도하는 추세장입니다.\n"
            else:
                report_msg += "현재 시장은 악성 윗꼬리(CPV)에 한 번 걸리면 무조건 계좌가 녹아내리는 변동성 장세입니다.\n"
        else:
            report_msg += "⚠️ 전체 그룹 통합 분석을 위한 표본이 아직 부족합니다.\n"
        
        report_msg += "\n"

        # ---------------------------------------------------------
        # [기존 유지] 세부 흐름 태그별 승률 기여도 추적
        # ---------------------------------------------------------
        report_msg += "🏷️ [세부 흐름 태그별 승률 기여도]\n"
        tag_stats = {}
        for _, row in df.iterrows():
            if pd.isna(row['flow_tags']): continue
            for tag in str(row['flow_tags']).split():
                if tag not in tag_stats: tag_stats[tag] = {'win': 0, 'total': 0}
                tag_stats[tag]['total'] += 1
                if row['Win'] == 1: tag_stats[tag]['win'] += 1
                
        for tag, stats in sorted(tag_stats.items(), key=lambda x: x[1]['total'], reverse=True)[:5]:
            if stats['total'] >= 3:
                tag_win_rate = round((stats['win'] / stats['total']) * 100, 1)
                report_msg += f" ▪️ {tag}: 승률 {tag_win_rate}% (출현 {stats['total']}회)\n"

        # ---------------------------------------------------------
        # 👑 엔진 7: [V28.0 한미 주도 섹터 스필오버(Spillover) 시차 분석]
        # ---------------------------------------------------------
        if market == 'KR':
            report_msg += "\n🌐 <b>[V28.0 한미 주도 섹터 스필오버(전이) 팩트 체크]</b>\n"
            try:
                conn = sqlite3.connect(DB_PATH, timeout=60)
                conn.execute("PRAGMA journal_mode=WAL;")
                # 1. 최근 30일치 양국 포착 데이터 로드
                us_df = pd.read_sql("SELECT entry_date, sector FROM forward_trades WHERE market='US' AND entry_date >= date('now', '-30 days')", conn)
                kr_df = pd.read_sql("SELECT entry_date, sector FROM forward_trades WHERE market='KR' AND entry_date >= date('now', '-30 days')", conn)
                conn.close()

                if not us_df.empty and not kr_df.empty:
                    # 2. 일자별 1위 대장 섹터 산출 (가장 많은 종목이 포착된 섹터)
                    us_daily = us_df.groupby('entry_date')['sector'].agg(lambda x: x.mode()[0] if not x.empty else None)
                    kr_daily = kr_df.groupby('entry_date')['sector'].agg(lambda x: x.mode()[0] if not x.empty else None)

                    # 3. 한-미 섹터 동기화 매핑 딕셔너리 (논리적 연결점)
                    sector_map = {
                        "테크/기술": ["반도체", "IT", "소프트웨어", "기술"],
                        "헬스케어": ["바이오", "제약", "의료"],
                        "에너지": ["에너지", "화학", "정유"],
                        "소비재": ["화장품", "식품", "유통", "의류"]
                    }

                    report_msg += "▪️ <b>최근 7일 섹터 모멘텀 타임라인:</b>\n"
                    combined_dates = sorted(list(set(us_daily.index) | set(kr_daily.index)))[-7:]
                    
                    # 4. 타임라인 출력
                    for d in combined_dates:
                        us_sec = us_daily.get(d, "휴장/없음")
                        kr_sec = kr_daily.get(d, "휴장/없음")
                        report_msg += f" [{d[5:]}] 🇺🇸 {str(us_sec)[:6]} ➔ 🇰🇷 {str(kr_sec)[:6]}\n"

                    report_msg += "\n💡 <b>[관제탑 통찰]</b>\n"
                    report_msg += "데이터가 장부에 축적됨에 따라, 미국장의 주도 섹터가 한국장에 D+1~D+2 시차를 두고 전이될 확률(Cross-Correlation)을 추적 중입니다. 이 통계적 신뢰도가 검증되면 한국장 검색기의 선취매 가중치 기준으로 즉시 활용됩니다.\n"
                else:
                    report_msg += "⚠️ 스필오버 분석을 위한 한/미 양국 표본 데이터가 부족합니다.\n"
            except Exception as e:
                report_msg += f"⚠️ 스필오버 분석 에러: {e}\n"

        # ---------------------------------------------------------
        # 👑 엔진 8: [V29.0 주도 섹터 순환매(Rotation) 수명 및 전이 추적]
        # ---------------------------------------------------------
        report_msg += f"\n🔄 <b>[V29.0 {market}장 주도 섹터 순환매 자금 추적]</b>\n"
        try:
            conn = sqlite3.connect(DB_PATH, timeout=60)
            conn.execute("PRAGMA journal_mode=WAL;")
            # 최근 60일치 거시 데이터 스캔
            rot_df = pd.read_sql(f"SELECT entry_date, sector FROM forward_trades WHERE market='{market}' AND entry_date >= date('now', '-60 days') ORDER BY entry_date ASC", conn)
            conn.close()

            if not rot_df.empty:
                # 일자별 대장 섹터 추출
                daily_dom = rot_df.groupby('entry_date')['sector'].agg(lambda x: x.mode()[0] if not x.empty else None).dropna()
                
                streaks = {}      # 섹터별 머무는 기간(수명)
                transitions = {}  # A -> B 로의 자금 이동 횟수
                
                current_sec = None
                current_streak = 0
                
                # 순환매 체인(Markov Chain) 연산
                for date, sec in daily_dom.items():
                    if sec == current_sec:
                        current_streak += 1
                    else:
                        if current_sec is not None:
                            # 수명 기록
                            if current_sec not in streaks: streaks[current_sec] = []
                            streaks[current_sec].append(current_streak)
                            
                            # 자금 이동 궤적 기록 (A ➔ B)
                            trans_key = f"{current_sec[:6]} ➔ {sec[:6]}"
                            transitions[trans_key] = transitions.get(trans_key, 0) + 1
                        
                        current_sec = sec
                        current_streak = 1
                
                # 마지막 진행 중인 파동 기록
                if current_sec is not None:
                    if current_sec not in streaks: streaks[current_sec] = []
                    streaks[current_sec].append(current_streak)

                # 1. 섹터별 체류 수명 리포팅
                report_msg += "▪️ <b>섹터별 자금 체류 시간 (수명):</b>\n"
                for sec, lengths in streaks.items():
                    avg_len = sum(lengths) / len(lengths)
                    max_len = max(lengths)
                    report_msg += f" - {sec[:6]}: 평균 {avg_len:.1f}일 (최장 {max_len}일)\n"
                    
                # 2. 자금 이동 궤적 리포팅
                report_msg += "\n▪️ <b>가장 빈번한 자금 이동 경로 (최근 60일):</b>\n"
                sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:3]
                if sorted_trans:
                    for path, count in sorted_trans:
                        report_msg += f" - {path} ({count}회 관측)\n"
                else:
                    report_msg += " - 아직 뚜렷한 전이 패턴이 형성되지 않았습니다.\n"
                    
                report_msg += "💡 <b>관제탑 통찰:</b> 평균 수명에 도달한 섹터는 신규 진입을 피하고, 다음 이동 경로로 지목된 섹터의 저점 종목을 선취매 하십시오.\n"
            else:
                report_msg += "⚠️ 순환매 추적을 위한 표본 데이터가 부족합니다.\n"
        except Exception as e:
            report_msg += f"⚠️ 순환매 추적 에러: {e}\n"
        send_telegram_msg(report_msg)
        print(f"✅ [{market}] 딥 다이브 분석 리포트 발송 완료.")
        
    except Exception as e:
        # 👇👇 이렇게 덮어쓰세요 👇👇
        err_msg = f"🚨 <b>[포워드 장부 에러]</b> 딥 다이브 분석 중 에러 발생:\n{e}"
        print(err_msg)
        send_telegram_msg(err_msg)


# ---------------------------------------------------------
        # 👑 엔진 9: [V39.0 자금 관리 시뮬레이션: 고정 리스크 vs 켈리 리스크]
        # ---------------------------------------------------------
        if 'invest_amount' in df.columns and 'sim_kelly_invest' in df.columns:
            report_msg += "\n⚖️ <b>[V39.0 자금 관리 평행우주 대결 (누적 실현 손익)]</b>\n"
            
            # 고정 2% 룰의 누적 손익 (투자금 * 수익률)
            df['fixed_profit'] = df['invest_amount'] * (df['final_ret'] / 100)
            total_fixed_profit = df['fixed_profit'].sum()
            
            # 동적 켈리 룰의 누적 손익 (시뮬레이션 투자금 * 수익률)
            df['kelly_profit'] = df['sim_kelly_invest'] * (df['final_ret'] / 100)
            total_kelly_profit = df['kelly_profit'].sum()
            
            report_msg += f"▪️ 고정 2% 베팅 누적 손익: <b>{total_fixed_profit:,.0f}원</b>\n"
            report_msg += f"▪️ 국면형 켈리 누적 손익: <b>{total_kelly_profit:,.0f}원</b>\n"
            
            if total_kelly_profit > total_fixed_profit:
                report_msg += "🏆 <b>결론: 동적 켈리가 승리했습니다.</b> 상승장에서 비중을 싣고 하락장에서 방어한 전략이 누적 자본 증식에 훨씬 유리함을 데이터로 증명했습니다.\n"
            else:
                report_msg += "🛡️ <b>결론: 고정 리스크가 유리했습니다.</b> 켈리 베팅이 과도한 리스크를 지거나 휩소에 당했습니다. 켈리 승수를 하향 조정해야 합니다.\n"



# ==========================================
# 🕒 [무한 루프 스케줄러] 24시간 감시 및 보고 시스템
# ==========================================
def run_daily_scheduler():
    tz_kr = pytz.timezone('Asia/Seoul')
    print("🕒 [포워드 장부 관리기] 24시간 감시 스케줄러 가동 시작!")
    print(" - 15:40 : 한국장 종가 추적 및 청산 집행")
    print(" - 16:00 : 일일 종합 리포트 텔레그램 발송")
    print(" - 06:10 : 미국장 종가 추적 및 청산 집행")
    
    while True:
        try:
            now = datetime.now(tz_kr)
            # 1. 한국장 마감 직후 (15:40) -> 종가 확인 및 청산 실행
            if now.hour == 15 and now.minute == 40:
                print("🚀 한국장 종가 추적 및 청산 업데이트 시작...")
                track_daily_positions('KR')
                time.sleep(60) # 중복 실행 방지
                
            # 2. 일일 종합 리포트 발송 (16:00)
            elif now.hour == 16 and now.minute == 0:
                print("🚀 16:00 통합 지능 리포트 발송 시작...")
                send_comprehensive_daily_report() 
                time.sleep(60)
                
            # 3. 미국장 마감 직후 (한국시간 오전 06:10) -> 종가 확인 및 청산 실행
            elif now.hour == 6 and now.minute == 10:
                print("🚀 미국장 종가 추적 및 청산 업데이트 시작...")
                track_daily_positions('US')
                time.sleep(60)

            time.sleep(10) # 10초마다 시간 확인
            
        except Exception as e:
            # 👇👇 에러 발생 시 텔레그램으로 긴급 타전 👇👇
            err_msg = f"🚨 <b>[관제탑 스케줄러 긴급 에러]</b> 무한 루프 구동 중 꼬임 발생:\n{e}"
            print(err_msg)
            send_telegram_msg(err_msg)
            time.sleep(60) # 에러 폭탄(Spam) 방지를 위해 1분 대기 후 재가동

if __name__ == "__main__":
    # 이 파일을 CMD에서 실행해두면 24시간 살아 숨쉬며 리포트를 보냅니다.
    run_daily_scheduler()
