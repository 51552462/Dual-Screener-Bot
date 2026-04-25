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
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except: pass

def init_forward_db():
    """장부 테이블 생성 및 V12.0 필수 컬럼 안전 추가"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
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
def try_add_virtual_position(market, code, name, sig_type, score, ep, facts, sector="유망섹터"):
    init_forward_db()
    code_str = str(code).zfill(6) if market == 'KR' else str(code)
    
    # 💡 [V13.0 가상매매] 10점 단위 정밀 버킷 생성 (예: 85점 -> 80점대)
    score_bucket = int(score // 10) * 10
    if score_bucket >= 100: score_bucket = 90 # 100점은 90점대 최상위 티어로 병합
    tier_label = f"{score_bucket}점대"

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
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
    sig_type = f"{sig_type} {track_tag}"

    # 👇👇 [추가] V22.0 7D DNA 실시간 유도탄 (코사인 유사도 매칭) 👇👇
    try:
        sys_config = load_system_config()
        table_name = f"{market}_{code_str}"
        hist_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY Date DESC LIMIT 150", conn).sort_values('Date')
        
        if len(hist_df) >= 150:
            c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
            
            # 실시간 7D 벡터 연산 (관제탑의 Engine 4.8과 동일한 로직)
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
            
            new_vec = np.array([cpv, tb, bbe, rs_slope, vcp_ratio, vol_flow, ma_conv])
            new_vec = np.nan_to_num(new_vec)
            
            def cosine_sim(a, b):
                norm_a, norm_b = np.linalg.norm(a), np.linalg.norm(b)
                return np.dot(a, b) / (norm_a * norm_b) if norm_a > 0 and norm_b > 0 else 0
                
            trap_sims, alpha_sims = [], []
            for k, v_dict in sys_config.items():
                if "DNA_TRAP" in k and isinstance(v_dict, dict):
                    t_vec = np.array([v_dict.get('cpv',0), v_dict.get('tb',0), v_dict.get('bbe',0), v_dict.get('rs',0), v_dict.get('vcp',0), v_dict.get('vol',0), v_dict.get('ma',0)])
                    trap_sims.append(cosine_sim(new_vec, np.nan_to_num(t_vec)))
                elif "DNA_ALPHA" in k and isinstance(v_dict, dict):
                    a_vec = np.array([v_dict.get('cpv',0), v_dict.get('tb',0), v_dict.get('bbe',0), v_dict.get('rs',0), v_dict.get('vcp',0), v_dict.get('vol',0), v_dict.get('ma',0)])
                    alpha_sims.append(cosine_sim(new_vec, np.nan_to_num(a_vec)))
                    
            max_trap = max(trap_sims) if trap_sims else 0
            max_alpha = max(alpha_sims) if alpha_sims else 0
            
            # 🛡️ 페일세이프: 참사주 DNA와 85% 이상 일치하면 무조건 기각
            if max_trap >= 0.85 and max_trap > max_alpha:
                conn.close()
                return False, f"🚨 [DNA 방어막] 과거 참사주 DNA와 {max_trap*100:.1f}% 일치하여 매수 강제 기각"
            
            # 🚀 슈퍼 부스트: 대장주 DNA와 85% 이상 일치하면 시그널 태그 강화
            if max_alpha >= 0.85:
                sig_type += f" [🌟DNA {max_alpha*100:.0f}% 일치]"
    except Exception as e:
        print(f"DNA 벡터 매칭 에러: {e}")
    # 👆👆 [추가 끝] 👆👆

    # 3. 가상 매매 장부에 팩트 데이터와 함께 기록
    cursor.execute('''
        INSERT INTO forward_trades

    # 3. 가상 매매 장부에 팩트 데이터와 함께 기록
    cursor.execute('''
        INSERT INTO forward_trades 
        (entry_date, market, code, name, sector, sig_type, tier, total_score, dyn_rs, dyn_cpv, dyn_tb, entry_price, v_cpv, v_yang, v_energy, v_rs, max_high, min_low)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        today_str, market, code_str, name, sector, sig_type, tier_label, score,
        facts.get('dyn_rs', 0), facts.get('dyn_cpv', 0), facts.get('dyn_tb', 0), ep,
        facts.get('v_cpv', 0), facts.get('v_yang', 0), facts.get('v_energy', 0), facts.get('v_rs', 0),
        ep, ep
    ))
    conn.commit()
    conn.close()
    
    return True, f"🎯 {tier_label} 가상매매 편입 성공 ({current_daily_count+1}/2): {name} ({score:.1f}점)"

# ==========================================
# 2. 매일 종가 흐름 추적 및 청산 엔진 (DB 기반)
# ==========================================
def track_daily_positions(market):
    init_forward_db()
    conn = sqlite3.connect(DB_PATH)
    
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
            
            # 현재 누적 수익률 계산
            current_ret_pct = ((c - ep) / ep) * 100
            
            # 👇👇 [추가] V15.0 ABC 토너먼트 병렬 연산 👇👇
            abc_sets = {
                'live_a': sys_config, # 루트 자체가 라이브(A)
                'cand_b': sys_config.get("CANDIDATE_PARAMS", {}), # 대기실 후보(B)
                'champ_c': sys_config.get("CHAMPION_PARAMS", {})  # 명예의 전당(C)
            }

            for key, params in abc_sets.items():
                if not params: continue
                # 각 셋트에서 손절선(SL) 추출 (없으면 기본값 -3.5)
                sl_limit = params.get("DYNAMIC_MAE_SL", -3.5)
                
                if current_ret_pct <= sl_limit:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=?, {key}_status=? WHERE id=?", (sl_limit, "CLOSED_LOSS", r['id']))
                else:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # 👇👇 [추가] V17.0 청산 평행우주 (STAT 우선 vs TECH 우선) 가상 대결 👇👇
            # 1. STAT 맹신 (MFE 도달 시 즉시 익절, 데드크로스 무시)
            if r.get('sim_stat_status', 'OPEN') == 'OPEN':
                if current_ret_pct <= dyn_mae_sl:
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif current_ret_pct >= dyn_mfe_tp:
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_WIN' WHERE id=?", (dyn_mfe_tp, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # 2. TECH 맹신 (MFE 무시하고 데드크로스 날 때까지 무한 홀딩. 계좌 보호용 MAE만 유지)
            if r.get('sim_tech_status', 'OPEN') == 'OPEN':
                if current_ret_pct <= dyn_mae_sl:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif is_tech_exit:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_WIN' WHERE id=?", (current_ret_pct, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=? WHERE id=?", (current_ret_pct, r['id']))
            # 👆👆 [추가 끝] 👆👆

            # 💡 [팩트] 관제탑이 학습한 비선형 수학적 한계점 로드

            # 💡 [팩트] 관제탑이 학습한 비선형 수학적 한계점 로드
            dyn_mae_sl = sys_config.get("DYNAMIC_MAE_SL", -3.5)
            dyn_mfe_tp = sys_config.get("DYNAMIC_MFE_TP", 10.0)

            # 1순위: MFE/MAE 절대 한계점 도달 시 무조건 청산 (가장 강력한 비선형 규칙)
            if current_ret_pct <= dyn_mae_sl:
                do_exit, exit_rsn, actual_exit_type = True, f"수학적 MAE 이탈 칼손절 ({dyn_mae_sl}%)", "STAT_MAE"
            elif current_ret_pct >= dyn_mfe_tp:
                do_exit, exit_rsn, actual_exit_type = True, f"수학적 MFE 도달 기계적 익절 ({dyn_mfe_tp}%)", "STAT_MFE"
                
            # 2순위: 한계점 내부에서 움직일 경우, 국면(Regime) 모드에 따른 추세/시간 청산
            if not do_exit:
                if active_mode == "TECH":
                    if is_tech_exit: do_exit, exit_rsn, actual_exit_type = True, "기술적 추세 이탈 (ZLEMA/데드)", "TECH"
                elif active_mode == "STAT":
                    if new_bars >= opt_time_stop: do_exit, exit_rsn, actual_exit_type = True, f"통계적 유통기한 만료 ({opt_time_stop}일)", "STAT_TIME"
                    elif c <= sl_price: do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 수학적 손절", "STAT_ATR"
                else: # HYBRID
                    if new_bars >= opt_time_stop: do_exit, exit_rsn, actual_exit_type = True, f"하이브리드 타임스탑 ({opt_time_stop}일)", "HYBRID_TIME"
                    elif c <= sl_price: do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 방어 손절", "HYBRID_ATR"
                    elif is_tech_exit: do_exit, exit_rsn, actual_exit_type = True, "하이브리드 추세 이탈 익절", "HYBRID_TECH"

            # 5. DB 업데이트 실행 (청산 시)
            if do_exit:
                ret = round(((c - ep) / ep) * 100, 2)
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
def send_daily_summary_report():
    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(tz_kr).strftime('%Y-%m-%d')
    
    report_msg = f"📊 [포워드 테스팅 일일 종합 리포트]\n📅 {today_str} 기준\n\n"

    try:
        conn = sqlite3.connect(DB_PATH)
        
        for market in ['KR', 'US']:
            # 승률 계산
            cursor = conn.execute("SELECT COUNT(*), SUM(CASE WHEN final_ret > 0 THEN 1 ELSE 0 END) FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'", (market,))
            total_closed, wins = cursor.fetchone()
            total_closed = total_closed or 0
            wins = wins or 0
            win_rate = round((wins / total_closed) * 100, 1) if total_closed > 0 else 0.0
            
            report_msg += f"📈 [{market}장 누적 실전 승률]: {win_rate}% (총 {total_closed}건 검증)\n"
            
            # 보유 쿼터 현황 (9단계 정밀 표시)
            cursor = conn.execute("SELECT tier, COUNT(*) FROM forward_trades WHERE market=? AND status='OPEN' GROUP BY tier", (market,))
            tier_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            report_msg += f"📦 [{market}장 9단계 정밀 포트폴리오]\n"
            for t in range(10, 100, 10):
                label = f"{t}점대"
                count = tier_counts.get(label, 0)
                report_msg += f" - {label}: {count}/20\n"
            report_msg += "\n"

            # 한국장/미국장 주도 섹터 완벽 분리 집계
            report_msg += f"🔥 [{market}장 최근 7일 알고리즘 주도 섹터 TOP 3]\n"
            query = f"SELECT sector, COUNT(*) as cnt FROM forward_trades WHERE entry_date >= date('now', '-7 days') AND market='{market}' GROUP BY sector ORDER BY cnt DESC LIMIT 3"
            for row in conn.execute(query).fetchall():
                report_msg += f" 🎯 {row[0]} ({row[1]}개 포착)\n"
            report_msg += "\n"
            
        conn.close()
        
    except Exception as e:
        report_msg += f"\n에러 발생: {e}"

    report_msg += "\n💡 (모든 지표와 백분위 데이터는 DB에 완벽히 박제 중입니다.)"
    send_telegram_msg(report_msg)
    print(f"✅ 16:00 일일 종합 리포트 텔레그램 발송 완료.")

# ==========================================
# 4. [방향성 5,6,7번] 퀀트 딥 다이브 분석 엔진 (특징 추출 및 티어별 성적표)
# ==========================================
def run_deep_dive_analysis(market='KR'):
    """
    미래 데이터(포워드 테스팅)를 기반으로 내 시스템의 과최적화를 검증하고,
    대박/참사 종목의 DNA와 티어별 진짜 승률을 텔레그램으로 보고합니다.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
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

        send_telegram_msg(report_msg)
        print(f"✅ [{market}] 딥 다이브 분석 리포트 발송 완료.")
        
    except Exception as e:
        print(f"⚠️ 딥 다이브 분석 중 에러: {e}")

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
        now = datetime.now(tz_kr)
        
        # 1. 한국장 마감 직후 (15:40) -> 종가 확인 및 청산 실행
        if now.hour == 15 and now.minute == 40:
            print("🚀 한국장 종가 추적 및 청산 업데이트 시작...")
            track_daily_positions('KR')
            time.sleep(60) # 중복 실행 방지
            
        # 2. 일일 종합 리포트 발송 (16:00)
        elif now.hour == 16 and now.minute == 0:
            print("🚀 16:00 일일 종합 리포트 발송 시작...")
            send_daily_summary_report()
            time.sleep(60)
            
        # 3. 미국장 마감 직후 (한국시간 오전 06:10) -> 종가 확인 및 청산 실행
        elif now.hour == 6 and now.minute == 10:
            print("🚀 미국장 종가 추적 및 청산 업데이트 시작...")
            track_daily_positions('US')
            time.sleep(60)

        time.sleep(10) # 10초마다 시간 확인

if __name__ == "__main__":
    # 이 파일을 CMD에서 실행해두면 24시간 살아 숨쉬며 리포트를 보냅니다.
    run_daily_scheduler()
