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
        
    # 2. 🎯 [티어별 정밀 통제] 오늘 해당 시장 & 해당 점수대에서 몇 개를 샀는지 체크 (하루 최대 2개)
    tz = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    today_str = datetime.now(tz).strftime('%Y-%m-%d')
    
    check_query = "SELECT COUNT(*) FROM forward_trades WHERE entry_date=? AND market=? AND tier=?"
    cursor.execute(check_query, (today_str, market, tier_label))
    current_daily_count = cursor.fetchone()[0]
    
    if current_daily_count >= 2:
        conn.close()
        return False, f"오늘의 {tier_label} 표본 2개 모두 확보됨 (스킵)"

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
                
                # 각 로직별 수익률 및 상태 저장 (실제 장부 메인 상태는 LIVE_A가 결정)
                if current_ret_pct <= sl_limit:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=?, {key}_status=? WHERE id=?", (sl_limit, "CLOSED_LOSS", r['id']))
                else:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=? WHERE id=?", (current_ret_pct, r['id']))
            # 👆👆 [추가 끝] 👆👆

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
                send_telegram_msg(f"🤖 [{market} 관제탑 제어] {icon}: {r['name']} ({r['total_score']}점)\n▪️ 수익: {ret}%\n▪️ 모드: {active_mode}\n▪️ 사유: {exit_rsn}\n▪️ 태그: {flow_tags}")
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
            
            # 보유 쿼터 현황
            cursor = conn.execute("SELECT tier, COUNT(*) FROM forward_trades WHERE market=? AND status='OPEN' GROUP BY tier", (market,))
            tier_counts = {row[0]: row[1] for row in cursor.fetchall()}
            total_active = sum(tier_counts.values())
            
            report_msg += f"📦 [현재 포트폴리오: 총 {total_active}종목]\n"
            report_msg += f" - 70~100점: {tier_counts.get('70~100점대', 0)}/30\n"
            report_msg += f" - 40~69점: {tier_counts.get('40~70점대', 0)}/15\n"
            report_msg += f" - 10~39점: {tier_counts.get('10~30점대', 0)}/15\n\n"
        
        # 💡 [방향성 3번] 최근 7일 주도 섹터(돈이 몰리는 곳) 추출
        report_msg += "🔥 [최근 7일 알고리즘 주도 섹터 TOP 3]\n"
        query = "SELECT sector, COUNT(*) as cnt FROM forward_trades WHERE entry_date >= date('now', '-7 days') GROUP BY sector ORDER BY cnt DESC LIMIT 3"
        for row in conn.execute(query).fetchall():
            report_msg += f" 🎯 {row[0]} ({row[1]}개 포착)\n"
            
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
        # 이미 청산(결과가 확정된) 종목들만 불러옵니다.
        df = pd.read_sql(f"SELECT * FROM forward_trades WHERE market='{market}' AND status LIKE 'CLOSED%'", conn)
        conn.close()
        
        if len(df) < 10:
            print(f"⚠️ [{market}] 아직 통계를 낼 만큼 청산된 데이터가 충분하지 않습니다. (최소 10개 필요)")
            return

        df['Win'] = np.where(df['final_ret'] > 0, 1, 0)
        
        report_msg = f"🔬 [{market}장 포워드 테스팅 딥 다이브 분석]\n(총 {len(df)}개 실전 검증 데이터 기반)\n\n"

        # ---------------------------------------------------------
        # [방향성 6번 해결] 티어별(10~30, 40~70, 70~100) 실전 성적표 추적
        # ---------------------------------------------------------
        report_msg += "📊 [티어별 실전 성적표]\n"
        tier_grp = df.groupby('tier').agg(
            매매건수=('id', 'count'),
            승률=('Win', lambda x: round(x.mean() * 100, 1)),
            평균수익=('final_ret', lambda x: round(x[x > 0].mean(), 2) if len(x[x > 0]) > 0 else 0),
            평균손실=('final_ret', lambda x: round(x[x <= 0].mean(), 2) if len(x[x <= 0]) > 0 else 0)
        ).reset_index()
        
        for _, r in tier_grp.iterrows():
            report_msg += f"🏅 {r['tier']} (총 {r['매매건수']}건)\n"
            report_msg += f" ↳ 실전 승률: {r['승률']}% | 평균 익절 +{r['평균수익']}% / 평균 손절 {r['평균손실']}%\n"
        report_msg += "\n"

        # ---------------------------------------------------------
        # [방향성 5, 6번 해결] 어떤 지표가 진짜 도움이 되었나? (과최적화 솎아내기)
        # ---------------------------------------------------------
        winners = df[df['final_ret'] > 5.0]  # 대박 종목 기준 (5% 이상 수익)
        losers = df[df['final_ret'] < -3.0]  # 참사 종목 기준 (-3% 이하 손실)
        
        report_msg += "🧬 [대박 vs 참사 종목 DNA(특징) 대조]\n"
        if len(winners) > 0 and len(losers) > 0:
            # 절대 수치가 아닌 '동적 백분위(dyn_)'를 대조하여 지표의 유효성 검증
            w_rs, l_rs = winners['dyn_rs'].mean(), losers['dyn_rs'].mean()
            w_cpv, l_cpv = winners['dyn_cpv'].mean(), losers['dyn_cpv'].mean()
            
            report_msg += f"📈 대박 종목 평균: RS 상위 {(10-w_rs)*11.1:.1f}% | 찐양봉 상위 {(10-winners['dyn_tb'].mean())*11.1:.1f}%\n"
            report_msg += f"💀 참사 종목 평균: RS 상위 {(10-l_rs)*11.1:.1f}% | 찐양봉 상위 {(10-losers['dyn_tb'].mean())*11.1:.1f}%\n"
            
            # 피드백 자동 도출 (어떤 지표가 도움이 되고 무용지물인지)
            if w_rs > l_rs + 1.0:
                report_msg += "💡 결론: 상대강도(RS)가 높을수록 실전 수익률에 매우 큰 도움이 됨. (RS 가중치 신뢰도 높음)\n"
            else:
                report_msg += "💡 결론: RS는 실전 결과와 큰 연관이 없음. (다른 지표 가중치를 올려야 함)\n"
        else:
            report_msg += "데이터가 더 누적되어야 DNA 대조가 가능합니다.\n"
        report_msg += "\n"

        # ---------------------------------------------------------
        # [방향성 6번 해결] 세부 특징(태그)별 승률 기여도 추적
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
            if stats['total'] >= 3: # 최소 3번 이상 등장한 태그만
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
