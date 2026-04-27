import sqlite3
import pandas as pd
import numpy as np
import os
import json
import time   # 👈 이 줄을 반드시 추가하십시오!
from datetime import datetime, timedelta
import pytz
import requests
import yfinance as yf
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# 💡 [환경 설정]
# ==========================================
TELEGRAM_TOKEN_MAIN = "7988939051:AAG4FqMzzz12vd7Crzt8DVPWiL3fMHM8tEc"
TELEGRAM_CHAT_ID    = "6838834566"
DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')

LOOKBACK_DAYS = 14
SMOOTHING_ALPHA = 0.3 
START_DATE = datetime.now() + timedelta(days=LOOKBACK_DAYS)

# ==========================================
# 💡 [유틸리티 함수]
# ==========================================
def send_telegram_report(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN_MAIN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try: requests.post(url, json=payload, timeout=10)
    except Exception as e: print(f"텔레그램 전송 실패: {e}")

def load_or_create_config():
    if not os.path.exists(CONFIG_PATH):
        default_config = {
            "ACTIVE_EXIT_MODE": "HYBRID",
            "WEIGHT_S1": 1.0, "WEIGHT_S4": 1.0,
            "ACCOUNT_SIZE": 20000000,   # 💡 2,000만 원 (수정됨)
            "RISK_PCT": 0.02            # 💡 고정 리스크 2% (손절 시 최대 40만 원 타격)
        }
        with open(CONFIG_PATH, 'w') as f: json.dump(default_config, f, indent=4)
        return default_config
    with open(CONFIG_PATH, 'r') as f: return json.load(f)

def save_config(config_data):
    with open(CONFIG_PATH, 'w') as f: json.dump(config_data, f, indent=4)

def calculate_metrics(df_subset):
    """승률과 손익비(PF) 반환"""
    if len(df_subset) == 0: return 0.0, 0.0
    wins = df_subset[df_subset['final_ret'] > 0]
    losses = df_subset[df_subset['final_ret'] <= 0]
    win_rate = (len(wins) / len(df_subset)) * 100
    gross_profit = wins['final_ret'].sum()
    gross_loss = abs(losses['final_ret'].sum())
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 99.9
    return win_rate, profit_factor

# ==========================================
# 🚀 [메인 분석 엔진] 
# ==========================================
def run_autonomous_analysis():
    print(f"🚀 [자율 관제탑] 거시 경제(VIX) 기반 동적 룩백 윈도우 스캔 시작...")
    
    # ---------------------------------------------------------
    # 👑 [사전 작업] 변동성(VIX & KOSPI) 기반 동적 룩백(Lookback) 결정
    # ---------------------------------------------------------
    dyn_lookback = 14 # 기본값
    vix_status = "데이터 없음"
    regime = "분석 중"
    w_s1, w_s4 = 1.0, 1.0 
    
    try:
        # 💡 [V24.0] SPY(시총비중), ^VIX(공포), RSP(동일비중) 데이터 동시 로드
        df_idx = yf.download("SPY ^VIX RSP", period="1y", interval="1d", group_by="ticker", progress=False)
        
        # 데이터 추출 (멀티인덱스 대응)
        spy_c = df_idx['SPY']['Close'].dropna() if 'SPY' in df_idx.columns.levels[0] else df_idx['Close']['SPY'].dropna()
        vix_c = df_idx['^VIX']['Close'].dropna() if '^VIX' in df_idx.columns.levels[0] else df_idx['Close']['^VIX'].dropna()
        rsp_c = df_idx['RSP']['Close'].dropna() if 'RSP' in df_idx.columns.levels[0] else df_idx['Close']['RSP'].dropna()
        
        spy_last, vix_last = spy_c.iloc[-1], vix_c.iloc[-1]
        spy_ema200 = spy_c.ewm(span=200, adjust=False).mean().iloc[-1]
        
        # 💡 [핵심] 시장 폭(Breadth) 계산: (현재 RSP/SPY 비율) / (50일 평균 RSP/SPY 비율)
        # 1.0보다 낮으면 대형주만 오르는 '취약한 장세', 높으면 낙수효과가 있는 '건강한 장세'
        breadth_ratio = (rsp_c.iloc[-1] / spy_c.iloc[-1]) / (rsp_c.rolling(50).mean().iloc[-1] / spy_c.rolling(50).mean().iloc[-1])

        # 1. 기본 국면 및 비중 설정 (지수 위치 기준)
        if spy_last > spy_ema200 and vix_last < 18:
            regime = "Bull (상승장)"
            base_w1, base_w4 = 1.2, 0.8
        else:
            regime = "Bear/Chop (하락/횡보)"
            base_w1, base_w4 = 0.5, 1.5

        # 2. 🚨 [V24.0 핵심] 시장 폭에 따른 비중 패널티/보너스 (지수 착시 방어)
        breadth_status = "건강 (Broad)"
        if breadth_ratio < 0.97: 
            breadth_status = "취약 (Narrow/쏠림)"
            base_w1 *= 0.5  # 공격(S1) 비중 반토막 (함정 방어)
            base_w4 *= 1.2  # 방어/눌림(S4) 비중 강화
        elif breadth_ratio > 1.03: 
            breadth_status = "강력 (확산)"
            base_w1 *= 1.2  # 공격(S1) 비중 추가 확대

        w_s1, w_s4 = round(base_w1, 2), round(base_w4, 2)
        
        # 3. VIX 기반 동적 룩백 설정 결합
        if vix_last >= 28.0:
            dyn_lookback = 7
            regime = "Bear (극단적 공포장)"
            w_s1, w_s4 = 0.0, 2.0
            vix_status = f"VIX 폭발({vix_last:.1f}) | 폭:{breadth_ratio:.2f}({breadth_status}) - 룩백 7일"
        elif vix_last >= 18.0:
            dyn_lookback = 15
            vix_status = f"VIX 경계({vix_last:.1f}) | 폭:{breadth_ratio:.2f}({breadth_status}) - 룩백 15일"
        else:
            dyn_lookback = 45
            vix_status = f"VIX 평온({vix_last:.1f}) | 폭:{breadth_ratio:.2f}({breadth_status}) - 룩백 45일"
            
    except Exception as e:
        print(f"거시 지표 로드 에러: {e}")

    # 1. 계산된 [동적 룩백]으로 DB 데이터 로드
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        start_date = (datetime.now() - timedelta(days=dyn_lookback)).strftime('%Y-%m-%d')
        query = f"SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND entry_date >= '{start_date}'"
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"DB 로드 에러: {e}")
        return

    if len(df) < 10:
        send_telegram_report(f"⚠️ <b>[자율 관제탑]</b>\n\n거시 국면 전환으로 룩백이 {dyn_lookback}일로 조정되었으나, 해당 기간 내 청산 표본이 10건 미만입니다. 이번 주 조율을 스킵합니다.")
        return

    current_config = load_or_create_config()
    current_config["WEIGHT_S1"], current_config["WEIGHT_S4"] = w_s1, w_s4
    
    report_lines = [f"<b>📊 [System B 자율 조율 리포트]</b>\n"]
    report_lines.append(f"<b>[1. 동적 거시 국면 판독 (Regime)]</b>\n▪️ 상태: {regime}\n▪️ <b>동적 룩백: {vix_status}</b>\n🚨 <b>액션:</b> S1 비중 {w_s1}배 / S4 비중 {w_s4}배 강제 조율\n")

    # ---------------------------------------------------------
    # 🛡️ 엔진 1.7.5: [V45.0 DNA 변위(Drift) 감지 선제적 방어막]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[1.7.5 DNA 변위 기반 선제적 국면 검증]</b>")
    dna_drift_warning = False
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        # 최근 진입한 10개 종목의 DNA 매칭 성적과 참사주 일치도 데이터 로드
        recent_dna_df = pd.read_sql("SELECT entry_cos_score, entry_dtw_score FROM forward_trades ORDER BY id DESC LIMIT 10", conn)
        conn.close()
        
        if len(recent_dna_df) >= 5:
            avg_alpha_sim = recent_dna_df['entry_cos_score'].mean()
            # 💡 핵심 로직: 지수가 BULL(상승장)이어도 종목들의 대장주 DNA 일치율이 
            # 갑자기 60% 밑으로 떨어지거나, 참사주 냄새가 짙어지면 'DNA 변위'로 간주
            if avg_alpha_sim < 0.65:
                dna_drift_warning = True
                report_lines.append(f"🚨 <b>[DNA 변위 감지]</b> 지수는 상승장이나 포착 종목의 대장주 일치율이 {avg_alpha_sim*100:.1f}%로 급감했습니다.")
                report_lines.append("⚠️ <b>조치:</b> 지수 판독 결과를 무시하고 '방어(CHOP)' 모드로 선제 전환합니다.")
    except: pass

    # 국면 판독 결과 강제 보정 (지수보다 DNA 우선)
    if dna_drift_warning and "Bull" in regime:
        regime = "Chop (DNA 변위로 인한 선제적 방어)"
        w_s1, w_s4 = 0.5, 1.2 # 공격 비중 강제 축소
    # 👆👆 [V45.0 엔진 끝] 👆👆

    # ---------------------------------------------------------
    # 👑 엔진 1.8: [V32.0 국면별 독립 기억소(Regime Memory) 로드]
    # ---------------------------------------------------------
    regime_key = "BULL" if "Bull" in regime else ("BEAR" if "극단적" in regime else "CHOP")
    last_analysed_regime = current_config.get("LAST_ANALYSED_REGIME", "")

    if last_analysed_regime != regime_key:
        report_lines.append(f"\n🔄 <b>[V32.0 국면 전환 감지]</b> {last_analysed_regime} ➔ {regime_key}")
        
        # 💾 과거 해당 국면의 챔피언 파라미터 뭉치 로드 (Zero-Lag)
        regime_memory = current_config.get(f"{regime_key}_CHAMPION_PARAMS", {})
        if regime_memory:
            for k, v in regime_memory.items(): current_config[k] = v
            report_lines.append(f"💾 <b>[기억소 로드]</b> 과거 {regime_key} 국면의 황금 파라미터를 즉시 복구했습니다.")
        
        current_config["LAST_ANALYSED_REGIME"] = regime_key

    # ---------------------------------------------------------
    # 👑 엔진 1.9: [V39.0 국면별 데이터 기반 켈리 베팅(Kelly Criterion) 도출]
    # ---------------------------------------------------------
    report_lines.append(f"\n<b>[1.9 {regime_key} 국면 최적 켈리(Kelly) 베팅 사이즈 조율]</b>")
    
    # 1. 내 장부에서 '현재와 동일한 국면(Regime)'에 진입했던 청산 종목들만 추출
    regime_df = df[df['entry_regime'] == regime_key] if 'entry_regime' in df.columns else df
    
    if len(regime_df) >= 10: # 데이터가 충분할 때만 켈리 공식 가동
        r_wins = regime_df[regime_df['final_ret'] > 0]
        r_loses = regime_df[regime_df['final_ret'] <= 0]
        
        r_win_rate = len(r_wins) / len(regime_df)
        r_pf = r_wins['final_ret'].sum() / (abs(r_loses['final_ret'].sum()) + 0.1)
        
        # 2. 켈리 공식 적용: f = W - (1-W)/R (안전성을 위해 Half-Kelly 적용)
        if r_pf > 0:
            kelly_fraction = r_win_rate - ((1 - r_win_rate) / r_pf)
            half_kelly = kelly_fraction / 2.0
            # 3. 리스크 허용 범위 강제 바운딩 (최소 0.2% ~ 최대 3.0%)
            optimal_risk = max(0.002, min(0.030, half_kelly * 0.1)) # 자본 보존을 위해 스케일 다운
        else:
            optimal_risk = 0.002 # 승률/손익비가 박살난 상태면 0.2% 극방어 모드
            
        report_lines.append(f"▪️ {regime_key} 과거 성적: 승률 {r_win_rate*100:.1f}% | PF {r_pf:.2f}")
        report_lines.append(f"💡 <b>수학적 최적 리스크(Half-Kelly): 계좌의 {optimal_risk*100:.2f}% (동적 스케일링)</b>")
    else:
        # 데이터가 부족하면 인간의 뇌피셜이 아닌, 가장 보수적인 베이스라인(1.0%) 적용
        optimal_risk = 0.01
        report_lines.append(f"▪️ 표본 부족으로 안전 베이스라인(1.0%) 적용")

    current_config["DYNAMIC_KELLY_RISK"] = round(optimal_risk, 4)
    current_config["CURRENT_REGIME_KEY"] = regime_key

    # ---------------------------------------------------------
    # 👑 엔진 2: 점수 티어 및 초정밀 필터 검증
    # ---------------------------------------------------------
    # (... 기존 엔진 2 코드 그대로 이어짐 ...)
    report_lines.append("<b>[2. 필터 및 티어 승률 검증]</b>")
    t1_wr, t1_pf = calculate_metrics(df[df['total_score'] >= 80])
    sub_wr, sub_pf = calculate_metrics(df[(df['total_score'] >= 50) & (df['total_score'] < 80)])
    report_lines.append(f"▪️ 1티어(80점↑): 승률 {t1_wr:.1f}% | PF {t1_pf:.2f}")
    report_lines.append(f"▪️ 서브(50~79점): 승률 {sub_wr:.1f}% | PF {sub_pf:.2f}")
    
    if 'is_death_combo' in df.columns:
        dc_wr, dc_pf = calculate_metrics(df[df['is_death_combo'] == 1])
        report_lines.append(f"▪️ 데스콤보 타점: 승률 {dc_wr:.1f}% (낮을수록 정상 방어 중)\n")

    # ---------------------------------------------------------
    # 👑 엔진 3: 날것(Raw) 파라미터 스무딩 (베이지안 업데이트)
    # ---------------------------------------------------------
    report_lines.append("<b>[3. 네임스페이스 스무딩 (진입점 교정)]</b>")
    kr_s1_df = df[(df['market'] == 'KR') & (df['sig_type'].str.contains('S1'))]
    winners_rs = kr_s1_df[kr_s1_df['final_ret'] > 0]['v_rs'].dropna()
    
    if len(winners_rs) >= 5:
        raw_new_rs = np.percentile(winners_rs, 25) 
        old_rs = current_config.get("KR_S1_RS_CUTOFF", 165.0)
        smoothed_rs = round((old_rs * (1 - SMOOTHING_ALPHA)) + (raw_new_rs * SMOOTHING_ALPHA), 2)
        current_config["KR_S1_RS_CUTOFF"] = smoothed_rs
        report_lines.append(f"▪️ KR_S1_RS: {old_rs} ➔ <b>{smoothed_rs}</b> (새 파동 30% 스며듦)\n")
    else:
        report_lines.append("▪️ 표본 부족으로 진입점 스무딩 스킵\n")

    # ---------------------------------------------------------
    # 👑 엔진 4 ~ 6: [V51.0 다중 뇌(Multi-Brain) 자율 분할 최적화 엔진]
    # ---------------------------------------------------------
    # 1. 종목별 출신 성분(Namespace) 매핑 함수
    def map_namespace(row):
        m = row['market']
        st = str(row['sig_type'])
        ns = f"{m}_MASTER_S1" # 기본값
        if "S4" in st: ns = f"{m}_MASTER_S4"
        if "눌림" in st: ns = f"{m}_NULRIM_S4" if "S4" in st else f"{m}_NULRIM_S1"
        if "5선" in st: ns = f"{m}_5EMA_S1"
        return ns

    if 'market' in df.columns and 'sig_type' in df.columns:
        df['namespace'] = df.apply(map_namespace, axis=1)
    else:
        df['namespace'] = "KR_MASTER_S1" # Fail-safe

    unique_namespaces = df['namespace'].unique()

    report_lines.append(f"\n🧠 <b>[V51.0 다중 뇌(Multi-Brain) 분할 최적화 가동]</b>\n발견된 독립 전략 방: {', '.join(unique_namespaces)}")
    oos_barrier = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')

    for target_ns in unique_namespaces:
        ns_df = df[df['namespace'] == target_ns].copy()
        if len(ns_df) < 5: continue # 해당 전략의 표본 부족 시 스킵
        
        report_lines.append(f"\n=========================================")
        report_lines.append(f"🧬 <b>[{target_ns} 전용 뇌수술 진행]</b> (표본: {len(ns_df)}개)")
        
        # --- [엔진 4: 독립 앙상블 생성] ---
        def get_period_stats(train_days):
            s_date = (datetime.now() - timedelta(days=14 + train_days)).strftime('%Y-%m-%d')
            p_df = ns_df[(ns_df['entry_date'] >= s_date) & (ns_df['entry_date'] < oos_barrier)].copy()
            n_trades = len(p_df)
            
            if n_trades < 5:
                if n_trades > 0 and (len(p_df[p_df['final_ret'] > 0]) == 0 or p_df['final_ret'].mean() <= -3.0): return "TOXIC"
                return None

            win_s = p_df[p_df['final_ret'] > 0]
            lose_s = p_df[p_df['final_ret'] <= 0]
            win_rate = len(win_s) / n_trades if n_trades > 0 else 0
            avg_win = win_s['final_ret'].mean() if len(win_s) > 0 else 0
            avg_loss = abs(lose_s['final_ret'].mean()) if len(lose_s) > 0 else 0.1
            expectancy = (win_rate * avg_win) - ((1.0 - win_rate) * avg_loss)
            
            if (expectancy < 0.5) and (n_trades >= 5): return "NO_EDGE"
            if (0.85 <= (avg_win/(avg_loss+0.1)) <= 1.25) and (n_trades < 20): return "NOISE"

            p_df['mae_pct'] = (p_df['min_low'] - p_df['entry_price']) / p_df['entry_price'] * 100
            p_df['mfe_pct'] = (p_df['max_high'] - p_df['entry_price']) / p_df['entry_price'] * 100
            
            opt_alpha, opt_trap, opt_dtw = 0.75, 0.75, 2.5
            is_drought = len(p_df[p_df['entry_date'] >= (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')]) == 0
            if 'entry_cos_score' in p_df.columns and len(win_s) >= 3:
                opt_alpha = np.percentile(win_s['entry_cos_score'].dropna(), 15)
                opt_dtw = np.percentile(win_s['entry_dtw_score'].dropna(), 85)
                if len(lose_s) >= 3: opt_trap = np.percentile(lose_s['entry_cos_score'].dropna(), 50)
            elif is_drought:
                opt_alpha, opt_dtw, opt_trap = 0.60, 3.5, 0.85

            raw_sl = np.percentile(win_s['mae_pct'].dropna(), 15) if len(win_s) >= 3 else -3.5
            raw_tp = np.percentile(win_s['mfe_pct'].dropna(), 50) if len(win_s) >= 3 else 10.0
            
            return {"sl": raw_sl, "tp": raw_tp, "fatal_cpv": np.percentile(lose_s['v_cpv'].dropna(), 90) if len(lose_s) >= 3 else 0.85, "alpha_limit": opt_alpha, "trap_limit": opt_trap, "dtw_limit": opt_dtw}

        t14, t30, t60 = get_period_stats(14), get_period_stats(30), get_period_stats(60)
        is_toxic = False
        for name, t_stat in zip(['14일', '30일', '60일'], [t14, t30, t60]):
            if t_stat == "TOXIC": is_toxic = True; report_lines.append(f"🚨 {name}: <b>[TOXIC 붕괴]</b> 강제 방어")
            elif t_stat == "NO_EDGE": report_lines.append(f"✂️ {name}: <b>[기댓값 미달]</b> 배제.")
        
        # 💡 [핵심] 파라미터를 저장할 때 '전략의 방 이름(target_ns)'을 열쇠에 붙여서 독립 보관
        cand_key = f"{target_ns}_CANDIDATE_PARAMS"
        if is_toxic:
            current_config[cand_key] = {"DYNAMIC_MAE_SL": -2.5, "DYNAMIC_MFE_TP": 10.0, "TREE_FATAL_CPV": 0.70, "DYNAMIC_ALPHA_LIMIT": 0.85, "DYNAMIC_TRAP_LIMIT": 0.70, "DYNAMIC_DTW_LIMIT": 1.5}
        else:
            valid = [s for s in [t14, t30, t60] if isinstance(s, dict)]
            if len(valid) >= 2:
                w = [0.5, 0.3, 0.2] if t14 and t30 and t60 else [1/len(valid)] * len(valid)
                current_config[cand_key] = {
                    "DYNAMIC_MAE_SL": round(sum(s['sl']*w[i] for i,s in enumerate(valid)), 2),
                    "DYNAMIC_MFE_TP": round(sum(s['tp']*w[i] for i,s in enumerate(valid)), 2),
                    "TREE_FATAL_CPV": round(sum(s['fatal_cpv']*w[i] for i,s in enumerate(valid)), 2),
                    "DYNAMIC_ALPHA_LIMIT": round(sum(s['alpha_limit']*w[i] for i,s in enumerate(valid)), 3),
                    "DYNAMIC_TRAP_LIMIT": round(sum(s['trap_limit']*w[i] for i,s in enumerate(valid)), 3),
                    "DYNAMIC_DTW_LIMIT": round(sum(s['dtw_limit']*w[i] for i,s in enumerate(valid)), 3)
                }
                report_lines.append(f"▪️ 앙상블 생성: SL {current_config[cand_key]['DYNAMIC_MAE_SL']}% / TP {current_config[cand_key]['DYNAMIC_MFE_TP']}%")

        # --- [엔진 5: 독립 STAT vs TECH 결투] ---
        if 'sim_stat_ret' in ns_df.columns:
            st_df = ns_df[ns_df['sim_stat_status'].str.contains('CLOSED', na=False)]
            te_df = ns_df[ns_df['sim_tech_status'].str.contains('CLOSED', na=False)]
            s_pf = (st_df[st_df['sim_stat_ret']>0]['sim_stat_ret'].sum()) / abs(st_df[st_df['sim_stat_ret']<=0]['sim_stat_ret'].sum() + 0.1) if len(st_df)>0 else 0
            t_pf = (te_df[te_df['sim_tech_ret']>0]['sim_tech_ret'].sum()) / abs(te_df[te_df['sim_tech_ret']<=0]['sim_tech_ret'].sum() + 0.1) if len(te_df)>0 else 0
            winner = "TECH" if t_pf > s_pf * 1.1 else "STAT"
            current_config[f"{target_ns}_ACTIVE_EXIT_MODE"] = winner
            report_lines.append(f"▪️ 청산 결투: {winner} 모드가 우세함")

        # --- [엔진 6: 독립 OOS 진검승부 및 챔피언 승격] ---
        train_df = ns_df[ns_df['entry_date'] < oos_barrier]
        test_df = ns_df[ns_df['entry_date'] >= oos_barrier]
        
        def get_eq(ret_s):
            v = ret_s.dropna().values
            if len(v) < 3: return sum(v)
            return np.percentile([(np.prod(1+np.random.choice(v, size=len(v), replace=True)/100.0)-1)*100 for _ in range(1000)], 5)

        results = {}
        for col in ['live_a_ret', 'cand_b_ret', 'champ_c_ret']:
            if col in test_df.columns: results[col] = get_eq(test_df[col])
        
        if results:
            win_k = max(results, key=results.get)
            report_lines.append(f"▪️ OOS 성적(복리): LIVE({results.get('live_a_ret',0):.2f}%) B({results.get('cand_b_ret',0):.2f}%) C({results.get('champ_c_ret',0):.2f}%)")
            
            if win_k == 'cand_b_ret' and results['cand_b_ret'] > results.get('live_a_ret', 0) * 1.05:
                current_config[f"{target_ns}_CHAMPION_PARAMS"] = current_config.get(f"{target_ns}_LIVE_PARAMS", {})
                current_config[f"{target_ns}_LIVE_PARAMS"] = current_config.get(cand_key, {})
                report_lines.append("🏆 <b>[신규 승격]</b> B가 실전 배치됩니다.")
            elif win_k == 'champ_c_ret' and results['champ_c_ret'] > results.get('live_a_ret', 0) * 1.05:
                current_config[f"{target_ns}_LIVE_PARAMS"] = current_config.get(f"{target_ns}_CHAMPION_PARAMS", {})
                report_lines.append("♻️ <b>[챔피언 귀환]</b> C가 복귀합니다.")
                
    # ---------------------------------------------------------
    # 👑 엔진 6.5: [V30.0 알파 반감기(Alpha Decay) 및 노화 부검 엔진]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[6.5 알파 반감기(Alpha Decay) 수명 추적]</b>")
    promo_date_str = current_config.get("LIVE_A_PROMOTION_DATE", None)
    
    if promo_date_str:
        promo_date = datetime.strptime(promo_date_str, '%Y-%m-%d')
        days_alive = (datetime.now() - promo_date).days
        
        # 승격(생일) 이후의 실전 데이터만 추출
        decay_df = df[df['entry_date'] >= promo_date_str]
        
        if len(decay_df) >= 8 and days_alive >= 3:
            # 반감기 분할 연산 (전반전 vs 후반전)
            half_point = len(decay_df) // 2
            early_phase = decay_df.iloc[:half_point]
            late_phase = decay_df.iloc[half_point:]
            
            _, early_pf = calculate_metrics(early_phase)
            _, late_pf = calculate_metrics(late_phase)
            
            report_lines.append(f"▪️ 현재 룰 생존 기간: <b>{days_alive}일차</b> (표본 {len(decay_df)}개)")
            report_lines.append(f"▪️ 승격 초기 PF: {early_pf:.2f} ➔ 최근(노화) PF: {late_pf:.2f}")
            
            # 🚨 [알파 붕괴 판정] 손익비가 초기 대비 30% 이상 날아갔거나 1.0 미만일 때
            if late_pf < early_pf * 0.7 or late_pf < 1.0:
                report_lines.append("🚨 <b>[알파 반감기 도달]</b> 룰의 수명이 다했습니다. 선제적 파라미터 폐기를 집행합니다.")
                
                # 🔬 [노화 원인 정밀 부검]
                late_losers = late_phase[late_phase['final_ret'] <= 0]
                if len(late_losers) >= 3:
                    avg_cpv = late_losers['dyn_cpv'].mean()
                    avg_breadth = late_losers['entry_breadth'].mean() if 'entry_breadth' in late_losers.columns else 1.0
                    
                    if avg_breadth < 0.98: 
                        cause = "거시적 시장 폭(Breadth) 붕괴. 지수 착시로 인한 무차별 하락장 전개."
                    elif (10-avg_cpv)*11.1 > 60: 
                        cause = "극단적 윗꼬리(CPV) 급증. 세력들이 해당 타점(룰)을 역이용하여 물량을 넘김."
                    else: 
                        cause = "해당 룰에 대한 시장 참여자들의 역이용 (과최적화 알파 소멸)."
                        
                    report_lines.append(f"💡 <b>[노화 원인 분석]</b>: {cause}")
                    
                # 🛡️ 메타-최적화: 14일을 기다리지 않고 즉각 베이스라인으로 강제 초기화
                current_config["DYNAMIC_MAE_SL"] = -5.0
                current_config["DYNAMIC_MFE_TP"] = 10.0
                report_lines.append("💡 조치: 다음 앙상블이 나올 때까지 가장 보수적인 안전 모드로 회귀합니다.")
            else:
                report_lines.append("✅ <b>[알파 엣지 유지 중]</b> 현재 파라미터가 시장에서 여전히 강력하게 작동 중입니다.")
        else:
            report_lines.append(f"▪️ 생존 {days_alive}일차: 반감기를 판독하기엔 아직 표본이 부족합니다.")
    else:
        current_config["LIVE_A_PROMOTION_DATE"] = datetime.now().strftime('%Y-%m-%d')
        report_lines.append("▪️ 알파 반감기 추적을 위한 최초 승격일을 오늘로 기록했습니다.")

    # ---------------------------------------------------------
    # 👑 엔진 7: [V53.0 듀얼 리그(Standard vs Supernova) 진검승부 결산]
    # ---------------------------------------------------------
    report_lines.append("\n⚔️ <b>[V53.0 듀얼 리그(Standard vs Supernova) 성적 대결]</b>")
    
    # 1. 진영별 데이터 분리
    std_df = df[df['sig_type'].str.contains('STANDARD', na=False)]
    sn_df = df[df['sig_type'].str.contains('SUPERNOVA', na=False)]
    
    def get_league_stats(target_df):
        if len(target_df) == 0: return 0.0, 0.0
        # 복리 누적 수익률 계산
        eq_growth = (np.prod(1 + target_df['final_ret'].dropna() / 100.0) - 1) * 100
        win_rate = (len(target_df[target_df['final_ret'] > 0]) / len(target_df)) * 100
        return eq_growth, win_rate

    std_growth, std_wr = get_league_stats(std_df)
    sn_growth, sn_wr = get_league_stats(sn_df)
    
    report_lines.append(f"▪️ <b>오리지널 진영:</b> 누적 {std_growth:.2f}% | 승률 {std_wr:.1f}% (표본 {len(std_df)}개)")
    report_lines.append(f"▪️ <b>초신성 선취매:</b> 누적 {sn_growth:.2f}% | 승률 {sn_wr:.1f}% (표본 {len(sn_df)}개)")
    
    if len(std_df) > 0 and len(sn_df) > 0:
        if sn_growth > std_growth:
            report_lines.append("🏆 <b>이번 주 승자: [초신성 진영]</b> - 폭등주 타임머신 역추적 데이터 마이닝이 시장을 이기고 있습니다.")
        else:
            report_lines.append("🛡️ <b>이번 주 승자: [오리지널 진영]</b> - 전통적 기술 필터가 휩소를 방어하며 더 안정적으로 우상향 중입니다.")
    else:
        report_lines.append("⚠️ 아직 양 진영의 결산 표본이 모이지 않았습니다.")

    # ==========================================
    # 🚀 최종 저장 및 발송 (단 1번만 실행)
    # ==========================================
    save_config(current_config)
    send_telegram_report("\n".join(report_lines))
    print("✅ 분석 완료! JSON 파일 덮어쓰기 및 텔레그램 발송 성공.")

# ==========================================
# 🕒 루프 실행기
# ==========================================
def system_main_loop():
    tz = pytz.timezone('Asia/Seoul')
    print(f"🕒 [완전 자율 오토파일럿 V12.0] 대기 중... (첫 조율: {START_DATE.strftime('%Y-%m-%d')})")
    
    while True:
        try:
            now = datetime.now(tz)
            if now > START_DATE.replace(tzinfo=tz):
                if now.weekday() == 5 and now.hour == 10 and now.minute == 0:
                    run_autonomous_analysis()
                    time.sleep(65) 
            time.sleep(30)
        except Exception as e:
            err_msg = f"🚨 <b>[오토파일럿 뇌수술 에러]</b> 주말 자율 학습 중 에러 발생:\n{e}"
            print(err_msg)
            send_telegram_report(err_msg)
            time.sleep(300) # 에러 후 5분 대기

if __name__ == "__main__":
    system_main_loop()
