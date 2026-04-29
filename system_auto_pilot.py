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
TELEGRAM_TOKEN_MAIN = "7988939051:AAH18gmMs9syze2g4zo7Xd2stMdyREg66rI"
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
        
        # 👇👇 [수정] 초신성 태그 감지 시 전용 네임스페이스 즉시 반환 👇👇
        if "SUPERNOVA" in st: 
            return f"{m}_SUPERNOVA_MASTER"
            
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
    # 👑 엔진 7: [V53.0 무차별 통합 리그 결산]
    # ---------------------------------------------------------
    report_lines.append("\n⚔️ <b>[V53.0 통합 시스템 데스매치 결산]</b>")
    
    # 1. 오리지널 vs 초신성 베이스라인 대결
    std_df = df[df['sig_type'].str.contains('STANDARD', na=False)]
    sn_df = df[df['sig_type'].str.contains('SUPERNOVA', na=False)]
    
    # 2. ABC 평행우주별 '진짜 승자' 판독 (Out-of-Sample)
    # 초신성 진영의 후보(B) 성적이 기존 오리지널 실전(A)을 이기고 있다면?
    sn_b_ret = sn_df['cand_b_ret'].mean() if not sn_df.empty else -99
    std_a_ret = std_df['live_a_ret'].mean() if not std_df.empty else -99
    
    if sn_b_ret > std_a_ret:
        report_lines.append(f"🔥 <b>[시스템 추월 발생]</b> 초신성 후보군({sn_b_ret:+.2f}%)이 오리지널 실전({std_a_ret:+.2f}%)을 앞질렀습니다.")
        # 조치: 초신성 진영의 비중을 대폭 상향하고 오리지널은 관찰 모드로 전환
        current_config["WEIGHT_SUPERNOVA"] = 1.6
        current_config["WEIGHT_STANDARD"] = 0.4
        report_lines.append("🚀 <b>액션:</b> 다음 주 자본의 80%를 초신성 엔진에 우선 배정합니다.")
    else:
        report_lines.append(f"🛡️ <b>[방어 성공]</b> 오리지널 실전 엔진이 초신성 도전군을 방어해냈습니다.")
        current_config["WEIGHT_SUPERNOVA"] = 0.8
        current_config["WEIGHT_STANDARD"] = 1.2

    # ---------------------------------------------------------
    # 👑 엔진 8: [V55.0 초신성 실전 흐름 역추적 및 MFE 가중치 템플릿 진화]
    # ---------------------------------------------------------
    report_lines.append("\n🔬 <b>[V55.0 초신성 실전 DNA 검증 및 MFE 가중치 템플릿]</b>")
    
    # 1. 가상매매 장부에서 청산 완료된 '초신성' 데이터만 발췌
    sn_closed = df[(df['sig_type'].str.contains('SUPERNOVA', na=False)) & (df['status'].str.contains('CLOSED', na=False))]
    
    if len(sn_closed) >= 5:
        # 2. [미래 흐름 연결성] 실전에서 MFE(최대 수익률) 10% 이상을 달성한 '진짜 대박주'만 추출
        high_mfe_sn = sn_closed[sn_closed['mfe'] >= 10.0]
        
        if not high_mfe_sn.empty:
            report_lines.append(f"▪️ <b>실전 고수익(MFE 10%↑) 초신성 표본:</b> {len(high_mfe_sn)}개 발견")
            
            # 3. 승리한 초신성들의 '실제 DNA 수치값' 평균 산출 (MFE 가중치 템플릿)
            real_cpv = high_mfe_sn['dyn_cpv'].mean()
            real_tb = high_mfe_sn['dyn_tb'].mean()
            real_bbe = high_mfe_sn['v_energy'].mean()
            
            # 기존 과거 백테스트 템플릿(Centroid) 값 가져오기 (비교용)
            multi_templates = current_config.get(f"DNA_SUPERNOVA_{high_mfe_sn['market'].iloc[0]}_MULTI", {})
            
            report_lines.append(f"💡 <b>[과거 하드코딩 vs 실전 MFE DNA 대조]</b>")
            
            # 4. 랭크 및 그룹별(RANK_A, RANK_B 등) 하드코딩 수치 생존 추적 및 오차율 계산
            for rank in ['RANK_A', 'RANK_B', 'RANK_C', 'RANK_D']:
                rank_df = high_mfe_sn[high_mfe_sn['sig_type'].str.contains(rank, na=False)]
                if not rank_df.empty:
                    rank_mfe = rank_df['mfe'].mean()
                    rank_cpv = rank_df['dyn_cpv'].mean()
                    rank_tb = rank_df['dyn_tb'].mean()
                    
                    report_lines.append(f" ↳ <b>[{rank}]</b> 평균 MFE: <b>{rank_mfe:.1f}%</b> 달성")
                    report_lines.append(f"    - 실전 CPV: {rank_cpv:.2f} | 실전 TB: {rank_tb:.1f}")
            
            # 5. [메타 최적화] 오리지널 로직의 '스무딩(Smoothing)' 오토 추적 시스템 이식
            # 과거의 템플릿을 한 번에 갈아엎지 않고, SMOOTHING_ALPHA(0.3) 비율만큼만 시장 흐름을 부드럽게 흡수합니다.
            
            old_mfe_template = current_config.get("DNA_SUPERNOVA_MFE_WEIGHTED", {"cpv": real_cpv, "tb": real_tb})
            
            # (기존 값 * 0.7) + (새로운 실전 값 * 0.3) = 점진적 오토 추적
            smoothed_cpv = (old_mfe_template["cpv"] * (1 - SMOOTHING_ALPHA)) + (real_cpv * SMOOTHING_ALPHA)
            smoothed_tb = (old_mfe_template["tb"] * (1 - SMOOTHING_ALPHA)) + (real_tb * SMOOTHING_ALPHA)
            smoothed_bbe = (old_mfe_template.get("bbe", real_bbe) * (1 - SMOOTHING_ALPHA)) + (real_bbe * SMOOTHING_ALPHA)
            
            current_config["DNA_SUPERNOVA_MFE_WEIGHTED"] = {
                "cpv": round(smoothed_cpv, 3),
                "tb": round(smoothed_tb, 3),
                "bbe": round(smoothed_bbe, 3),
                "last_updated": datetime.now().strftime('%Y-%m-%d')
            }
            
            report_lines.append(f"\n🧬 <b>[MFE 황금 템플릿 오토 스무딩]</b>")
            report_lines.append(f" ↳ CPV: {old_mfe_template.get('cpv', real_cpv):.2f} ➔ <b>{smoothed_cpv:.2f}</b>")
            report_lines.append(f" ↳ TB: {old_mfe_template.get('tb', real_tb):.1f} ➔ <b>{smoothed_tb:.1f}</b>")
            report_lines.append(f" ↳ BBE: {old_mfe_template.get('bbe', real_bbe):.1f} ➔ <b>{smoothed_bbe:.1f}</b>")

    # 👇👇 [기존 엔진 9 영역을 이걸로 완전히 덮어쓰세요] 👇👇
    # ---------------------------------------------------------
    # 👑 엔진 9: [V56.0 초신성 내부 서브-데스매치 & 컷오프 자율 튜닝]
    # ---------------------------------------------------------
    report_lines.append("\n⚙️ <b>[V56.0 초신성 내부 결투 및 자율 튜닝]</b>")
    
    # 코사인 진영과 ML박스 진영의 유동적 컷오프 자율 튜닝 (독립 진행)
    for tag_key, config_key in [("COSINE", "DYNAMIC_SUPERNOVA_CUTOFF"), ("MLBOX", "DYNAMIC_ML_BOX_CUTOFF")]:
        sub_df = df[(df['sig_type'].str.contains(tag_key, na=False)) & (df['status'].str.contains('CLOSED', na=False))]
        
        curr_val = current_config.get(config_key, 0.50) # 기본 50%
        
        if len(sub_df) >= 5:
            wr = len(sub_df[sub_df['final_ret'] > 0]) / len(sub_df)
            pf = sub_df[sub_df['final_ret'] > 0]['final_ret'].sum() / (abs(sub_df[sub_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
            
            report_lines.append(f"▪️ [{tag_key} 타점]: 승률 {wr*100:.1f}% | PF {pf:.2f} (표본 {len(sub_df)}개)")
            
            if wr < 0.45: # 승률 낮으면 허들 조이기
                new_val = min(0.90, curr_val + 0.05)
                current_config[config_key] = round(new_val, 2)
                report_lines.append(f" 🚨 <b>[방어력 강화]</b> 승률 저조 ➔ 허들을 {new_val*100:.0f}%로 상향 조율")
            elif wr > 0.65 and len(sub_df) < 10: # 승률 좋은데 표본 적으면 그물 넓히기
                new_val = max(0.40, curr_val - 0.03)
                current_config[config_key] = round(new_val, 2)
                report_lines.append(f" 🔥 <b>[공격적 포착]</b> 승률 우수 ➔ 허들을 {new_val*100:.0f}%로 하향 조율")
            else:
                report_lines.append(f" ✅ <b>[최적 균형]</b> 현재 커트라인({curr_val*100:.0f}%) 유지")
        else:
            report_lines.append(f"▪️ [{tag_key} 타점]: 현재 커트라인 {curr_val*100:.0f}% (표본 데이터 수집 중)")

    # ---------------------------------------------------------
    # 💀 엔진 10: [V60.0 초신성 템플릿 생존 토너먼트 (자연 도태)]
    # ---------------------------------------------------------
    report_lines.append("\n💀 <b>[V60.0 진화론 기반 템플릿 도태 심판]</b>")
    
    # 초신성 태그로 진입하여 청산 완료된 전체 데이터 추출
    sn_all_closed = df[(df['sig_type'].str.contains('SUPERNOVA_초입', na=False)) & (df['status'].str.contains('CLOSED', na=False))]
    
    for mkt in ['KR', 'US']:
        multi_key = f"DNA_SUPERNOVA_{mkt}_MULTI"
        if multi_key not in current_config: continue
        
        market_templates = current_config[multi_key]
        culled_list = []
        
        # 현재 살아있는 각 템플릿 버전에 대해 성적 평가
        for template_name in list(market_templates.keys()):
            # 해당 템플릿 이름표를 달고 진입했던 매매 내역만 필터링
            t_trades = sn_all_closed[sn_all_closed['sig_type'].str.contains(template_name, na=False)]
            
            # 💡 [도태 기준] 최소 5번 이상 매매해 본 템플릿만 평가대에 올림
            if len(t_trades) >= 5:
                t_wins = t_trades[t_trades['final_ret'] > 0]
                t_wr = len(t_wins) / len(t_trades)
                t_pf = t_wins['final_ret'].sum() / (abs(t_trades[t_trades['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
                
                # 🚨 [사형 선고] 승률 35% 미만이거나, 손익비가 1.0(본전)이 안 되면 영구 삭제
                if t_wr < 0.35 or t_pf < 1.0:
                    del market_templates[template_name]
                    culled_list.append(f"{template_name} (승률 {t_wr*100:.1f}%, PF {t_pf:.2f})")
        
        # 도태된 결과를 JSON에 반영
        current_config[multi_key] = market_templates
        
        if culled_list:
            report_lines.append(f"▪️ <b>{mkt}장 도태 집행: {len(culled_list)}개 유전자 영구 삭제</b>")
            for c_name in culled_list: 
                report_lines.append(f"  ❌ {c_name}")
        else:
            report_lines.append(f"▪️ {mkt}장: 검증 대상이 없거나, 모든 유전자가 생존 기준을 통과했습니다.")

    # ==========================================
    # 🚀 최종 저장 및 발송 (단 1번만 실행)
    # ==========================================
    save_config(current_config)
    send_telegram_report("\n".join(report_lines))
    print("✅ 분석 완료! JSON 파일 덮어쓰기 및 텔레그램 발송 성공.")

# ==========================================
# 👑 엔진 11: [V100.0 주간 흐름(Flow) 총결산 마스터 리포트]
# ==========================================
def send_weekly_flow_master_report():
    """일주일간의 하루하루 자금 흐름, 승률 변화, 섹터 이동 궤적을 총결산하는 마스터 결과지"""
    tz_kr = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz_kr)
    week_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d')
    today_str = now.strftime('%Y-%m-%d')
    
    sys_config = load_or_create_config()
    regime = sys_config.get("CURRENT_REGIME_KEY", "UNKNOWN")
    base_seed = sys_config.get("ACCOUNT_SIZE", 20000000)

    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        
        report_msg = f"🗺️ <b>[V100.0 퀀트 팩토리 주간 흐름(Flow) 총결산]</b>\n📅 기간: {week_ago} ~ {today_str}\n"
        report_msg += "━━━━━━━━━━━━━━━━━━\n"

        for market in ['KR', 'US']:
            market_icon = "🇰🇷" if market == 'KR' else "🇺🇸"
            report_msg += f"\n{market_icon} <b>[{market} 일주일 자금 및 섹터 흐름 궤적]</b>\n"
            
            # ------------------------------------------------
            # 1. 일자별(Day-by-Day) 자금 흐름 및 승률 궤적
            # ------------------------------------------------
            report_msg += f"🗓️ <b>[일자별 실현 손익 및 승률 타임라인]</b>\n"
            
            cursor = conn.execute("""
                SELECT exit_date, 
                       SUM((sim_kelly_invest * final_ret) / 100) as daily_pnl,
                       SUM(CASE WHEN final_ret > 0 THEN 1 ELSE 0 END) as wins,
                       COUNT(*) as total
                FROM forward_trades 
                WHERE market=? AND exit_date >= ? AND status LIKE 'CLOSED%'
                GROUP BY exit_date ORDER BY exit_date ASC
            """, (market, week_ago))
            
            daily_stats = cursor.fetchall()
            weekly_pnl = 0.0
            
            if daily_stats:
                for row in daily_stats:
                    e_date, d_pnl, wins, total = row[0], row[1] or 0.0, row[2], row[3]
                    d_wr = (wins / total) * 100 if total > 0 else 0
                    weekly_pnl += d_pnl
                    # 날짜에서 월-일만 추출 (예: 05-28)
                    short_date = e_date[5:]
                    icon = "🔴" if d_pnl < 0 else "🟢"
                    report_msg += f" {icon} {short_date}: <b>{d_pnl:+,.0f}원</b> (승률 {d_wr:.0f}% / {total}건 청산)\n"
                
                report_msg += f" 💰 <b>주간 누적 실현 손익: {weekly_pnl:+,.0f} 원</b>\n"
            else:
                report_msg += " ↳ 이번 주 청산 데이터가 없습니다.\n"

            # ------------------------------------------------
            # 2. 일주일간 섹터 자금 이동 궤적 (요일별 흐름)
            # ------------------------------------------------
            report_msg += f"\n🔄 <b>[주간 주도 섹터 진화 궤적]</b>\n"
            cursor = conn.execute("""
                SELECT entry_date, sector 
                FROM forward_trades 
                WHERE market=? AND entry_date >= ? 
                ORDER BY entry_date ASC
            """, (market, week_ago))
            
            rot_df = pd.DataFrame(cursor.fetchall(), columns=['entry_date', 'sector'])
            if not rot_df.empty:
                daily_dom = rot_df.groupby('entry_date')['sector'].agg(lambda x: x.mode()[0] if not x.empty else None).dropna()
                flow_path = []
                for d, s in daily_dom.items():
                    flow_path.append(f"{s[:4]}({d[5:]})")
                
                # ➔ 화살표로 이어붙여서 일주일의 흐름을 한눈에 시각화
                report_msg += f" 🌊 <b>흐름:</b> {' ➔ '.join(flow_path)}\n"
            else:
                report_msg += " ↳ 섹터 편입 데이터가 없습니다.\n"

            # ------------------------------------------------
            # 3. 주간 MVP 로직 (이번 주 가장 돈을 많이 벌어온 로직)
            # ------------------------------------------------
            report_msg += f"\n🏆 <b>[이번 주 MVP 시그널 엔진]</b>\n"
            cursor = conn.execute("""
                SELECT sig_type, SUM((sim_kelly_invest * final_ret) / 100) as profit, COUNT(*) 
                FROM forward_trades 
                WHERE market=? AND exit_date >= ? AND status LIKE 'CLOSED%'
                GROUP BY sig_type ORDER BY profit DESC LIMIT 3
            """, (market, week_ago))
            
            top_sigs = cursor.fetchall()
            if top_sigs:
                for i, row in enumerate(top_sigs):
                    sig, pnl, cnt = row[0], row[1] or 0.0, row[2]
                    clean_sig = str(sig).split(']')[0] + "]" if "]" in str(sig) else str(sig)[:15]
                    medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
                    report_msg += f" {medal} {clean_sig}: <b>{pnl:+,.0f}원</b> 기여 ({cnt}건)\n"
            else:
                report_msg += " ↳ MVP 데이터가 없습니다.\n"

        # ------------------------------------------------
        # 4. 관제탑 주말 메타 최적화 결과 요약 (Before & After)
        # ------------------------------------------------
        report_msg += f"\n⚙️ <b>[주말 관제탑 자율 튜닝 결과 요약]</b>\n"
        report_msg += f" ▪️ <b>현재 국면:</b> {regime}\n"
        report_msg += f" ▪️ <b>동적 켈리 비중:</b> {sys_config.get('DYNAMIC_KELLY_RISK', 0.01)*100:.1f}%\n"
        report_msg += f" ▪️ <b>초신성 허들:</b> 코사인 {sys_config.get('DYNAMIC_SUPERNOVA_CUTOFF', 0.50)*100:.0f}% | ML박스 {sys_config.get('DYNAMIC_ML_BOX_CUTOFF', 0.50)*100:.0f}%\n"
        report_msg += f" ▪️ <b>로직 수명:</b> 최초 작동일로부터 {(now - datetime.strptime(sys_config.get('LIVE_A_PROMOTION_DATE', today_str), '%Y-%m-%d').replace(tzinfo=tz_kr)).days}일차 유지 중\n"

        conn.close()
    except Exception as e:
        report_msg += f"\n⚠️ 주간 리포트 생성 중 에러: {e}"

    report_msg += "\n━━━━━━━━━━━━━━━━━━\n💡 <i>시스템이 일주일간 시장의 궤적을 어떻게 흡수하고 진화했는지 증명하는 마스터 결과지입니다.</i>"
    send_telegram_report(report_msg)

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
                # 1. 토요일 오전 10시 정각: 1주일치 데이터를 모아 파라미터 자율 최적화 (뇌수술)
                if now.weekday() == 5 and now.hour == 10 and now.minute == 0:
                    print("🚀 주말 관제탑 자율 튜닝(뇌수술)을 시작합니다...")
                    run_autonomous_analysis()
                    time.sleep(60) 
                    
                # 2. 토요일 오전 10시 5분: 뇌수술 결과를 포함하여 일주일간의 흐름 총결산 리포트 발송
                elif now.weekday() == 5 and now.hour == 10 and now.minute == 5:
                    print("🚀 주간 흐름(Flow) 마스터 총결산 리포트를 발송합니다...")
                    send_weekly_flow_master_report()
                    time.sleep(60)
                    
            time.sleep(30)
        except Exception as e:
            err_msg = f"🚨 <b>[오토파일럿 뇌수술 에러]</b> 주말 자율 학습 중 에러 발생:\n{e}"
            print(err_msg)
            send_telegram_report(err_msg)
            time.sleep(300) # 에러 후 5분 대기

if __name__ == "__main__":
    system_main_loop()
