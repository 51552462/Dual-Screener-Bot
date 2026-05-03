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
TELEGRAM_TOKEN_MAIN = "8709452406:AAHGVhTN8hu1ujA_xYUR8GvMPrd-qpMoSRk"
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
    # 1. 파일이 아예 없을 때 처음 생성하는 기본값 세팅
    if not os.path.exists(CONFIG_PATH):
        default_config = {
            "ACTIVE_EXIT_MODE": "HYBRID",
            "WEIGHT_S1": 1.0, "WEIGHT_S4": 1.0,
            "ACCOUNT_SIZE": 20000000,         # 💡 각 로직별 기본 시드 2,000만 원
            "RISK_PCT": 0.02,                 # 💡 고정 리스크 2%
            "CENTRAL_TREASURY_KR": 600000000, # 🏦 [수정] 한국장 초기 국고 6억 원
            "CENTRAL_TREASURY_US": 600000000  # 🏦 [수정] 미국장 초기 국고 6억 원
        }
        with open(CONFIG_PATH, 'w') as f: json.dump(default_config, f, indent=4)
        return default_config
        
    # 2. 기존 파일이 있을 때 읽어오기
    with open(CONFIG_PATH, 'r') as f: 
        config = json.load(f)
        
    # 💡 [국고 자동 입금 로직] 기존 파일에 국고(Treasury) 데이터가 없다면 알아서 6억씩 채워줍니다.
    need_save = False
    if "CENTRAL_TREASURY_KR" not in config:
        config["CENTRAL_TREASURY_KR"] = 600000000  # 6억 원
        need_save = True
    if "CENTRAL_TREASURY_US" not in config:
        config["CENTRAL_TREASURY_US"] = 600000000  # 6억 원
        need_save = True
        
    # 변경 사항이 있으면 JSON 파일에 덮어쓰기
    if need_save:
        with open(CONFIG_PATH, 'w') as f: json.dump(config, f, indent=4)
        print("🏦 [국고 세팅 완료] 시스템에 한국 6억, 미국 6억의 초기 자본이 성공적으로 세팅되었습니다.")
        
    return config

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
        
        # 💡 [핵심 교체] 시장 폭(Breadth) 시계열 배열 생성 및 120일 동적 밴드(상하위 10%) 추출
        breadth_series = (rsp_c / spy_c) / (rsp_c.rolling(50).mean() / spy_c.rolling(50).mean())
        breadth_current = breadth_series.iloc[-1]
        
        # 120일 기준 상/하위 10% 커트라인 (과거 고정값 0.97, 1.03 대체)
        breadth_lower_limit = breadth_series.rolling(120).quantile(0.10).iloc[-1]
        breadth_upper_limit = breadth_series.rolling(120).quantile(0.90).iloc[-1]

        # 💡 [핵심 교체] VIX 120일 평균 및 표준편차 연산 (Z-Score 동적 밴드)
        vix_120_mean = vix_c.rolling(120).mean().iloc[-1]
        vix_120_std = vix_c.rolling(120).std().iloc[-1]
        
        # 현재 VIX의 Z-Score 산출 (최근 120일 대비 얼마나 비정상적으로 튀었는가?)
        current_vix_zscore = (vix_last - vix_120_mean) / vix_120_std if vix_120_std > 0 else 0
        
        # 1. 기본 국면 및 비중 설정 (동적 VIX Z-Score 기준)
        # 평온장(Bull) 기준: 현재 VIX가 120일 평균 미만일 때 (Z-Score < 0)
        if spy_last > spy_ema200 and current_vix_zscore < 0:
            regime = "Bull (상승장)"
            base_w1, base_w4 = 1.2, 0.8
        else:
            regime = "Bear/Chop (하락/횡보)"
            base_w1, base_w4 = 0.5, 1.5

        # 2. 🚨 시장 폭(Breadth) 하위/상위 10% 동적 밴드 이탈 시 비중 패널티/보너스
        breadth_status = "건강 (Broad)"
        if breadth_current < breadth_lower_limit: 
            breadth_status = "취약 (Narrow/쏠림)"
            base_w1 *= 0.5  # 공격(S1) 비중 반토막 (함정 방어)
            base_w4 *= 1.2  # 방어/눌림(S4) 비중 강화
        elif breadth_current > breadth_upper_limit: 
            breadth_status = "강력 (확산)"
            base_w1 *= 1.2  # 공격(S1) 비중 추가 확대

        w_s1, w_s4 = round(base_w1, 2), round(base_w4, 2)
        
        # 3. VIX 기반 동적 룩백 설정 (하드코딩 삭제 및 Z-Score 적용)
        if current_vix_zscore >= 1.5:  # 과거 28.0 하드코딩 대체 (1.5 표준편차 이상 폭등)
            dyn_lookback = 7
            regime = "Bear (극단적 공포장)"
            w_s1, w_s4 = 0.0, 2.0
            vix_status = f"VIX 판독: 120일 평균({vix_120_mean:.1f}) 대비 Z-Score {current_vix_zscore:.2f} 급등 (Bear)"
        elif current_vix_zscore >= 0.0: # 평균 이상
            dyn_lookback = 15
            vix_status = f"VIX 판독: 120일 평균({vix_120_mean:.1f}) 대비 Z-Score {current_vix_zscore:.2f} 경계 (Chop)"
        else:
            dyn_lookback = 45
            regime = "Bull (상승장)"
            vix_status = f"VIX 판독: 120일 평균({vix_120_mean:.1f}) 대비 Z-Score {current_vix_zscore:.2f} 안정 (Bull)"
            
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
    # 👇👇 [수정] 논리 구조 완벽 개선 👇👇
    if dna_drift_warning:
        current_config["OVERDRIVE_ALLOWED"] = False 
        report_lines.append("🛑 <b>[오버드라이브 킬스위치 가동]</b> DNA 변위 감지로 인해 모든 오버드라이브를 강제 차단합니다.")
        
        if "Bull" in regime:
            regime = "Chop (DNA 변위로 인한 선제적 방어)"
            w_s1, w_s4 = 0.5, 1.2 # 공격 비중 강제 축소
    else:
        current_config["OVERDRIVE_ALLOWED"] = True # 정상 시 허용
    # 👆👆 [수정 완료] 👆👆

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
                    
                # 👇👇 [핵심 진화] 꼼수 산수 삭제, 최근 20개 표본 '진짜 시장 데이터' 역추적 자율 튜닝 👇👇
                recent_20 = decay_df.tail(20)
                
                if not recent_20.empty:
                    # 1. new_tp: 최근 20개 종목 MFE의 75백분위수 (상위 25% 타점)
                    recent_mfe_series = ((recent_20['max_high'] - recent_20['entry_price']) / recent_20['entry_price'] * 100)
                    raw_new_tp = np.percentile(recent_mfe_series.dropna(), 75) if len(recent_mfe_series) > 0 else 10.0
                    
                    # 2. new_sl: 최근 20개 중 '패배 종목'의 MAE 평균값 * 0.8
                    recent_losers = recent_20[recent_20['final_ret'] <= 0]
                    if not recent_losers.empty:
                        loser_mae_avg = ((recent_losers['min_low'] - recent_losers['entry_price']) / recent_losers['entry_price'] * 100).mean()
                        raw_new_sl = loser_mae_avg * 0.8
                    else:
                        raw_new_sl = -3.5 # 패배 종목이 아예 없을 경우의 Fail-safe
                    
                    # 3. 지나치게 파격적인 수치가 들어오지 못하도록 최소한의 안전 바운딩
                    new_sl = round(max(-10.0, min(-2.0, raw_new_sl)), 1) 
                    new_tp = round(max(3.0, min(20.0, raw_new_tp)), 1)   
                else:
                    new_sl, new_tp = -3.5, 10.0 # 데이터가 아예 없을 경우의 Fail-safe
                
                current_config["DYNAMIC_MAE_SL"] = new_sl
                current_config["DYNAMIC_MFE_TP"] = new_tp
                
                report_lines.append(f"🚨 <b>[노화 발생 및 엣지 자율 튜닝]</b> 최근 20개 종목의 장중 실전 데이터를 역추적했습니다.")
                report_lines.append(f" ↳ 시스템 익절선(TP): <b>MFE 상위 25% 타점인 {new_tp:.1f}%</b>로 세팅 완료.")
                report_lines.append(f" ↳ 시스템 손절선(SL): <b>패배 종목 평균 MAE의 80%인 {new_sl:.1f}%</b>로 세팅 완료.")
                # 👆👆 [진화 및 덮어쓰기 완료] 👆👆
            else:
                report_lines.append("✅ <b>[알파 엣지 유지 중]</b> 현재 파라미터가 시장에서 여전히 강력하게 작동 중입니다.")
        else:
            report_lines.append(f"▪️ 생존 {days_alive}일차: 반감기를 판독하기엔 아직 표본이 부족합니다.")
    else:
        current_config["LIVE_A_PROMOTION_DATE"] = datetime.now().strftime('%Y-%m-%d')
        report_lines.append("▪️ 알파 반감기 추적을 위한 최초 승격일을 오늘로 기록했습니다.")

    # ---------------------------------------------------------
    # 👑 엔진 7: [V103.0 통합 시스템 데스매치 결산 (자본주의 복리 배분)]
    # ---------------------------------------------------------
    report_lines.append("\n⚔️ <b>[V103.0 통합 시스템 데스매치 결산]</b>")
    
    # 1. 오리지널 vs 초신성 베이스라인 대결
    std_df = df[df['sig_type'].str.contains('STANDARD', na=False)]
    sn_df = df[df['sig_type'].str.contains('SUPERNOVA', na=False)]
    
    # 2. 평행우주 성적 판독 (Out-of-Sample)
    sn_b_ret = sn_df['cand_b_ret'].mean() if not sn_df.empty else -99
    std_a_ret = std_df['live_a_ret'].mean() if not std_df.empty else -99
    
    if sn_b_ret > std_a_ret:
        report_lines.append(f"🔥 <b>[시스템 추월 발생]</b> 초신성 후보군({sn_b_ret:+.2f}%)이 오리지널 실전({std_a_ret:+.2f}%)을 앞질렀습니다.")
    else:
        report_lines.append(f"🛡️ <b>[방어 성공]</b> 오리지널 실전 엔진({std_a_ret:+.2f}%)이 초신성 도전군({sn_b_ret:+.2f}%)을 방어해냈습니다.")
        
    # 💡 [핵심] 인위적인 WEIGHT(1.6 vs 0.4) 강제 배분 로직 완전 삭제
    report_lines.append("✅ <b>알림:</b> 인위적 가중치(WEIGHT) 배분 로직이 삭제되었습니다. 개별 시드의 복리 성장이 곧 자본 배분입니다.")

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
    # ⚙️ 엔진 9.5: [오버드라이브 허들 자율 튜닝부]
    # ---------------------------------------------------------
    od_fails = df[(df['exit_reason'].str.contains('오버드라이브 실패', na=False)) | (df['exit_reason'].str.contains('방어 손절', na=False))]
    if len(od_fails) >= 5: # 실패가 5건 이상 누적 시
        old_energy = current_config.get("OVERDRIVE_ENERGY_HURDLE", 20.0)
        new_energy = min(50.0, old_energy * 1.5) # 1.5배 상향하되 한도(50.0) 설정
        
        if old_energy != new_energy:
            current_config["OVERDRIVE_ENERGY_HURDLE"] = round(new_energy, 1)
            report_lines.append(f"\n⚙️ <b>[오버드라이브 튜닝]</b> 추세 추종 실패 누적 감지 ➔ 오버드라이브 가동 요구 에너지(v_energy) 허들을 {old_energy:.1f}에서 {new_energy:.1f}으로 1.5배 상향 (깐깐하게 적용)")

    # ---------------------------------------------------------
    # 💀 엔진 10: [V60.0 초신성 템플릿 생존 토너먼트 및 국고 환수]
    # ---------------------------------------------------------
    report_lines.append("\n💀 <b>[V60.0 진화론 도태 심판 및 국고 환수]</b>")
    
    sn_all_closed = df[(df['sig_type'].str.contains('SUPERNOVA_초입', na=False)) & (df['status'].str.contains('CLOSED', na=False))]
    
    for mkt in ['KR', 'US']:
        multi_key = f"DNA_SUPERNOVA_{mkt}_MULTI"
        if multi_key not in current_config: continue
        
        market_templates = current_config[multi_key]
        treasury_key = f"CENTRAL_TREASURY_{mkt}"
        current_treasury = current_config.get(treasury_key, 0)
        culled_list = []
        
        for template_name in list(market_templates.keys()):
            t_trades = sn_all_closed[sn_all_closed['sig_type'].str.contains(template_name, na=False)]
            
            if len(t_trades) >= 5:
                t_wins = t_trades[t_trades['final_ret'] > 0]
                t_wr = len(t_wins) / len(t_trades)
                t_pf = t_wins['final_ret'].sum() / (abs(t_trades[t_trades['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
                
                # 🚨 [사형 선고 및 자금 회수]
                if t_wr < 0.35 or t_pf < 1.0:
                    del market_templates[template_name]
                    
                    # 💡 [신규 추가] 해당 로직의 최종 잔고 역산 및 국고 반환
                    total_pnl = (t_trades['sim_kelly_invest'] * t_trades['final_ret'] / 100).sum()
                    final_balance = 20000000 + total_pnl
                    
                    # 국고에 잔고 더하기 (Plus)
                    current_treasury += final_balance 
                    
                    culled_list.append(f"{template_name} (회수금: {final_balance:,.0f}원)")
        
        current_config[multi_key] = market_templates
        current_config[treasury_key] = current_treasury # 업데이트된 국고 저장
        
        if culled_list:
            report_lines.append(f"▪️ <b>{mkt}장 도태 집행 및 국고 환수 완료</b>")
            for c_name in culled_list: 
                report_lines.append(f"  ❌ {c_name}")
            report_lines.append(f"💰 {mkt} 국고 총액: {current_treasury:,.0f}원")

    # ---------------------------------------------------------
    # 👑 엔진 11.5: [V104.5 마르코프 체인 기반 다음 순환매 섹터 예측 및 저장]
    # ---------------------------------------------------------
    report_lines.append("\n🔮 <b>[V104.5 마르코프 체인 기반 순환매 예측 및 저장]</b>")
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        # 최근 60일치 포착 데이터 로드
        rot_df = pd.read_sql("SELECT entry_date, sector FROM forward_trades WHERE entry_date >= date('now', '-60 days') ORDER BY entry_date ASC", conn)
        conn.close()

        if not rot_df.empty:
            # 일자별 1위 대장 섹터 산출
            daily_dom = rot_df.groupby('entry_date')['sector'].agg(lambda x: x.mode()[0] if not x.empty else None).dropna()
            
            transitions = {}
            current_sec = None
            
            # 마르코프 체인 연산 (A ➔ B 자금 이동 궤적 추적)
            for date, sec in daily_dom.items():
                if current_sec is not None and current_sec != sec:
                    t_key = f"{current_sec}➔{sec}"
                    transitions[t_key] = transitions.get(t_key, 0) + 1
                current_sec = sec
                
            if transitions:
                # 가장 빈번하게 발생한 이동 경로(1위) 추출
                top_transition = max(transitions.items(), key=lambda x: x[1])
                top_path = top_transition[0] # 예: "헬스케어➔반도체"
                predicted_sector = top_path.split('➔')[1]
                
                # 👇👇 [핵심 누락 복구] 예측된 도착지 섹터를 관제탑 JSON에 실제 저장 👇👇
                current_config["PREDICTED_NEXT_SECTOR"] = predicted_sector
                # 👆👆 [저장 완료] 👆👆
                
                report_lines.append(f"▪️ <b>최빈 자금 이동 궤적:</b> {top_path} ({top_transition[1]}회 관측)")
                report_lines.append(f"🎯 <b>조치:</b> 다음 주도 섹터를 <b>'{predicted_sector}'</b>(으)로 예측하여 관제탑(JSON)에 각인 완료.")
            else:
                report_lines.append("▪️ 뚜렷한 자금 이동 궤적이 없어 예측을 보류합니다.")
        else:
            report_lines.append("▪️ 순환매 추적을 위한 데이터가 부족합니다.")
    except Exception as e:
        report_lines.append(f"▪️ 순환매 예측 에러: {e}")

    # ---------------------------------------------------------
    # 👑 엔진 12: [V105.0 순환매 예측 로직 자율 검증 및 가중치 부여]
    # ---------------------------------------------------------
    report_lines.append("\n🔄 <b>[V105.0 순환매 예측 로직 자율 검증]</b>")
    
    # 태그 유무로 일반 매매와 선취매 매매를 완벽히 분리
    rot_df = df[df['sig_type'].str.contains('#순환매_선취매', na=False)]
    std_df = df[~df['sig_type'].str.contains('#순환매_선취매', na=False)]
    
    def get_pf(target_df):
        if len(target_df) == 0: return 0
        wins = target_df[target_df['final_ret'] > 0]['final_ret'].sum()
        loses = abs(target_df[target_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1
        return wins / loses

    # 최소 표본 3개 이상일 때만 수학적 검증 진행
    if len(rot_df) >= 3:
        rot_pf = get_pf(rot_df)
        std_pf = get_pf(std_df)
        
        report_lines.append(f" ▪️ 예측그룹 PF: {rot_pf:.2f} vs 일반그룹 PF: {std_pf:.2f}")
        
        # 💡 [자율 진화 핵심] 1.5배 우위 증명 시 가중치 플래그 활성화
        if rot_pf > std_pf * 1.5:
            current_config["ROTATION_ADVANTAGE_ACTIVE"] = True
            report_lines.append("🚀 <b>[검증 성공]</b> 순환매 선취매 우위 증명 ➔ 다음 주 <b>켈리 비중 2배</b> 적용")
        else:
            current_config["ROTATION_ADVANTAGE_ACTIVE"] = False
            report_lines.append("🛡️ <b>[검증 실패]</b> 예측 우위 부족 ➔ 일반 베팅 유지")
    else:
        report_lines.append(" ▪️ 표본 부족으로 순환매 자율 검증 스킵")

    # ---------------------------------------------------------
    # 👑 엔진 13: [V106.0 주차별 로직 일관성 추적 및 시계열 DNA 부검]
    # ---------------------------------------------------------
    report_lines.append("\n⏳ <b>[V106.0 주차별 일관성 추적 및 시계열 DNA 부검]</b>")
    
    try:
        # 💡 [핵심 교정] VIX 동적 룩백에 의해 메인 df가 7일/15일로 잘렸을 경우를 대비하여,
        # 4주치(30일) 청산 데이터를 DB에서 독립적으로 무조건 로드합니다.
        conn = sqlite3.connect(DB_PATH, timeout=60)
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # 👇👇 [치명적 버그 픽스] 장부 오염 방지: R&D 가상 데이터가 S급 챔피언으로 둔갑해 국고를 축내는 현상 원천 차단 👇👇
        query = f"SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND sig_type NOT LIKE '%[R&D_%' AND exit_date >= '{thirty_days_ago}'"
        df_closed_30d = pd.read_sql(query, conn)
        # 👆👆 [픽스 완료] 👆👆
        
        conn.close()

        import re
        def get_core_group(sig):
            sig = str(sig).replace('💀[기각/관찰용] ', '')
            sig = re.sub(r'^\[.*?\]\s*', '', sig)
            return sig.split(' [')[0]

        if not df_closed_30d.empty:
            df_closed_30d['group'] = df_closed_30d['sig_type'].apply(get_core_group)
            df_closed_30d['exit_date_dt'] = pd.to_datetime(df_closed_30d['exit_date'])
            now_dt = datetime.now()
            df_closed_30d['week_idx'] = (now_dt - df_closed_30d['exit_date_dt']).dt.days // 7
            
            group_weekly_pf = {}
            
            for group in df_closed_30d['group'].unique():
                g_df = df_closed_30d[df_closed_30d['group'] == group]
                weekly_pfs = {}
                for w in range(4): # 0주차 ~ 3주차 (최근 1개월)
                    w_df = g_df[g_df['week_idx'] == w]
                    if len(w_df) >= 3: 
                        w_wins = w_df[w_df['final_ret'] > 0]['final_ret'].sum()
                        w_loses = abs(w_df[w_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1
                        weekly_pfs[w] = w_wins / w_loses
                    else:
                        weekly_pfs[w] = None
                group_weekly_pf[group] = weekly_pfs

            consistent_good, consistent_bad, consistent_mid = [], [], []

            for g, pfs in group_weekly_pf.items():
                valid_pfs = [p for p in pfs.values() if p is not None]
                if len(valid_pfs) >= 2: # 최소 2주 이상 활동 검증
                    if all(p >= 1.2 for p in valid_pfs): consistent_good.append(g)      
                    elif all(p <= 0.8 for p in valid_pfs): consistent_bad.append(g)     
                    else: consistent_mid.append(g)                                      
                    
            report_lines.append(f"▪️ <b>장기 우상향(S급) 로직:</b> {', '.join(consistent_good) if consistent_good else '없음'}")
            report_lines.append(f"▪️ <b>장기 우하향(폐급) 로직:</b> {', '.join(consistent_bad) if consistent_bad else '없음'}")

            def extract_cohort_dna(group_list):
                if not group_list: return "표본 없음"
                tgt_df = df_closed_30d[df_closed_30d['group'].isin(group_list)]
                if tgt_df.empty: return "표본 없음"
                c = tgt_df['dyn_cpv'].mean()
                t = tgt_df['dyn_tb'].mean()
                e = tgt_df['v_energy'].mean()
                return f"CPV {c:.2f} | 찐양봉 {t:.1f}배 | 응축 {e:.1f}"

            report_lines.append(f"\n💡 <b>[우상향 로직 절대 공통 DNA]</b>\n ↳ {extract_cohort_dna(consistent_good)}")
            report_lines.append(f"↔️ <b>[횡보/중간 로직 공통 DNA]</b>\n ↳ {extract_cohort_dna(consistent_mid)}")
            report_lines.append(f"💀 <b>[우하향 로직 만성질환 DNA]</b>\n ↳ {extract_cohort_dna(consistent_bad)}")
            
            # 💡 [자율 진화] 장기 우상향 DNA를 MFE 황금 타점으로 강제 흡수 (엔진 8과 시너지)
            if consistent_good:
                best_df = df_closed_30d[df_closed_30d['group'].isin(consistent_good)]
                current_config["DNA_SUPERNOVA_MFE_WEIGHTED"] = {
                    "cpv": round(best_df['dyn_cpv'].mean(), 3),
                    "tb": round(best_df['dyn_tb'].mean(), 3),
                    "bbe": round(best_df['v_energy'].mean(), 3),
                    "last_updated": datetime.now().strftime('%Y-%m-%d')
                }
                report_lines.append("✅ <b>조치:</b> 장기 우상향 DNA를 시스템의 황금 타점(MFE 템플릿)으로 강제 동기화 완료.")

                # 👇👇 [핵심 복구] S급 로직 자본 스노우볼링 (국고 1,000만 원 포상금 지급) 👇👇
                bonus_amount = 10000000 # 1,000만 원 특별 투입
                for top_logic in consistent_good:
                    # 1. 해당 챔피언 로직이 소속된 국가(KR or US) 파악
                    mkt_prefix = best_df[best_df['group'] == top_logic]['market'].iloc[0]
                    t_key = f"CENTRAL_TREASURY_{mkt_prefix}"
                    
                    # 2. 해당 국가의 국고에 포상금을 줄 돈이 남아있는지 팩트 체크
                    if current_config.get(t_key, 0) >= bonus_amount:
                        # 3. 국고에서 1,000만 원 차감 (마이너스)
                        current_config[t_key] -= bonus_amount
                        
                        # 4. 해당 로직의 개별 복리 시드 계좌에 1,000만 원 입금 (플러스)
                        bonus_key = f"BONUS_SEED_{top_logic}"
                        current_config[bonus_key] = current_config.get(bonus_key, 0) + bonus_amount
                        
                        report_lines.append(f"💰 <b>[자본 스노우볼링]</b> 4주 연속 우상향 증명! S급 로직 '{top_logic}' 장부에 {mkt_prefix} 국고 보너스 1,000만 원 투입 완료.")
                # 👆👆 [복구 완료] 👆👆
        else:
            report_lines.append(" ▪️ 시계열 추적을 위한 청산 데이터가 아직 부족합니다.")
    except Exception as e:
        report_lines.append(f" ▪️ 시계열 분석 에러: {e}")


    # 👇👇 [신규 추가] 엔진 14: R&D 샌드박스 역추적 및 CSV 머신러닝 시너지 연동 👇👇
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        rnd_df = pd.read_sql("SELECT * FROM forward_trades WHERE sig_type = '[R&D_평균볼륨군]' AND status LIKE 'CLOSED%'", conn)
        conn.close()
        
        if len(rnd_df) >= 5:
            # Winner: MFE 10% 이상 & 최종 수익 마감 / Loser: MAE 손절(수익률 0 이하)
            rnd_winners = rnd_df[(rnd_df['mfe'] >= 10.0) & (rnd_df['final_ret'] > 0)]
            rnd_losers = rnd_df[rnd_df['final_ret'] <= 0]
            
            if len(rnd_winners) > 0 and len(rnd_losers) > 0:
                w_cpv, l_cpv = rnd_winners['dyn_cpv'].mean(), rnd_losers['dyn_cpv'].mean()
                w_tb, l_tb = rnd_winners['dyn_tb'].mean(), rnd_losers['dyn_tb'].mean()
                w_bbe, l_bbe = rnd_winners['v_energy'].mean(), rnd_losers['v_energy'].mean()
                
                # 텔레그램 리포트 최하단에 분리 출력
                rnd_report = "\n🧪 <b>[R&D 실험실 역추적 결과: 40~70점대 평균볼륨군]</b>\n"
                rnd_report += f"▪️ 표본수: 승리(Winner) {len(rnd_winners)}개 vs 패배(Loser) {len(rnd_losers)}개\n"
                rnd_report += f"💡 <b>[돌연변이 공통점 DNA 차집합]</b>\n"
                
                if w_bbe > l_bbe: rnd_report += f" ↳ 승리 종목은 패배 종목보다 '응축 에너지(BBE)'가 평균 {w_bbe/l_bbe:.1f}배 높음.\n"
                if w_tb > l_tb: rnd_report += f" ↳ 승리 종목은 패배 종목보다 '진짜양봉(TB)'이 평균 {w_tb/l_tb:.1f}배 강력함.\n"
                rnd_report += f" ↳ (평균 수치 대조) Winner [CPV: {w_cpv:.2f} | BBE: {w_bbe:.1f}] vs Loser [CPV: {l_cpv:.2f} | BBE: {l_bbe:.1f}]\n"
                
                # 👑 CSV 파이프라인 연동 (초신성과 동일한 양식으로 마이닝 데이터 적재)
                csv_path = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Supernova_Flow_Tracking_Master.csv')
                csv_data = []
                for _, r in rnd_winners.iterrows():
                    csv_data.append({
                        '종목코드': str(r['code']).zfill(6) if r['market'] == 'KR' else str(r['code']),
                        '시장': r['market'],
                        '랭크': 'R&D_MUTANT',
                        '[D_Day_당일] 평균_CPV': round(r['dyn_cpv'], 4),
                        '[D_Day_당일] 평균_진짜양봉(TB)': round(r['dyn_tb'], 4),
                        '[D_Day_당일] 평균_응축에너지(BBE)': round(r['v_energy'], 4),
                        '[D_Day_당일] 진모멘텀(TML)': 0.0, # R&D에는 없으므로 0 대체
                        '[D_Day_당일] 평균_시장강도(RS)': round(r['v_rs'], 4) if pd.notna(r['v_rs']) else 0.0
                    })
                
                # 👇👇 [핵심 진화] 헤더 누락 방지 및 파일 생성 로직 교정 👇👇
                if csv_data:
                    df_csv = pd.DataFrame(csv_data)
                    # 파일이 없을 때만 헤더를 True로 써서 컬럼명이 꼬이는 것을 완벽히 방지
                    write_header = not os.path.exists(csv_path)
                    df_csv.to_csv(csv_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                    rnd_report += f"\n💾 <b>[마이닝 연동]</b> {len(csv_data)}개의 R&D 돌연변이 DNA가 K-Means 학습용 CSV에 추가 적재되었습니다."
                # 👆👆 [수정 완료] 👆👆
                
                report_lines.append(rnd_report)
    except Exception as e:
        report_lines.append(f"\n⚠️ R&D 실험실 에러: {e}")
    # 👆👆 [신규 추가 끝] 👆👆

        # 👇👇 [신규 추가] 엔진 16: STANDARD 오리지널 대박주 CSV 마이닝 파이프라인 👇👇
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        # ORIGINAL 진영의 청산된 종목만 로드
        std_df = pd.read_sql("SELECT * FROM forward_trades WHERE sig_type LIKE '%[STANDARD_ORIGINAL]%' AND status LIKE 'CLOSED%'", conn)
        conn.close()
        
        # 조건: 실전에서 MFE 10% 이상 찍어본 진짜 대박주만 핀셋 추출
        std_winners = std_df[(std_df['mfe'] >= 10.0) & (std_df['final_ret'] > 0)]
        
        if len(std_winners) > 0:
            csv_path_std = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Standard_Flow_Master.csv')
            csv_data_std = []
            
            for _, r in std_winners.iterrows():
                csv_data_std.append({
                    '종목코드': str(r['code']).zfill(6) if r['market'] == 'KR' else str(r['code']),
                    '시장': r['market'],
                    '랭크': 'STANDARD_WINNER',
                    '[D_Day_당일] 평균_CPV': round(r['dyn_cpv'], 4),
                    '[D_Day_당일] 평균_진짜양봉(TB)': round(r['dyn_tb'], 4),
                    '[D_Day_당일] 평균_응축에너지(BBE)': round(r['v_energy'], 4),
                    '[D_Day_당일] 진모멘텀(TML)': 0.0,
                    '[D_Day_당일] 평균_시장강도(RS)': round(r['v_rs'], 4) if pd.notna(r['v_rs']) else 0.0
                })
            
            df_csv_std = pd.DataFrame(csv_data_std)
            write_header = not os.path.exists(csv_path_std)
            df_csv_std.to_csv(csv_path_std, mode='a', header=write_header, index=False, encoding='utf-8-sig')
            
            report_lines.append(f"💾 <b>[오리지널 마이닝 연동]</b> {len(csv_data_std)}개의 STANDARD 대박주 DNA가 듀얼 트랙 진화를 위해 ML 파이프라인에 전송되었습니다.")
    except Exception as e:
        report_lines.append(f"\n⚠️ STANDARD 마이닝 파이프라인 에러: {e}")
    # 👆👆 [신규 추가 끝] 👆👆

    # ==========================================
    # 🚀 최종 저장 및 발송 (중복 제거 완료)
    # ==========================================
    save_config(current_config)
    send_telegram_report("\n".join(report_lines))
    print("✅ 분석 완료! JSON 파일 덮어쓰기 및 텔레그램 발송 성공.")

# ==========================================
# 👑 엔진 11: [V100.0 주간 흐름(Flow) 총결산 마스터 리포트]
# ==========================================

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
