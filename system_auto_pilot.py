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
    # 👑 엔진 4: [V40.0 True Walk-Forward 다중 타임프레임 앙상블]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[4. 다중 타임프레임 앙상블(Train Set) 최적화]</b>")
    
    # 💡 [V40.0 핵심] 최근 14일은 OOS(미지의 데이터)이므로 학습에서 완벽히 격리(차단)
    oos_barrier = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')

    def get_period_stats(train_days):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=60)
            conn.execute("PRAGMA journal_mode=WAL;")
            
            # 💡 [시간축 교정] 훈련 시작일 = 현재 - 14일(OOS격리) - 학습일수(Train)
            s_date = (datetime.now() - timedelta(days=14 + train_days)).strftime('%Y-%m-%d')
            
            # 쿼리: s_date부터 oos_barrier(14일 전) 직전까지만 긁어옴 (답안지 훔쳐보기 원천 차단)
            p_df = pd.read_sql(f"SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND entry_date >= '{s_date}' AND entry_date < '{oos_barrier}'", conn)
            conn.close()
            
            n_trades = len(p_df)
            if n_trades < 5: 
                # 👇👇 [추가] V44.0 독성 시장(Toxic Market) 판독 👇👇
                if n_trades > 0:
                    toxic_wins = len(p_df[p_df['final_ret'] > 0])
                    toxic_avg_ret = p_df['final_ret'].mean()
                    # 단 1~4개의 표본이라도 전패(승률 0%)이거나, 평균 수익률이 -3% 이하라면
                    if toxic_wins == 0 or toxic_avg_ret <= -3.0:
                        return "TOXIC" # 단순 표본 부족이 아닌 시장 붕괴로 규정
                return None # 순수한 표본 부족 (평화롭지만 타점이 안 온 상태)
                # 👆👆 [추가 끝] 👆👆
            
            win_s = p_df[p_df['final_ret'] > 0]
            lose_s = p_df[p_df['final_ret'] <= 0]
            
            gross_profit = win_s['final_ret'].sum() if len(win_s) > 0 else 0
            gross_loss = abs(lose_s['final_ret'].sum()) + 0.1
            pf = gross_profit / gross_loss
            
            # 컷오프 룰: 손익비(PF)가 0.85 ~ 1.25 사이인 횡보 장세 기각
            is_meaningless_chop = (0.85 <= pf <= 1.25) and (n_trades < 20)
            if is_meaningless_chop:
                return "NOISE" 

            p_df['mae_pct'] = (p_df['min_low'] - p_df['entry_price']) / p_df['entry_price'] * 100
            p_df['mfe_pct'] = (p_df['max_high'] - p_df['entry_price']) / p_df['entry_price'] * 100
            
            win_s = p_df[p_df['final_ret'] > 0]
            lose_s = p_df[p_df['final_ret'] <= 0]

            # [V35.0 자율 커트라인 도출]
            opt_alpha_limit, opt_trap_limit, opt_dtw_limit = 0.75, 0.75, 2.5
            if 'entry_cos_score' in p_df.columns and len(win_s) >= 3:
                opt_alpha_limit = np.percentile(win_s['entry_cos_score'].dropna(), 15)
                opt_dtw_limit = np.percentile(win_s['entry_dtw_score'].dropna(), 85)
                if len(lose_s) >= 3:
                    opt_trap_limit = np.percentile(lose_s['entry_cos_score'].dropna(), 50)

            # [V37.0 파라미터 절벽 검증 및 고원 확보]
            raw_sl = np.percentile(win_s['mae_pct'], 15) if len(win_s) >= 3 else -3.5
            raw_tp = np.percentile(win_s['mfe_pct'], 50) if len(win_s) >= 3 else 10.0
            robust_sl = raw_sl
            if len(win_s) >= 5:
                cliff_zone_wins = win_s[(win_s['mae_pct'] <= raw_sl + 0.5) & (win_s['mae_pct'] >= raw_sl - 0.5)]
                if len(cliff_zone_wins) / len(win_s) >= 0.30:
                    robust_sl = raw_sl - 1.0 

            return {
                "sl": robust_sl,
                "tp": raw_tp,
                "fatal_cpv": np.percentile(lose_s['v_cpv'].dropna(), 90) if len(lose_s) >= 3 else 0.85,
                "alpha_limit": opt_alpha_limit,
                "trap_limit": opt_trap_limit,
                "dtw_limit": opt_dtw_limit
            }
        except Exception as e:
            print(f"앙상블 에러: {e}")
            return None

    # 3. [앙상블 실행] 14일, 30일, 60일의 지혜를 블렌딩
    t14, t30, t60 = get_period_stats(14), get_period_stats(30), get_period_stats(60)
    
    report_lines.append("\n<b>[4. 다중 타임프레임 앙상블 및 신뢰도 컷오프]</b>")
    
    # 👇👇 [추가] V44.0 TOXIC 인터셉트 및 텔레그램 피드백 👇👇
    is_toxic = False
    for name, t_stat in zip(['14일', '30일', '60일'], [t14, t30, t60]):
        if t_stat == "TOXIC":
            report_lines.append(f"🚨 {name} 데이터: <b>[TOXIC 붕괴]</b> 표본은 극소수이나 전패/급락 상태입니다.")
            is_toxic = True
        elif t_stat == "NOISE": 
            report_lines.append(f"🛡️ {name} 데이터: <b>무의미한 횡보(노이즈)</b>로 판독되어 앙상블 배제.")
        elif not t_stat: 
            report_lines.append(f"▪️ {name} 데이터: 순수 표본 부족으로 배제.")

    # TOXIC 발동 시 앙상블 무시하고 '극단적 방어 모드' 후보군 강제 생성
    if is_toxic:
        report_lines.append("🚨 <b>[V44.0 독성 장세 세이프가드 발동]</b> 데이터 부족을 가장한 폭락장입니다. '기존 수치 유지'를 거부하고 최강 방어 파라미터를 강제 하달합니다.")
        if "CANDIDATE_PARAMS" not in current_config: 
            current_config["CANDIDATE_PARAMS"] = {}
        
        current_config["CANDIDATE_PARAMS"] = {
            "DYNAMIC_MAE_SL": -2.5,       # 💡 손절선을 -2.5%로 바짝 당겨 철통 방어
            "DYNAMIC_MFE_TP": 10.0,
            "TREE_FATAL_CPV": 0.70,       # 💡 윗꼬리 허용치 대폭 축소 (0.7 이상 무조건 기각)
            "DYNAMIC_ALPHA_LIMIT": 0.85,  # 💡 대장주 DNA와 85% 이상 똑같아야만 진입
            "DYNAMIC_TRAP_LIMIT": 0.70,   # 💡 참사주 냄새만 나도 기각
            "DYNAMIC_DTW_LIMIT": 1.5      # 💡 궤적이 1.5 이하로 거의 완벽하게 똑같아야 통과
        }
        report_lines.append("▪️ 앙상블 결과: <b>안전판 강제 생성 완료 (SL -2.5% / DNA 85%↑)</b>")

    else:
        # NOISE나 None, TOXIC이 아닌 순수 유효 데이터만 필터링
        valid = [s for s in [t14, t30, t60] if isinstance(s, dict)]
        
        if len(valid) >= 2:
            # 단기 데이터가 있으면 가중 평균(5:3:2), 없으면 유효 데이터끼리 평균 적용
            w = [0.5, 0.3, 0.2] if t14 and t30 and t60 else [1/len(valid)] * len(valid)
            
            ensemble_sl = sum(s['sl'] * w[i] for i, s in enumerate(valid))
            ensemble_tp = sum(s['tp'] * w[i] for i, s in enumerate(valid))
            ensemble_cpv = sum(s['fatal_cpv'] * w[i] for i, s in enumerate(valid))
            ensemble_alpha = sum(s['alpha_limit'] * w[i] for i, s in enumerate(valid))
            ensemble_trap = sum(s['trap_limit'] * w[i] for i, s in enumerate(valid))
            ensemble_dtw = sum(s['dtw_limit'] * w[i] for i, s in enumerate(valid))
            
            if "CANDIDATE_PARAMS" not in current_config: 
                current_config["CANDIDATE_PARAMS"] = {}
            
            current_config["CANDIDATE_PARAMS"] = {
                "DYNAMIC_MAE_SL": round(ensemble_sl, 2),
                "DYNAMIC_MFE_TP": round(ensemble_tp, 2),
                "TREE_FATAL_CPV": round(ensemble_cpv, 2),
                "DYNAMIC_ALPHA_LIMIT": round(ensemble_alpha, 3), 
                "DYNAMIC_TRAP_LIMIT": round(ensemble_trap, 3),   
                "DYNAMIC_DTW_LIMIT": round(ensemble_dtw, 3)      
            }
            
            report_lines.append(f"▪️ 앙상블 손절/익절 후보(B): <b>{round(ensemble_sl, 2)}% / {round(ensemble_tp, 2)}%</b>")
            report_lines.append(f"▪️ 앙상블 기각 CPV 후보(B): <b>{round(ensemble_cpv, 2)}</b> (대기실 격리)")
            report_lines.append("💡 팩트: 유효한 데이터만을 블렌딩하여 안정적인 후보군을 생성했습니다.")
        else:
            report_lines.append("⚠️ 앙상블을 위한 장기 데이터 표본이 부족하여 기존 수치 유지")
    # 👆👆 [수정 끝] 👆👆

    # ---------------------------------------------------------
    # 👑 엔진 4.8: [Multi-Centroid DNA] 대장주 & 참사주 Top 3 독립 궤적 추출 (7D 텐서)
    # ---------------------------------------------------------
    report_lines.append("\n<b>[4.8 대장주 및 참사주 Top 3 궤적 독립 추출 (도플갱어 템플릿)]</b>")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        
        # 1. 대박 종목 Top 3 (수익률 최상위) & 참사/횡보 종목 Top 3 (손실/타임컷 최하위) 색출
        alphas = df[df['final_ret'] >= 15.0].sort_values(by='final_ret', ascending=False).head(3)
        traps = df[df['final_ret'] <= -5.0].sort_values(by='final_ret', ascending=True).head(3)

        def extract_7d_vector(row):
            table_name = f"{row['market']}_{row['code']}"
            idx_table = 'US_SPY' if row['market'] == 'US' else 'KR_KOSDAQ_IDX' # 💡 벤치마크 테이블
            entry_dt = row['entry_date']
            try:
                # 종목 데이터와 벤치마크(시장) 데이터를 동일 기간(150일)으로 동시 로드
                query = f"SELECT * FROM {table_name} WHERE Date < '{entry_dt}' ORDER BY Date DESC LIMIT 150"
                idx_query = f"SELECT * FROM {idx_table} WHERE Date < '{entry_dt}' ORDER BY Date DESC LIMIT 150"
                
                hist_df = pd.read_sql(query, conn).sort_values('Date')
                idx_df = pd.read_sql(idx_query, conn).sort_values('Date')
                
                if len(hist_df) >= 150 and len(idx_df) >= 150:
                    c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
                    idx_c = idx_df['Close'].values
                    
                    # 1. Raw 7D 연산
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
                    
                    # 2. 💡 [V33.0] Z-Score 정규화 (시장 인플레이션 제거)
                    # 벤치마크 수익률(Mean) 및 변동성(StdDev) 추출
                    idx_rs = ((idx_c[-1] - idx_c[0]) / idx_c[0]) * 100
                    idx_vol = pd.Series(idx_c).pct_change().std() * 100 * np.sqrt(252) # 연율화 변동성 프록시
                    safe_vol = idx_vol if idx_vol > 0.1 else 1.0
                    
                    # 극심한 인플레이션을 겪는 지표(RS, BBE)를 시장 베이스라인으로 Z-Score 치환
                    z_rs = (rs_slope - idx_rs) / safe_vol
                    z_bbe = bbe / safe_vol  # 에너지는 시장 변동성 대비 비율로 스케일링
                    
                    # 정규화된 텐서 리턴
                    return {'name': row['name'], 'cpv': cpv, 'tb': tb, 'bbe': z_bbe, 'rs': z_rs, 'vcp': vcp_ratio, 'vol': vol_flow, 'ma': ma_conv}
            except Exception as e: 
                return None
            return None

        # 2. JSON에 Rank 1~3 독립 저장 (대장주)
        alpha_count = 0
        for idx, row in enumerate(alphas.iterrows(), 1):
            vec = extract_7d_vector(row[1])
            if vec:
                current_config[f"DNA_ALPHA_RANK{idx}"] = vec
                report_lines.append(f"🟢 [Alpha {idx}위] {vec['name']} 5개월 궤적 저장 완료")
                alpha_count += 1
        
        if alpha_count == 0: report_lines.append("▪️ 슈퍼 알파 표본 부족으로 대장주 궤적 추출 스킵")

        # 3. JSON에 Rank 1~3 독립 저장 (참사주)
        trap_count = 0
        for idx, row in enumerate(traps.iterrows(), 1):
            vec = extract_7d_vector(row[1])
            if vec:
                current_config[f"DNA_TRAP_RANK{idx}"] = vec
                report_lines.append(f"🔴 [Trap {idx}위] {vec['name']} (참사) 5개월 궤적 저장 완료")
                trap_count += 1
                
        if trap_count == 0: report_lines.append("▪️ 참사 표본 부족으로 참사주 궤적 추출 스킵")

        conn.close()
    except Exception as e:
        report_lines.append(f"⚠️ 다중 궤적 추출 에러: {e}")

    # ---------------------------------------------------------
    # 👑 엔진 4.9: [V27.0 예측 오차율(Tracking Error) 및 세이프티 가드]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[4.9 예측 오차 검증 및 세이프티 가드]</b>")
    
    # 1. 최근 14일 데이터의 MAE(최대낙폭) 변동성 측정
    # 실제 발생한 낙폭과 우리가 설정했던 손절선(-3.5% 등) 사이의 괴리 분석
    df['mae_error'] = abs(df['min_low'] - df['entry_price']) / df['entry_price'] * 100
    avg_mae = df['mae_error'].mean()
    std_mae = df['mae_error'].std() # 낙폭의 표준편차 (시장 발작 지수)

    # 💡 [핵심] 예측 오차율 계산: 평소 변동성 대비 최근 변동성이 1.5배 이상 튀었는가?
    # (과거 60일 데이터가 있다면 더 정확하나, 여기선 현재 셋셋의 안정성 검증)
    tracking_error_score = std_mae / (abs(current_config.get("DYNAMIC_MAE_SL", -3.5)) + 0.1)
    
    is_failsafe_mode = False
    if tracking_error_score > 1.2 or std_mae > 5.0: # 오차율이 너무 높거나, 평균 낙폭 편차가 5%를 넘을 때
        is_failsafe_mode = True
        report_lines.append(f"🚨 <b>안전 모드(Failsafe) 발동!</b> (오차율: {tracking_error_score:.2f} | 편차: {std_mae:.2f})")
        report_lines.append("💡 사유: 시장 변동성이 통제 범위를 벗어났습니다. 모든 조율값을 무시하고 베이스라인으로 복귀합니다.")
        
        # 🛡️ 최보수적 베이스라인으로 강제 회귀 (Override)
        current_config["DYNAMIC_MAE_SL"] = -5.0  # 더 넓은 방어선 (시장 발작 대응)
        current_config["DYNAMIC_MFE_TP"] = 10.0  # 표준 수익선
        current_config["TREE_FATAL_CPV"] = 0.75  # 윗꼬리 필터 대폭 강화 (속임수 방어)
        
        # Candidate B(후보군) 삭제 (오염된 데이터 학습 방지)
        if "CANDIDATE_PARAMS" in current_config:
            del current_config["CANDIDATE_PARAMS"]
    else:
        report_lines.append(f"✅ 예측 안정성 확인 (오차율: {tracking_error_score:.2f} | 편차: {std_mae:.2f})")
    
    # ---------------------------------------------------------
    # 👑 엔진 5: [V17.0 청산 우선순위 데스매치 및 DNA 분석 (STAT vs TECH)]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[5. 청산 우선순위 데스매치 및 인과 분석]</b>")
    if 'sim_stat_ret' in df.columns and 'sim_tech_ret' in df.columns:
        # 종료된 시뮬레이션 결과 추출
        stat_df = df[df['sim_stat_status'].str.contains('CLOSED', na=False)]
        tech_df = df[df['sim_tech_status'].str.contains('CLOSED', na=False)]
        
        stat_pf = (stat_df[stat_df['sim_stat_ret']>0]['sim_stat_ret'].sum()) / abs(stat_df[stat_df['sim_stat_ret']<=0]['sim_stat_ret'].sum() + 0.1) if len(stat_df)>0 else 0
        tech_pf = (tech_df[tech_df['sim_tech_ret']>0]['sim_tech_ret'].sum()) / abs(tech_df[tech_df['sim_tech_ret']<=0]['sim_tech_ret'].sum() + 0.1) if len(tech_df)>0 else 0
        
        report_lines.append(f"▪️ MFE 목표가(STAT) 우선 PF: <b>{stat_pf:.2f}</b>")
        report_lines.append(f"▪️ 추세추종(TECH) 무한홀딩 PF: <b>{tech_pf:.2f}</b>")
        
        if tech_pf > stat_pf * 1.1:
            winner_mode = "TECH"
            report_lines.append("🏆 <b>승리: [TECH 우선]</b> (MFE 익절이 오히려 추세의 수익을 깎아먹고 있습니다. 끝까지 홀딩하세요.)")
            
            # 💡 [공통점 분석] TECH가 압도적으로 유리했던 종목들의 DNA 추적
            tech_winners = df[(df['sim_tech_ret'] > df['sim_stat_ret'] + 5.0)]
            if len(tech_winners) >= 3:
                rs_mean = tech_winners['dyn_rs'].mean()
                report_lines.append(f"💡 <b>[추세 추종(무한 홀딩) 성공 DNA]</b>: RS 상위 {(10-rs_mean)*11.1:.1f}% 종목들. 상대강도가 강한 대장주는 단기 목표가(MFE)를 무시하고 데드크로스까지 놔둬야 합니다.")
        else:
            winner_mode = "STAT"
            report_lines.append("🏆 <b>승리: [STAT 우선]</b> (데드크로스를 기다리면 수익을 다 토해냅니다. 목표가 도달 시 기계적으로 챙기세요.)")
            
            # 💡 [공통점 분석] STAT이 압도적으로 유리했던 종목들의 DNA 추적
            stat_winners = df[(df['sim_stat_ret'] > df['sim_tech_ret'] + 2.0)]
            if len(stat_winners) >= 3:
                cpv_mean = stat_winners['dyn_cpv'].mean()
                report_lines.append(f"💡 <b>[단기 목표가 익절(STAT) 성공 DNA]</b>: 캔들지배력 상위 {(10-cpv_mean)*11.1:.1f}% 종목들. 매도 압력이 큰 캔들 패턴은 슈팅을 줄 때 욕심부리지 말고 즉시 도망가야 합니다.")

        current_config["ACTIVE_EXIT_MODE"] = winner_mode
        report_lines.append(f"🚨 <b>액션:</b> 다음 주 청산 가이드를 <b>[{winner_mode}]</b> 모드로 강제 고정합니다.")
    else:
        report_lines.append("⚠️ 장부에 시뮬레이션 컬럼이 부족하여 대결을 보류합니다.")
    # ---------------------------------------------------------
    # 👑 엔진 6: [V31.0 Walk-Forward (OOS) 데이터 격리 및 진검승부]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[6. OOS(Out-of-Sample) 진검승부 및 승격]</b>")
    if 'live_a_ret' in df.columns:
        # 💡 [V31.0 핵심] 데이터 격리 (Train vs Test 분리)
        # 현재 기준 14일 전 날짜를 임계점으로 설정
        oos_threshold = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        
        # 학습 구간 (Train): 14일 이전의 과거 데이터
        train_df = df[df['entry_date'] < oos_threshold]
        # 검증 구간 (Test/OOS): 최근 14일 데이터 (한 번도 보지 못한 미지의 데이터)
        test_df = df[df['entry_date'] >= oos_threshold]
        
        report_lines.append(f"▪️ 데이터 격리: Train 표본 {len(train_df)}개 vs OOS 표본 {len(test_df)}개")
        
        # 👇👇 [수정] V36.0 몬테카를로 부트스트래핑(Monte Carlo Bootstrapping) 엔진 👇👇
        def get_bootstrapped_pf(returns_series, n_iterations=1000, confidence_level=5):
            """1,000번의 평행우주를 생성하여 하위 5%(최악의 상황)의 손익비를 추출"""
            returns = returns_series.dropna().values
            if len(returns) < 5: # 표본이 너무 적으면 일반 PF 리턴
                win, lose = returns[returns > 0], returns[returns <= 0]
                return np.sum(win) / (abs(np.sum(lose)) + 0.1)

            pfs = []
            # 1. 1,000번의 무작위 복원 추출(Resampling)
            for _ in range(n_iterations):
                sample = np.random.choice(returns, size=len(returns), replace=True)
                win = sample[sample > 0]
                lose = sample[sample <= 0]
                pf = np.sum(win) / (abs(np.sum(lose)) + 0.1)
                pfs.append(pf)

            # 2. 1,000개의 평행우주 중 하위 5%(가장 운이 없었던 상황)의 PF 반환
            return np.percentile(pfs, confidence_level)

            results = {}
            report_lines.append("\n🎲 <b>[V36.0 몬테카를로 1,000회 시뮬레이션 가동]</b>")
            
            for col in ['live_a_ret', 'cand_b_ret', 'champ_c_ret']:
                if col in test_df.columns:
                    # 기존의 단순 계산(환상) PF
                    raw_win = test_df[test_df[col] > 0][col].sum()
                    raw_lose = abs(test_df[test_df[col] <= 0][col].sum() + 0.1)
                    raw_pf = raw_win / raw_lose
                    
                    # 💡 [V36.0] 운이 제거된 하위 5%의 절대 방어력 PF
                    strict_pf = get_bootstrapped_pf(test_df[col])
                    results[col] = strict_pf
                    
                    if raw_pf > 0:
                        report_lines.append(f"▪️ {col[:6].upper()} ➔ 단순 PF: {raw_pf:.2f} | <b>운 제거 PF(하위5%): {strict_pf:.2f}</b>")
            # 👆👆 [수정 끝] 👆👆
            
            if results:
                winner_key = max(results, key=results.get)
                report_lines.append(f"▪️ [OOS 성적] LIVE(A): {results.get('live_a_ret', 0):.2f} | CAND(B): {results.get('cand_b_ret', 0):.2f} | CHAMP(C): {results.get('champ_c_ret', 0):.2f}")
                
                # 💡 [V32.0 버그 픽스] 승격 여부 추적 스위치
                is_promoted = False 

                if winner_key == 'cand_b_ret' and results['cand_b_ret'] > results.get('live_a_ret', 0) * 1.05:
                    # 챔피언 백업
                    current_config["CHAMPION_PARAMS"] = {
                        "DYNAMIC_MAE_SL": current_config.get("DYNAMIC_MAE_SL", -3.5),
                        "DYNAMIC_MFE_TP": current_config.get("DYNAMIC_MFE_TP", 10.0),
                        "TREE_FATAL_CPV": current_config.get("TREE_FATAL_CPV", 0.85)
                    }
                    cand = current_config.get("CANDIDATE_PARAMS", {})
                    if cand:
                        for k, v in cand.items(): current_config[k] = v
                    current_config["LIVE_A_PROMOTION_DATE"] = datetime.now().strftime('%Y-%m-%d')
                    report_lines.append("🏆 <b>[신규 로직 승격]</b> CAND(B)가 OOS 검증을 통과하여 실전 배치됩니다.")
                    is_promoted = True # 💡 승격 활성화
                    
                elif winner_key == 'champ_c_ret' and results['champ_c_ret'] > results.get('live_a_ret', 0) * 1.05:
                    champ = current_config.get("CHAMPION_PARAMS", {})
                    if champ:
                        for k, v in champ.items(): current_config[k] = v
                    current_config["LIVE_A_PROMOTION_DATE"] = datetime.now().strftime('%Y-%m-%d')
                    report_lines.append("♻️ <b>[챔피언 귀환]</b> CHAMP(C)가 미지의 데이터에서 최고 성적을 내어 복귀합니다.")
                    is_promoted = True # 💡 승격 활성화

                # 👇👇 [V32.0 & V35.0 시너지] 국면별 금고 영구 저장 👇👇
                if is_promoted:
                    regime_key = current_config.get("LAST_ANALYSED_REGIME", "CHOP")
                    # SL/TP 뿐만 아니라 V35.0에서 자율 도출된 임계값들까지 통째로 기억소에 저장하여 '기억 상실' 원천 차단
                    current_config[f"{regime_key}_CHAMPION_PARAMS"] = {
                        "DYNAMIC_MAE_SL": current_config.get("DYNAMIC_MAE_SL"),
                        "DYNAMIC_MFE_TP": current_config.get("DYNAMIC_MFE_TP"),
                        "TREE_FATAL_CPV": current_config.get("TREE_FATAL_CPV"),
                        "DYNAMIC_ALPHA_LIMIT": current_config.get("DYNAMIC_ALPHA_LIMIT"),
                        "DYNAMIC_TRAP_LIMIT": current_config.get("DYNAMIC_TRAP_LIMIT"),
                        "DYNAMIC_DTW_LIMIT": current_config.get("DYNAMIC_DTW_LIMIT")
                    }
                    report_lines.append(f"🗳️ <b>[기억소 갱신]</b> {regime_key} 국면의 모든 최적화 파라미터가 금고에 저장되었습니다.")
                # 👆👆 [시너지 강화 끝] 👆👆
                
                # 💡 [오답 노트 추출] 패배한 케이스의 공통점 (오답 분석은 전체 표본 사용)
                losers = df[df[winner_key] < 0]
                if len(losers) >= 5:
                    report_lines.append(f"\n💀 <b>[오답 노트: 패배한 {len(losers)}개 케이스 팩트 분석]</b>")
                    l_rs = losers['dyn_rs'].mean()
                    l_cpv = losers['dyn_cpv'].mean()
                    
                    report_lines.append(f"▪️ 패배 평균: RS 상위 {(10-l_rs)*11.1:.1f}% | 캔들지배력 상위 {(10-l_cpv)*11.1:.1f}%")
                    
                    if (10-l_cpv)*11.1 > 50:
                        report_lines.append("💡 결론: 윗꼬리가 긴 악성 캔들(CPV)에서 휩소가 집중적으로 발생. CPV 컷오프를 더 낮춰야 함.")
                    elif (10-l_rs)*11.1 > 50:
                        report_lines.append("💡 결론: 시장 소외주(Low RS)에서 손실이 집중 발생. 추세 필터 강화 필요.")
                    else:
                        report_lines.append("💡 결론: 특정 지표 쏠림보다는 거시 시장(VIX) 폭락의 영향이 컸음.")
    else:
        report_lines.append("⚠️ 장부에 ABC 컬럼이 부족하여 토너먼트를 보류합니다.")

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
        now = datetime.now(tz)
        if now > START_DATE.replace(tzinfo=tz):
            if now.weekday() == 5 and now.hour == 10 and now.minute == 0:
                run_autonomous_analysis()
                time.sleep(65) 
        time.sleep(30)

if __name__ == "__main__":
    system_main_loop()
