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
            "WEIGHT_S1": 1.0, "WEIGHT_S4": 1.0
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
        # VIX, SPY, KOSPI 데이터 동시 로드
        df_idx = yf.download("SPY ^VIX ^KS11", period="1y", interval="1d", group_by="ticker", progress=False)
        spy_c = df_idx['SPY']['Close'].dropna()
        vix_c = df_idx['^VIX']['Close'].dropna()
        
        spy_last, vix_last = spy_c.iloc[-1], vix_c.iloc[-1]
        spy_ema200 = spy_c.ewm(span=200, adjust=False).mean().iloc[-1]
        
        # 💡 [핵심] VIX 기반 룩백 윈도우 및 국면 비중 동적 조율
        if vix_last >= 28.0:
            dyn_lookback = 7  # 🚨 극단적 공포장: 낡은 기억 폐기, 최근 7일에만 초집중 (기민성 MAX)
            regime, w_s1, w_s4 = "Bear (극단적 공포장)", 0.0, 2.0 # S1 매수 전면 금지
            vix_status = f"VIX 폭발 ({vix_last:.1f}) - 기억력 7일로 초압축"
        elif vix_last >= 18.0:
            dyn_lookback = 15 # ⚠️ 변동성 장세: 표준 룩백 적용
            regime, w_s1, w_s4 = "Chop (변동성/조정장)", 0.5, 1.5
            vix_status = f"VIX 경계 ({vix_last:.1f}) - 기억력 15일 유지"
        else:
            dyn_lookback = 45 # 🌊 대세 상승/평온장: 노이즈 필터링, 긴 추세 확인 (안정성 MAX)
            regime, w_s1, w_s4 = "Bull (대세 상승장)", 1.5, 0.5
            vix_status = f"VIX 평온 ({vix_last:.1f}) - 기억력 45일로 확장"
    except Exception as e:
        print(f"거시 지표 로드 에러: {e}")

    # 1. 계산된 [동적 룩백]으로 DB 데이터 로드
    try:
        conn = sqlite3.connect(DB_PATH)
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
    # 👑 엔진 4: [V16.0 Multi-Timeframe Ensemble] 앙상블 최적화
    # ---------------------------------------------------------
    report_lines.append("\n<b>[4. 다중 타임프레임 앙상블(14/30/60d) 최적화]</b>")
    
    def get_period_stats(days):
        """특정 기간의 최적 파라미터 날것(Raw) 추출"""
        try:
            conn = sqlite3.connect(DB_PATH)
            s_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            p_df = pd.read_sql(f"SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND entry_date >= '{s_date}'", conn)
            conn.close()
            
            if len(p_df) < 5: return None
            
            p_df['mfe_pct'] = (p_df['max_high'] - p_df['entry_price']) / p_df['entry_price'] * 100
            p_df['mae_pct'] = (p_df['min_low'] - p_df['entry_price']) / p_df['entry_price'] * 100
            
            win_subset = p_df[p_df['final_ret'] > 0]
            lose_subset = p_df[p_df['final_ret'] <= 0]
            
            return {
                "sl": np.percentile(win_subset['mae_pct'], 15) if len(win_subset) >= 3 else -3.5,
                "tp": np.percentile(win_subset['mfe_pct'], 50) if len(win_subset) >= 3 else 10.0,
                "fatal_cpv": np.percentile(lose_subset['v_cpv'].dropna(), 90) if len(lose_subset) >= 3 else 0.85
            }
        except: return None

    # 1. 시계열별 데이터 수집 (14일/30일/60일)
    t14 = get_period_stats(14)
    t30 = get_period_stats(30)
    t60 = get_period_stats(60)

    # 2. 가중치 앙상블 연산 (단기 50% : 중기 30% : 장기 20%)
    valid_stats = [s for s in [t14, t30, t60] if s is not None]
    if len(valid_stats) >= 2:
        # 단기 데이터가 있으면 가중 평균(5:3:2), 없으면 유효 데이터끼리 평균 적용
        w = [0.5, 0.3, 0.2] if t14 and t30 and t60 else [1/len(valid_stats)] * len(valid_stats)
        
        ensemble_sl = sum(s['sl'] * w[i] for i, s in enumerate(valid_stats))
        ensemble_tp = sum(s['tp'] * w[i] for i, s in enumerate(valid_stats))
        ensemble_cpv = sum(s['fatal_cpv'] * w[i] for i, s in enumerate(valid_stats))
        
        # 3. 학습 결과를 후보군(B) 대기실로 격리 보관
        if "CANDIDATE_PARAMS" not in current_config: 
            current_config["CANDIDATE_PARAMS"] = {}
        
        current_config["CANDIDATE_PARAMS"] = {
            "DYNAMIC_MAE_SL": round(ensemble_sl, 2),
            "DYNAMIC_MFE_TP": round(ensemble_tp, 2),
            "TREE_FATAL_CPV": round(ensemble_cpv, 2)
        }
        
        report_lines.append(f"▪️ 앙상블 손절/익절 후보(B): <b>{round(ensemble_sl, 2)}% / {round(ensemble_tp, 2)}%</b>")
        report_lines.append(f"▪️ 앙상블 기각 CPV 후보(B): <b>{round(ensemble_cpv, 2)}</b> (대기실 격리)")
        report_lines.append("💡 팩트: 14/30/60일 데이터를 5:3:2로 블렌딩하여 안정적인 후보군을 생성했습니다.")
    else:
        report_lines.append("⚠️ 앙상블을 위한 장기 데이터 표본이 부족하여 기존 수치 유지")

    # ---------------------------------------------------------
    # 👑 엔진 4.8: [Multi-Centroid DNA] 대장주 & 참사주 Top 3 독립 궤적 추출 (7D 텐서)
    # ---------------------------------------------------------
    report_lines.append("\n<b>[4.8 대장주 및 참사주 Top 3 궤적 독립 추출 (도플갱어 템플릿)]</b>")
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # 1. 대박 종목 Top 3 (수익률 최상위) & 참사/횡보 종목 Top 3 (손실/타임컷 최하위) 색출
        alphas = df[df['final_ret'] >= 15.0].sort_values(by='final_ret', ascending=False).head(3)
        traps = df[df['final_ret'] <= -5.0].sort_values(by='final_ret', ascending=True).head(3)

        def extract_7d_vector(row):
            table_name = f"{row['market']}_{row['code']}"
            entry_dt = row['entry_date']
            try:
                query = f"SELECT * FROM {table_name} WHERE Date < '{entry_dt}' ORDER BY Date DESC LIMIT 150"
                hist_df = pd.read_sql(query, conn).sort_values('Date')
                
                if len(hist_df) >= 150:
                    c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
                    
                    # 7D 연산 (시계열 흐름)
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
                    
                    return {'name': row['name'], 'cpv': cpv, 'tb': tb, 'bbe': bbe, 'rs': rs_slope, 'vcp': vcp_ratio, 'vol': vol_flow, 'ma': ma_conv}
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
    # 👑 엔진 6: [V15.0 ABC Tournament Arena & 오답 노트]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[6. ABC 토너먼트 데스매치 및 오답 노트]</b>")
    if 'live_a_ret' in df.columns:
        results = {}
        for col in ['live_a_ret', 'cand_b_ret', 'champ_c_ret']:
            if col in df.columns:
                # 손익비(PF) 연산
                pf = (df[df[col] > 0][col].sum()) / abs(df[df[col] <= 0][col].sum() + 0.1)
                results[col] = pf
        
        if results:
            winner_key = max(results, key=results.get)
            report_lines.append(f"▪️ LIVE(A): {results.get('live_a_ret', 0):.2f} | CAND(B): {results.get('cand_b_ret', 0):.2f} | CHAMP(C): {results.get('champ_c_ret', 0):.2f}")
            
            # 💡 [승격 엔진] 5% 이상 확실한 우위가 있을 때만 교체
            if winner_key == 'cand_b_ret' and results['cand_b_ret'] > results.get('live_a_ret', 0) * 1.05:
                # 챔피언 백업
                current_config["CHAMPION_PARAMS"] = {
                    "DYNAMIC_MAE_SL": current_config.get("DYNAMIC_MAE_SL", -3.5),
                    "DYNAMIC_MFE_TP": current_config.get("DYNAMIC_MFE_TP", 10.0),
                    "TREE_FATAL_CPV": current_config.get("TREE_FATAL_CPV", 0.85)
                }
                # 라이브 승격
                cand = current_config.get("CANDIDATE_PARAMS", {})
                if cand:
                    for k, v in cand.items(): current_config[k] = v
                report_lines.append("🏆 <b>[신규 로직 승격]</b> CAND(B)가 압승하여 실전(A)으로 배치되었습니다.")
                
            elif winner_key == 'champ_c_ret' and results['champ_c_ret'] > results.get('live_a_ret', 0) * 1.05:
                champ = current_config.get("CHAMPION_PARAMS", {})
                if champ:
                    for k, v in champ.items(): current_config[k] = v
                report_lines.append("♻️ <b>[챔피언 귀환]</b> 과거의 CHAMP(C)가 더 우수하여 다시 라이브(A)로 복귀합니다.")
            else:
                report_lines.append("🛡️ <b>[라이브 방어]</b> LIVE(A)가 방어에 성공했습니다. 현재 세팅을 유지합니다.")

            # 💡 [오답 노트 추출] 패배한 케이스의 공통점
            losers = df[df[winner_key] < 0]
            if len(losers) >= 5:
                report_lines.append(f"\n💀 <b>[오답 노트: 패배한 {len(losers)}개 케이스 팩트 분석]</b>")
                l_rs = losers['dyn_rs'].mean()
                l_cpv = losers['dyn_cpv'].mean()
                
                report_lines.append(f"▪️ 패배 종목 평균: RS 상위 {(10-l_rs)*11.1:.1f}% | 캔들지배력 상위 {(10-l_cpv)*11.1:.1f}%")
                
                if (10-l_cpv)*11.1 > 50:
                    report_lines.append("💡 결론: 윗꼬리가 긴 악성 캔들(CPV)에서 휩소가 집중적으로 발생. CPV 컷오프를 더 낮춰야 함.")
                elif (10-l_rs)*11.1 > 50:
                    report_lines.append("💡 결론: 시장 소외주(Low RS)에서 손실이 집중 발생. 추세가 강한 종목 위주로 필터 강화 필요.")
                else:
                    report_lines.append("💡 결론: 특정 지표 쏠림보다는 거시 시장(VIX) 폭락의 영향이 컸음.")
    else:
        report_lines.append("⚠️ 장부에 ABC 컬럼이 부족하여 토너먼트를 보류합니다.")

    

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
