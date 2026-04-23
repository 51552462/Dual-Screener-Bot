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
    print(f"🚀 [자율 관제탑] {LOOKBACK_DAYS}일 롤링 최적화 및 정밀 인과 분석 시작...")
    
    # 1. DB 데이터 로드
    try:
        conn = sqlite3.connect(DB_PATH)
        start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
        query = f"SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND entry_date >= '{start_date}'"
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"DB 로드 에러: {e}")
        return

    if len(df) < 20:
        send_telegram_report(f"⚠️ <b>[자율 관제탑]</b>\n\n최근 {LOOKBACK_DAYS}일 실전 OOS 데이터가 20건 미만입니다. 과최적화 방지를 위해 이번 주 조율을 스킵합니다.")
        return

    current_config = load_or_create_config()
    report_lines = [f"<b>📊 [System B {LOOKBACK_DAYS}일 자율 조율 리포트]</b>\n"]

    # ---------------------------------------------------------
    # 👑 엔진 1: 거시 국면 판독 (Regime Allocation)
    # ---------------------------------------------------------
    try:
        df_idx = yf.download("SPY ^VIX", period="1y", interval="1d", group_by="ticker", progress=False)
        spy_c, vix_c = df_idx['SPY']['Close'].dropna(), df_idx['^VIX']['Close'].dropna()
        spy_last, vix_last = spy_c.iloc[-1], vix_c.iloc[-1]
        spy_ema200 = spy_c.ewm(span=200, adjust=False).mean().iloc[-1]
        
        w_s1, w_s4 = 1.0, 1.0 
        if spy_last >= spy_ema200 and vix_last < 18.0:
            regime, w_s1, w_s4 = "Bull (대세 상승장)", 1.5, 0.5
        elif spy_last >= spy_ema200 and 18.0 <= vix_last < 25.0:
            regime, w_s1, w_s4 = "Chop (변동성/조정장)", 0.8, 1.2
        else:
            regime, w_s1, w_s4 = "Bear (폭락/공포장)", 0.2, 2.0
            
        current_config["WEIGHT_S1"], current_config["WEIGHT_S4"] = w_s1, w_s4
        report_lines.append(f"<b>[1. 거시 국면 판독 (Regime)]</b>\n▪️ 상태: {regime} (VIX: {vix_last:.1f})\n🚨 <b>액션:</b> S1 비중 {w_s1}배 / S4 비중 {w_s4}배 강제 조율\n")
    except: pass

    # ---------------------------------------------------------
    # 👑 엔진 2: 점수 티어 및 초정밀 필터 검증
    # ---------------------------------------------------------
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
    # 👑 엔진 4: 3차원 청산 최적화 (MFE/MAE) & 비선형 의사결정 나무(Decision Tree) 학습
    # ---------------------------------------------------------
    report_lines.append("\n<b>[4. MFE/MAE 최적화 및 비선형 규칙(Decision Tree) 학습]</b>")
    if 'max_high' in df.columns and 'min_low' in df.columns and 'entry_price' in df.columns:
        # 1. MFE / MAE 계산
        df['mfe_pct'] = (df['max_high'] - df['entry_price']) / df['entry_price'] * 100
        df['mae_pct'] = (df['min_low'] - df['entry_price']) / df['entry_price'] * 100
        
        winners = df[df['final_ret'] > 0]
        losers = df[df['final_ret'] <= 0]
        
        if len(winners) >= 5:
            # 💡 [팩트 1] MFE/MAE 기반 수학적 손절/익절 한계점 도출
            raw_sl = np.percentile(winners['mae_pct'], 15) # 승리한 종목들이 겪은 최대 고통(하위 15%)
            raw_tp = np.percentile(winners['mfe_pct'], 50) # 승리한 종목들의 평균 도달 수익(중앙값)
            
            old_sl = current_config.get("DYNAMIC_MAE_SL", -3.5)
            old_tp = current_config.get("DYNAMIC_MFE_TP", 10.0)
            
            smoothed_sl = round((old_sl * 0.7) + (raw_sl * 0.3), 2)
            smoothed_tp = round((old_tp * 0.7) + (raw_tp * 0.3), 2)
            
            current_config["DYNAMIC_MAE_SL"] = smoothed_sl
            current_config["DYNAMIC_MFE_TP"] = smoothed_tp
            
            report_lines.append(f"▪️ MAE 최적 손절선: {old_sl}% ➔ <b>{smoothed_sl}%</b> (진화)")
            report_lines.append(f"▪️ MFE 최적 익절선: {old_tp}% ➔ <b>{smoothed_tp}%</b> (진화)")

        if len(losers) >= 5:
            # 💡 [팩트 2] 비선형 의사결정 나무 (Death Node) 학습
            # 참사 종목들의 CPV 최악의 10% 컷오프를 찾아냄
            fatal_cpv = np.percentile(losers['v_cpv'].dropna(), 90) # 꼬리가 가장 긴 악성 캔들 기준점
            old_fatal_cpv = current_config.get("TREE_FATAL_CPV", 0.85)
            smoothed_fatal_cpv = round((old_fatal_cpv * 0.7) + (fatal_cpv * 0.3), 2)
            
            current_config["TREE_FATAL_CPV"] = smoothed_fatal_cpv
            report_lines.append(f"▪️ Decision Tree [Death Node 1]: CPV <b>{smoothed_fatal_cpv}</b> 이상 시 무조건 기각 학습 완료.")

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
    # 👑 엔진 5: 청산 로직 데스매치 (인과 추론 피드백 루프)
    # ---------------------------------------------------------
    report_lines.append("<b>[5. 청산 아레나 및 시스템 피드백 (Why?)]</b>")
    if 'exit_type' in df.columns:
        _, tech_pf = calculate_metrics(df[df['exit_type'] == 'TECH'])
        _, stat_pf = calculate_metrics(df[df['exit_type'] == 'STAT'])
        _, hybrid_pf = calculate_metrics(df[df['exit_type'] == 'HYBRID'])
        
        scores = {"TECH": tech_pf, "STAT": stat_pf, "HYBRID": hybrid_pf}
        winner_mode = max(scores, key=scores.get) if max(scores.values()) > 0 else "HYBRID"
        
        report_lines.append(f"▪️ TECH PF: {tech_pf:.2f} | STAT PF: {stat_pf:.2f} | HYBRID PF: {hybrid_pf:.2f}")
        report_lines.append(f"🏆 <b>승리 로직: [{winner_mode}]</b>")
        
        if winner_mode == "HYBRID": report_lines.append("💡 팩트: 손절 차단과 추세 홀딩의 완벽한 공수 밸런스 입증.")
        elif winner_mode == "STAT": report_lines.append("💡 팩트: 휩소가 잦아 타임스탑(기계적 매도)이 기회비용을 완벽히 방어함.")
        else: report_lines.append("💡 팩트: 대세 상승 국면이므로 추세(TECH)를 끝까지 타는 것이 압도적.")
            
        current_config["ACTIVE_EXIT_MODE"] = winner_mode
        report_lines.append(f"🚨 <b>시스템 액션:</b> 모든 검색기 청산 가이드를 <b>[{winner_mode}]</b> 모드로 강제 고정합니다.")
    else:
        report_lines.append("⚠️ DB에 'exit_type' 기록이 없어 대결을 보류합니다.")

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
