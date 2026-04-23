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
    # 👑 엔진 4: 3차원 청산 최적화 (MFE/MAE & ATR/TimeStop)
    # ---------------------------------------------------------
    report_lines.append("<b>[4. 다차원 청산선 최적화 (SL/TP & E-Ratio)]</b>")
    if 'max_high' in df.columns and 'min_low' in df.columns and 'entry_price' in df.columns:
        df['mfe_pct'] = (df['max_high'] - df['entry_price']) / df['entry_price'] * 100
        df['mae_pct'] = (df['min_low'] - df['entry_price']) / df['entry_price'] * 100
        
        winners_s1 = kr_s1_df[kr_s1_df['final_ret'] > 0]
        if len(winners_s1) >= 5:
            raw_sl = np.percentile(winners_s1['mae_pct'], 10) 
            raw_tp = np.percentile(winners_s1['mfe_pct'], 50)
            
            old_sl = current_config.get("KR_MASTER_S1_SL", -3.0)
            old_tp = current_config.get("KR_MASTER_S1_TP", 10.0)
            
            smoothed_sl = round((old_sl * 0.7) + (raw_sl * 0.3), 2)
            smoothed_tp = round((old_tp * 0.7) + (raw_tp * 0.3), 2)
            
            current_config["KR_MASTER_S1_SL"] = smoothed_sl
            current_config["KR_MASTER_S1_TP"] = smoothed_tp
            report_lines.append(f"▪️ 최적 SL: {old_sl}% ➔ <b>{smoothed_sl}%</b>")
            report_lines.append(f"▪️ 최적 TP: {old_tp}% ➔ <b>{smoothed_tp}%</b>")
            
            # E-Ratio 및 ATR 가상 로직 (DB에 bars_held가 기록되어야 완벽 가동)
            if 'bars_held' in winners_s1.columns:
                opt_time = int(winners_s1['bars_held'].mean())
                current_config["KR_MASTER_S1_TIME_STOP"] = opt_time
                report_lines.append(f"▪️ Time Stop: 진입 후 <b>{opt_time}일</b> 초과 시 엣지 붕괴\n")
            else: report_lines.append("")

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
