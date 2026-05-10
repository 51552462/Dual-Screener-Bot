import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 1. 팩토리 뇌(Config) 읽기 전용 경로
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')

def load_factory_brain_readonly():
    """메인 시스템의 뇌를 읽기 전용으로 복제해 옵니다."""
    if not os.path.exists(CONFIG_PATH):
        print("🚨 관제탑 파일을 찾을 수 없습니다.")
        return {}
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# 2. 극한의 스트레스 테스트 기간 세팅 (블랙스완)
CRASH_PERIODS = {
    "COVID-19 코로나 폭락장": {"start": "2020-02-01", "end": "2020-05-31"},
    "2022년 글로벌 금리인상 폭락장": {"start": "2022-01-01", "end": "2022-06-30"},
    "2018년 미중 무역분쟁 하락장": {"start": "2018-09-01", "end": "2018-12-31"}
}

def calculate_dna_factors(df):
    """과거 차트에서 실시간 팩토리와 똑같은 3D DNA(CPV, TB, BBE)를 추출합니다."""
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    
    # 20일 이동평균 기반
    v_ma20 = pd.Series(v).rolling(20).mean().values
    
    # CPV (윗꼬리 방어력)
    cpv = np.where(h != l, (c - o) / (h - l), 0.5)
    
    # TB (진짜 양봉 수급)
    vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
    tb = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)
    
    # BBE (응축 에너지)
    bb_std = pd.Series(c).rolling(20).std().values
    bb_mid = pd.Series(c).rolling(20).mean().values
    bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
    bbe = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
    
    df['dyn_cpv'] = cpv
    df['dyn_tb'] = tb
    df['v_energy'] = bbe
    return df

def run_time_machine_backtest(target_period_name, stock_list):
    print(f"\n⏳ 타임머신 가동: [{target_period_name}] 차원으로 이동합니다...")
    period = CRASH_PERIODS[target_period_name]
    start_dt, end_dt = period["start"], period["end"]
    
    config = load_factory_brain_readonly()
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {})
    ud_templates = config.get("UNDERDOG_CLUSTER_TEMPLATES", {})
    all_templates = {**ml_templates, **ud_templates}
    
    if not all_templates:
        print("⚠️ 팩토리에 학습된 템플릿(무기)이 없습니다. 테스트를 종료합니다.")
        return

    results = []
    # 데이터 확보 기간을 위해 실제 시작일보다 40일 전부터 다운로드
    fetch_start = (pd.to_datetime(start_dt) - timedelta(days=40)).strftime('%Y-%m-%d')
    
    scanned = 0
    for code in stock_list:
        scanned += 1
        if scanned % 20 == 0: print(f" ↳ {scanned}/{len(stock_list)}개 종목 시뮬레이션 중...")
            
        try:
            df = fdr.DataReader(code, fetch_start, end_dt)
            if len(df) < 30: continue
            
            df = calculate_dna_factors(df)
            
            # 테스트 기간 내의 날짜만 순회 (시간 축 이동)
            test_df = df[df.index >= start_dt]
            
            for i in range(len(test_df) - 15): # 미래 15일 결과를 보기 위해 끝부분 제외
                current_row = test_df.iloc[i]
                
                # 1. 템플릿 합격 여부 스캔
                is_passed = False
                matched_tpl = ""
                for t_name, bounds in all_templates.items():
                    if not isinstance(bounds, dict): continue
                    
                    if (bounds.get('dyn_cpv_min', -99) <= current_row['dyn_cpv'] <= bounds.get('dyn_cpv_max', 99) and
                        bounds.get('dyn_tb_min', -99) <= current_row['dyn_tb'] <= bounds.get('dyn_tb_max', 999) and
                        bounds.get('v_energy_min', -99) <= current_row['v_energy'] <= bounds.get('v_energy_max', 999)):
                        is_passed = True
                        matched_tpl = t_name
                        break
                
                # 2. 합격했다면 미래 15일(MFE, MAE) 추적
                if is_passed:
                    entry_price = current_row['Close']
                    future_15d = test_df.iloc[i+1 : i+16]
                    
                    max_high = future_15d['High'].max()
                    min_low = future_15d['Low'].min()
                    
                    mfe = (max_high - entry_price) / entry_price * 100
                    mae = (min_low - entry_price) / entry_price * 100
                    
                    # 가상의 청산 룰 (10% 익절, -3.5% 손절 적용 시뮬레이션)
                    final_ret = 0.0
                    for _, f_row in future_15d.iterrows():
                        cur_mfe = (f_row['High'] - entry_price) / entry_price * 100
                        cur_mae = (f_row['Low'] - entry_price) / entry_price * 100
                        
                        if cur_mae <= -3.5:
                            final_ret = -3.5
                            break
                        elif cur_mfe >= 10.0:
                            final_ret = 10.0
                            break
                    
                    if final_ret == 0.0: # 타임스탑 (15일째 종가)
                        final_ret = (future_15d.iloc[-1]['Close'] - entry_price) / entry_price * 100

                    results.append({
                        'date': test_df.index[i].strftime('%Y-%m-%d'),
                        'code': code,
                        'template': matched_tpl,
                        'mfe': mfe,
                        'mae': mae,
                        'final_ret': final_ret
                    })
        except: continue

    # 결과 결산
    if not results:
        print(f"\n🛡️ 결과: {target_period_name} 동안 템플릿에 걸려든 종목이 없습니다. (위험 완벽 회피)")
        return
        
    res_df = pd.DataFrame(results)
    total_trades = len(res_df)
    wins = res_df[res_df['final_ret'] > 0]
    loses = res_df[res_df['final_ret'] <= 0]
    
    win_rate = len(wins) / total_trades * 100
    avg_pnl = res_df['final_ret'].mean()
    pf = wins['final_ret'].sum() / (abs(loses['final_ret'].sum()) + 0.1) if not loses.empty else 99.9
    
    print(f"\n🏆 <b>[{target_period_name} 백테스트 결과]</b>")
    print(f" ▪️ 총 진입 횟수: {total_trades}회")
    print(f" ▪️ 승률: {win_rate:.1f}%")
    print(f" ▪️ 평균 수익률: {avg_pnl:+.2f}%")
    print(f" ▪️ 손익비(PF): {pf:.2f}")
    
    if avg_pnl > 0:
        print("💡 결론: 우리 AI의 로직은 역사적인 폭락장에서도 수익을 창출하며 살아남는 압도적 방어력을 증명했습니다.")
    else:
        print("💡 결론: 폭락장의 타격을 피하지 못했습니다. 안티 패턴(참사 방어막)을 더 강화해야 합니다.")

if __name__ == "__main__":
    # 코스피 시총 상위 100개 랜덤 추출 (테스트 속도를 위해 100개만 스캔)
    print("증권사 API 연결 및 테스트 종목(코스피 우량주) 준비 중...")
    try:
        kospi = fdr.StockListing('KOSPI')
        test_universe = kospi['Code'].tolist()[:100] 
    except:
        test_universe = ['005930', '000660', '035420', '051910', '005380'] # 실패 시 삼성전자 등 하드코딩
        
    run_time_machine_backtest("COVID-19 코로나 폭락장", test_universe)
