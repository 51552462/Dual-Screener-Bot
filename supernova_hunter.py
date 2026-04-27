# supernova_hunter.py (V52.0 타임머신 폭등주 역추적 엔진)
import os, time, json, sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as f: return json.load(f)
    return {}

def save_config(data):
    with open(CONFIG_PATH, 'w') as f: json.dump(data, f, indent=4)

def extract_dna_at_date(code, market, target_date):
    """지정된 날짜(폭등 직전) 기준으로 150일치 과거 데이터를 로드하여 DNA 텐서 추출"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        table_name = f"{market}_{code}"
        idx_table = 'US_SPY' if market == 'US' else 'KR_KOSDAQ_IDX'
        
        # 💡 [핵심] 현재가 아닌, target_date(폭등 전날) 이전의 과거 데이터만 가져옴 (미래 참조 차단)
        hist_df = pd.read_sql(f"SELECT * FROM {table_name} WHERE Date < '{target_date}' ORDER BY Date DESC LIMIT 150", conn).sort_values('Date')
        idx_df = pd.read_sql(f"SELECT * FROM {idx_table} WHERE Date < '{target_date}' ORDER BY Date DESC LIMIT 150", conn).sort_values('Date')
        conn.close()
        
        if len(hist_df) < 100 or len(idx_df) < 100: return None
        
        c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
        idx_c = idx_df['Close'].values
        
        # 1. 7D 팩트 벡터 연산
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
        
        idx_rs = ((idx_c[-1] - idx_c[0]) / idx_c[0]) * 100
        idx_vol = pd.Series(idx_c).pct_change().std() * 100 * np.sqrt(252)
        safe_vol = idx_vol if idx_vol > 0.1 else 1.0
        
        excess_return = rs_slope - idx_rs
        defiance_premium = abs(idx_rs) * 1.5 if (idx_rs < 0 and excess_return > 0) else 0.0
        z_rs = (excess_return + defiance_premium) / safe_vol
        z_bbe = bbe / safe_vol

        c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
        new_shape = np.mean(np.array_split(c_norm, 20), axis=1).tolist()

        return {'cpv': cpv, 'tb': tb, 'bbe': z_bbe, 'rs': z_rs, 'vcp': vcp_ratio, 'vol': vol_flow, 'ma': ma_conv, 'shape': new_shape}
    except: return None

def hunt_supernovas(market):
    print(f"\n🚀 [{market}] 타임머신 역추적 엔진 가동...")
    conn = sqlite3.connect(DB_PATH)
    
    now = datetime.now()
    d_1w = (now - timedelta(days=7)).strftime('%Y-%m-%d')
    d_1m = (now - timedelta(days=30)).strftime('%Y-%m-%d')
    d_3m = (now - timedelta(days=90)).strftime('%Y-%m-%d')
    
    # DB에 저장된 모든 종목의 최근 수익률을 스캔하여 Top 10 추출 (생략된 슈도코드 대신 팩트 쿼리)
    # 실제로는 각 테이블을 순회하며 수익률을 구해야 하나, 연산 속도를 위해 forward_trades 테이블의 승리 종목을 활용합니다.
    query = f"SELECT code, name, entry_date, final_ret FROM forward_trades WHERE market='{market}' AND final_ret > 15.0 ORDER BY final_ret DESC"
    winners = pd.read_sql(query, conn)
    conn.close()
    
    if len(winners) < 5:
        print("⚠️ 표본이 부족하여 초신성 템플릿 갱신을 스킵합니다.")
        return
        
    # 중복 제거
    winners = winners.drop_duplicates(subset=['code'])
    top_supernovas = winners.head(10)
    
    dna_list = []
    print(f"🔬 대박주 {len(top_supernovas)}개의 폭등 전야(기점) 관상 역추출 중...")
    
    for _, row in top_supernovas.iterrows():
        # 폭등이 시작되었던 기점(entry_date)을 타깃으로 타임머신 로드
        dna = extract_dna_at_date(row['code'], market, row['entry_date'])
        if dna: dna_list.append(dna)
        
    if not dna_list: return
    
    # 추출된 DNA들의 교집합(평균)을 구하여 센트로이드 생성
    centroid = {
        'name': f"SUPERNOVA_{market}_CENTROID",
        'cpv': np.mean([d['cpv'] for d in dna_list]),
        'tb': np.mean([d['tb'] for d in dna_list]),
        'bbe': np.mean([d['bbe'] for d in dna_list]),
        'rs': np.mean([d['rs'] for d in dna_list]),
        'vcp': np.mean([d['vcp'] for d in dna_list]),
        'vol': np.mean([d['vol'] for d in dna_list]),
        'ma': np.mean([d['ma'] for d in dna_list]),
        'shape': np.mean([d['shape'] for d in dna_list], axis=0).tolist()
    }
    
    # 금고에 업데이트
    config = load_config()
    config[f"DNA_SUPERNOVA_{market}"] = centroid
    save_config(config)
    print(f"✅ [{market}] 초신성 템플릿(Centroid) 금고 업데이트 완료!")

if __name__ == "__main__":
    hunt_supernovas('KR')
    hunt_supernovas('US')
