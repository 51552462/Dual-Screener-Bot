# supernova_hunter.py (V53.2 글로벌 초신성 역추적 & 텔레그램 보고 엔진)
import os, time, json, sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
import FinanceDataReader as fdr
import concurrent.futures
from datetime import datetime, timedelta
import pytz
import warnings
from io import StringIO
import requests
warnings.filterwarnings('ignore')
import auto_forward_tester as aft
scanned_today_cache = {'KR': set(), 'US': set()}

# ==========================================
# 💡 [환경 설정 및 텔레그램 세팅]
# ==========================================
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')
TELEGRAM_TOKEN = "7988939051:AAG4FqMzzz12vd7Crzt8DVPWiL3fMHM8tEc"
TELEGRAM_CHAT_ID = "6838834566"

def send_telegram_msg(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except: pass

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as f: return json.load(f)
    return {}

def save_config(data):
    with open(CONFIG_PATH, 'w') as f: json.dump(data, f, indent=4)

# ==========================================
# 💡 [전체 상장 종목 리스트 수집기]
# ==========================================
def get_krx_list():
    headers = {'User-Agent': 'Mozilla/5.0'}
    df_ks = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt", headers=headers, verify=False).text), header=0)[0]
    df_kq = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt", headers=headers, verify=False).text), header=0)[0]
    df = pd.concat([df_ks, df_kq])
    df['Code'] = df['종목코드'].astype(str).str.zfill(6)
    df = df.rename(columns={'회사명': 'Name'})
    junk_pattern = '스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권'
    return df[~df['Name'].str.contains(junk_pattern, regex=True)][['Code', 'Name']].drop_duplicates('Code')

def get_us_list():
    try:
        df_nasdaq = fdr.StockListing('NASDAQ')
        df_nyse = fdr.StockListing('NYSE')
        df_amex = fdr.StockListing('AMEX')
        df = pd.concat([df_nasdaq, df_nyse, df_amex])
        df = df[df['Symbol'].str.isalpha()]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].rename(columns={'Symbol': 'Code'}).drop_duplicates('Code')
    except: return pd.DataFrame()

# ==========================================
# 💡 [핵심] 타임머신 DNA 추출기 (한/미 시장 완벽 분리형 3단계 기만술 및 랭크 정밀 필터)
# ==========================================
def extract_dna_from_df(df_raw, benchmarks, target_date, rank_name="UNKNOWN", market="KR"):
    try:
        # 💡 [수정] 150일 -> 200일로 늘려 6개월(약 125 거래일) 이상의 데이터를 완벽 확보
        hist_df = df_raw[df_raw.index <= target_date].tail(200).copy()
        if len(hist_df) < 130: return None # 6개월 데이터가 안 되면 기각
        
        c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
        trd_val_eok = (c * v) / 100_000_000 
        
        for n in [10, 20, 30, 60, 112, 224]:
            hist_df[f'EMA{n}'] = hist_df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
        
        is_aligned_30 = (hist_df['EMA10'] > hist_df['EMA20']) & (hist_df['EMA20'] > hist_df['EMA30'])
        is_aligned_112 = is_aligned_30 & (hist_df['EMA30'] > hist_df['EMA60']) & (hist_df['EMA60'] > hist_df['EMA112'])
        
        v_ma20 = pd.Series(v).rolling(20).mean().values
        cpv = np.where(h != l, (c - o) / (h - l), 0.5)
        vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
        tb = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)
        
        bb_std = pd.Series(c).rolling(20).std().values
        bb_mid = pd.Series(c).rolling(20).mean().values
        bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
        bbe = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
        
        idx_arr = np.arange(len(hist_df))
        r_val = hist_df['EMA10'].rolling(10).corr(pd.Series(idx_arr, index=hist_df.index)).fillna(0)
        r_squared = r_val * r_val
        ema10_3 = hist_df['EMA10'].shift(3).fillna(hist_df['EMA10'])
        ema_roc = np.where(ema10_3 != 0, ((hist_df['EMA10'] - ema10_3) / ema10_3) * 5000, 0)
        tml = np.where(is_aligned_30, ema_roc * (r_squared ** 2), 0)
        hist_df['TML'] = tml
        hist_df['ALL_UP'] = (tml > 0) & is_aligned_30 & (hist_df['EMA112'] > hist_df['EMA224'])

        # 이격도 산출
        prev_c = np.roll(c, 1); prev_c[0] = c[0]
        tr_arr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
        hist_df['ATR20'] = pd.Series(tr_arr).ewm(alpha=1/20, adjust=False, min_periods=0).mean()
        spread_10_20 = np.where(hist_df['EMA10'] > hist_df['EMA20'], ((hist_df['EMA10'] - hist_df['EMA20']) / hist_df['ATR20']) * 100, 0)
        spread_112_224 = np.where(hist_df['EMA112'] > hist_df['EMA224'], ((hist_df['EMA112'] - hist_df['EMA224']) / hist_df['ATR20']) * 100, 0)

        # ---------------------------------------------------------
        # 👑 [슈퍼노바 정리본 100% 반영: 한국/미국 분리 3단계 기만술 가동]
        # ---------------------------------------------------------
        dday_idx = len(hist_df) - 1
        t7_idx = max(0, dday_idx - 5)
        t30_idx = max(0, dday_idx - 20)
        t120_idx = max(0, dday_idx - 120)
        # 👇👇 [추가] 6개월 전 장기 매집/횡보 판독 로직 👇👇
        # 6개월 전 시점의 변동성(ATR) 대비 가격이 밴드 내에 수렴하고 있었는지 확인
        long_term_base = (hist_df['ATR20'].iloc[t120_idx] / hist_df['Close'].iloc[t120_idx] * 100) < 5.0
        if "Rank A" in rank_name and not long_term_base:
            return None # A랭크(6개월 장기 매집형)인데 6개월 전 횡보 매집 구간이 없으면 가차 없이 기각
        
        c_20 = c[max(0, dday_idx-20)] if dday_idx >= 20 else c[0]
        stock_ret = ((c[dday_idx] - c_20) / c_20) * 100 if c_20 > 0 else 0

        if market == 'KR':
            idx_c = benchmarks['KR'][benchmarks['KR'].index <= target_date].tail(150)['Close'].values
            idx_20 = idx_c[max(0, dday_idx-20)] if dday_idx >= 20 else idx_c[0]
            idx_ret = ((idx_c[dday_idx] - idx_20) / idx_20) * 100 if idx_20 > 0 else 0.0001
            rs = np.full(len(c), (stock_ret / (idx_ret if idx_ret != 0 else 0.0001)) * 100)

            # 💡 [한국장 공통 3단계 기만술]
            q1_aligned = is_aligned_112.iloc[t30_idx]
            q1_all_up = hist_df['ALL_UP'].iloc[max(0, t30_idx-5):t30_idx+1].sum() <= 1
            pass_q1 = (not q1_aligned) and q1_all_up
            
            q2_cpv = cpv[t7_idx]
            pass_q2 = (-0.2 <= q2_cpv <= 0.0) 
            
            q3_tml = tml[dday_idx]
            q3_vol_surge = trd_val_eok[dday_idx] > np.mean(trd_val_eok[max(0, dday_idx-20):dday_idx]) * 1.5
            pass_q3 = (q3_tml >= 10.0) and q3_vol_surge
            
            if not (pass_q1 and pass_q2 and pass_q3): return None

            # 💡 [한국장 랭크별 세부 로직 범위 100% 하드코딩 필터링]
            if "Rank A" in rank_name:
                c_30 = (-0.1<=cpv[t30_idx]<=0.0) and (45.4<=tb[t30_idx]<=69.9) and (5.5<=bbe[t30_idx]<=14.7) and (5.4<=tml[t30_idx]<=48.9) and (0.8<=trd_val_eok[t30_idx]<=22.2)
                c_7 = (-0.0<=cpv[t7_idx]<=0.2) and (45.7<=tb[t7_idx]<=78.6) and (0.0<=tml[t7_idx]<=59.4) and (0.0<=spread_112_224[t7_idx]<=8.8)
                c_0 = (0.5<=cpv[dday_idx]<=1.0) and (8.7<=tb[dday_idx]<=14.9) and (12.3<=bbe[dday_idx]<=42.0) and (9.7<=tml[dday_idx]<=272.0) and (38.8<=trd_val_eok[dday_idx]<=599.8)
                if not (c_30 and c_7 and c_0): return None
            elif "Rank B" in rank_name:
                c_30 = (-0.1<=cpv[t30_idx]<=0.0) and (49.0<=tb[t30_idx]<=69.0) and (5.4<=bbe[t30_idx]<=11.4) and (37.0<=rs[t30_idx]<=359.0) and (1.0<=trd_val_eok[t30_idx]<=12.5)
                c_7 = (-0.0<=cpv[t7_idx]<=0.2) and (56.5<=tb[t7_idx]<=103.8) and (0.0<=spread_112_224[t7_idx]<=1.4)
                c_0 = (0.5<=cpv[dday_idx]<=1.0) and (14.8<=bbe[dday_idx]<=39.9) and (20.6<=tml[dday_idx]<=310.4) and (0.0<=spread_10_20[dday_idx]<=75.0) and (40.5<=trd_val_eok[dday_idx]<=461.5)
                if not (c_30 and c_7 and c_0): return None
            elif "Rank C" in rank_name:
                c_30 = (-0.1<=cpv[t30_idx]<=0.0) and (48.2<=tb[t30_idx]<=75.8) and (5.1<=bbe[t30_idx]<=10.1) and (3.0<=trd_val_eok[t30_idx]<=23.5)
                c_7 = (-360.2<=rs[t7_idx]<=151.2) and (4.5<=tml[t7_idx]<=72.7) and (23.8<=spread_10_20[t7_idx]<=62.0)
                c_0 = (9.6<=bbe[dday_idx]<=29.8) and (39.5<=tml[dday_idx]<=387.1) and (0.0<=spread_112_224[dday_idx]<=33.9) and (54.0<=trd_val_eok[dday_idx]<=213.0)
                if not (c_30 and c_7 and c_0): return None
            elif "Rank D" in rank_name:
                c_30 = (-0.1<=cpv[t30_idx]<=0.0) and (5.3<=bbe[t30_idx]<=8.3) and (-130.5<=rs[t30_idx]<=164.0) and (0.0<=spread_10_20[t30_idx]<=1.9)
                c_7 = (36.1<=tb[t7_idx]<=90.4) and (0.0<=tml[t7_idx]<=30.1) and (7.2<=trd_val_eok[t7_idx]<=79.2)
                c_0 = (16.6<=bbe[dday_idx]<=32.3) and (13.7<=tml[dday_idx]<=269.0) and (-1491.9<=rs[dday_idx]<=680.1) and (109.8<=trd_val_eok[dday_idx]<=1419.5)
                if not (c_30 and c_7 and c_0): return None
                
            final_rs = rs[-1]

        elif market == 'US':
            spy_c = benchmarks['SPY'][benchmarks['SPY'].index <= target_date].tail(150)['Close'].values
            qqq_c = benchmarks['QQQ'][benchmarks['QQQ'].index <= target_date].tail(150)['Close'].values
            
            spy_20 = spy_c[max(0, dday_idx-20)] if dday_idx >= 20 else spy_c[0]
            qqq_20 = qqq_c[max(0, dday_idx-20)] if dday_idx >= 20 else qqq_c[0]
            spy_ret = ((spy_c[dday_idx] - spy_20) / spy_20) * 100 if spy_20 > 0 else 0.0001
            qqq_ret = ((qqq_c[dday_idx] - qqq_20) / qqq_20) * 100 if qqq_20 > 0 else 0.0001
            
            rs_spy = np.full(len(c), (stock_ret / (spy_ret if spy_ret != 0 else 0.0001)) * 100)
            rs_qqq = np.full(len(c), (stock_ret / (qqq_ret if qqq_ret != 0 else 0.0001)) * 100)

            # 💡 [미국장 3단계 밀집 구간 100% 하드코딩 필터링]
            q1 = (-0.0 <= cpv[t30_idx] <= 0.1) and (3.2 <= bbe[t30_idx] <= 6.6) and (-339.2 <= rs_spy[t30_idx] <= 539.6) and (-108.3 <= rs_qqq[t30_idx] <= 510.7) and (0.0 <= tml[t30_idx] <= 48.9)
            q2 = (0.1 <= cpv[t7_idx] <= 0.2) and (30.8 <= tb[t7_idx] <= 50.9) and (3.1 <= bbe[t7_idx] <= 8.2) and (-626.6 <= rs_qqq[t7_idx] <= 182.3)
            q3 = (0.3 <= cpv[dday_idx] <= 0.8) and (5.6 <= tb[dday_idx] <= 12.0) and (9.0 <= bbe[dday_idx] <= 16.6) and (0.0 <= tml[dday_idx] <= 247.2) and (-804.4 <= rs_spy[dday_idx] <= 1323.4) and (-1338.8 <= rs_qqq[dday_idx] <= 761.9) and (0.0 <= spread_10_20[dday_idx] <= 75.8)

            if not (q1 and q2 and q3): return None
            final_rs = rs_spy[-1]

        # 형상 압축 및 반환
        c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
        new_shape = np.mean(np.array_split(c_norm, 20), axis=1).tolist()
        
        return {
            'rank_name': rank_name,
            'cpv': cpv[-1], 'tb': tb[-1], 'bbe': bbe[-1], 'rs': final_rs, 
            'vcp': 1.0, 'vol': 1.0, 'ma': 0.0, 'shape': new_shape,
            'tml': tml[dday_idx], 'trd_val': trd_val_eok[-1]
        }
    except: return None

# ==========================================
# 🚀 메인 역추적 로직 (Rank A~D 및 미국/한국 분리 마이닝)
# ==========================================
def hunt_supernovas(market):
    print(f"\n🚀 [{market}] 전체 시장 3단계 기만술 타임머신 역추적 가동...")
    send_telegram_msg(f"⏳ <b>[{market} 초신성 타임머신 가동]</b>\n전체 상장 종목을 대상으로 과거 노이즈를 제거하고 '3단계 기만술'을 통과한 찐 대박주만 스캔합니다. (약 10~20분 소요)")
    
    now = datetime.now()
    start_date = (now - timedelta(days=200)).strftime('%Y-%m-%d')
    
    try:
        if market == 'US':
            spy_df = yf.download('SPY', start=start_date, progress=False)
            qqq_df = yf.download('QQQ', start=start_date, progress=False)
            spy_df.index = pd.to_datetime(spy_df.index).tz_localize(None)
            qqq_df.index = pd.to_datetime(qqq_df.index).tz_localize(None)
            benchmarks = {'SPY': spy_df, 'QQQ': qqq_df}
        else:
            idx_df = fdr.DataReader('069500', start_date)
            idx_df.index = pd.to_datetime(idx_df.index).tz_localize(None)
            benchmarks = {'KR': idx_df}
    except: return

    stock_list = get_krx_list() if market == 'KR' else get_us_list()
    tickers = stock_list['Code'].tolist()
    name_map = dict(zip(stock_list['Code'], stock_list['Name']))
    
    results = []
    scanned_count = 0
    
    def process_ticker(code):
        try:
            df = fdr.DataReader(code, start_date) if market == 'KR' else yf.download(code, start=start_date, progress=False)
            if df.empty or len(df) < 130: return None
            df.index = pd.to_datetime(df.index).tz_localize(None)
            
            c = df['Close'].values
            if c[-1] < (1000 if market == 'KR' else 3.0): return None 
            
            ret_1w = (c[-1] - c[-8]) / c[-8] * 100 if len(c) >= 8 else 0
            ret_1m = (c[-1] - c[-20]) / c[-20] * 100 if len(c) >= 20 else 0
            ret_3m = (c[-1] - c[-60]) / c[-60] * 100 if len(c) >= 60 else 0
            ret_6m = (c[-1] - c[-120]) / c[-120] * 100 if len(c) >= 120 else 0
            
            return {'code': code, 'df': df, 'ret_1w': ret_1w, 'ret_1m': ret_1m, 'ret_3m': ret_3m, 'ret_6m': ret_6m}
        except: return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        for res in executor.map(process_ticker, tickers):
            scanned_count += 1
            if scanned_count % 500 == 0:
                print(f"   ↳ 진행 중... {scanned_count}/{len(tickers)}개 스캔 완료")
            if res: results.append(res)
            
    if not results: return
    
    res_df = pd.DataFrame(results)
    
    top_6m = res_df.sort_values('ret_6m', ascending=False).head(10)
    rem_3m = res_df[~res_df['code'].isin(top_6m['code'])]
    top_3m = rem_3m.sort_values('ret_3m', ascending=False).head(10)
    rem_1m = rem_3m[~rem_3m['code'].isin(top_3m['code'])]
    top_1m = rem_1m.sort_values('ret_1m', ascending=False).head(10)
    rem_1w = rem_1m[~rem_1m['code'].isin(top_1m['code'])]
    top_1w = rem_1w.sort_values('ret_1w', ascending=False).head(10)
    
    supernovas = []
    for _, r in top_6m.iterrows(): supernovas.append((r['code'], r['df'], now, "🥇 Rank A: '6개월' 장기 매집형 (진성 대장주)"))
    for _, r in top_3m.iterrows(): supernovas.append((r['code'], r['df'], now, "🥈 Rank B: '3개월' 중기 매집형 (강력한 스윙 추세주)"))
    for _, r in top_1m.iterrows(): supernovas.append((r['code'], r['df'], now, "🥉 Rank C: '1개월' 단기 매집형 (트렌드 테마주)"))
    for _, r in top_1w.iterrows(): supernovas.append((r['code'], r['df'], now, "🏅 Rank D: '10일' 초단기/밈(Meme) 작전주"))

    dna_list = []
    rank_counts = {"A":0, "B":0, "C":0, "D":0}
    
    for code, df, target_date, rank_name in supernovas:
        dna = extract_dna_from_df(df, benchmarks, target_date.strftime('%Y-%m-%d'), rank_name, market)
        if dna: 
            dna_list.append(dna)
            if "Rank A" in rank_name: rank_counts["A"] += 1
            elif "Rank B" in rank_name: rank_counts["B"] += 1
            elif "Rank C" in rank_name: rank_counts["C"] += 1
            elif "Rank D" in rank_name: rank_counts["D"] += 1
        
    if not dna_list: return
    
    if not dna_list: return
    
    # 👇👇 [기존 단일 평균(Centroid) 멍청한 로직 완전 삭제 후 교체] 👇👇
    # 👑 1. 랭크별로 DNA 1차 분류
    rank_dnas = {"A": [], "B": [], "C": [], "D": []}
    for d in dna_list:
        if "Rank A" in d['rank_name']: rank_dnas["A"].append(d)
        elif "Rank B" in d['rank_name']: rank_dnas["B"].append(d)
        elif "Rank C" in d['rank_name']: rank_dnas["C"].append(d)
        elif "Rank D" in d['rank_name']: rank_dnas["D"].append(d)

    market_templates = {}
    
    # 👑 2. 랭크 내에서 다시 '조용한 매집(Stealth)'과 '변동성 폭발(Volatile)' 2차 정밀 분리
    for rank, dnas in rank_dnas.items():
        if not dnas: continue
        
        # BBE(응축 에너지)의 중간값을 기준으로 조용함과 변동성을 가름 (대표님 지적 완벽 반영)
        median_bbe = np.median([d['bbe'] for d in dnas])
        
        stealth_dnas = [d for d in dnas if d['bbe'] <= median_bbe]
        volatile_dnas = [d for d in dnas if d['bbe'] > median_bbe]
        
        def make_template(sub_dnas):
            if not sub_dnas: return None
            return {
                'cpv': np.mean([d['cpv'] for d in sub_dnas]),
                'tb': np.mean([d['tb'] for d in sub_dnas]),
                'bbe': np.mean([d['bbe'] for d in sub_dnas]),
                'rs': np.mean([d['rs'] for d in sub_dnas]),
                'shape': np.mean([d['shape'] for d in sub_dnas], axis=0).tolist()
            }
            
        if stealth_dnas:
            market_templates[f"RANK_{rank}_STEALTH"] = make_template(stealth_dnas)
        if volatile_dnas:
            market_templates[f"RANK_{rank}_VOLATILE"] = make_template(volatile_dnas)

    config = load_config()
    # 단일 DNA가 아닌 '다중 템플릿(Multi)' 사전을 통째로 관제탑에 저장
    config[f"DNA_SUPERNOVA_{market}_MULTI"] = market_templates 
    save_config(config)
    
    # 👇👇 [텔레그램 리포트 내용도 다중 템플릿에 맞게 수정] 👇👇
    report_msg = f"🚀 <b>[{market} 다차원 초신성 역추적 완료]</b>\n"
    report_msg += "💡 단순 평균의 오류를 제거하고, 랭크별/패턴별(조용한 매집 vs 변동성 폭발)로 DNA를 완벽히 분리 추출했습니다.\n\n"
    report_msg += f"🧪 <b>[발견된 세력 패턴 거울(템플릿) 수: 총 {len(market_templates)}개]</b>\n"
    
    for t_name, t_dna in market_templates.items():
        style = "🤫 조용한 매집형" if "STEALTH" in t_name else "🌋 변동성 폭발형"
        report_msg += f"▪️ {t_name} ({style}) ➔ BBE: {t_dna['bbe']:.1f} | RS: {t_dna['rs']:.1f}\n"

    report_msg += f"\n💡 <i>실시간 장중 스캐너가 위 {len(market_templates)}개의 거울 중 단 하나라도 50% 이상 일치하는 종목을 발견하면 즉시 진입합니다.</i>"
    
    send_telegram_msg(report_msg)
    print(f"✅ [{market}] 다차원 DNA 템플릿 갱신 완료!")

# ==========================================
# 🚀 [신규 엔진] 초신성 실시간 스나이퍼 (절대 수치 기반 코사인 매칭)
# ==========================================
def execute_supernova_live_scan(market):
    print(f"\n🦅 [{market}] 초신성 스나이퍼 스캔 가동 (절대 수치 코사인 매칭)...")
    
    # 💡 [핵심] 대표님의 하드코딩 수치를 기반으로 한 '완벽한 타점(Ideal Vector)' 정의
    # 순서: [CPV, TB, BBE]
    ideal_templates = {}
    
    if market == 'KR':
        # Rank A: c_0 = cpv(0.5~1.0), tb(8.7~14.9), bbe(12.3~42.0)
        ideal_templates['RANK_A_장기매집'] = np.array([0.75, 11.8, 27.15])
        # Rank B: c_0 = cpv(0.5~1.0), bbe(14.8~39.9) / tb는 A와 C의 중간 10.0 가정
        ideal_templates['RANK_B_중기스윙'] = np.array([0.75, 10.0, 27.35])
        # Rank C: c_0 = bbe(9.6~29.8) / cpv 0.6, tb 8.0 가정
        ideal_templates['RANK_C_단기테마'] = np.array([0.60, 8.0, 19.70])
        # Rank D: c_0 = bbe(16.6~32.3) / cpv 0.6, tb 8.0 가정
        ideal_templates['RANK_D_초단기밈'] = np.array([0.60, 8.0, 24.45])
    elif market == 'US':
        # US: q3 = cpv(0.3~0.8), tb(5.6~12.0), bbe(9.0~16.6)
        ideal_templates['US_MEME_슈팅'] = np.array([0.55, 8.8, 12.80])

    # 💡 [V55.0 관제탑 연동] 만약 실전 MFE 가중치로 진화한 템플릿이 있다면 추가 로드
    config = load_config()
    mfe_weighted = config.get("DNA_SUPERNOVA_MFE_WEIGHTED")
    if mfe_weighted:
        ideal_templates['MFE_진화형_황금타점'] = np.array([mfe_weighted['cpv'], mfe_weighted['tb'], mfe_weighted.get('bbe', 20.0)])

    stock_list = get_krx_list() if market == 'KR' else get_us_list()
    tickers = stock_list['Code'].tolist()
    
    def get_similarity(vec1, vec2):
        n1, n2 = np.linalg.norm(vec1), np.linalg.norm(vec2)
        return np.dot(vec1, vec2) / (n1 * n2) if n1 > 0 and n2 > 0 else 0

    try:
        conn = sqlite3.connect(aft.DB_PATH, timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT code FROM forward_trades WHERE market=? AND status='OPEN'", (market,))
        open_positions = {row[0] for row in cursor.fetchall()}
        conn.close()
    except: open_positions = set()

    for code in tickers:
        if code in open_positions or code in scanned_today_cache[market]:
            continue
            
        try:
            df = fdr.DataReader(code, (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')) if market == 'KR' else yf.download(code, period="2mo", progress=False)
            if df.empty or len(df) < 20: continue
            
            c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
            current_close = c[-1]
            
            # 잡주 필터링 (동전주, 소외주 원천 차단)
            if market == 'KR' and current_close < 1000: continue 
            if market == 'US' and current_close < 1.0: continue  
            if np.mean(v[-5:]) < 50000: continue                 
            
            # 현재 종목의 DNA 벡터 추출 (RS 제외, 순수 캔들/수급 에너지 3차원 대조)
            v_ma20 = pd.Series(v).rolling(20).mean().values
            cpv = np.where(h != l, (c - o) / (h - l), 0.5)[-1]
            vol_mult = (v[-1] / v_ma20[-1]) if v_ma20[-1] > 0 else 1.0
            tb = vol_mult / max(cpv, 0.01) if cpv > 0 else vol_mult / 0.01
            bb_std = pd.Series(c).rolling(20).std().values[-1]
            bb_mid = pd.Series(c).rolling(20).mean().values[-1]
            bb_width = (4 * bb_std) / bb_mid if bb_mid > 0 else 0.01
            bbe = (1.0 / bb_width) * vol_mult if bb_width > 0 else 0
            
            # 💡 대조 벡터: [CPV, TB, BBE] 3차원
            current_vec = np.nan_to_num(np.array([cpv, tb, bbe]))
            
            best_sim = 0.0
            best_pattern_name = "UNKNOWN"
            
            # 그룹/랭크별 하드코딩 절대 수치와 코사인 유사도 1:1 대결
            for t_name, base_vec in ideal_templates.items():
                sim = get_similarity(current_vec, base_vec)
                
                if sim > best_sim:
                    best_sim = sim
                    best_pattern_name = t_name
            
            # 50% 이상 일치 시 강제 편입 및 태깅
            if best_sim >= 0.50:
                is_success, msg = aft.try_add_virtual_position(
                    market=market, 
                    code=code, 
                    name=stock_list[stock_list['Code']==code]['Name'].values[0],
                    sig_type=f"[SUPERNOVA_초입] {best_pattern_name}", # 💡 "RANK_A_장기매집" 등 명확한 이름표 각인
                    score=best_sim * 100, 
                    ep=current_close,
                    facts={'dyn_cpv': cpv, 'dyn_tb': tb, 'v_energy': bbe},
                    trade_source="SUPERNOVA" 
                )
                
                if is_success:
                    scanned_today_cache[market].add(code)
                    send_telegram_msg(f"🦅 <b>[초신성 정밀 타격]</b>\n{code} ({best_pattern_name})\n일치율: {best_sim*100:.1f}%\n대표님의 절대 수치 기준과 부합하여 가상매매 장부에 편입했습니다.")
        except: pass
# ==========================================
# 🕒 [메인 스케줄러] 타임머신(과거) + 스나이퍼(실시간) 병렬 가동
# ==========================================
def run_miner_scheduler():
    """1주일에 한 번 과거 데이터를 마이닝하여 템플릿을 갱신하는 봇"""
    tz_kr = pytz.timezone('Asia/Seoul')
    while True:
        try:
            now = datetime.now(tz_kr)
            # 매주 월요일 17:00 템플릿 갱신
            if now.weekday() == 0 and now.hour == 17 and now.minute == 0:
                hunt_supernovas('KR')
                hunt_supernovas('US')
                time.sleep(65) 
            time.sleep(30)
        except Exception as e:
            time.sleep(60)

def run_live_sniper_scheduler():
    """매일 4번 지정된 시간에 실시간 시장을 스캔하고 쏘는 봇"""
    tz_kr = pytz.timezone('Asia/Seoul')
    print("🕒 [초신성 실시간 스나이퍼] 대기 중...")
    print(" - 🇰🇷 한국 타격: 09:00, 09:30, 15:00, 16:00 (KST)")
    print(" - 🇺🇸 미국 타격: 23:30, 00:00, 05:00, 06:00 (KST)")
    
    global scanned_today_cache
    last_cleared_day = datetime.now(tz_kr).day

    while True:
        try:
            now = datetime.now(tz_kr)
            time_str = f"{now.hour:02d}:{now.minute:02d}"
            
            # 날짜가 바뀌면 어제 쐈던 기록(캐시) 초기화
            if now.day != last_cleared_day:
                scanned_today_cache = {'KR': set(), 'US': set()}
                last_cleared_day = now.day

            kr_target_times = ["09:00", "09:30", "15:00", "16:00"]
            us_target_times = ["23:30", "00:00", "05:00", "06:00"]
            
            if time_str in kr_target_times:
                execute_supernova_live_scan('KR')
                time.sleep(65) 
                
            elif time_str in us_target_times:
                execute_supernova_live_scan('US')
                time.sleep(65) 

            time.sleep(20) 
            
        except Exception as e:
            print(f"스나이퍼 스케줄러 에러: {e}")
            time.sleep(60)

if __name__ == "__main__":
    import threading
    
    print("🚀 [초기화] 즉시 1회 타임머신 스캔을 시작하여 최신 템플릿을 만듭니다...")
    hunt_supernovas('KR')
    hunt_supernovas('US')
    
    # 1. 템플릿 갱신 마이너는 백그라운드 스레드로 분리
    t_miner = threading.Thread(target=run_miner_scheduler, daemon=True)
    t_miner.start()
    
    # 2. 실시간 진입 스나이퍼는 메인 스레드에서 무한 실행
    run_live_sniper_scheduler()
# 👆👆 [덮어쓰기 끝] 👆👆
