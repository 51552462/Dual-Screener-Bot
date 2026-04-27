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
# 💡 [핵심] 타임머신 DNA 추출기
# ==========================================
def extract_dna_from_df(df_raw, idx_df, target_date):
    try:
        hist_df = df_raw[df_raw.index < target_date].tail(150)
        i_df = idx_df[idx_df.index < target_date].tail(150)
        
        if len(hist_df) < 100 or len(i_df) < 100: return None
        
        c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
        idx_c = i_df['Close'].values
        
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
        
        z_rs = ((rs_slope - idx_rs) + (abs(idx_rs) * 1.5 if idx_rs < 0 and rs_slope > idx_rs else 0)) / safe_vol
        z_bbe = bbe / safe_vol

        c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
        new_shape = np.mean(np.array_split(c_norm, 20), axis=1).tolist()

        return {'cpv': cpv, 'tb': tb, 'bbe': z_bbe, 'rs': z_rs, 'vcp': vcp_ratio, 'vol': vol_flow, 'ma': ma_conv, 'shape': new_shape}
    except: return None

# ==========================================
# 🚀 메인 역추적 로직
# ==========================================
def hunt_supernovas(market):
    print(f"\n🚀 [{market}] 전체 시장 타임머신 역추적 가동...")
    send_telegram_msg(f"⏳ <b>[{market} 초신성 타임머신 가동]</b>\n전체 상장 종목을 대상으로 1주/1달/3달 대박주를 스캔합니다. (약 10~20분 소요)")
    
    now = datetime.now()
    start_date = (now - timedelta(days=180)).strftime('%Y-%m-%d')
    
    idx_ticker = '069500' if market == 'KR' else 'SPY'
    try:
        idx_df = fdr.DataReader(idx_ticker, start_date) if market == 'KR' else yf.download(idx_ticker, start=start_date, progress=False)
        idx_df.index = pd.to_datetime(idx_df.index).tz_localize(None)
    except: return

    stock_list = get_krx_list() if market == 'KR' else get_us_list()
    tickers = stock_list['Code'].tolist()
    name_map = dict(zip(stock_list['Code'], stock_list['Name']))
    
    results = []
    scanned_count = 0
    
    def process_ticker(code):
        try:
            df = fdr.DataReader(code, start_date) if market == 'KR' else yf.download(code, start=start_date, progress=False)
            if df.empty or len(df) < 100: return None
            df.index = pd.to_datetime(df.index).tz_localize(None)
            
            c = df['Close'].values
            if c[-1] < (1000 if market == 'KR' else 3.0): return None 
            
            ret_1w = (c[-1] - c[-5]) / c[-5] * 100 if len(c) >= 5 else 0
            ret_1m = (c[-1] - c[-20]) / c[-20] * 100 if len(c) >= 20 else 0
            ret_3m = (c[-1] - c[-60]) / c[-60] * 100 if len(c) >= 60 else 0
            
            return {'code': code, 'df': df, 'ret_1w': ret_1w, 'ret_1m': ret_1m, 'ret_3m': ret_3m}
        except: return None

    # 💡 터미널 답답함 방지를 위해 500개마다 진행률 출력
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        for res in executor.map(process_ticker, tickers):
            scanned_count += 1
            if scanned_count % 500 == 0:
                print(f"   ↳ 진행 중... {scanned_count}/{len(tickers)}개 스캔 완료")
            if res: results.append(res)
            
    if not results: return
    
    res_df = pd.DataFrame(results)
    
    top_1w = res_df.sort_values('ret_1w', ascending=False).head(10)
    rem_1m = res_df[~res_df['code'].isin(top_1w['code'])]
    top_1m = rem_1m.sort_values('ret_1m', ascending=False).head(10)
    rem_3m = rem_1m[~rem_1m['code'].isin(top_1m['code'])]
    top_3m = rem_3m.sort_values('ret_3m', ascending=False).head(10)
    
    supernovas = []
    for _, r in top_1w.iterrows(): supernovas.append((r['code'], r['df'], now - timedelta(days=7)))
    for _, r in top_1m.iterrows(): supernovas.append((r['code'], r['df'], now - timedelta(days=30)))
    for _, r in top_3m.iterrows(): supernovas.append((r['code'], r['df'], now - timedelta(days=90)))

    dna_list = []
    analyzed_names = []
    
    for code, df, target_date in supernovas:
        dna = extract_dna_from_df(df, idx_df, target_date.strftime('%Y-%m-%d'))
        if dna: 
            dna_list.append(dna)
            analyzed_names.append(name_map.get(code, str(code)))
        
    if not dna_list: return
    
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
    
    config = load_config()
    config[f"DNA_SUPERNOVA_{market}"] = centroid
    save_config(config)
    
    # 💡 [핵심] 회원님 텔레그램으로 상세 분석 결과 발송!
    report_msg = f"🚀 <b>[{market} 초신성 역추적 분석 완료]</b>\n"
    report_msg += f"최근 1~3개월 폭등 대장주 {len(dna_list)}개의 '폭등 직전 DNA'를 추출하여 선취매 템플릿을 갱신했습니다.\n\n"
    
    report_msg += f"🧪 <b>[이번 주 초신성 추출 표본]</b>\n"
    sample_str = ", ".join(analyzed_names[:10]) + (" 등..." if len(analyzed_names) > 10 else "")
    report_msg += f"▪️ {sample_str}\n\n"

    report_msg += f"🧬 <b>[폭등 전야 공통점(Centroid) 기준값]</b>\n"
    report_msg += f"▪️ 캔들지배력(CPV): <b>{centroid['cpv']:.2f}</b>\n"
    report_msg += f"▪️ 진짜양봉(TB): <b>{centroid['tb']:.2f}</b>\n"
    report_msg += f"▪️ 응축에너지(BBE): <b>{centroid['bbe']:.2f}</b>\n"
    report_msg += f"▪️ 상대강도(RS): <b>{centroid['rs']:.2f}</b>\n\n"
    report_msg += f"💡 <i>이 관상과 50% 이상 일치하는 종목은 즉시 [SUPERNOVA] 태그를 달고 장부에 편입됩니다.</i>"
    
    send_telegram_msg(report_msg)
    print(f"✅ [{market}] 초신성 템플릿 갱신 및 텔레그램 발송 완료!")

def run_scheduler():
    tz_kr = pytz.timezone('Asia/Seoul')
    print("🕒 [초신성 타임머신 역추적기] 스케줄러 대기 중...")
    
    while True:
        try:
            now = datetime.now(tz_kr)
            if now.weekday() == 0 and now.hour == 17 and now.minute == 0:
                hunt_supernovas('KR')
                hunt_supernovas('US')
                time.sleep(65) 
            time.sleep(30)
        except Exception as e:
            time.sleep(60)

if __name__ == "__main__":
    print("🚀 [초기화] 즉시 1회 타임머신 스캔을 시작합니다...")
    hunt_supernovas('KR')
    hunt_supernovas('US')
    run_scheduler()
