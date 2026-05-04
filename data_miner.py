import pandas as pd
import numpy as np
import json
import os
import sqlite3
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# =====================================================================
# 1. 환경 설정 및 데이터 로드
# =====================================================================
CSV_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Supernova_Flow_Tracking_Master.csv')
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')
FORWARD_DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')


def _ohlcv_table_name(market, code) -> str:
    m = str(market).upper().strip()
    c = str(code).strip()
    if m == 'KR':
        return f"KR_{c.zfill(6)}"
    return f"US_{c.replace('.', '-')}"


def _compute_tml_supernova_last(hdf: pd.DataFrame):
    """supernova_hunter.extract_dna_from_df와 동일한 TML 정의(시계열 마지막 봉)."""
    if hdf is None or len(hdf) < 130:
        return None
    need = {'Open', 'High', 'Low', 'Close', 'Volume'}
    if not need.issubset(set(hdf.columns)):
        return None
    hist_df = hdf.copy()
    c = hist_df['Close'].values
    o = hist_df['Open'].values
    h = hist_df['High'].values
    l = hist_df['Low'].values
    v = hist_df['Volume'].values
    for n in [10, 20, 30, 60, 112, 224]:
        hist_df[f'EMA{n}'] = hist_df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    is_aligned_30 = (hist_df['EMA10'] > hist_df['EMA20']) & (hist_df['EMA20'] > hist_df['EMA30'])
    idx_arr = np.arange(len(hist_df))
    r_val = hist_df['EMA10'].rolling(10).corr(pd.Series(idx_arr, index=hist_df.index)).fillna(0)
    r_squared = r_val * r_val
    ema10_3 = hist_df['EMA10'].shift(3).fillna(hist_df['EMA10'])
    ema_roc = np.where(ema10_3 != 0, ((hist_df['EMA10'] - ema10_3) / ema10_3) * 5000, 0)
    tml = np.where(is_aligned_30, ema_roc * (r_squared ** 2), 0)
    tv = float(tml[-1])
    if not np.isfinite(tv):
        return None
    return tv


def _tml_at_entry_from_sqlite(conn, market, code, entry_date_str) -> float:
    """entry_date 이하 최근 200영업일 OHLCV로 TML 산출(extract_dna와 동일 윈도). 실패 시 None."""
    tbl = _ohlcv_table_name(market, code)
    q = f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" WHERE Date <= ? ORDER BY Date DESC LIMIT 200'
    try:
        sub = pd.read_sql(q, conn, params=(entry_date_str,))
    except Exception:
        return None
    if sub is None or sub.empty or len(sub) < 130:
        return None
    sub = sub.sort_values('Date')
    sub['Date'] = pd.to_datetime(sub['Date'])
    sub = sub.set_index('Date')
    return _compute_tml_supernova_last(sub)


def _evaluate_alpha_formula(df, formula):
    """supernova_hunter.evaluate_alpha_formula와 동일 네임스페이스(순환 import 방지용 복제)."""
    if df is None or df.empty:
        return None
    need_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    if not all(c in df.columns for c in need_cols):
        return None
    O = df['Open']
    H = df['High']
    L = df['Low']
    C = df['Close']
    V = df['Volume']

    def add(a, b):
        return a + b

    def sub(a, b):
        return a - b

    def mul(a, b):
        return a * b

    def div(a, b):
        safe_b = b.replace(0, np.nan) if isinstance(b, pd.Series) else (np.nan if b == 0 else b)
        return a / safe_b

    def rolling_mean(x, w):
        return x.rolling(int(w)).mean()

    def rolling_std(x, w):
        return x.rolling(int(w)).std()

    env = {
        'O': O, 'H': H, 'L': L, 'C': C, 'V': V,
        'add': add, 'sub': sub, 'mul': mul, 'div': div,
        'rolling_mean': rolling_mean, 'rolling_std': rolling_std
    }
    try:
        result = eval(str(formula).strip(), {"__builtins__": {}}, env)
        if isinstance(result, pd.Series):
            return result.replace([np.inf, -np.inf], np.nan)
    except Exception:
        return None
    return None


def _fetch_ohlcv_for_alpha(conn, market, code_str):
    if conn is None:
        return None
    tbl = _ohlcv_table_name(market, code_str)
    try:
        sub = pd.read_sql(
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date DESC LIMIT 300',
            conn,
        )
    except Exception:
        return None
    if sub is None or sub.empty or len(sub) < 60:
        return None
    return sub.sort_values('Date')


def _append_evolved_alpha_columns(clean_df: pd.DataFrame, evolved: dict):
    """관제탑 EVOLVED_ALPHA_FACTORS 수식을 행별 OHLCV로 평가해 clean_df에 [EVOLVED_*] 컬럼 추가."""
    meta = []
    if not isinstance(evolved, dict) or not evolved:
        return clean_df, meta
    keys = [k for k in evolved.keys() if isinstance(k, str) and str(evolved.get(k) or '').strip()]
    keys.sort(key=lambda k: (len(k), k))

    conn = None
    try:
        if os.path.exists(FORWARD_DB_PATH):
            conn = sqlite3.connect(FORWARD_DB_PATH, timeout=30)
    except Exception:
        conn = None

    out = clean_df.copy()
    for slot_key in keys:
        formula = str(evolved[slot_key]).strip()
        col_name = f"[EVOLVED_{slot_key}]"
        vals = []
        for _, row in out.iterrows():
            code = row.get('종목코드')
            mkt = row.get('시장', 'KR')
            if code is None or (isinstance(code, float) and pd.isna(code)):
                vals.append(np.nan)
                continue
            mkt_u = str(mkt).upper().strip() if mkt is not None and not (isinstance(mkt, float) and pd.isna(mkt)) else 'KR'
            s_code = str(code).strip()
            if mkt_u == 'KR':
                digits = ''.join(ch for ch in s_code if ch.isdigit())
                if not digits:
                    vals.append(np.nan)
                    continue
                code_str = digits.zfill(6)[-6:]
            else:
                code_str = s_code.replace('.', '-')
            ohlcv = _fetch_ohlcv_for_alpha(conn, mkt_u, code_str)
            if ohlcv is None:
                vals.append(np.nan)
                continue
            ser = _evaluate_alpha_formula(ohlcv, formula)
            if ser is None:
                vals.append(np.nan)
                continue
            s = pd.to_numeric(ser, errors='coerce').dropna()
            if s.empty:
                vals.append(np.nan)
            else:
                v = float(s.iloc[-1])
                vals.append(v if np.isfinite(v) else np.nan)
        out[col_name] = vals
        meta.append((slot_key, col_name))

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    return out, meta


def _merge_forward_winner_dna_into_df(df: pd.DataFrame, target_features: list) -> pd.DataFrame:
    """최근 30일 청산·final_ret>0 종목 DNA 병합. TML은 market_data.sqlite OHLCV로 extract_dna와 동일 계산."""
    cpv_f, tb_f, bbe_f, tml_f, rs_f = target_features
    if not os.path.exists(FORWARD_DB_PATH):
        return df
    cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    sql = """
        SELECT market, code, entry_date, dyn_cpv, v_cpv, dyn_tb, v_energy, dyn_rs, v_rs
        FROM forward_trades
        WHERE status LIKE 'CLOSED%'
          AND final_ret > 0
          AND exit_date IS NOT NULL AND exit_date != ''
          AND exit_date >= ?
    """
    conn = None
    try:
        conn = sqlite3.connect(FORWARD_DB_PATH, timeout=30)
        raw = pd.read_sql(sql, conn, params=(cutoff,))
    except Exception as e:
        print(f"⚠️ [실전 DNA 병합] forward_trades 로드 실패(기존 CSV만 사용): {e}")
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        return df
    if raw is None or raw.empty:
        try:
            conn.close()
        except Exception:
            pass
        return df
    rows = []
    for _, row in raw.iterrows():
        mkt = row.get('market')
        code = row.get('code')
        ent = row.get('entry_date')
        if pd.isna(mkt) or pd.isna(code) or ent is None or (isinstance(ent, float) and pd.isna(ent)):
            continue
        entry_str = str(ent)[:10]
        tml_v = _tml_at_entry_from_sqlite(conn, mkt, code, entry_str)
        if tml_v is None:
            continue
        cpv_u = float(row.get('dyn_cpv') or 0) if abs(float(row.get('dyn_cpv') or 0)) > 1e-9 else float(row.get('v_cpv') or 0)
        rs_u = float(row.get('dyn_rs') or 0) if abs(float(row.get('dyn_rs') or 0)) > 1e-9 else float(row.get('v_rs') or 0)
        rows.append({
            cpv_f: round(cpv_u, 4),
            tb_f: round(float(row.get('dyn_tb') or 0), 4),
            bbe_f: round(float(row.get('v_energy') or 0), 4),
            tml_f: round(float(tml_v), 4),
            rs_f: round(rs_u, 4),
        })
    try:
        conn.close()
    except Exception:
        pass
    if not rows:
        print(f"⚠️ [실전 DNA 병합] TML 역산 가능한 청산 승리 건 없음 (exit_date >= {cutoff})")
        return df
    winner_rows = pd.DataFrame(rows)
    print(f"📈 [실전 DNA 병합] forward_trades 양(+) 청산 {len(winner_rows)}건 추가 (exit_date >= {cutoff}, TML=OHLCV 역산)")
    return pd.concat([df, winner_rows], ignore_index=True)

def load_or_create_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# 👇👇 [추가] JSON 원자적 저장(Atomic Save) 엔진 👇👇
def save_config(config_data):
    """[V110.0] JSON 원자적 저장: 에러 시 데이터 증발 원천 차단"""
    temp_path = f"{CONFIG_PATH}.temp"
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CONFIG_PATH) 
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        print(f"⚠️ JSON 관제탑 원자적 저장 실패: {e}")
# 👆👆 [원자적 저장 완료] 👆👆

def run_cluster_mining():
    print("🚀 [V112.0 초신성 CSV 순도 100% 정밀 마이닝 및 클러스터링 가동]")
    
    try:
        df = pd.read_csv(CSV_PATH)
    except FileNotFoundError:
        print(f"🚨 에러: '{CSV_PATH}' 파일을 찾을 수 없습니다.")
        return

    # 💡 [핵심] 오직 'D-Day(하루 전날)'의 팩트 수치만 분석에 사용합니다.
    target_features = [
        '[D_Day_당일] 평균_CPV', 
        '[D_Day_당일] 평균_진짜양봉(TB)', 
        '[D_Day_당일] 평균_응축에너지(BBE)', 
        '[D_Day_당일] 진모멘텀(TML)',
        '[D_Day_당일] 평균_시장강도(RS)'
    ]

    # // [수정 후] K-Means 학습 행에 forward_trades(최근 1개월·final_ret>0) DNA concat
    df = _merge_forward_winner_dna_into_df(df, target_features)

    # 1차: 누락된 데이터가 있는 행은 안전하게 제거
    clean_df = df.dropna(subset=target_features).copy()
    
    # 👇👇 [치명적 버그 픽스] K-Means 가동 전 '승리자(Winner) 데이터 정제' 파이프라인 추가 👇👇
    initial_count = len(clean_df)
    
    # 만약 기존 CSV에 '최대수익률(MFE)' 같은 컬럼이 존재한다면 최우선으로 수익 필터링
    if '최대수익률(MFE)' in clean_df.columns:
        clean_df = clean_df[clean_df['최대수익률(MFE)'] >= 5.0]
    # '결과' 컬럼이 있다면 승리(Win)만 필터링
    elif '결과' in clean_df.columns:
        clean_df = clean_df[clean_df['결과'].str.contains('Win|성공', case=False, na=False)]
    else:
        # CSV 구조상 결과 컬럼이 없을 경우, 논리적으로 절대 폭발할 수 없는 최악의 쓰레기 데이터만 강제 커트
        # 예: 윗꼬리가 너무 심하거나(CPV < 0.2), 응축 에너지(BBE)가 바닥인 종목들
        clean_df = clean_df[(clean_df['[D_Day_당일] 평균_CPV'] >= 0.3) & (clean_df['[D_Day_당일] 평균_응축에너지(BBE)'] >= 3.0)]

    filtered_count = len(clean_df)
    print(f"🔬 [데이터 정제 완료] 초기 표본 {initial_count}개 ➔ 자해 로직(쓰레기 데이터) {initial_count - filtered_count}개 필터링 ➔ 찐 대박주 {filtered_count}개 압축 완료.")
    # 👆👆 [승리자 필터링 완료] 👆👆

    if len(clean_df) < 10:
        print("⚠️ 찐 대박주 데이터가 너무 적습니다 (최소 10개 필요). 오버피팅(과최적화) 방지를 위해 이번 마이닝을 중단합니다.")
        return

    print(f"✅ 총 {len(clean_df)}개의 완벽한 폭발 전야(D-Day) 표본을 확보했습니다.")

    mine_cfg = load_or_create_config()
    evolved_factors = mine_cfg.get("EVOLVED_ALPHA_FACTORS")
    cluster_alpha_meta = []
    cluster_features = list(target_features)
    if isinstance(evolved_factors, dict) and evolved_factors:
        tmp_alpha_df, cluster_alpha_meta = _append_evolved_alpha_columns(clean_df, evolved_factors)
        extra_cols = [c for _, c in cluster_alpha_meta]
        if extra_cols:
            need_cols = target_features + extra_cols
            tmp_ok = tmp_alpha_df.dropna(subset=need_cols)
            if len(tmp_ok) >= 10:
                clean_df = tmp_ok.copy()
                cluster_features = need_cols
                print(f"📐 EVOLVED 알파 {len(extra_cols)}차원 병합 — K-Means 입력 {len(cluster_features)}D")
            else:
                print(f"⚠️ EVOLVED 알파 병합 후 표본 {len(tmp_ok)}개(<10) — 5차원 클러스터만 수행")

    # =====================================================================
    # 2. 다차원 데이터 클러스터링 (K-Means 3분할)
    # =====================================================================
    # AI가 데이터를 공평하게 판단할 수 있도록 스케일링(정규화) 진행
    X = clean_df[cluster_features].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 3개의 성격이 다른 템플릿(클러스터)으로 쪼개기
    num_clusters = 3
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
    clean_df['Cluster'] = kmeans.fit_predict(X_scaled)
    
    # =====================================================================
    # 3. 클러스터별 Min-Max Bounding Box 추출
    # =====================================================================
    mined_templates = {}
    
    for i in range(num_clusters):
        cluster_data = clean_df[clean_df['Cluster'] == i]
        
        # 각 수치별 하위 10% ~ 상위 90% 범위를 추출 (극단적 노이즈를 살짝 깎아내어 범위의 신뢰도를 높임)
        # 대표님의 지시대로 '단일 값'이 아닌 'Min~Max 범위'로 저장합니다.
        template_range = {
            'cpv_min': round(cluster_data['[D_Day_당일] 평균_CPV'].quantile(0.10), 2),
            'cpv_max': round(cluster_data['[D_Day_당일] 평균_CPV'].quantile(0.90), 2),
            
            'tb_min': round(cluster_data['[D_Day_당일] 평균_진짜양봉(TB)'].quantile(0.10), 1),
            'tb_max': round(cluster_data['[D_Day_당일] 평균_진짜양봉(TB)'].quantile(0.90), 1),
            
            'bbe_min': round(cluster_data['[D_Day_당일] 평균_응축에너지(BBE)'].quantile(0.10), 1),
            'bbe_max': round(cluster_data['[D_Day_당일] 평균_응축에너지(BBE)'].quantile(0.90), 1),
            
            'tml_min': round(cluster_data['[D_Day_당일] 진모멘텀(TML)'].quantile(0.10), 1),
            'tml_max': round(cluster_data['[D_Day_당일] 진모멘텀(TML)'].quantile(0.90), 1),
            
            'rs_min': round(cluster_data['[D_Day_당일] 평균_시장강도(RS)'].quantile(0.10), 1),
            'rs_max': round(cluster_data['[D_Day_당일] 평균_시장강도(RS)'].quantile(0.90), 1)
        }
        for slot_key, col_name in cluster_alpha_meta:
            if col_name in cluster_data.columns:
                template_range[f'alpha_{slot_key}_min'] = round(cluster_data[col_name].quantile(0.10), 4)
                template_range[f'alpha_{slot_key}_max'] = round(cluster_data[col_name].quantile(0.90), 4)
        
        # 클러스터의 특징(성격)을 자동으로 네이밍하기 위한 팩트 체크
        avg_bbe = cluster_data['[D_Day_당일] 평균_응축에너지(BBE)'].mean()
        avg_cpv = cluster_data['[D_Day_당일] 평균_CPV'].mean()
        
        if avg_bbe >= clean_df['[D_Day_당일] 평균_응축에너지(BBE)'].median():
            nature = "강응축_폭발형"
        elif avg_cpv > 0.7:
            nature = "매집봉_스텔스형"
        else:
            nature = "혼조세_돌연변이형"
            
        template_name = f"CLUSTER_{i+1}_{nature}"
        mined_templates[template_name] = template_range
        
        print(f"\n🧬 <b>[{template_name}]</b> (종목 수: {len(cluster_data)}개)")
        print(f" ↳ CPV 범위: {template_range['cpv_min']} ~ {template_range['cpv_max']}")
        print(f" ↳ BBE 범위: {template_range['bbe_min']} ~ {template_range['bbe_max']}")

    # 👇👇 [신규 추가] STANDARD 오리지널 CSV 듀얼 트랙 클러스터링 👇👇
    STD_CSV_PATH = 'Standard_Flow_Master.csv'
    std_templates = {}
    try:
        std_df = pd.read_csv(STD_CSV_PATH)
        std_clean_df = std_df.dropna(subset=target_features).copy()
        std_cluster_alpha_meta = []
        std_cluster_features = list(target_features)
        if (
            isinstance(evolved_factors, dict)
            and evolved_factors
            and '종목코드' in std_clean_df.columns
            and '시장' in std_clean_df.columns
        ):
            s_tmp, std_cluster_alpha_meta = _append_evolved_alpha_columns(std_clean_df, evolved_factors)
            s_extra = [c for _, c in std_cluster_alpha_meta]
            if s_extra:
                s_need = target_features + s_extra
                s_ok = s_tmp.dropna(subset=s_need)
                if len(s_ok) >= 10:
                    std_clean_df = s_ok.copy()
                    std_cluster_features = s_need

        if len(std_clean_df) >= 10:
            print(f"✅ STANDARD 듀얼트랙용 표본 {len(std_clean_df)}개 확보. K-Means 마이닝 시작...")
            X_std = std_clean_df[std_cluster_features].values
            scaler_std = StandardScaler()
            X_std_scaled = scaler_std.fit_transform(X_std)
            
            kmeans_std = KMeans(n_clusters=3, random_state=42, n_init=10)
            std_clean_df['Cluster'] = kmeans_std.fit_predict(X_std_scaled)
            
            for i in range(3):
                cluster_data = std_clean_df[std_clean_df['Cluster'] == i]
                if len(cluster_data) == 0: continue
                
                template_range = {
                    'cpv_min': round(cluster_data['[D_Day_당일] 평균_CPV'].quantile(0.10), 2),
                    'cpv_max': round(cluster_data['[D_Day_당일] 평균_CPV'].quantile(0.90), 2),
                    'tb_min': round(cluster_data['[D_Day_당일] 평균_진짜양봉(TB)'].quantile(0.10), 1),
                    'tb_max': round(cluster_data['[D_Day_당일] 평균_진짜양봉(TB)'].quantile(0.90), 1),
                    'bbe_min': round(cluster_data['[D_Day_당일] 평균_응축에너지(BBE)'].quantile(0.10), 1),
                    'bbe_max': round(cluster_data['[D_Day_당일] 평균_응축에너지(BBE)'].quantile(0.90), 1)
                }
                for slot_key, col_name in std_cluster_alpha_meta:
                    if col_name in cluster_data.columns:
                        template_range[f'alpha_{slot_key}_min'] = round(cluster_data[col_name].quantile(0.10), 4)
                        template_range[f'alpha_{slot_key}_max'] = round(cluster_data[col_name].quantile(0.90), 4)
                
                std_templates[f"STD_CLUSTER_{i+1}"] = template_range
    except FileNotFoundError:
        print("⚠️ STANDARD CSV 파일이 아직 생성되지 않았습니다. 대기합니다.")
    except Exception as e:
        print(f"⚠️ STANDARD 마이닝 에러: {e}")
    # 👆👆 [신규 추가 끝] 👆👆

    # =====================================================================
    # 4. JSON 관제탑에 마이닝된 템플릿 업데이트
    # =====================================================================
    config = load_or_create_config()
    config['LIVE_CLUSTER_TEMPLATES'] = mined_templates
    
    # 👇👇 [치명적 버그 픽스] STANDARD 오리지널 마이닝 템플릿 JSON 누락 복구 👇👇
    config['LIVE_STANDARD_CLUSTER_TEMPLATES'] = std_templates
    # 👆👆 [복구 완료] 👆👆
    
    save_config(config)
    print("\n✅ 마이닝 완료! 초신성 및 STANDARD 듀얼 트랙 클러스터 템플릿(Min-Max)이 관제탑 JSON에 성공적으로 저장되었습니다.")

if __name__ == "__main__":
    run_cluster_mining()
