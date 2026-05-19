import pandas as pd
import numpy as np
import ast
import json
import os
import time
import random
import sqlite3
from datetime import datetime, timedelta
from sklearn.mixture import GaussianMixture
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
    """수식 문자열을 안전한 네임스페이스에서 평가해 시계열을 반환. (AST 샌드박스 검증 추가)"""
    if df is None or getattr(df, 'empty', True):
        return None

    # 1. 샌드박스 검증: 허용된 변수/함수만 있는지, 트리가 너무 깊지 않은지 AST 사전 검사
    ALLOWED_NAMES = {'O', 'H', 'L', 'C', 'V', 'add', 'sub', 'mul', 'div', 'rolling_mean', 'rolling_std'}
    try:
        formula_str = str(formula).strip()
        tree = ast.parse(formula_str, mode='eval')
        node_count = 0
        for node in ast.walk(tree):
            node_count += 1
            if node_count > 150:  # 무한 루프나 비정상적으로 깊은 수식(메모리 폭발) 사전 차단
                return None
            if isinstance(node, ast.Name) and node.id not in ALLOWED_NAMES:
                return None
    except Exception:
        return None

    # 2. 기존 환경 변수 셋업 (원본 100% 유지)
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
        safe_b = b.replace(0, float('nan')) if hasattr(b, 'replace') else (float('nan') if b == 0 else b)
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

    # 3. 안전이 검증된 수식만 eval() 실행
    try:
        import numpy as np
        import pandas as pd
        result = eval(formula_str, {"__builtins__": {}}, env)
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

def load_config(max_retries=5):
    """
    [장갑차 로직] JSONDecodeError 및 파일 잠금(Lock) 방어막 적용
    """
    if not os.path.exists(CONFIG_PATH):
        return {}

    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] 관제탑 뇌(JSON) 읽기 최종 실패 (동시 쓰기 과부하): {e}")
                return {}
    return {}


def load_or_create_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    return load_config()


def save_config(config_data, max_retries=5):
    """
    [장갑차 로직] 임시 파일 원자적(Atomic) 덮어쓰기 및 권한 방어막 적용
    """
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] 관제탑 뇌(JSON) 쓰기 최종 실패: {e}")
        except Exception as e:
            print(f"⚠️ 설정 파일 원자적 저장 중 알 수 없는 에러: {e}")
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass
            return False
    return False

def run_cluster_mining():
    print("🚀 [V65.0 초신성 CSV 데이터 마이닝 및 클러스터링 가동]")
    
    try:
        df = pd.read_csv(CSV_PATH)
        df = df.drop_duplicates(subset=['종목코드', '[D_Day_당일] 진모멘텀(TML)'], keep='last')
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

    # 누락된 데이터가 있는 행은 안전하게 제거
    clean_df = df.dropna(subset=target_features).copy()
    
    if len(clean_df) < 10:
        print("⚠️ 데이터가 너무 적습니다 (최소 10개 필요). 마이닝을 중단합니다.")
        return

    print(f"✅ 총 {len(clean_df)}개의 완벽한 폭발 전야(D-Day) 표본을 확보했습니다.")

    mine_cfg = load_or_create_config()
    evolved_factors = mine_cfg.get("EVOLVED_ALPHA_FACTORS")
    cluster_alpha_meta = []
    cluster_features = list(target_features)
    if isinstance(evolved_factors, dict) and evolved_factors:
        # 💡 [시공간 오염 방지] 과거 데이터에 현재 알파 수식을 평가하는 로직 차단 (순수 5D 차원만 유지)
        pass

    # =====================================================================
    # 2. 다차원 데이터 클러스터링 (GMM 3혼합)
    # =====================================================================
    # AI가 데이터를 공평하게 판단할 수 있도록 스케일링(정규화) 진행
    X = clean_df[cluster_features].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 3개의 성격이 다른 템플릿(클러스터)으로 쪼개기
    num_clusters = 3
    # 💡 [V_NEXT 진화 로직 적용] K-Means 대신 GMM으로 군집 평균(μ)·공분산(Σ) 추정
    gmm = GaussianMixture(n_components=num_clusters, covariance_type='full', random_state=42)
    clean_df['Cluster'] = gmm.fit_predict(X_scaled)
    
    # =====================================================================
    # 3. 클러스터별 Min-Max Bounding Box 추출
    # =====================================================================
    mined_templates = {}
    z90 = 1.6448536269514722  # 양측 90% 신뢰구간의 z값
    
    for i in range(num_clusters):
        cluster_data = clean_df[clean_df['Cluster'] == i]
        if cluster_data.empty:
            continue

        # 💡 [V_NEXT 진화 로직 적용] GMM μ/Σ 기반 각 차원 90% 신뢰구간을 원스케일 Min-Max로 역변환
        try:
            mu_scaled = np.asarray(gmm.means_[i], dtype=float)
            cov_scaled = np.asarray(gmm.covariances_[i], dtype=float)
            diag_scaled = np.diag(cov_scaled) if cov_scaled.ndim == 2 else np.asarray(cov_scaled, dtype=float)
            diag_scaled = np.where(np.isfinite(diag_scaled), diag_scaled, 0.0)
            diag_scaled = np.clip(diag_scaled, 0.0, None)
            sigma_orig = np.sqrt(diag_scaled) * np.asarray(scaler.scale_, dtype=float)
            mu_orig = np.asarray(scaler.mean_, dtype=float) + (mu_scaled * np.asarray(scaler.scale_, dtype=float))
        except Exception:
            # 방어적 폴백: GMM 파라미터 해석 실패 시 기존 표본 분위값으로 우회
            mu_orig = cluster_data[cluster_features].mean().values
            sigma_orig = cluster_data[cluster_features].std(ddof=0).fillna(0.0).values
        
        ci_map = {}
        for idx, feat in enumerate(cluster_features):
            try:
                lo = float(mu_orig[idx] - z90 * sigma_orig[idx])
                hi = float(mu_orig[idx] + z90 * sigma_orig[idx])
            except Exception:
                lo = float(cluster_data[feat].quantile(0.10))
                hi = float(cluster_data[feat].quantile(0.90))
            if lo > hi:
                lo, hi = hi, lo
            ci_map[feat] = (lo, hi)

        template_range = {
            'cpv_min': round(ci_map['[D_Day_당일] 평균_CPV'][0], 2),
            'cpv_max': round(ci_map['[D_Day_당일] 평균_CPV'][1], 2),
            
            'tb_min': round(ci_map['[D_Day_당일] 평균_진짜양봉(TB)'][0], 1),
            'tb_max': round(ci_map['[D_Day_당일] 평균_진짜양봉(TB)'][1], 1),
            
            'bbe_min': round(ci_map['[D_Day_당일] 평균_응축에너지(BBE)'][0], 1),
            'bbe_max': round(ci_map['[D_Day_당일] 평균_응축에너지(BBE)'][1], 1),
            
            'tml_min': round(ci_map['[D_Day_당일] 진모멘텀(TML)'][0], 1),
            'tml_max': round(ci_map['[D_Day_당일] 진모멘텀(TML)'][1], 1),
            
            'rs_min': round(ci_map['[D_Day_당일] 평균_시장강도(RS)'][0], 1),
            'rs_max': round(ci_map['[D_Day_당일] 평균_시장강도(RS)'][1], 1)
        }
        for slot_key, col_name in cluster_alpha_meta:
            if col_name in ci_map:
                template_range[f'alpha_{slot_key}_min'] = round(ci_map[col_name][0], 4)
                template_range[f'alpha_{slot_key}_max'] = round(ci_map[col_name][1], 4)
        
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

    # =====================================================================
    # 4. JSON 관제탑에 마이닝된 템플릿 업데이트
    # =====================================================================
    # 💡 [핵심] 과거의 훌륭한 클러스터 결과를 매주 날려버리지 않고 누적(Append)하여 저장
    config = load_or_create_config()
    existing_ml_templates = config.get('LIVE_CLUSTER_TEMPLATES', {})
    if not isinstance(existing_ml_templates, dict): existing_ml_templates = {}
    
    # 새 템플릿에 주차(Week) 태그를 붙여서 병합
    week_tag = datetime.now().strftime('%y%m%d')
    for k, v in mined_templates.items():
        existing_ml_templates[f"{k}_{week_tag}"] = v
        
    # 최대 15개 유지 (오래된 것 삭제)
    if len(existing_ml_templates) > 15:
        sorted_keys = sorted(existing_ml_templates.keys())
        excess = len(existing_ml_templates) - 15
        for k in sorted_keys[:excess]:
            existing_ml_templates.pop(k, None)
            
    config['LIVE_CLUSTER_TEMPLATES'] = existing_ml_templates
    save_config(config)
    print("\n✅ 마이닝 완료! 3개의 클러스터 템플릿(Min-Max)이 관제탑 JSON에 저장되었습니다.")

if __name__ == "__main__":
    run_cluster_mining()
