import pandas as pd
import numpy as np
import json
import os
import time
import random
import sqlite3
from datetime import datetime
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from factory_data_paths import system_config_json_path
from market_db_paths import MARKET_DATA_DB_PATH

DB_PATH = MARKET_DATA_DB_PATH
CONFIG_PATH = system_config_json_path()

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

def run_underdog_mining():
    print("🧟 [언더독 마이닝 공장 가동] 0~60점대 반란주 데이터 스캔 중...")
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        # 💡 핵심: 총점 60점 이하 & 수익률 15% 이상 대박주만 추출
        query = """
            SELECT dyn_cpv, dyn_tb, v_energy, dyn_rs 
            FROM forward_trades 
            WHERE status LIKE 'CLOSED%' 
              AND total_score <= 60 
              AND final_ret >= 15.0
        """
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"🚨 DB 로드 에러: {e}")
        return

    # 결측치 제거 및 데이터 부족 방어
    df = df.dropna()
    if len(df) < 10:
        print(f"⚠️ 표본 부족 (현재 {len(df)}개). 마이닝을 위해 최소 10개의 언더독 대박주가 필요합니다.")
        return

    print(f"✅ 총 {len(df)}개의 언더독 대박주 표본을 확보했습니다. 클러스터링을 시작합니다.")

    features = ['dyn_cpv', 'dyn_tb', 'v_energy', 'dyn_rs']
    X = df[features].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    num_clusters = min(3, len(df) // 5) # 데이터가 적으면 클러스터 수를 줄임
    gmm = GaussianMixture(n_components=num_clusters, covariance_type='full', random_state=42)
    df['Cluster'] = gmm.fit_predict(X_scaled)
    
    mined_templates = {}
    z90 = 1.645 # 90% 신뢰구간
    
    for i in range(num_clusters):
        cluster_data = df[df['Cluster'] == i]
        if cluster_data.empty: continue

        # GMM 파라미터를 역스케일링하여 Min-Max Bounding Box 추출
        try:
            mu_scaled = np.asarray(gmm.means_[i], dtype=float)
            cov_scaled = np.asarray(gmm.covariances_[i], dtype=float)
            diag_scaled = np.diag(cov_scaled) if cov_scaled.ndim == 2 else np.asarray(cov_scaled, dtype=float)
            diag_scaled = np.clip(np.nan_to_num(diag_scaled), 0.0, None)
            sigma_orig = np.sqrt(diag_scaled) * np.asarray(scaler.scale_, dtype=float)
            mu_orig = np.asarray(scaler.mean_, dtype=float) + (mu_scaled * np.asarray(scaler.scale_, dtype=float))
        except:
            mu_orig = cluster_data[features].mean().values
            sigma_orig = cluster_data[features].std(ddof=0).fillna(0.0).values
            
        template_range = {}
        for idx, feat in enumerate(features):
            lo, hi = float(mu_orig[idx] - z90 * sigma_orig[idx]), float(mu_orig[idx] + z90 * sigma_orig[idx])
            if lo > hi: lo, hi = hi, lo
            template_range[f"{feat}_min"] = round(lo, 3)
            template_range[f"{feat}_max"] = round(hi, 3)
            
        avg_energy = cluster_data['v_energy'].mean()
        nature = "강응축반란형" if avg_energy > df['v_energy'].mean() else "수급폭발형"
        template_name = f"UD_CLUSTER_{i+1}_{nature}"
        mined_templates[template_name] = template_range
        
        print(f"\n🧬 <b>[{template_name}]</b> (종목 수: {len(cluster_data)}개)")
        print(f" ↳ 에너지 범위: {template_range['v_energy_min']} ~ {template_range['v_energy_max']}")

    # JSON 관제탑에 언더독 템플릿 독립 업데이트
    config = load_or_create_config()
    ud_templates = config.get('UNDERDOG_CLUSTER_TEMPLATES', {})
    if not isinstance(ud_templates, dict): ud_templates = {}
    
    week_tag = datetime.now().strftime('%y%m%d')
    for k, v in mined_templates.items():
        ud_templates[f"{k}_{week_tag}"] = v
        
    # 최대 10개 유지
    if len(ud_templates) > 10:
        sorted_keys = sorted(ud_templates.keys())
        excess = len(ud_templates) - 10
        for k in sorted_keys[:excess]:
            ud_templates.pop(k, None)
            
    config['UNDERDOG_CLUSTER_TEMPLATES'] = ud_templates
    save_config(config)
    print("\n✅ 언더독 마이닝 완료! 관제탑 JSON에 독립적으로 저장되었습니다.")

if __name__ == "__main__":
    run_underdog_mining()
