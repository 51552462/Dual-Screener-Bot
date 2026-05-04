import pandas as pd
import numpy as np
import json
import os
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# =====================================================================
# 1. 환경 설정 및 데이터 로드
# =====================================================================
CSV_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Supernova_Flow_Tracking_Master.csv')
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')

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

    # =====================================================================
    # 2. 다차원 데이터 클러스터링 (K-Means 3분할)
    # =====================================================================
    # AI가 데이터를 공평하게 판단할 수 있도록 스케일링(정규화) 진행
    X = clean_df[target_features].values
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
        
        if len(std_clean_df) >= 10:
            print(f"✅ STANDARD 듀얼트랙용 표본 {len(std_clean_df)}개 확보. K-Means 마이닝 시작...")
            X_std = std_clean_df[target_features].values
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
