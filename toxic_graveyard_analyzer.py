import os
import json
import sqlite3
import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier, _tree
from datetime import datetime

DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')

def load_config():
    if not os.path.exists(CONFIG_PATH): return {}
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f: return json.load(f)

def save_config(config):
    temp_path = f"{CONFIG_PATH}.temp"
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CONFIG_PATH)
    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        print(f"⚠️ JSON 저장 에러: {e}")

def get_toxic_rules(tree, feature_names):
    """의사결정나무에서 '참사주(Toxic)' 클래스로 분류되는 잎 노드의 조건만 추출합니다."""
    tree_ = tree.tree_
    toxic_rules = {}
    rule_idx = 1
    
    def recurse(node, bounds):
        nonlocal rule_idx
        if tree_.feature[node] != _tree.TREE_UNDEFINED:
            name = feature_names[tree_.feature[node]]
            threshold = tree_.threshold[node]

            left_bounds = bounds.copy()
            left_bounds[f"{name}_max"] = min(left_bounds.get(f"{name}_max", 9999), threshold)
            recurse(tree_.children_left[node], left_bounds)

            right_bounds = bounds.copy()
            right_bounds[f"{name}_min"] = max(right_bounds.get(f"{name}_min", -9999), threshold)
            recurse(tree_.children_right[node], right_bounds)
        else:
            class_idx = np.argmax(tree_.value[node][0])
            if class_idx == 1 and tree_.n_node_samples[node] >= 5:
                purity = tree_.value[node][0][1] / np.sum(tree_.value[node][0])
                if purity >= 0.8:
                    rule_dict = {k: round(v, 3) for k, v in bounds.items()}
                    toxic_rules[f"TOXIC_PATTERN_{rule_idx}"] = rule_dict
                    rule_idx += 1

    recurse(0, {})
    return toxic_rules

def run_graveyard_autopsy():
    print("💀 [오답노트 블랙박스 부검소] 참사주 독성 패턴(Anti-Pattern) 머신러닝 추출 중...")

    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
        query = "SELECT dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret FROM forward_trades WHERE status LIKE 'CLOSED%'"
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"🚨 DB 로드 실패: {e}")
        return

    df = df.dropna()
    if len(df[df['final_ret'] <= -7.0]) < 10:
        print("⚠️ 아직 부검할 참사주(-7% 이하) 표본이 10개 미만입니다. 마이닝을 대기합니다.")
        return

    df['is_toxic'] = np.where(df['final_ret'] <= -7.0, 1, 0)

    features = ['dyn_cpv', 'dyn_tb', 'v_energy', 'dyn_rs']
    X = df[features]
    y = df['is_toxic']

    clf = DecisionTreeClassifier(max_depth=3, class_weight='balanced', random_state=42)
    clf.fit(X, y)

    toxic_patterns = get_toxic_rules(clf, features)

    if toxic_patterns:
        config = load_config()
        # 코사인 면역용 ANTI_PATTERNS(리스트)와 분리 — 스나이퍼는 TOXIC_ML_ANTIPATTERNS만 소모
        config['TOXIC_ML_ANTIPATTERNS'] = toxic_patterns
        save_config(config)
        print(f"✅ 부검 완료! {len(toxic_patterns)}개의 치명적 독성 방어막이 관제탑에 업데이트되었습니다.")
        for k, v in toxic_patterns.items():
            print(f" ↳ 💀 {k}: {v}")
    else:
        print("💡 현재 데이터에서는 뚜렷한 맹독성 다중 조건이 발견되지 않았습니다.")

if __name__ == "__main__":
    run_graveyard_autopsy()
