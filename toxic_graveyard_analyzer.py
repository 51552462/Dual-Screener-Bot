import os
import json
import time
import random
import sqlite3
import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier, _tree
from datetime import datetime

DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')


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


def save_config(config, max_retries=5):
    """
    [장갑차 로직] 임시 파일 원자적(Atomic) 덮어쓰기 및 권한 방어막 적용
    """
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
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


def prune_old_anti_patterns(config, days_to_live=90):
    """
    생성된 지 N일이 지난 낡은 독성 패턴을 자동 삭제 (Analysis Paralysis 방지)
    """
    anti_patterns = config.get('ANTI_PATTERNS', {})
    if not isinstance(anti_patterns, dict) or not anti_patterns:
        config['ANTI_PATTERNS'] = {} if not isinstance(anti_patterns, dict) else anti_patterns
        return config

    now = datetime.now()
    keys_to_delete = []

    for rule_id, rule_data in list(anti_patterns.items()):
        if not isinstance(rule_data, dict):
            keys_to_delete.append(rule_id)
            continue
        created_at_str = rule_data.get('created_at')
        if created_at_str:
            try:
                created_at_date = datetime.strptime(created_at_str, "%Y-%m-%d")
                if (now - created_at_date).days > days_to_live:
                    keys_to_delete.append(rule_id)
            except (ValueError, TypeError):
                pass
        else:
            keys_to_delete.append(rule_id)

    for key in keys_to_delete:
        del anti_patterns[key]

    if keys_to_delete:
        print(f"🧹 [생태계 정화] {len(keys_to_delete)}개의 낡은 독성 방어막(90일 경과 또는 무기한)을 삭제했습니다.")

    config['ANTI_PATTERNS'] = anti_patterns
    return config

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

    config = load_config()
    config = prune_old_anti_patterns(config)

    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
        query = "SELECT dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret FROM forward_trades WHERE status LIKE 'CLOSED%'"
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"🚨 DB 로드 실패: {e}")
        save_config(config)
        return

    df = df.dropna()
    if len(df[df['final_ret'] <= -7.0]) < 10:
        print("⚠️ 아직 부검할 참사주(-7% 이하) 표본이 10개 미만입니다. 마이닝을 대기합니다.")
        save_config(config)
        return

    df['is_toxic'] = np.where(df['final_ret'] <= -7.0, 1, 0)

    features = ['dyn_cpv', 'dyn_tb', 'v_energy', 'dyn_rs']
    X = df[features]
    y = df['is_toxic']

    clf = DecisionTreeClassifier(max_depth=3, class_weight='balanced', random_state=42)
    clf.fit(X, y)

    toxic_patterns = get_toxic_rules(clf, features)

    if toxic_patterns:
        created_at = datetime.now().strftime("%Y-%m-%d")
        toxic_patterns_dated = {}
        for k, v in toxic_patterns.items():
            rule = dict(v)
            rule["created_at"] = created_at
            toxic_patterns_dated[k] = rule

        config['TOXIC_ML_ANTIPATTERNS'] = toxic_patterns_dated
        anti = config.get('ANTI_PATTERNS', {})
        if not isinstance(anti, dict):
            anti = {}
        anti.update(toxic_patterns_dated)
        config['ANTI_PATTERNS'] = anti

        save_config(config)
        print(f"✅ 부검 완료! {len(toxic_patterns_dated)}개의 치명적 독성 방어막이 관제탑에 업데이트되었습니다.")
        for k, v in toxic_patterns_dated.items():
            print(f" ↳ 💀 {k}: {v}")
    else:
        print("💡 현재 데이터에서는 뚜렷한 맹독성 다중 조건이 발견되지 않았습니다.")
        save_config(config)

if __name__ == "__main__":
    run_graveyard_autopsy()
