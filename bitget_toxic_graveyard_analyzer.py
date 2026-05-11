import json
import os
import random
import sqlite3
import time
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")


def load_config(max_retries=5):
    """
    [장갑차 로직] JSONDecodeError 및 파일 잠금(Lock) 방어막 적용
    """
    if not os.path.exists(CONFIG_PATH):
        return {}

    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
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
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
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
                    rule_dict = {k: round(v, 4) for k, v in bounds.items()}
                    toxic_rules[f"TOXIC_PATTERN_{rule_idx:02d}"] = rule_dict
                    rule_idx += 1

    recurse(0, {})
    return toxic_rules


def _mine_for_market(df: pd.DataFrame, market_type: str):
    if df.empty:
        return {}

    # 코인 변동성 감안: -8% 이하를 독성 라벨로 정의
    df["is_toxic"] = np.where(pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) <= -8.0, 1, 0)
    if int(df["is_toxic"].sum()) < 10:
        return {}

    features = ["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"]
    X = df[features]
    y = df["is_toxic"]
    clf = DecisionTreeClassifier(max_depth=3, class_weight="balanced", random_state=42)
    clf.fit(X, y)
    rules = get_toxic_rules(clf, features)

    created_at = datetime.now().strftime("%Y-%m-%d")
    out = {}
    for k, v in rules.items():
        row = dict(v)
        row["created_at"] = created_at
        row["market_type"] = str(market_type).lower()
        out[f"{market_type.upper()}_{k}"] = row
    return out


def run_graveyard_autopsy():
    print("💀 [Bitget 오답노트 블랙박스 부검소] 참사주 독성 패턴(Anti-Pattern) 머신러닝 추출 중...")

    config = load_config()
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
        query = """
            SELECT market_type, dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%'
        """
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"🚨 DB 로드 실패: {e}")
        save_config(config)
        return

    df = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs", "final_ret"])
    if df.empty:
        print("⚠️ 부검 가능한 청산 데이터가 없습니다.")
        save_config(config)
        return

    mined = {}
    for mt in ("spot", "futures"):
        sub = df[df["market_type"].astype(str).str.lower() == mt].copy()
        if sub.empty:
            continue
        mined.update(_mine_for_market(sub, mt))

    if mined:
        config["TOXIC_ML_ANTIPATTERNS"] = mined
        anti = config.get("ANTI_PATTERNS", {})
        if not isinstance(anti, dict):
            anti = {}
        anti.update(mined)
        config["ANTI_PATTERNS"] = anti
        save_config(config)
        print(f"✅ 부검 완료! {len(mined)}개의 Bitget 독성 방어막이 관제탑에 업데이트되었습니다.")
    else:
        print("💡 현재 데이터에서는 뚜렷한 맹독성 다중 조건이 발견되지 않았습니다.")
        save_config(config)


if __name__ == "__main__":
    run_graveyard_autopsy()
