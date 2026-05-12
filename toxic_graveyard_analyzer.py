import os
import json
import time
import random
import re
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
    cfg_dir = os.path.dirname(CONFIG_PATH)
    if cfg_dir:
        try:
            os.makedirs(cfg_dir, exist_ok=True)
        except OSError:
            pass
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


def _sector_bucket_for_tree(s) -> str:
    """`auto_forward_tester.try_add_virtual_position` 의 map_standard_sector 와 동일 버킷 (평가기와 정합)."""
    s_str = str(s).lower()
    if any(k in s_str for k in ["반도체", "it", "ai", "소프트웨어", "모바일", "테크", "데이터"]):
        return "반도체/IT"
    if any(k in s_str for k in ["바이오", "헬스", "의료", "제약"]):
        return "바이오/헬스케어"
    if any(k in s_str for k in ["배터리", "2차전지", "화학", "에너지", "정유"]):
        return "에너지/화학"
    if any(k in s_str for k in ["금융", "은행", "증권", "지주", "투자"]):
        return "금융/지주"
    if any(k in s_str for k in ["기계", "조선", "방산", "산업재", "로봇", "전력"]):
        return "산업재/기계"
    if any(k in s_str for k in ["소비", "유통", "식품", "화장품", "엔터", "미디어"]):
        return "소비재/엔터"
    return "기타/혼합"


def normalize_toxic_bounds(bounds: dict) -> dict:
    """
    원-핫 더미 경계(sector_*_min, weekday_*_min)를 JSON용 sector_match / weekday_match 로 치환.
    트리는 보통 임계 0.5 기준으로 이진 분할하므로 min>=0.499 이면 해당 더미 '참(1)' 구간.
    """
    sector_label = None
    weekday_val = None
    out: dict = {}

    for k, v in bounds.items():
        ms = re.match(r"^sector_(.+)_(min|max)$", str(k))
        if ms:
            label, mm = ms.group(1), ms.group(2)
            try:
                fv = float(v)
            except (TypeError, ValueError):
                fv = float("nan")
            if mm == "min" and fv >= 0.499:
                sector_label = label
            continue

        mw = re.match(r"^weekday_(\d+)_(min|max)$", str(k))
        if mw:
            dstr, mm = mw.group(1), mw.group(2)
            try:
                fv = float(v)
            except (TypeError, ValueError):
                fv = float("nan")
            if mm == "min" and fv >= 0.499:
                try:
                    weekday_val = int(dstr)
                except ValueError:
                    pass
            continue

        if isinstance(v, (int, float, np.floating, np.integer)):
            out[k] = round(float(v), 3)
        else:
            out[k] = v

    if sector_label is not None:
        out["sector_match"] = sector_label
    if weekday_val is not None:
        out["weekday_match"] = int(weekday_val)
    return out


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
                    rule_dict = normalize_toxic_bounds(bounds)
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
        query = (
            "SELECT entry_date, sector, dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret "
            "FROM forward_trades WHERE status LIKE 'CLOSED%'"
        )
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"🚨 DB 로드 실패: {e}")
        save_config(config)
        return

    df = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs", "final_ret"])
    df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
    df = df.dropna(subset=["entry_date"])
    df["weekday"] = df["entry_date"].dt.weekday.astype(int)
    df = df.loc[df["weekday"] < 5].copy()
    df["sector_bucket"] = df["sector"].map(lambda x: _sector_bucket_for_tree(x if pd.notna(x) else ""))

    if len(df[df['final_ret'] <= -7.0]) < 10:
        print("⚠️ 아직 부검할 참사주(-7% 이하) 표본이 10개 미만입니다. 마이닝을 대기합니다.")
        save_config(config)
        return

    df['is_toxic'] = np.where(df['final_ret'] <= -7.0, 1, 0)

    num_features = ['dyn_cpv', 'dyn_tb', 'v_energy', 'dyn_rs']
    X_num = df[num_features].astype(float)
    X_cat = pd.get_dummies(
        df[["sector_bucket", "weekday"]].astype({"sector_bucket": str, "weekday": int}),
        columns=["sector_bucket", "weekday"],
        prefix=["sector", "weekday"],
    ).astype(float)
    X = pd.concat([X_num, X_cat], axis=1)
    y = df['is_toxic']
    feature_names = list(X.columns)

    clf = DecisionTreeClassifier(max_depth=3, class_weight='balanced', random_state=42)
    clf.fit(X, y)

    toxic_patterns = get_toxic_rules(clf, feature_names)

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
