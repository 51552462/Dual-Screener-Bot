import json
import os
import random
import sqlite3
import time

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree

from bitget.config_hub import load_config, save_config
from bitget.infra.bounded_reads import forward_toxic_graveyard_closed_sql
from bitget.infra.clock import utc_date_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.shared_db_connector import get_connection

DB_PATH = market_data_db_path()
logger = get_logger("bitget.toxic_graveyard_analyzer")


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

    created_at = utc_date_str()
    out = {}
    for k, v in rules.items():
        row = dict(v)
        row["created_at"] = created_at
        row["market_type"] = str(market_type).lower()
        out[f"{market_type.upper()}_{k}"] = row
    return out


def run_graveyard_autopsy():
    logger.info("[toxic graveyard] mining anti-pattern decision tree from disasters")

    config = load_config()
    try:
        conn = get_connection(DB_PATH, read_only=True, check_same_thread=False)
        q, params = forward_toxic_graveyard_closed_sql()
        df = pd.read_sql(q, conn, params=params)
        conn.close()
    except Exception as e:
        log_exception(logger, "toxic graveyard DB load failed: %s", e)
        save_config(config)
        return

    df = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs", "final_ret"])
    if df.empty:
        logger.warning("no closed trades available for graveyard autopsy")
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
        logger.info("graveyard autopsy complete: %s toxic shields updated", len(mined))
    else:
        logger.info("no clear multi-condition toxic patterns found")
        save_config(config)


if __name__ == "__main__":
    run_graveyard_autopsy()
