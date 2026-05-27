from __future__ import annotations

import os
import json
import time
import random
import re
import sqlite3
import uuid
import pandas as pd
import numpy as np
from sklearn.inspection import permutation_importance
from sklearn.tree import DecisionTreeClassifier, _tree
from datetime import datetime, timedelta

try:
    from config_manager import CONFIG_PATH
except Exception:
    CONFIG_PATH = os.path.join(
        os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "system_config.json"
    )

try:
    from market_db_paths import market_db_read_path
except Exception:
    def market_db_read_path() -> str:
        return os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "market_data.sqlite")


def _forward_trades_db_uri_ro() -> str:
    """`auto_forward_tester.forward_trades` 와 동일 파일(스냅샷 우선) — 윈도우 경로 URI 안전."""
    p = os.path.abspath(market_db_read_path())
    return "file:" + p.replace("\\", "/") + "?mode=ro"

MIN_RULES_REQUIRED = 3
_STRICT_REL_FLOOR = 0.03
_STRICT_Z_MIN = 1.28


def _hybrid_toxic_labels(final_ret: pd.Series, exit_type: pd.Series) -> pd.Series:
    """
    독성 라벨: (1) final_ret <= -7% 또는
    (2) exit_type이 STAT_MAE / ZOMBIE_FORCE_CLOSE 이고 final_ret <= -4%.
    """
    fr = pd.to_numeric(final_ret, errors="coerce")
    et = exit_type.fillna("").astype(str).str.strip()
    cond1 = fr <= -7.0
    cond2 = et.isin(["STAT_MAE", "ZOMBIE_FORCE_CLOSE"]) & (fr <= -4.0)
    return pd.Series(np.where(cond1 | cond2, 1, 0), index=final_ret.index, dtype=int)


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
    cfg_dir = os.path.dirname(CONFIG_PATH)
    if cfg_dir:
        try:
            os.makedirs(cfg_dir, exist_ok=True)
        except OSError:
            pass
    for attempt in range(max_retries):
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
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
    anti_patterns = config.get("ANTI_PATTERNS", {})
    if not isinstance(anti_patterns, dict) or not anti_patterns:
        config["ANTI_PATTERNS"] = {} if not isinstance(anti_patterns, dict) else anti_patterns
        return config

    now = datetime.now()
    keys_to_delete = []

    for rule_id, rule_data in list(anti_patterns.items()):
        if not isinstance(rule_data, dict):
            keys_to_delete.append(rule_id)
            continue
        created_at_str = rule_data.get("created_at")
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

    config["ANTI_PATTERNS"] = anti_patterns
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


def _features_used_in_raw_bounds(bounds: dict) -> set[str]:
    """트리 경로 bounds 키(`dyn_cpv_min`, `sector_X_min` 등)에서 원-핫/수치 컬럼명 집합."""
    out: set[str] = set()
    for k in bounds:
        ks = str(k)
        if ks.endswith("_min") or ks.endswith("_max"):
            out.add(ks.rsplit("_", 1)[0])
    return out


def _rule_passes_permutation_importance(
    bounds: dict,
    feature_names: list[str],
    imp_mean: np.ndarray,
    imp_std: np.ndarray,
    rel_floor: float = 0.03,
    z_min: float = 1.28,
) -> bool:
    """
    잎 규칙에 등장하는 모든 분할 특성이, 참사주(클래스 1) 탐지에 대한
    순열 중요도(permutation importance, average_precision) 기준으로
    '사실상 0'이 아니어야 통과 (과적합·우연 분할 억제).
    """
    used = _features_used_in_raw_bounds(bounds)
    if not used:
        return False
    name_to_idx = {n: i for i, n in enumerate(feature_names)}
    mx = float(np.max(imp_mean)) if imp_mean.size else 0.0
    abs_floor = max(1e-10, rel_floor * (mx + 1e-12))

    for fname in used:
        j = name_to_idx.get(fname)
        if j is None:
            return False
        m = float(imp_mean[j])
        s = float(imp_std[j]) + 1e-12
        if m < abs_floor and (m / s) < z_min:
            return False
    return True


def _fit_pruned_decision_tree(X: pd.DataFrame, y: pd.Series, *, relaxed: bool = False) -> DecisionTreeClassifier:
    """
    Cost-complexity pruning + min_samples_leaf 로 복잡도 억제.
    ccp_alpha는 학습 데이터에서 pruning path로 값 1개 선택.
    relaxed=True 이면 더 작은 ccp_alpha(약한 가지치기).
    """
    base = DecisionTreeClassifier(
        max_depth=3,
        min_samples_leaf=5,
        min_samples_split=max(10, 2 * 5),
        class_weight="balanced",
        random_state=42,
        ccp_alpha=0.0,
    )
    path = base.cost_complexity_pruning_path(X, y)
    ccp_alphas = np.asarray(path.ccp_alphas, dtype=float)
    if ccp_alphas.size and np.nanmax(ccp_alphas) > 0:
        if relaxed:
            idx = int(max(0, min(len(ccp_alphas) // 8, len(ccp_alphas) - 1)))
        else:
            idx = int(max(0, min(len(ccp_alphas) // 4, len(ccp_alphas) - 1)))
        alpha_pick = float(ccp_alphas[idx])
    else:
        alpha_pick = 0.0

    clf = DecisionTreeClassifier(
        max_depth=3,
        min_samples_leaf=5,
        min_samples_split=max(10, 2 * 5),
        class_weight="balanced",
        random_state=42,
        ccp_alpha=alpha_pick,
    )
    clf.fit(X, y)
    return clf


def get_toxic_rules(
    tree: DecisionTreeClassifier,
    feature_names: list[str],
    imp_mean: np.ndarray,
    imp_std: np.ndarray,
    *,
    rel_floor: float = _STRICT_REL_FLOOR,
    z_min: float = _STRICT_Z_MIN,
) -> dict:
    """의사결정나무에서 '참사주(Toxic)' 클래스로 분류되는 잎 노드의 조건만 추출합니다."""
    tree_ = tree.tree_
    toxic_rules: dict = {}
    rule_idx = 1

    def recurse(node: int, bounds: dict) -> None:
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
            class_idx = int(np.argmax(tree_.value[node][0]))
            if class_idx == 1 and tree_.n_node_samples[node] >= 5:
                purity = tree_.value[node][0][1] / np.sum(tree_.value[node][0])
                if purity >= 0.8:
                    if not _rule_passes_permutation_importance(
                        bounds,
                        feature_names,
                        imp_mean,
                        imp_std,
                        rel_floor=rel_floor,
                        z_min=z_min,
                    ):
                        return
                    rule_dict = normalize_toxic_bounds(bounds)
                    toxic_rules[f"TOXIC_PATTERN_{rule_idx}"] = rule_dict
                    rule_idx += 1

    recurse(0, {})
    return toxic_rules


def _permutation_vectors(
    clf: DecisionTreeClassifier, X: pd.DataFrame, y: pd.Series, n_rep: int
) -> tuple[np.ndarray, np.ndarray]:
    try:
        perm = permutation_importance(
            clf,
            X,
            y,
            n_repeats=n_rep,
            random_state=42,
            scoring="average_precision",
            n_jobs=1,
        )
        return np.asarray(perm.importances_mean, dtype=float), np.asarray(
            perm.importances_std, dtype=float
        )
    except Exception as e:
        print(f"⚠️ 순열 중요도 계산 실패(규칙 필터 생략): {e}")
        pm = np.maximum(clf.feature_importances_, 0.0)
        return pm, np.zeros_like(pm, dtype=float)


def run_graveyard_autopsy():
    print("💀 [오답노트 블랙박스 부검소] 참사주 독성 패턴(Anti-Pattern) 머신러닝 추출 중...")

    config = load_config()
    config = prune_old_anti_patterns(config)

    rolling_days = 90
    mw = config.get("META_GOVERNOR_WINDOWS") if isinstance(config.get("META_GOVERNOR_WINDOWS"), dict) else {}
    if mw.get("graveyard_rolling_days") is not None:
        try:
            rolling_days = int(mw["graveyard_rolling_days"])
        except (TypeError, ValueError):
            rolling_days = 90
    elif config.get("TOXIC_GRAVEYARD_ROLLING_DAYS") is not None:
        try:
            rolling_days = int(config["TOXIC_GRAVEYARD_ROLLING_DAYS"])
        except (TypeError, ValueError):
            rolling_days = 90
    rolling_days = int(os.environ.get("TOXIC_GRAVEYARD_ROLLING_DAYS", str(rolling_days)))
    try:
        import pytz

        d = datetime.now(pytz.timezone("Asia/Seoul")).date()
    except Exception:
        d = datetime.now().date()
    cutoff = (d - timedelta(days=max(30, rolling_days))).isoformat()
    db_used = market_db_read_path()

    try:
        conn = sqlite3.connect(_forward_trades_db_uri_ro(), uri=True, check_same_thread=False)
        query = (
            "SELECT entry_date, sector, dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret, exit_type, exit_date "
            "FROM forward_trades "
            "WHERE market = 'KR' AND status LIKE 'CLOSED%' "
            "AND date(IFNULL(NULLIF(trim(exit_date), ''), entry_date)) >= date(?)"
        )
        df = pd.read_sql(query, conn, params=[cutoff])
        conn.close()
    except Exception as e:
        print(f"🚨 DB 로드 실패: {e}")
        save_config(config)
        return

    print(f"📅 [Graveyard] db={db_used} rolling_since(KST base)={cutoff} closed_KR_rows={len(df)}")
    if df.empty:
        print(
            "⚠️ [Graveyard] rolling 윈도우 내 청산 표본이 0건입니다. "
            "forward_trades 경로·market/status·exit_date 포맷을 확인하세요."
        )
        save_config(config)
        return

    df = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs", "final_ret"])
    if "exit_type" not in df.columns:
        df["exit_type"] = ""
    df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
    df = df.dropna(subset=["entry_date"])
    df["weekday"] = df["entry_date"].dt.weekday.astype(int)
    df = df.loc[df["weekday"] < 5].copy()
    df["sector_bucket"] = df["sector"].map(lambda x: _sector_bucket_for_tree(x if pd.notna(x) else ""))

    df["is_toxic"] = _hybrid_toxic_labels(df["final_ret"], df["exit_type"])
    if int(df["is_toxic"].sum()) < 10:
        print("⚠️ 아직 부검할 독성 라벨(is_toxic=1) 표본이 10개 미만입니다. 마이닝을 대기합니다.")
        save_config(config)
        return

    num_features = ["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"]
    X_num = df[num_features].astype(float)
    X_cat = pd.get_dummies(
        df[["sector_bucket", "weekday"]].astype({"sector_bucket": str, "weekday": int}),
        columns=["sector_bucket", "weekday"],
        prefix=["sector", "weekday"],
    ).astype(float)
    X = pd.concat([X_num, X_cat], axis=1)
    y = df["is_toxic"]
    feature_names = list(X.columns)
    n_rep = int(min(32, max(8, len(y) // 4)))

    clf_s = _fit_pruned_decision_tree(X, y, relaxed=False)
    pm_s, ps_s = _permutation_vectors(clf_s, X, y, n_rep)
    toxic_patterns = get_toxic_rules(
        clf_s, feature_names, pm_s, ps_s, rel_floor=_STRICT_REL_FLOOR, z_min=_STRICT_Z_MIN
    )

    if len(toxic_patterns) < MIN_RULES_REQUIRED:
        print(
            f"[경고] 엄격한 필터 통과 규칙이 {len(toxic_patterns)}개로 부족하여, "
            "완화된(Relaxed) 기준으로 모델을 재학습합니다."
        )
        clf_r = _fit_pruned_decision_tree(X, y, relaxed=True)
        pm_r, ps_r = _permutation_vectors(clf_r, X, y, n_rep)
        toxic_patterns = get_toxic_rules(
            clf_r,
            feature_names,
            pm_r,
            ps_r,
            rel_floor=_STRICT_REL_FLOOR * 0.5,
            z_min=_STRICT_Z_MIN * 0.5,
        )

    if toxic_patterns:
        created_at = datetime.now().strftime("%Y-%m-%d")
        toxic_patterns_dated = {}
        for k, v in toxic_patterns.items():
            rule = dict(v)
            rule["created_at"] = created_at
            toxic_patterns_dated[k] = rule

        entry_min = df["entry_date"].min()
        entry_max = df["entry_date"].max()
        try:
            tw = f"{pd.Timestamp(entry_min).strftime('%Y-%m-%d')} to {pd.Timestamp(entry_max).strftime('%Y-%m-%d')}"
        except Exception:
            tw = "unknown to unknown"
        lineage = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "n_samples_used": int(len(df)),
            "training_window": tw,
            "version": str(uuid.uuid4()),
        }
        config["TOXIC_ML_ANTIPATTERNS"] = {
            "_metadata": lineage,
            "rules": toxic_patterns_dated,
        }
        anti = config.get("ANTI_PATTERNS", {})
        if not isinstance(anti, dict):
            anti = {}
        anti.update(toxic_patterns_dated)
        config["ANTI_PATTERNS"] = anti

        save_config(config)
        print(f"✅ 부검 완료! {len(toxic_patterns_dated)}개의 치명적 독성 방어막이 관제탑에 업데이트되었습니다.")
        for k, v in toxic_patterns_dated.items():
            print(f" ↳ 💀 {k}: {v}")
    else:
        print("💡 현재 데이터에서는 뚜렷한 맹독성 다중 조건이 발견되지 않았습니다.")
        save_config(config)


if __name__ == "__main__":
    run_graveyard_autopsy()
