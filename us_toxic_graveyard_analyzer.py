"""
US 전용 독성 패턴 ML (Project 2 / Black Hole 정합).

- 데이터: market_data.sqlite 의 forward_trades 중 market='US', CLOSED 만 (읽기 전용).
- 출력: 동일 디렉터리 us_toxic_ml_antipatterns.json 만 (KR system_config / toxic_graveyard_analyzer 미터치).
- 알고리즘: KR toxic_graveyard_analyzer 와 동등 — DecisionTree + ccp_alpha pruning + permutation importance 필터.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import uuid
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.tree import DecisionTreeClassifier, _tree

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "market_data.sqlite")
OUTPUT_JSON = os.path.join(_THIS_DIR, "us_toxic_ml_antipatterns.json")

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


def _lineage_metadata(
    n_samples: int,
    entry_min: Any = None,
    entry_max: Any = None,
) -> dict[str, Any]:
    if entry_min is not None and entry_max is not None:
        try:
            tw = (
                f"{pd.Timestamp(entry_min).strftime('%Y-%m-%d')} to "
                f"{pd.Timestamp(entry_max).strftime('%Y-%m-%d')}"
            )
        except Exception:
            tw = "unknown to unknown"
    else:
        tw = "N/A to N/A"
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "n_samples_used": int(n_samples),
        "training_window": tw,
        "version": str(uuid.uuid4()),
    }


def _atomic_write_json(path: str, obj: dict[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _us_sector_bucket_for_tree(s: object) -> str:
    """
    US 롱 장부 sector 문자열 → 트리용 버킷 (영문 키워드 중심).
    blackhole_hunter._us_sector_bucket_for_tree 와 동일 정의 유지 권장.
    """
    s_str = str(s).lower()
    if any(
        k in s_str
        for k in (
            "technology",
            "software",
            "semiconductor",
            "semi ",
            "it ",
            "internet",
            "computer",
            "tech",
            "saas",
            "cloud",
            "cyber",
        )
    ):
        return "US_Technology"
    if any(
        k in s_str
        for k in (
            "health",
            "biotech",
            "pharma",
            "medical",
            "drug",
            "life sci",
            "healthcare",
        )
    ):
        return "US_Healthcare"
    if any(
        k in s_str
        for k in (
            "financial",
            "bank",
            "insurance",
            "capital",
            "asset manag",
            "reit",
            "mortgage",
        )
    ):
        return "US_Financials"
    if any(
        k in s_str
        for k in (
            "energy",
            "oil",
            "gas",
            "petrol",
            "solar",
            "renewable",
            "coal",
        )
    ):
        return "US_Energy"
    if any(
        k in s_str
        for k in (
            "consumer",
            "retail",
            "restaurant",
            "apparel",
            "luxury",
            "food",
            "beverage",
            "household",
        )
    ):
        return "US_Consumer"
    if any(
        k in s_str
        for k in (
            "industrial",
            "machinery",
            "aerospace",
            "defense",
            "construction",
            "electrical",
            "transport",
        )
    ):
        return "US_Industrials"
    if any(
        k in s_str
        for k in (
            "communication",
            "telecom",
            "media",
            "entertainment",
        )
    ):
        return "US_Communication"
    if any(k in s_str for k in ("material", "chemical", "mining", "steel", "gold", "packaging")):
        return "US_Materials"
    if any(k in s_str for k in ("utility", "utilities", "electric", "water util")):
        return "US_Utilities"
    if any(k in s_str for k in ("real estate", "reit")):
        return "US_RealEstate"
    return "US_Other"


def normalize_toxic_bounds(bounds: dict) -> dict:
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


def get_us_toxic_rules(
    tree: DecisionTreeClassifier,
    feature_names: list[str],
    imp_mean: np.ndarray,
    imp_std: np.ndarray,
    *,
    rel_floor: float = _STRICT_REL_FLOOR,
    z_min: float = _STRICT_Z_MIN,
) -> dict[str, dict]:
    tree_ = tree.tree_
    toxic_rules: dict[str, dict] = {}
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
                    toxic_rules[f"US_TOXIC_PATTERN_{rule_idx}"] = rule_dict
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
        print(f"⚠️ 순열 중요도 실패(대체): {e}")
        pm = np.maximum(clf.feature_importances_, 0.0)
        return pm, np.zeros_like(pm, dtype=float)


def run_us_graveyard_autopsy() -> None:
    print("💀 [US 오답노트 부검소] US CLOSED forward_trades 독성 ML 추출 (KR 파이프라인 미사용)…")

    if not os.path.exists(DB_PATH):
        print(f"🚨 DB 없음: {DB_PATH}")
        _atomic_write_json(
            OUTPUT_JSON,
            {
                "_metadata": _lineage_metadata(0),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "market": "US",
                "patterns": {},
                "error": "database_missing",
            },
        )
        return

    try:
        with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False) as conn:
            df = pd.read_sql(
                """
                SELECT entry_date, sector, dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret, exit_type
                FROM forward_trades
                WHERE UPPER(TRIM(market)) = 'US' AND status LIKE 'CLOSED%'
                """,
                conn,
            )
    except Exception as e:
        print(f"🚨 US DB 로드 실패: {e}")
        _atomic_write_json(
            OUTPUT_JSON,
            {
                "_metadata": _lineage_metadata(0),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "market": "US",
                "patterns": {},
                "error": str(e),
            },
        )
        return

    df = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs", "final_ret"])
    if "exit_type" not in df.columns:
        df["exit_type"] = ""
    df["entry_date"] = pd.to_datetime(df["entry_date"], errors="coerce")
    df = df.dropna(subset=["entry_date"])

    s = pd.to_datetime(df["entry_date"], errors="coerce")
    try:
        if getattr(s.dtype, "tz", None) is None:
            ts_et = s.dt.tz_localize(
                "America/New_York", ambiguous="infer", nonexistent="shift_forward"
            )
        else:
            ts_et = s.dt.tz_convert("America/New_York")
        df["weekday"] = ts_et.dt.weekday.astype(int)
    except Exception:
        df["weekday"] = s.dt.weekday.astype(int)

    df = df.loc[df["weekday"] < 5].copy()
    df["sector_bucket"] = df["sector"].map(lambda x: _us_sector_bucket_for_tree(x if pd.notna(x) else ""))

    df["is_toxic"] = _hybrid_toxic_labels(df["final_ret"], df["exit_type"])
    if int(df["is_toxic"].sum()) < 10:
        print("⚠️ US 독성 라벨(is_toxic=1) 표본이 10개 미만 — JSON 만 갱신(빈 patterns).")
        emin, emax = df["entry_date"].min(), df["entry_date"].max()
        _atomic_write_json(
            OUTPUT_JSON,
            {
                "_metadata": _lineage_metadata(int(len(df)), emin, emax),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "market": "US",
                "n_closed_rows": int(len(df)),
                "patterns": {},
                "note": "insufficient_toxic_samples",
            },
        )
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
    toxic_patterns = get_us_toxic_rules(
        clf_s, feature_names, pm_s, ps_s, rel_floor=_STRICT_REL_FLOOR, z_min=_STRICT_Z_MIN
    )

    if len(toxic_patterns) < MIN_RULES_REQUIRED:
        print(
            f"[경고] 엄격한 필터 통과 규칙이 {len(toxic_patterns)}개로 부족하여, "
            "완화된(Relaxed) 기준으로 모델을 재학습합니다."
        )
        clf_r = _fit_pruned_decision_tree(X, y, relaxed=True)
        pm_r, ps_r = _permutation_vectors(clf_r, X, y, n_rep)
        toxic_patterns = get_us_toxic_rules(
            clf_r,
            feature_names,
            pm_r,
            ps_r,
            rel_floor=_STRICT_REL_FLOOR * 0.5,
            z_min=_STRICT_Z_MIN * 0.5,
        )
    created_at = datetime.now().strftime("%Y-%m-%d")
    dated: dict[str, dict] = {}
    for k, v in toxic_patterns.items():
        r = dict(v)
        r["created_at"] = created_at
        dated[k] = r

    emin, emax = df["entry_date"].min(), df["entry_date"].max()
    payload = {
        "_metadata": _lineage_metadata(int(len(df)), emin, emax),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market": "US",
        "n_closed_rows": int(len(df)),
        "n_toxic_label": int(y.sum()),
        "patterns": dated,
    }
    _atomic_write_json(OUTPUT_JSON, payload)

    if dated:
        print(f"✅ US 부검 완료 → {OUTPUT_JSON} ({len(dated)} 규칙)")
        for k, v in dated.items():
            print(f" ↳ 💀 {k}: {v}")
    else:
        print("💡 US 데이터에서 통과한 독성 규칙이 없습니다. 빈 patterns 로 저장했습니다.")


if __name__ == "__main__":
    run_us_graveyard_autopsy()
