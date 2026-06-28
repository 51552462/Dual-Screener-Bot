"""
Clustered Immune Vaccine — 용량 최적화형 자가 면역 시스템 (Mission 6).

문제: 실전에서 도태된 실패 템플릿(찌꺼기)을 전부 ANTI_PATTERNS 에 쌓으면 용량 초과·탐색 병목.

해법(O(K) 초경량 면역):
  1) 실패 템플릿(승률<40% 등)을 toxic bbox 로 ANTI_PATTERNS 에 등록 — 단, 용량 상한(기본 500) 적용.
  2) 상한 초과 시 K-Means(폴백: Agglomerative)로 실패 벡터를 유사도 군집화.
  3) 각 군집의 '핵심 중심점(Centroid)'만 압축 백신(spanning bbox)으로 남기고,
     개별 원본 찌꺼기는 영구 삭제(Pruning). 백신은 여전히 실제 차단 bbox 로 기능한다.

ANTI_PATTERNS 는 dict / list 둘 다 지원하며 원래 타입을 보존한다.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

DEFAULT_CAPACITY_CAP = int(os.environ.get("ANTI_PATTERNS_CAPACITY_CAP", "500") or "500")
DEFAULT_TARGET_CENTROIDS = int(os.environ.get("ANTI_PATTERNS_TARGET_CENTROIDS", "64") or "64")

_VEC_FIELDS = (
    ("dyn_cpv_min", "dyn_cpv_max"),
    ("dyn_tb_min", "dyn_tb_max"),
    ("v_energy_min", "v_energy_max"),
)


# ---------------------------------------------------------------------------
# ANTI_PATTERNS 정규화 (dict/list 양립)
# ---------------------------------------------------------------------------
def _as_items(ap: Any) -> Tuple[List[Tuple[str, Dict[str, Any]]], str]:
    """ANTI_PATTERNS → [(key, entry_dict)], 원본 컨테이너 타입('dict'|'list') 반환."""
    if isinstance(ap, dict):
        return [(str(k), v) for k, v in ap.items() if isinstance(v, dict)], "dict"
    if isinstance(ap, list):
        return [(f"_idx_{i}", v) for i, v in enumerate(ap) if isinstance(v, dict)], "list"
    return [], "dict"


def _rebuild(container_type: str, entries: List[Tuple[str, Dict[str, Any]]]) -> Any:
    if container_type == "list":
        return [v for _k, v in entries]
    return {k: v for k, v in entries}


def _vectorize(entry: Dict[str, Any]) -> Optional[np.ndarray]:
    """toxic bbox → 중심좌표 벡터 [cpv, tb, bbe]. 필수 필드 없으면 None."""
    vec: List[float] = []
    for lo_k, hi_k in _VEC_FIELDS:
        if lo_k not in entry or hi_k not in entry:
            return None
        try:
            lo = float(entry[lo_k])
            hi = float(entry[hi_k])
        except (TypeError, ValueError):
            return None
        vec.append((lo + hi) / 2.0)
    return np.array(vec, dtype=np.float64)


def _span_bbox(members: List[Dict[str, Any]]) -> Dict[str, Any]:
    """군집 멤버들을 모두 덮는 spanning bbox(중심점 백신). 차단 규약 그대로 유지."""
    out: Dict[str, Any] = {
        "source": "CLUSTERED_VACCINE",
        "label": f"VACCINE_x{len(members)}",
        "market": str((members[0].get("market") if members else "GLOBAL") or "GLOBAL"),
        "members": len(members),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    for lo_k, hi_k in _VEC_FIELDS:
        los = [float(m[lo_k]) for m in members if lo_k in m]
        his = [float(m[hi_k]) for m in members if hi_k in m]
        if los and his:
            out[lo_k] = round(min(los), 6)
            out[hi_k] = round(max(his), 6)
    return out


def _kmeans_labels(X: np.ndarray, k: int) -> np.ndarray:
    """sklearn KMeans (실패 시 Agglomerative, 그것도 실패하면 numpy 양자화)."""
    k = max(1, min(int(k), X.shape[0]))
    # 표준화(스케일 차이 큰 cpv/tb/bbe 보정)
    mu = X.mean(axis=0)
    sd = X.std(axis=0)
    sd[sd < 1e-9] = 1.0
    Xs = (X - mu) / sd
    try:
        from sklearn.cluster import KMeans

        return KMeans(n_clusters=k, n_init=4, random_state=42).fit_predict(Xs)
    except Exception:
        pass
    try:
        from sklearn.cluster import AgglomerativeClustering

        return AgglomerativeClustering(n_clusters=k).fit_predict(Xs)
    except Exception:
        pass
    # 폴백: 1차원 사영 후 분위수 양자화
    proj = Xs[:, 0]
    edges = np.quantile(proj, np.linspace(0, 1, k + 1)[1:-1]) if k > 1 else np.array([])
    return np.digitize(proj, edges)


# ---------------------------------------------------------------------------
# 핵심: 군집 압축 가지치기
# ---------------------------------------------------------------------------
def prune_anti_patterns(
    cfg: Dict[str, Any],
    *,
    capacity_cap: int = DEFAULT_CAPACITY_CAP,
    target_centroids: int = DEFAULT_TARGET_CENTROIDS,
) -> Dict[str, Any]:
    """
    ANTI_PATTERNS 가 capacity_cap 초과 시 군집화로 target_centroids 개 백신으로 압축.
    반환: {pruned, before, after, vaccines, report}
    """
    ap = cfg.get("ANTI_PATTERNS")
    items, ctype = _as_items(ap)
    before = len(items)
    if before <= int(capacity_cap):
        return {"pruned": False, "before": before, "after": before, "vaccines": 0,
                "report": f"용량 {before} ≤ 상한 {capacity_cap} — 가지치기 불필요"}

    vec_items: List[Tuple[str, Dict[str, Any]]] = []
    vecs: List[np.ndarray] = []
    other_items: List[Tuple[str, Dict[str, Any]]] = []
    for k, v in items:
        x = _vectorize(v)
        if x is None:
            other_items.append((k, v))
        else:
            vec_items.append((k, v))
            vecs.append(x)

    new_entries: List[Tuple[str, Dict[str, Any]]] = []
    vaccines = 0

    if vecs:
        X = np.vstack(vecs)
        k = max(1, min(int(target_centroids), X.shape[0]))
        labels = _kmeans_labels(X, k)
        for cl in sorted(set(int(l) for l in labels)):
            members = [vec_items[i][1] for i in range(len(vec_items)) if int(labels[i]) == cl]
            if not members:
                continue
            new_entries.append((f"VACCINE_{cl:03d}", _span_bbox(members)))
            vaccines += 1

    # 비-벡터 엔트리(만료 예정 찌꺼기)는 최신 것 위주로 남은 예산만큼만 보존
    budget = max(0, int(capacity_cap) - len(new_entries))
    if other_items:
        def _ts(kv: Tuple[str, Dict[str, Any]]) -> str:
            return str(kv[1].get("created_at") or "")
        other_sorted = sorted(other_items, key=_ts, reverse=True)
        new_entries.extend(other_sorted[:budget])

    if len(new_entries) > int(capacity_cap):
        new_entries = new_entries[: int(capacity_cap)]

    cfg["ANTI_PATTERNS"] = _rebuild(ctype, new_entries)
    after = len(new_entries)
    return {
        "pruned": True,
        "before": before,
        "after": after,
        "vaccines": vaccines,
        "report": (
            f"면역 압축: {before}개 → {after}개 (백신 {vaccines}, 원본 보존 {after - vaccines}) "
            f"· O(K) 초경량 유지"
        ),
    }


def register_failed_template(
    cfg: Dict[str, Any],
    *,
    name: str,
    dna: Any,
    market: str = "GLOBAL",
    win_rate: Optional[float] = None,
    capacity_cap: int = DEFAULT_CAPACITY_CAP,
    target_centroids: int = DEFAULT_TARGET_CENTROIDS,
) -> Dict[str, Any]:
    """
    도태된 실패 템플릿(DNA=[cpv,tb,bbe])을 toxic bbox 로 ANTI_PATTERNS 에 등록 + 용량 상한 집행.
    """
    from regime_analog_engine import build_anti_pattern_bbox

    bbox = build_anti_pattern_bbox(dna, label=name, market=market, source="DEEP_EVOLVED_FAIL")
    if not bbox:
        return {"registered": False, "reason": "invalid_dna"}
    if win_rate is not None:
        bbox["fail_win_rate"] = round(float(win_rate), 4)

    ap = cfg.get("ANTI_PATTERNS")
    if isinstance(ap, list):
        ap.append(bbox)
    elif isinstance(ap, dict):
        ap[f"IMMUNE_{name}_{datetime.now().strftime('%y%m%d%H%M%S')}"] = bbox
    else:
        ap = {f"IMMUNE_{name}_{datetime.now().strftime('%y%m%d%H%M%S')}": bbox}
    cfg["ANTI_PATTERNS"] = ap

    prune = prune_anti_patterns(cfg, capacity_cap=capacity_cap, target_centroids=target_centroids)
    return {"registered": True, "prune": prune}


def run_clustered_immune_maintenance(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    persist: bool = True,
    capacity_cap: int = DEFAULT_CAPACITY_CAP,
    target_centroids: int = DEFAULT_TARGET_CENTROIDS,
) -> Dict[str, Any]:
    """
    [Priority 3 백그라운드] ANTI_PATTERNS 용량 점검 → 초과 시 군집 압축 → 영속화.
    오토파일럿 주말 배치(또는 수동)에서 호출.
    """
    loaded_here = False
    if cfg is None:
        from config_manager import load_system_config

        cfg = dict(load_system_config())
        loaded_here = True

    result = prune_anti_patterns(cfg, capacity_cap=capacity_cap, target_centroids=target_centroids)

    if persist and result.get("pruned"):
        try:
            from config_manager import set_config_value

            set_config_value("ANTI_PATTERNS", cfg.get("ANTI_PATTERNS"))
        except Exception as e:
            result["persist_error"] = str(e)
    result["loaded_here"] = loaded_here
    return result


if __name__ == "__main__":
    # 자가 점검: 700개 가짜 실패 bbox 생성 → 상한 500 → 64 백신으로 압축되는지.
    import random

    rng = random.Random(7)
    fake = {}
    for i in range(700):
        cpv = rng.uniform(0.2, 0.9)
        tb = rng.uniform(5, 20)
        bbe = rng.uniform(8, 25)
        fake[f"IMMUNE_F{i}"] = {
            "source": "DEEP_EVOLVED_FAIL", "label": f"F{i}", "market": "KR",
            "dyn_cpv_min": cpv - 0.05, "dyn_cpv_max": cpv + 0.05,
            "dyn_tb_min": tb - 1, "dyn_tb_max": tb + 1,
            "v_energy_min": bbe - 1, "v_energy_max": bbe + 1,
            "created_at": f"2026-01-{(i % 28) + 1:02d}",
        }
    cfg = {"ANTI_PATTERNS": fake}
    res = run_clustered_immune_maintenance(cfg, persist=False)
    print(res["report"])
    print("after keys sample:", list(cfg["ANTI_PATTERNS"].keys())[:6])
