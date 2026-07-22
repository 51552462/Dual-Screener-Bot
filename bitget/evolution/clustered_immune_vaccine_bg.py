import numpy as np
from typing import Any, Dict, List, Tuple
from datetime import datetime

# 코인 스캐너의 속도를 지키기 위한 극단적 경량화 상한선
CAPACITY_CAP = 200
TARGET_CENTROIDS = 32

_VEC_FIELDS = [
    ("dyn_cpv_min", "dyn_cpv_max"),
    ("dyn_tb_min", "dyn_tb_max"),
    ("v_energy_min", "v_energy_max"),
    ("dyn_rs_min", "dyn_rs_max")  # [코인 특화] 상대강도(RS) 차원 추가
]

def _vectorize(entry: Dict[str, Any]) -> np.ndarray | None:
    """실패한 패턴의 Bounding Box 중심점을 4D 벡터로 추출합니다."""
    vec = []
    for lo_k, hi_k in _VEC_FIELDS:
        if lo_k not in entry or hi_k not in entry:
            # 4D 차원이 안 맞는 구형 데이터나 손상된 데이터는 배제
            return None
        try:
            val = (float(entry[lo_k]) + float(entry[hi_k])) / 2.0
            vec.append(val)
        except (TypeError, ValueError):
            return None
    return np.array(vec, dtype=np.float64)

def _span_bbox(members: List[Dict[str, Any]], cluster_id: int) -> Dict[str, Any]:
    """군집화된 찌꺼기들을 하나의 거대한 '방어 백신(Bounding Box)'으로 압축합니다."""
    out = {
        "source": "CLUSTERED_VACCINE_BG",
        "label": f"VACCINE_BG_x{len(members)}",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    for lo_k, hi_k in _VEC_FIELDS:
        los = [float(m[lo_k]) for m in members if lo_k in m]
        his = [float(m[hi_k]) for m in members if hi_k in m]
        if los and his:
            out[lo_k] = round(min(los), 4)
            out[hi_k] = round(max(his), 4)
    return out

def run_clustered_immune_maintenance(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    [아키텍트 수술] 코인 4D K-Means 백신 압축기
    ANTI_PATTERNS(독성 패턴)가 상한선을 넘으면, 머신러닝(K-Means)으로 유사한 실패 타점들을 묶어 
    단 32개의 거대한 방어 백신으로 압축합니다. 스캐너의 메모리(OOM)와 연산 속도를 완벽히 방어합니다.
    """
    ap = cfg.get("ANTI_PATTERNS", {})
    if not isinstance(ap, dict):
        ap = {}

    before_count = len(ap)
    if before_count <= CAPACITY_CAP:
        return {"report": f"용량 {before_count} ≤ 상한 {CAPACITY_CAP} (압축 불필요)"}

    vec_items: List[Tuple[str, Dict[str, Any]]] = []
    vecs: List[np.ndarray] = []
    other_items: List[Tuple[str, Dict[str, Any]]] = []

    for k, v in ap.items():
        if not isinstance(v, dict):
            continue
        x = _vectorize(v)
        if x is None:
            other_items.append((k, v))
        else:
            vec_items.append((k, v))
            vecs.append(x)

    new_ap = {}
    vaccines_created = 0

    if vecs:
        try:
            from sklearn.cluster import KMeans
            X = np.vstack(vecs)
            k_clusters = min(TARGET_CENTROIDS, X.shape[0])
            
            # 스케일링 후 K-Means 군집화
            mu, sd = X.mean(axis=0), X.std(axis=0)
            sd[sd < 1e-9] = 1.0
            X_scaled = (X - mu) / sd
            
            labels = KMeans(n_clusters=k_clusters, n_init=4, random_state=42).fit_predict(X_scaled)
            
            for cl in set(labels):
                members = [vec_items[i][1] for i in range(len(vec_items)) if labels[i] == cl]
                if members:
                    new_ap[f"VACCINE_BG_{cl:03d}"] = _span_bbox(members, cl)
                    vaccines_created += 1
        except Exception as e:
            return {"report": f"K-Means 압축 에러: {e}"}

    # 압축 불가능한 데이터(수동 블랙리스트 등)는 최신순으로 잘라서 보존
    budget = max(0, CAPACITY_CAP - vaccines_created)
    other_sorted = sorted(other_items, key=lambda kv: str(kv[1].get("created_at", "")), reverse=True)
    for k, v in other_sorted[:budget]:
        new_ap[k] = v

    cfg["ANTI_PATTERNS"] = new_ap
    
    return {
        "report": f"면역 압축: {before_count}개 → {len(new_ap)}개 (백신 {vaccines_created}개 생성)"
    }