"""
Institutional Regime Analog Engine — 현재 국면이 과거 어떤 국면과 얼마나 닮았는가(정량).

설계 철학(헤지펀드급):
  - 단순 유클리드/코사인이 아니라 변수 간 공분산을 고려하는 **마할라노비스 거리**
    D_M(x) = sqrt((x-μ)^T Σ^{-1} (x-μ)) 와, 시간축 왜곡을 흡수하는 **DTW(Dynamic Time
    Warping)** 를 앙상블해 `REGIME_ANALOG_SCORE`(0~1, %) 를 산출한다.
  - '현재 국면 벡터'는 외부 지수(MA20 이격·VIX·5일폭) + 내부 마찰(PRI 복합 Z) +
    거시 자산(매크로 센티넬 복합 Z) 을 N차원으로 융합한다.
  - 공분산 Σ 는 최근 국면 벡터 롤링 버퍼(REGIME_VECTOR_HISTORY)로 추정하고, 표본이
    부족하면 단위행렬(=유클리드)로 안전 강등한다.

이 모듈은 **읽기/계산 + config KV 기록**만 한다. 매매 로직(forward/shared.py)·자가진화
(template_evolution.py)는 여기서 산출된 `REGIME_ANALOG_SCORE` / archetype DNA 만 소비한다.

주요 config 키:
  - REGIME_ANALOG_SCORE          : 산출 결과(현재 벡터·최적 매칭 국면·점수·게이트 정보)
  - REGIME_VECTOR_HISTORY        : 최근 국면 벡터 롤링 버퍼(공분산/DTW 시퀀스 추정용)
  - REGIME_ARCHETYPE_DNA         : 과거 국면별 '전설적 승자' DNA(타임머신 모핑 타깃)
  - REGIME_ANALOG_GATE_ENABLED   : 선취매 안전 게이트 on/off (기본 True)
  - REGIME_ANALOG_FRONTRUN_MIN_SCORE : 선취매 허용 최소 유사도 (기본 0.80)
  - REGIME_ANALOG_MORPH_MIN_SCORE    : 타임머신 모핑 발동 최소 유사도 (기본 0.85)
  - REGIME_ANALOG_GATE_FAIL_OPEN     : 산출치 부재(콜드스타트) 시 게이트 개방 여부 (기본 True)
"""
from __future__ import annotations

import math
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# 상수 / config 키
# ---------------------------------------------------------------------------
ANALOG_SCORE_KEY = "REGIME_ANALOG_SCORE"
VECTOR_HISTORY_KEY = "REGIME_VECTOR_HISTORY"
ARCHETYPE_DNA_KEY = "REGIME_ARCHETYPE_DNA"
GATE_ENABLED_KEY = "REGIME_ANALOG_GATE_ENABLED"
FRONTRUN_MIN_SCORE_KEY = "REGIME_ANALOG_FRONTRUN_MIN_SCORE"
MORPH_MIN_SCORE_KEY = "REGIME_ANALOG_MORPH_MIN_SCORE"
GATE_FAIL_OPEN_KEY = "REGIME_ANALOG_GATE_FAIL_OPEN"
EPISODE_PENALTY_KEY = "REGIME_EPISODE_QPENALTY"  # 면역 학습: 국면별 Q-페널티

DEFAULT_FRONTRUN_MIN_SCORE = 0.80
DEFAULT_MORPH_MIN_SCORE = 0.85
# RL 면역 페널티 기본 하이퍼파라미터
DEFAULT_RL_ALPHA = 0.5
DEFAULT_RL_GAMMA = 0.9
DEFAULT_RL_PENALTY = 1.0
MIN_EPISODE_WEIGHT = 0.05  # 가중치 하한(완전 0 매칭 방지)
HISTORY_CAP = 180  # 롤링 버퍼 최대 길이 (≈ 6개월 일별)
TRAJECTORY_LEN = 14  # DTW 시퀀스 길이(최근 N일 국면 인덱스)

# 앙상블 가중치 / 스케일 (거리 → 유사도 변환)
W_MAHALANOBIS = 0.6
W_DTW = 0.4
_MAHA_SCALE = 3.0  # exp(-D_M / scale)
_DTW_SCALE = 2.5   # exp(-dtw / scale)
_RIDGE_LAMBDA = 1e-3  # Σ 정규화(특이행렬 방지)

# 국면 벡터 차원 순서 (해석 가능성을 위해 고정)
VECTOR_DIMS: Tuple[str, ...] = (
    "spx_ma20_dist",    # (SPX - MA20)/MA20
    "kospi_ma20_dist",  # (KOSPI - MA20)/MA20
    "vix_z",            # (VIX - 20)/10
    "pri_z",            # 내부 마찰 PRI 복합 Z (blended)
    "macro_z",          # 거시 센티넬 복합 Z
    "range5d_norm",     # (avg range5d% - 3.5)/3.5
)
N_DIMS = len(VECTOR_DIMS)


# ---------------------------------------------------------------------------
# 과거 역사적 국면(에피소드) 아카이브
#   centroid: VECTOR_DIMS 동일 차원의 대표 벡터
#   trajectory: DTW 비교용 정규화된 국면 인덱스 형상(상대 모양만 의미)
#   front_run_favorable: 선취매가 통계적으로 유리했던 국면인가
#   hist_win_proxy: 해당 국면 선취매 승률 프록시(0~1, 메타 정보)
# ---------------------------------------------------------------------------
HISTORICAL_EPISODES: Dict[str, Dict[str, Any]] = {
    "EXTREME_CRASH": {
        "regime": "DOWN",
        "centroid": [-0.12, -0.12, 1.8, -1.5, -1.4, 1.5],
        "trajectory": [0.3, 0.1, -0.2, -0.5, -0.8, -1.1, -1.4, -1.6, -1.7, -1.8, -1.85, -1.9, -1.95, -2.0],
        "front_run_favorable": False,
        "hist_win_proxy": 0.28,
        "desc": "서브프라임/코로나/22금리/18무역분쟁형 폭락",
    },
    "V_RECOVERY": {
        "regime": "UP",
        "centroid": [0.02, 0.02, 0.8, 1.0, 1.1, 0.8],
        "trajectory": [-1.8, -1.9, -1.6, -1.0, -0.4, 0.2, 0.7, 1.1, 1.4, 1.6, 1.75, 1.85, 1.9, 2.0],
        "front_run_favorable": True,
        "hist_win_proxy": 0.71,
        "desc": "폭락 후 V자 반등 — 선취매 최적 구간",
    },
    "MASSIVE_BULL": {
        "regime": "UP",
        "centroid": [0.05, 0.05, -0.6, 1.2, 1.3, 0.1],
        "trajectory": [0.2, 0.4, 0.55, 0.7, 0.85, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8],
        "front_run_favorable": True,
        "hist_win_proxy": 0.66,
        "desc": "20~21 유동성 초강세 대형 상승",
    },
    "CHOPPY_STAGNANT": {
        "regime": "SIDEWAYS",
        "centroid": [0.0, 0.0, 0.0, 0.0, 0.0, -0.6],
        "trajectory": [0.2, -0.3, 0.4, -0.2, 0.3, -0.4, 0.1, -0.1, 0.3, -0.3, 0.2, -0.2, 0.1, 0.0],
        "front_run_favorable": False,
        "hist_win_proxy": 0.41,
        "desc": "23년 중반 횡보·침체(추세 부재)",
    },
}

# 과거 국면별 '전설적 승자' DNA 기본 시드 (cpv, tb, bbe)
#   build_regime_archetype_dna() 가 실측 백테스트 센트로이드로 덮어쓸 수 있다.
DEFAULT_ARCHETYPE_DNA: Dict[str, Dict[str, Dict[str, float]]] = {
    "V_RECOVERY": {
        "KR": {"cpv": 0.72, "tb": 13.5, "bbe": 31.0},
        "US": {"cpv": 0.68, "tb": 12.0, "bbe": 27.5},
    },
    "MASSIVE_BULL": {
        "KR": {"cpv": 0.78, "tb": 12.5, "bbe": 29.0},
        "US": {"cpv": 0.72, "tb": 11.0, "bbe": 26.0},
    },
    "CHOPPY_STAGNANT": {
        "KR": {"cpv": 0.62, "tb": 8.5, "bbe": 20.0},
        "US": {"cpv": 0.58, "tb": 7.8, "bbe": 16.0},
    },
    "EXTREME_CRASH": {
        "KR": {"cpv": 0.55, "tb": 7.0, "bbe": 14.0},
        "US": {"cpv": 0.52, "tb": 6.5, "bbe": 12.0},
    },
}


# ---------------------------------------------------------------------------
# config 헬퍼 (in-memory dict 우선 → config_manager 폴백)
# ---------------------------------------------------------------------------
def _load_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(cfg, dict):
        return cfg
    try:
        from config_manager import load_system_config

        return load_system_config() or {}
    except Exception:
        return {}


def _set_cfg_value(key: str, value: Any) -> bool:
    try:
        from config_manager import set_config_value

        set_config_value(key, value)
        return True
    except Exception:
        return False


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Mission 9: 콜드 스타트 방어 — 동적 룩백 윈도우(Elastic Lookback)
# ---------------------------------------------------------------------------
def clamp_lookback_window(
    db_path: Optional[str],
    table: str,
    *,
    date_col: str = "date",
    requested_days: int,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    DB 조회 전 반드시 가장 오래된 날짜(MIN date)를 먼저 확인하고, 요청 기간보다 실제 보유
    데이터가 적으면 에러 대신 윈도우를 '보유 최대 기간'으로 자동 축소(Clamp)한다.

    반환: {start, end, requested_start, clamped, has_data, available_days, db_min, db_max}
    어떤 경우에도 예외를 던지지 않는다(콜드스타트 안전장치).
    """
    now = now or datetime.now()
    req_start = now - timedelta(days=max(0, int(requested_days)))
    info: Dict[str, Any] = {
        "requested_start": req_start.strftime("%Y-%m-%d"),
        "end": now.strftime("%Y-%m-%d"),
        "start": req_start.strftime("%Y-%m-%d"),
        "clamped": False,
        "has_data": False,
        "available_days": 0,
        "db_min": None,
        "db_max": None,
    }
    if not db_path or not os.path.isfile(db_path):
        return info
    # 테이블/컬럼명은 화이트리스트 검증(인젝션 방지)
    if not _safe_ident(table) or not _safe_ident(date_col):
        return info
    try:
        uri = str(db_path).replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=15)
    except sqlite3.Error:
        return info
    try:
        row = conn.execute(
            f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table}"'
        ).fetchone()
    except sqlite3.Error:
        conn.close()
        return info
    conn.close()
    if not row or row[0] is None:
        return info
    db_min = str(row[0])[:10]
    db_max = str(row[1])[:10] if row[1] is not None else db_min
    info["has_data"] = True
    info["db_min"] = db_min
    info["db_max"] = db_max
    try:
        min_dt = datetime.strptime(db_min, "%Y-%m-%d")
        max_dt = datetime.strptime(db_max, "%Y-%m-%d")
        info["available_days"] = max(0, (max_dt - min_dt).days)
        if min_dt > req_start:
            info["start"] = db_min
            info["clamped"] = True
    except ValueError:
        pass
    return info


def _safe_ident(name: str) -> bool:
    s = str(name or "")
    return bool(s) and len(s) <= 128 and all(c.isalnum() or c in ("_",) for c in s)


# ---------------------------------------------------------------------------
# Mission 8: 강화학습 기반 면역 체계 (RL Immune System)
# ---------------------------------------------------------------------------
def _episode_penalty_table(cfg: Dict[str, Any]) -> Dict[str, float]:
    raw = cfg.get(EPISODE_PENALTY_KEY) if isinstance(cfg, dict) else None
    out: Dict[str, float] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                out[str(k)] = float(v)
            except (TypeError, ValueError):
                continue
    return out


def episode_match_weight(episode: str, penalties: Dict[str, float]) -> float:
    """국면별 Q-페널티(≤0)를 매칭 가중치(0~1)로 변환. Q=0 → 1.0, Q↓ → 0 으로 감쇠."""
    q = float(penalties.get(str(episode), 0.0)) if isinstance(penalties, dict) else 0.0
    if q >= 0.0:
        return 1.0
    return max(MIN_EPISODE_WEIGHT, math.exp(q))


def penalize_episode(
    cfg: Optional[Dict[str, Any]],
    episode: str,
    *,
    alpha: float = DEFAULT_RL_ALPHA,
    gamma: float = DEFAULT_RL_GAMMA,
    penalty: float = DEFAULT_RL_PENALTY,
    persist: bool = True,
) -> Dict[str, Any]:
    r"""
    실패한 국면의 매칭 가중치를 RL 페널티 수식으로 영구 삭감한다.
        Q_{t+1}(s,a) = Q_t(s,a) - α·(Penalty + γ·max_{a'} Q_t(s',a'))
    여기서 max_{a'} Q_t 는 전체 국면 Q 의 최댓값(보통 0)이므로, Penalty 만큼 음의 방향 누적.
    반환: {episode, q_before, q_after, weight}.
    """
    cfg = _load_cfg(cfg)
    table = _episode_penalty_table(cfg)
    q_before = float(table.get(str(episode), 0.0))
    max_q = max([0.0] + list(table.values()))  # 보통 0(상한)
    q_after = q_before - float(alpha) * (float(penalty) + float(gamma) * max_q)
    table[str(episode)] = round(q_after, 6)
    if isinstance(cfg, dict):
        cfg[EPISODE_PENALTY_KEY] = table
    if persist:
        _set_cfg_value(EPISODE_PENALTY_KEY, table)
    return {
        "episode": str(episode),
        "q_before": round(q_before, 6),
        "q_after": round(q_after, 6),
        "weight": round(episode_match_weight(episode, table), 4),
    }


def build_anti_pattern_bbox(
    dna: Any,
    label: str,
    *,
    market: str = "GLOBAL",
    tol: float = 0.08,
    source: str = "DEEP_EVOLVED_FAIL",
) -> Dict[str, Any]:
    """
    실패한 템플릿 DNA(cpv,tb,bbe) 주변에 toxic bbox(_min/_max) 규칙을 만든다.
    toxic_antipattern_core.evaluate_toxic_bbox_match 가 실제로 차단하도록 키 규약을 맞춘다.
    """
    try:
        cpv, tb, bbe = float(dna[0]), float(dna[1]), float(dna[2])
    except (TypeError, ValueError, IndexError, KeyError):
        return {}

    def _band(v: float) -> Tuple[float, float]:
        w = max(abs(v) * float(tol), 1e-6)
        return round(v - w, 6), round(v + w, 6)

    cpv_lo, cpv_hi = _band(cpv)
    tb_lo, tb_hi = _band(tb)
    bbe_lo, bbe_hi = _band(bbe)
    return {
        "source": source,
        "label": str(label),
        "market": str(market).upper(),
        "dyn_cpv_min": cpv_lo,
        "dyn_cpv_max": cpv_hi,
        "dyn_tb_min": tb_lo,
        "dyn_tb_max": tb_hi,
        "v_energy_min": bbe_lo,
        "v_energy_max": bbe_hi,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# Mission 1-a: 현재 국면 벡터 구성
# ---------------------------------------------------------------------------
def build_current_regime_vector(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    pri_blend_z: Optional[float] = None,
    macro_z: Optional[float] = None,
) -> Dict[str, Any]:
    """
    외부 지수(REGIME_ANALYSIS) + 내부 PRI(blended composite_z) + 거시 센티넬을
    하나의 N차원 '현재 국면 벡터'로 융합한다. 네트워크 호출 없음(저장된 값만 사용).
    """
    cfg = _load_cfg(cfg)
    ra = cfg.get("REGIME_ANALYSIS") if isinstance(cfg.get("REGIME_ANALYSIS"), dict) else {}
    indices = ra.get("indices", {}) if isinstance(ra, dict) else {}

    def _ma20_dist(idx_key: str) -> float:
        snap = indices.get(idx_key, {}) if isinstance(indices, dict) else {}
        if not isinstance(snap, dict) or not snap.get("ok"):
            return 0.0
        close = _safe_float(snap.get("close"))
        ma20 = _safe_float(snap.get("ma20"))
        if ma20 <= 0:
            return 0.0
        return (close - ma20) / ma20

    def _range5d(idx_key: str) -> Optional[float]:
        snap = indices.get(idx_key, {}) if isinstance(indices, dict) else {}
        if not isinstance(snap, dict) or not snap.get("ok"):
            return None
        return _safe_float(snap.get("range5d_pct"))

    spx_dist = _ma20_dist("GSPC")
    kospi_dist = _ma20_dist("KOSPI")

    vix_close = ra.get("vix_close")
    vix_z = (_safe_float(vix_close, 20.0) - 20.0) / 10.0 if vix_close is not None else 0.0

    # 내부 PRI blended composite_z
    if pri_blend_z is None:
        pri_blend_z = _resolve_pri_blend_z()
    pri_z = _safe_float(pri_blend_z)

    # 거시 센티넬 composite_z (저장본 우선; 인자 우선)
    if macro_z is None:
        macro_z = _resolve_macro_z(cfg)
    macro_zf = _safe_float(macro_z)

    ranges = [r for r in (_range5d("GSPC"), _range5d("KOSPI")) if r is not None]
    avg_range = float(np.mean(ranges)) if ranges else 3.5
    range_norm = (avg_range - 3.5) / 3.5

    vector_map = {
        "spx_ma20_dist": round(spx_dist, 6),
        "kospi_ma20_dist": round(kospi_dist, 6),
        "vix_z": round(vix_z, 6),
        "pri_z": round(pri_z, 6),
        "macro_z": round(macro_zf, 6),
        "range5d_norm": round(range_norm, 6),
    }
    vector = [vector_map[d] for d in VECTOR_DIMS]
    # 데이터 완전성: REGIME_ANALYSIS 지수 OK 여부 기반
    completeness = 0.0
    for idx_key in ("GSPC", "KOSPI"):
        snap = indices.get(idx_key, {}) if isinstance(indices, dict) else {}
        if isinstance(snap, dict) and snap.get("ok"):
            completeness += 0.4
    if vix_close is not None:
        completeness += 0.2
    return {
        "vector": vector,
        "vector_map": vector_map,
        "data_completeness": round(min(1.0, completeness), 3),
    }


def _resolve_pri_blend_z() -> float:
    try:
        from weekly_proprietary_regime import load_weekly_shadow_pri

        data = load_weekly_shadow_pri()
        blended = data.get("blended") if isinstance(data, dict) else None
        if isinstance(blended, dict):
            return _safe_float(blended.get("composite_z"))
    except Exception:
        pass
    return 0.0


def _resolve_macro_z(cfg: Dict[str, Any]) -> float:
    snap = cfg.get("MACRO_SENTINEL_SNAPSHOT")
    if isinstance(snap, dict):
        return _safe_float(snap.get("composite_z"))
    return 0.0


# ---------------------------------------------------------------------------
# Mission 1-b: 마할라노비스 거리 + 공분산 추정
# ---------------------------------------------------------------------------
def mahalanobis_distance(
    x: np.ndarray, mu: np.ndarray, inv_cov: np.ndarray
) -> float:
    """D_M(x) = sqrt((x-μ)^T Σ^{-1} (x-μ)). 음수/NaN 방어."""
    diff = np.asarray(x, dtype=float) - np.asarray(mu, dtype=float)
    val = float(diff.T @ np.asarray(inv_cov, dtype=float) @ diff)
    if not math.isfinite(val) or val < 0:
        return float("inf")
    return math.sqrt(val)


def _estimate_inv_cov(history: List[List[float]]) -> Tuple[np.ndarray, str]:
    """
    롤링 버퍼 + 에피소드 센트로이드로 Σ 추정 → ridge 정규화 → 의사역행렬.
    표본 부족 시 단위행렬(=유클리드)로 안전 강등.
    """
    centroids = [np.asarray(ep["centroid"], dtype=float) for ep in HISTORICAL_EPISODES.values()]
    rows: List[np.ndarray] = list(centroids)
    if history:
        for h in history:
            arr = np.asarray(h, dtype=float)
            if arr.shape == (N_DIMS,) and np.all(np.isfinite(arr)):
                rows.append(arr)

    if len(rows) < N_DIMS + 2:
        return np.eye(N_DIMS), "euclidean_fallback"

    mat = np.vstack(rows)
    try:
        cov = np.cov(mat, rowvar=False)
        cov = cov + _RIDGE_LAMBDA * np.eye(N_DIMS)
        inv = np.linalg.pinv(cov)
        if not np.all(np.isfinite(inv)):
            return np.eye(N_DIMS), "euclidean_fallback"
        return inv, "mahalanobis"
    except np.linalg.LinAlgError:
        return np.eye(N_DIMS), "euclidean_fallback"


# ---------------------------------------------------------------------------
# Mission 1-c: DTW (Dynamic Time Warping)
# ---------------------------------------------------------------------------
def _znorm(seq: np.ndarray) -> np.ndarray:
    seq = np.asarray(seq, dtype=float)
    seq = seq[np.isfinite(seq)]
    if seq.size == 0:
        return seq
    mu = float(seq.mean())
    sd = float(seq.std(ddof=0))
    if sd < 1e-9:
        return seq - mu
    return (seq - mu) / sd


def dtw_distance(a: np.ndarray, b: np.ndarray) -> float:
    """
    1차원 시퀀스 a,b 사이의 DTW 거리(정규화). 두 시퀀스를 z-정규화한 뒤
    절대차 비용 누적 → 경로 길이로 나눠 길이 의존성을 줄인다.
    """
    a = _znorm(a)
    b = _znorm(b)
    n, m = a.size, b.size
    if n == 0 or m == 0:
        return float("inf")
    inf = float("inf")
    prev = np.full(m + 1, inf)
    prev[0] = 0.0
    for i in range(1, n + 1):
        cur = np.full(m + 1, inf)
        ai = a[i - 1]
        for j in range(1, m + 1):
            cost = abs(ai - b[j - 1])
            cur[j] = cost + min(prev[j], cur[j - 1], prev[j - 1])
        prev = cur
    total = prev[m]
    if not math.isfinite(total):
        return inf
    return total / float(n + m)


def _current_regime_index_series(history: List[List[float]], current_vec: List[float]) -> np.ndarray:
    """
    국면 벡터 시퀀스 → 1차원 '국면 인덱스' 시계열(DTW 입력).
    index = 5*(spx+kospi 이격) + 0.5*pri_z + 0.5*macro_z - 0.3*vix_z
    """
    rows = list(history) + [current_vec]
    out: List[float] = []
    for r in rows[-TRAJECTORY_LEN:]:
        arr = np.asarray(r, dtype=float)
        if arr.shape != (N_DIMS,):
            continue
        idx = (
            5.0 * (arr[0] + arr[1])
            + 0.5 * arr[3]
            + 0.5 * arr[4]
            - 0.3 * arr[2]
        )
        out.append(float(idx))
    return np.asarray(out, dtype=float)


# ---------------------------------------------------------------------------
# Mission 1-d: 앙상블 산출
# ---------------------------------------------------------------------------
def _sim_from_dist(dist: float, scale: float) -> float:
    if not math.isfinite(dist):
        return 0.0
    return float(math.exp(-dist / max(scale, 1e-6)))


def compute_regime_analog(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    persist: bool = True,
    pri_blend_z: Optional[float] = None,
    macro_z: Optional[float] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    현재 국면 벡터를 과거 에피소드들과 마할라노비스 + DTW 앙상블로 비교해
    REGIME_ANALOG_SCORE 를 산출한다. persist=True 면 config KV 에 기록한다.
    """
    cfg = _load_cfg(cfg)
    now = now or datetime.now()

    built = build_current_regime_vector(cfg, pri_blend_z=pri_blend_z, macro_z=macro_z)
    current_vec = built["vector"]
    x = np.asarray(current_vec, dtype=float)

    history = _load_vector_history(cfg)
    inv_cov, cov_mode = _estimate_inv_cov(history)

    cur_series = _current_regime_index_series(history, current_vec)
    dtw_available = cur_series.size >= 5

    penalties = _episode_penalty_table(cfg)

    per_episode: Dict[str, Dict[str, Any]] = {}
    best_name: Optional[str] = None
    best_sim = -1.0

    for name, ep in HISTORICAL_EPISODES.items():
        mu = np.asarray(ep["centroid"], dtype=float)
        d_m = mahalanobis_distance(x, mu, inv_cov)
        maha_sim = _sim_from_dist(d_m, _MAHA_SCALE)

        if dtw_available:
            d_dtw = dtw_distance(cur_series, np.asarray(ep["trajectory"], dtype=float))
            dtw_sim = _sim_from_dist(d_dtw, _DTW_SCALE)
            ensemble = W_MAHALANOBIS * maha_sim + W_DTW * dtw_sim
        else:
            d_dtw = None
            dtw_sim = maha_sim  # 시퀀스 부족 시 마할라노비스로 대체
            ensemble = maha_sim

        # 🛡️ 면역 페널티: 실전에서 실패해 처벌된 국면은 매칭 가중치를 영구 삭감.
        imm_w = episode_match_weight(name, penalties)
        ensemble *= imm_w

        per_episode[name] = {
            "regime": ep["regime"],
            "front_run_favorable": bool(ep["front_run_favorable"]),
            "hist_win_proxy": ep["hist_win_proxy"],
            "maha_dist": round(d_m, 4) if math.isfinite(d_m) else None,
            "maha_sim": round(maha_sim, 4),
            "dtw_dist": round(d_dtw, 4) if d_dtw is not None and math.isfinite(d_dtw) else None,
            "dtw_sim": round(dtw_sim, 4),
            "immune_weight": round(imm_w, 4),
            "ensemble_sim": round(ensemble, 4),
        }
        if ensemble > best_sim:
            best_sim = ensemble
            best_name = name

    best_ep = HISTORICAL_EPISODES.get(best_name or "", {})
    n_hist = len(history)
    confidence = round(
        min(1.0, n_hist / 30.0)
        * (1.0 if cov_mode == "mahalanobis" else 0.7)
        * max(0.4, float(built["data_completeness"])),
        3,
    )

    result: Dict[str, Any] = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "method": "mahalanobis+dtw",
        "vector_dims": list(VECTOR_DIMS),
        "current_vector": current_vec,
        "current_vector_map": built["vector_map"],
        "best_episode": best_name,
        "best_regime": best_ep.get("regime"),
        "best_episode_desc": best_ep.get("desc"),
        "score": round(max(0.0, best_sim), 4),
        "score_pct": round(max(0.0, best_sim) * 100.0, 1),
        "front_run_favorable": bool(best_ep.get("front_run_favorable", False)),
        "hist_win_proxy": best_ep.get("hist_win_proxy"),
        "covariance_mode": cov_mode,
        "dtw_available": bool(dtw_available),
        "n_history": n_hist,
        "data_completeness": built["data_completeness"],
        "confidence": confidence,
        "per_episode": per_episode,
    }

    if persist:
        _set_cfg_value(ANALOG_SCORE_KEY, result)
        _append_vector_history(current_vec, now=now)
    return result


# ---------------------------------------------------------------------------
# 롤링 벡터 버퍼
# ---------------------------------------------------------------------------
def _load_vector_history(cfg: Dict[str, Any]) -> List[List[float]]:
    raw = cfg.get(VECTOR_HISTORY_KEY)
    out: List[List[float]] = []
    if isinstance(raw, list):
        for item in raw:
            vec = item.get("vector") if isinstance(item, dict) else item
            if isinstance(vec, list) and len(vec) == N_DIMS:
                try:
                    out.append([float(v) for v in vec])
                except (TypeError, ValueError):
                    continue
    return out


def _append_vector_history(vector: List[float], *, now: Optional[datetime] = None) -> None:
    now = now or datetime.now()
    entry = {"ts": now.strftime("%Y-%m-%d %H:%M:%S"), "vector": [round(float(v), 6) for v in vector]}

    def _modifier(old: Any) -> Any:
        buf = old if isinstance(old, list) else []
        buf.append(entry)
        if len(buf) > HISTORY_CAP:
            buf = buf[-HISTORY_CAP:]
        return buf

    try:
        from config_manager import update_config_value

        update_config_value(VECTOR_HISTORY_KEY, _modifier)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Mission 2: 선취매 안전 게이트
# ---------------------------------------------------------------------------
def load_regime_analog(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = _load_cfg(cfg)
    val = cfg.get(ANALOG_SCORE_KEY)
    return val if isinstance(val, dict) else {}


def frontrun_gate(cfg: Optional[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
    """
    선취매(컷오프 완화·켈리 증액)를 허용할지 판정한다.

    허용 조건: 게이트 enabled + 현재 국면이 '선취매 유리 국면(favorable)' 과
    REGIME_ANALOG_FRONTRUN_MIN_SCORE 이상으로 유사할 때.
    산출치 부재(콜드스타트) 시 REGIME_ANALOG_GATE_FAIL_OPEN(기본 True)에 따른다.
    """
    cfg = _load_cfg(cfg)
    enabled = bool(cfg.get(GATE_ENABLED_KEY, True))
    if not enabled:
        return True, {"allowed": True, "reason": "gate_disabled"}

    fail_open = bool(cfg.get(GATE_FAIL_OPEN_KEY, True))
    analog = load_regime_analog(cfg)
    if not analog:
        return fail_open, {
            "allowed": fail_open,
            "reason": "no_analog_data_fail_open" if fail_open else "no_analog_data_blocked",
        }

    min_score = _safe_float(
        cfg.get(FRONTRUN_MIN_SCORE_KEY, DEFAULT_FRONTRUN_MIN_SCORE),
        DEFAULT_FRONTRUN_MIN_SCORE,
    )
    score = _safe_float(analog.get("score"))
    favorable = bool(analog.get("front_run_favorable", False))
    info = {
        "allowed": False,
        "score": round(score, 4),
        "min_score": min_score,
        "best_episode": analog.get("best_episode"),
        "best_regime": analog.get("best_regime"),
        "favorable": favorable,
        "confidence": analog.get("confidence"),
    }

    if not favorable:
        info["reason"] = "unfavorable_regime"
        return False, info
    if score < min_score:
        info["reason"] = "low_analog_score"
        return False, info

    info["allowed"] = True
    info["reason"] = "analog_match"
    return True, info


# ---------------------------------------------------------------------------
# Mission 3: 타임머신 모핑 타깃 (과거 승자 DNA)
# ---------------------------------------------------------------------------
def get_archetype_dna_store(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = _load_cfg(cfg)
    store = cfg.get(ARCHETYPE_DNA_KEY)
    if isinstance(store, dict) and store:
        return store
    return {k: {mk: dict(v) for mk, v in mkv.items()} for k, mkv in DEFAULT_ARCHETYPE_DNA.items()}


def resolve_morph_target_dna(
    cfg: Optional[Dict[str, Any]] = None,
    market: str = "KR",
) -> Optional[Dict[str, Any]]:
    """
    현재 국면이 '선취매 유리 과거 국면' 과 REGIME_ANALOG_MORPH_MIN_SCORE 이상 유사하면,
    그 과거 국면의 전설적 승자 DNA(cpv, tb, bbe)를 모핑 타깃으로 반환한다.
    조건 미충족 시 None → 호출자는 기존 '최근 승자 추종' 으로 폴백.
    """
    cfg = _load_cfg(cfg)
    analog = load_regime_analog(cfg)
    if not analog:
        return None

    min_score = _safe_float(
        cfg.get(MORPH_MIN_SCORE_KEY, DEFAULT_MORPH_MIN_SCORE), DEFAULT_MORPH_MIN_SCORE
    )
    score = _safe_float(analog.get("score"))
    favorable = bool(analog.get("front_run_favorable", False))
    episode = analog.get("best_episode")
    if not favorable or score < min_score or not episode:
        return None

    store = get_archetype_dna_store(cfg)
    ep_dna = store.get(episode) if isinstance(store, dict) else None
    if not isinstance(ep_dna, dict):
        return None
    mk_dna = ep_dna.get(str(market).upper())
    if not isinstance(mk_dna, dict):
        return None
    try:
        cpv = float(mk_dna["cpv"])
        tb = float(mk_dna["tb"])
        bbe = float(mk_dna["bbe"])
    except (KeyError, TypeError, ValueError):
        return None

    return {
        "episode": episode,
        "regime": analog.get("best_regime"),
        "score": round(score, 4),
        "dna": [cpv, tb, bbe],
        "source": "archetype_default" if not isinstance(cfg.get(ARCHETYPE_DNA_KEY), dict) else "archetype_config",
    }


def build_regime_archetype_dna(
    *,
    persist: bool = True,
    top_pct: float = 0.2,
    markets: Tuple[str, ...] = ("KR", "US"),
) -> Dict[str, Any]:
    """
    (선택·주말 무거운 작업) time_machine_backtester 로 과거 에피소드를 재생해
    각 국면에서 MFE 상위(top_pct) '전설적 승자'의 DNA 센트로이드를 추출 → REGIME_ARCHETYPE_DNA.

    FDR/네트워크 또는 백테스터 부재 시 DEFAULT_ARCHETYPE_DNA 시드를 반환(안전 강등).
    """
    seed = {k: {mk: dict(v) for mk, v in mkv.items()} for k, mkv in DEFAULT_ARCHETYPE_DNA.items()}
    try:
        import time_machine_backtester as tmb  # noqa: F401
    except Exception:
        if persist:
            _set_cfg_value(ARCHETYPE_DNA_KEY, seed)
        return {"built": False, "reason": "backtester_unavailable", "store": seed}

    builder = getattr(tmb, "extract_regime_winner_dna", None)
    if not callable(builder):
        # 백테스터에 전용 추출기가 없으면 시드 사용(코드 안정성 우선).
        if persist:
            _set_cfg_value(ARCHETYPE_DNA_KEY, seed)
        return {"built": False, "reason": "no_extractor", "store": seed}

    store: Dict[str, Any] = {}
    for name, ep in HISTORICAL_EPISODES.items():
        try:
            dna = builder(
                start=ep.get("start"),
                end=ep.get("end"),
                top_pct=top_pct,
                markets=markets,
            )
            if isinstance(dna, dict) and dna:
                store[name] = dna
        except Exception:
            continue

    merged = dict(seed)
    merged.update(store)
    if persist:
        _set_cfg_value(ARCHETYPE_DNA_KEY, merged)
    return {"built": bool(store), "episodes": list(store.keys()), "store": merged}


# ---------------------------------------------------------------------------
# 리포트 한 줄
# ---------------------------------------------------------------------------
def build_analog_report_line(cfg: Optional[Dict[str, Any]] = None) -> str:
    analog = load_regime_analog(cfg)
    if not analog:
        return "🛰️ <i>[국면 유사도] 미산출(콜드스타트)</i>"
    fav = "✅선취매 유리" if analog.get("front_run_favorable") else "⛔선취매 보수"
    return (
        f"🛰️ <b>[국면 유사도 엔진]</b> 현재 ≈ <b>{analog.get('best_episode')}</b>"
        f" ({analog.get('best_regime')}) · 일치도 <b>{analog.get('score_pct')}%</b>"
        f" · {fav} · conf {analog.get('confidence')}"
        f" <i>[{analog.get('covariance_mode')}]</i>"
    )


if __name__ == "__main__":
    out = compute_regime_analog(persist=False)
    print(f"best={out['best_episode']} score={out['score_pct']}% favorable={out['front_run_favorable']}")
    for k, v in out["per_episode"].items():
        print(f"  {k:16s} ensemble={v['ensemble_sim']:.3f} maha_sim={v['maha_sim']:.3f} dtw_sim={v['dtw_sim']:.3f}")
