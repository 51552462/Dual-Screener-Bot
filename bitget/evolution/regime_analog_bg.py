"""
Bitget Regime Analog Engine — 4차원 코인 국면 벡터 vs 역사적 에피소드.

주식 `regime_analog_engine`의 마할라노비스+DTW 패턴을
`coin_regime_vector` 4차원(dist/slope/atr/breadth)에 맞게 이식한다.
"""
from __future__ import annotations

import html
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from bitget.evolution.coin_regime_vector import (
    N_DIMS,
    VECTOR_DIMS,
    build_current_coin_regime_vector,
    load_vector_history_arrays,
    regime_index,
)
from bitget.infra.clock import utc_now

ANALOG_SCORE_KEY = "REGIME_ANALOG_SCORE_BG"
GATE_ENABLED_KEY = "REGIME_ANALOG_GATE_ENABLED"
FRONTRUN_MIN_SCORE_KEY = "REGIME_ANALOG_FRONTRUN_MIN_SCORE"
GATE_FAIL_OPEN_KEY = "REGIME_ANALOG_GATE_FAIL_OPEN"
DEFAULT_FRONTRUN_MIN_SCORE = 0.80
W_MAHALANOBIS = 0.6
W_DTW = 0.4
_MAHA_SCALE = 2.5
_DTW_SCALE = 2.0
_RIDGE_LAMBDA = 1e-3
TRAJECTORY_LEN = 14

COIN_HISTORICAL_EPISODES: Dict[str, Dict[str, Any]] = {
    "CRYPTO_WINTER_2022": {
        "regime": "DOWN",
        "centroid": [-0.85, -0.70, 0.55, -0.35],
        "trajectory": [-0.2, -0.5, -0.9, -1.2, -1.4, -1.5, -1.6, -1.7, -1.75, -1.8, -1.85, -1.9, -1.92, -1.95],
        "front_run_favorable": False,
        "hist_win_proxy": 0.32,
        "desc": "2022 암호화폴 베어마켓 · BTC -65%급",
    },
    "FTX_LUNA_CRASH": {
        "regime": "DOWN",
        "centroid": [-0.55, -0.90, 1.20, -0.50],
        "trajectory": [0.1, -0.3, -0.8, -1.5, -2.0, -2.2, -2.0, -1.8, -1.9, -2.0, -1.95, -1.9, -1.85, -1.8],
        "front_run_favorable": False,
        "hist_win_proxy": 0.25,
        "desc": "2022 FTX/루나 급락 · 유동성 쇼크",
    },
    "ETF_RALLY_2024": {
        "regime": "UP",
        "centroid": [0.65, 0.55, -0.15, 0.20],
        "trajectory": [-0.5, -0.2, 0.1, 0.4, 0.7, 1.0, 1.2, 1.4, 1.5, 1.6, 1.65, 1.7, 1.75, 1.8],
        "front_run_favorable": True,
        "hist_win_proxy": 0.68,
        "desc": "2024 BTC ETF 승인 랠리 · 기관 유입",
    },
    "ALT_SEASON_2021": {
        "regime": "UP",
        "centroid": [0.45, 0.40, 0.35, 0.85],
        "trajectory": [0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.1, 1.2, 1.3, 1.35, 1.4, 1.45, 1.5, 1.55],
        "front_run_favorable": True,
        "hist_win_proxy": 0.62,
        "desc": "2021 알트시즌 · ETH/BTC 브레드스 확대",
    },
    "CHOPPY_RANGE_2023": {
        "regime": "SIDEWAYS",
        "centroid": [0.05, 0.0, -0.10, 0.05],
        "trajectory": [0.2, -0.2, 0.15, -0.15, 0.1, -0.1, 0.05, -0.05, 0.08, -0.08, 0.0, 0.05, -0.05, 0.0],
        "front_run_favorable": False,
        "hist_win_proxy": 0.44,
        "desc": "2023 횡보·레인지 · 추세 부재",
    },
}


def _sim_from_dist(dist: float, scale: float) -> float:
    if not math.isfinite(dist):
        return 0.0
    return float(math.exp(-dist / max(scale, 1e-6)))


def _estimate_inv_cov(history: List[List[float]]) -> Tuple[np.ndarray, str]:
    rows: List[np.ndarray] = [
        np.asarray(ep["centroid"], dtype=float) for ep in COIN_HISTORICAL_EPISODES.values()
    ]
    for h in history:
        arr = np.asarray(h, dtype=float)
        if arr.shape == (N_DIMS,) and np.all(np.isfinite(arr)):
            rows.append(arr)
    if len(rows) < N_DIMS + 2:
        return np.eye(N_DIMS), "euclidean_fallback"
    mat = np.vstack(rows)
    try:
        cov = np.cov(mat, rowvar=False) + _RIDGE_LAMBDA * np.eye(N_DIMS)
        inv = np.linalg.pinv(cov)
        if not np.all(np.isfinite(inv)):
            return np.eye(N_DIMS), "euclidean_fallback"
        return inv, "mahalanobis"
    except np.linalg.LinAlgError:
        return np.eye(N_DIMS), "euclidean_fallback"


def _current_regime_index_series(history: List[List[float]], current_vec: List[float]) -> np.ndarray:
    rows = list(history) + [current_vec]
    out: List[float] = []
    for r in rows[-TRAJECTORY_LEN:]:
        if isinstance(r, list) and len(r) == N_DIMS:
            out.append(regime_index(r))
    return np.asarray(out, dtype=float)


def compute_coin_regime_analog(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    persist: bool = True,
    pri_blend_z: Optional[float] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """현재 코인 국면 vs 역사적 에피소드 유사도."""
    from bitget.evolution.coin_regime_vector import append_coin_regime_vector_history

    now = now or utc_now()
    built = build_current_coin_regime_vector(cfg)
    current_vec = built["vector"]
    x = np.asarray(current_vec, dtype=float)

    if pri_blend_z is not None:
        try:
            x = x.copy()
            x[0] = x[0] + 0.15 * float(pri_blend_z)
        except (IndexError, TypeError, ValueError):
            pass

    history = load_vector_history_arrays(cfg)
    inv_cov, cov_mode = _estimate_inv_cov(history)
    cur_series = _current_regime_index_series(history, current_vec)
    dtw_available = cur_series.size >= 5

    from regime_analog_engine import dtw_distance, mahalanobis_distance

    per_episode: Dict[str, Dict[str, Any]] = {}
    best_name: Optional[str] = None
    best_sim = -1.0

    for name, ep in COIN_HISTORICAL_EPISODES.items():
        mu = np.asarray(ep["centroid"], dtype=float)
        d_m = mahalanobis_distance(x, mu, inv_cov)
        maha_sim = _sim_from_dist(d_m, _MAHA_SCALE)
        if dtw_available:
            d_dtw = dtw_distance(cur_series, np.asarray(ep["trajectory"], dtype=float))
            dtw_sim = _sim_from_dist(d_dtw, _DTW_SCALE)
            ensemble = W_MAHALANOBIS * maha_sim + W_DTW * dtw_sim
        else:
            d_dtw = None
            dtw_sim = maha_sim
            ensemble = maha_sim
        per_episode[name] = {
            "regime": ep["regime"],
            "front_run_favorable": bool(ep["front_run_favorable"]),
            "hist_win_proxy": ep["hist_win_proxy"],
            "ensemble_sim": round(ensemble, 4),
        }
        if ensemble > best_sim:
            best_sim = ensemble
            best_name = name

    best_ep = COIN_HISTORICAL_EPISODES.get(best_name or "", {})
    n_hist = len(history)
    confidence = round(min(1.0, n_hist / 30.0) * (1.0 if cov_mode == "mahalanobis" else 0.7) * max(0.4, float(built["data_completeness"])), 3)

    result: Dict[str, Any] = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "method": "coin_mahalanobis+dtw",
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
        try:
            from bitget.infra.config_manager import update_config_value

            update_config_value(ANALOG_SCORE_KEY, lambda _old: result)
            append_coin_regime_vector_history(cfg)
        except Exception:
            pass
    return result


def format_regime_analog_brief(result: Optional[Dict[str, Any]] = None) -> str:
    if not isinstance(result, dict) or not result:
        try:
            from bitget.infra.config_manager import load_system_config

            raw = load_system_config().get(ANALOG_SCORE_KEY)
            result = raw if isinstance(raw, dict) else {}
        except Exception:
            result = {}
    if not result:
        return ""
    ep = html.escape(str(result.get("best_episode") or "—"), quote=False)
    desc = html.escape(str(result.get("best_episode_desc") or ""), quote=False)
    score = float(result.get("score_pct") or 0.0)
    fav = "✅" if result.get("front_run_favorable") else "⛔"
    return (
        f"\n🧭 <b>[코인 Regime Analog]</b> {ep} · 유사도 <b>{score:.1f}%</b> {fav}\n"
        f"<i>{desc}</i>\n"
    )


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def load_regime_analog(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if isinstance(cfg, dict):
        val = cfg.get(ANALOG_SCORE_KEY)
        if isinstance(val, dict):
            return val
    try:
        from bitget.infra.config_manager import load_system_config

        raw = load_system_config().get(ANALOG_SCORE_KEY)
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def frontrun_gate(cfg: Optional[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
    """
    선취매(순환매 커트라인 완화·켈리 증액) 허용 판정.

    주식 `regime_analog_engine.frontrun_gate` 와 동일 조건이나
    `REGIME_ANALOG_SCORE_BG` 를 SSOT 로 읽는다.
    """
    if isinstance(cfg, dict):
        gate_cfg = cfg
    else:
        try:
            from bitget.infra.config_manager import load_system_config

            gate_cfg = load_system_config() or {}
        except Exception:
            gate_cfg = {}

    enabled = bool(gate_cfg.get(GATE_ENABLED_KEY, True))
    if not enabled:
        return True, {"allowed": True, "reason": "gate_disabled"}

    fail_open = bool(gate_cfg.get(GATE_FAIL_OPEN_KEY, True))
    analog = load_regime_analog(gate_cfg)
    if not analog:
        return fail_open, {
            "allowed": fail_open,
            "reason": "no_analog_data_fail_open" if fail_open else "no_analog_data_blocked",
        }

    min_score = _safe_float(
        gate_cfg.get(FRONTRUN_MIN_SCORE_KEY, DEFAULT_FRONTRUN_MIN_SCORE),
        DEFAULT_FRONTRUN_MIN_SCORE,
    )
    score = _safe_float(analog.get("score"))
    favorable = bool(analog.get("front_run_favorable", False))
    info: Dict[str, Any] = {
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
