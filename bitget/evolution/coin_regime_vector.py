"""
코인 국면 벡터(Coin Regime Vector) — champion_genesis_bg의 마할라노비스/DTW 입력.

주식 `regime_analog_engine.build_current_regime_vector`는 SPX/KOSPI MA20 이격·VIX·
PRI·거시 센티넬 6차원을 쓰지만, 코인엔 그런 지수/VIX가 없다. 대신 이미
`bitget.auto_pilot.detect_coin_regime`가 매 사이클 계산해 두는 BTC 기반 4개 스칼라
(EMA200 이격·기울기·ATR·ETH/BTC 브레드스)를 그대로 재활용해 4차원 국면 벡터로 구성한다.

- REGIME_VECTOR_HISTORY_BG: config_kv 롤링 버퍼(최근 180개) — 공분산/DTW 추정용.
- 모든 함수는 예외를 삼키고 안전 폴백한다(리포트/거버넌스 경로에 부하·예외 전파 0).
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional

VECTOR_HISTORY_KEY = "REGIME_VECTOR_HISTORY_BG"
HISTORY_CAP = 180

VECTOR_DIMS: tuple = ("dist_ema200_z", "ema200_slope_z", "atr_z", "breadth_z")
N_DIMS = len(VECTOR_DIMS)

# 정규화 스케일(코인 국면의 전형적 변동폭 기준 — 무차원화)
_DIST_SCALE = 10.0     # BTC_DIST_FROM_EMA200_PCT 전형 범위 ±30%
_SLOPE_SCALE = 2.0     # EMA200_SLOPE_PCT 전형 범위 ±5%
_ATR_CENTER, _ATR_SCALE = 3.0, 2.0   # ATR_PCT 전형 3%대, 스프레드 2
_BREADTH_SCALE = 10.0  # ETH/BTC 브레드스 비율은 1.0 근방 ±10%


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _load_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(cfg, dict):
        return cfg
    try:
        from bitget.infra.config_manager import load_system_config

        return load_system_config() or {}
    except Exception:
        return {}


def build_current_coin_regime_vector(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """detect_coin_regime()가 이미 계산해 둔 스칼라만 재사용(네트워크/DB 호출 없음)."""
    cfg = _load_cfg(cfg)
    detail = cfg.get("CRYPTO_REGIME_DETAIL")
    detail = detail if isinstance(detail, dict) else {}

    dist_pct = _safe_float(detail.get("dist_from_ema200_pct", cfg.get("BTC_DIST_FROM_EMA200_PCT")))
    slope_pct = _safe_float(detail.get("ema200_slope_pct", cfg.get("BTC_EMA200_SLOPE_PCT")))
    atr_pct = _safe_float(detail.get("atr_pct", cfg.get("BTC_ATR_PCT")), _ATR_CENTER)
    breadth = _safe_float(detail.get("eth_btc_breadth", cfg.get("CRYPTO_BREADTH_ETH_BTC_REL")), 1.0)

    vector_map = {
        "dist_ema200_z": round(dist_pct / _DIST_SCALE, 6),
        "ema200_slope_z": round(slope_pct / _SLOPE_SCALE, 6),
        "atr_z": round((atr_pct - _ATR_CENTER) / _ATR_SCALE, 6),
        "breadth_z": round((breadth - 1.0) * _BREADTH_SCALE, 6),
    }
    vector = [vector_map[d] for d in VECTOR_DIMS]
    completeness = 1.0 if detail else (0.4 if (dist_pct or slope_pct) else 0.0)
    return {
        "vector": vector,
        "vector_map": vector_map,
        "data_completeness": round(min(1.0, completeness), 3),
    }


def regime_index(vec: List[float]) -> float:
    """국면 인덱스(스칼라) — DTW 시퀀스 입력. 상승/하락 방향성 + 변동성 역가중."""
    try:
        return (
            float(vec[0]) + float(vec[1]) - 0.3 * float(vec[2]) + 0.5 * float(vec[3])
        )
    except (IndexError, TypeError, ValueError):
        return 0.0


def load_vector_history(cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    cfg = _load_cfg(cfg)
    raw = cfg.get(VECTOR_HISTORY_KEY)
    out: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and isinstance(item.get("vector"), list):
                out.append(item)
    return out


def load_vector_history_arrays(cfg: Optional[Dict[str, Any]] = None) -> List[List[float]]:
    out: List[List[float]] = []
    for item in load_vector_history(cfg):
        vec = item.get("vector")
        if isinstance(vec, list) and len(vec) == N_DIMS:
            try:
                out.append([float(v) for v in vec])
            except (TypeError, ValueError):
                continue
    return out


def append_coin_regime_vector_history(
    cfg: Optional[Dict[str, Any]] = None, *, now: Optional[datetime] = None
) -> None:
    """현재 국면 벡터를 REGIME_VECTOR_HISTORY_BG 롤링 버퍼에 원자적으로 추가."""
    built = build_current_coin_regime_vector(cfg)
    now = now or datetime.now()
    entry = {
        "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
        "vector": [round(float(v), 6) for v in built["vector"]],
    }

    def _modifier(old: Any) -> Any:
        buf = old if isinstance(old, list) else []
        buf.append(entry)
        if len(buf) > HISTORY_CAP:
            buf = buf[-HISTORY_CAP:]
        return buf

    try:
        from bitget.infra.config_manager import update_config_value

        update_config_value(VECTOR_HISTORY_KEY, _modifier)
    except Exception:
        pass
