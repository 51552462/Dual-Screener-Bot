"""
순환매 예측 Self-improving — Whipsaw 방지용 부드러운 패널티 (P1 연동).

1회 예측 실패: confidence 유지(관망)
2회 연속 실패: confidence 소폭 감소 (EMA)
3회+ 연속 실패: 가중치 축소·ROTATION_ADVANTAGE 비활성 후보
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

DEFAULT_MISS_STREAK_WARN = 2
DEFAULT_MISS_STREAK_REDUCE = 3
DEFAULT_EMA_ALPHA = 0.35


def _f(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        return float(cfg.get(key, default))
    except (TypeError, ValueError):
        return float(default)


def _i(cfg: Dict[str, Any], key: str, default: int) -> int:
    try:
        return int(cfg.get(key, default))
    except (TypeError, ValueError):
        return int(default)


def apply_prediction_miss_smoothing(
    state: Dict[str, Any],
    *,
    hit: bool,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    state keys: confidence, miss_streak, ema_accuracy
    Returns updated state + action hints.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    out = dict(state)
    try:
        conf = float(out.get("confidence", 0.5))
    except (TypeError, ValueError):
        conf = 0.5
    streak = _i(out, "miss_streak", 0)
    try:
        ema = float(out.get("ema_accuracy", 0.5))
    except (TypeError, ValueError):
        ema = 0.5
    alpha = _f(cfg, "ROTATION_PRED_EMA_ALPHA", DEFAULT_EMA_ALPHA)
    warn_streak = _i(cfg, "ROTATION_PRED_MISS_WARN_STREAK", DEFAULT_MISS_STREAK_WARN)
    reduce_streak = _i(cfg, "ROTATION_PRED_MISS_REDUCE_STREAK", DEFAULT_MISS_STREAK_REDUCE)

    obs = 1.0 if hit else 0.0
    ema = alpha * obs + (1.0 - alpha) * ema
    out["ema_accuracy"] = round(ema, 3)

    if hit:
        out["miss_streak"] = 0
        out["penalty_action"] = "hold"
    else:
        streak += 1
        out["miss_streak"] = streak
        if streak < warn_streak:
            out["penalty_action"] = "hold"
        elif streak < reduce_streak:
            conf = max(0.35, conf - 0.05)
            out["penalty_action"] = "soft_warn"
        else:
            conf = max(0.25, conf - 0.12)
            out["penalty_action"] = "reduce_weight"
            out["suggest_disable_rotation_advantage"] = True

    out["confidence"] = round(conf, 3)
    return out
