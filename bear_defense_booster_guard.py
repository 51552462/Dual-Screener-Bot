"""
P0 — BEAR/HIGH_VOL attack booster lockdown (Architect directive).

Genesis / Ace 등 공격 부스터 Kelly multiplier를 방어 국면에서 hard cap.
유일한 예외: regime_analog_engine 이 V_RECOVERY + front_run_favorable + min score.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

DEFENSIVE_REGIMES = frozenset({"BEAR", "HIGH_VOL"})
BEAR_TEMPLATE_BANDIT_CEILING = 1.2  # P0-2: hot mutant Kelly pump cap in defensive regimes


def normalize_regime_for_guard(regime_key: Any) -> str:
    try:
        from meta_state_store import normalize_regime_key

        return normalize_regime_key(regime_key)
    except Exception:
        return str(regime_key or "UNKNOWN").strip().upper()


def is_defensive_regime(regime_key: Any) -> bool:
    return normalize_regime_for_guard(regime_key) in DEFENSIVE_REGIMES


def is_analog_v_recovery_unlock(sys_config: Optional[Dict[str, Any]] = None) -> bool:
    """V_RECOVERY analog — sole exception to bear booster clamp."""
    try:
        from regime_analog_engine import (
            DEFAULT_FRONTRUN_MIN_SCORE,
            FRONTRUN_MIN_SCORE_KEY,
            load_regime_analog,
        )

        cfg = sys_config if isinstance(sys_config, dict) else {}
        analog = load_regime_analog(cfg)
        if not analog:
            return False
        episode = str(analog.get("best_episode") or "").strip().upper()
        if episode != "V_RECOVERY":
            return False
        if not bool(analog.get("front_run_favorable", False)):
            return False
        try:
            min_score = float(cfg.get(FRONTRUN_MIN_SCORE_KEY, DEFAULT_FRONTRUN_MIN_SCORE))
        except (TypeError, ValueError):
            min_score = DEFAULT_FRONTRUN_MIN_SCORE
        score = float(analog.get("score") or 0)
        return score >= min_score
    except Exception:
        return False


def clamp_bear_attack_booster_mult(
    mult: float,
    regime_key: Any,
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    cap: float = 1.0,
) -> float:
    """
    BEAR/HIGH_VOL: return min(mult, cap) unless V_RECOVERY analog unlock.
    Non-defensive regimes or mult <= cap: unchanged.
    """
    try:
        m = float(mult)
    except (TypeError, ValueError):
        return 1.0
    cap_f = float(cap)
    if m <= cap_f:
        return m
    if not is_defensive_regime(regime_key):
        return m
    if is_analog_v_recovery_unlock(sys_config):
        return m
    return min(m, cap_f)


def ceiling_template_bandit_mult(
    mult: float,
    regime_key: Any,
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    ceiling: float = BEAR_TEMPLATE_BANDIT_CEILING,
) -> float:
    """
    P0-2: BEAR/HIGH_VOL — combined template_bandit mult 상한 (default 1.2).

    하향 밸브( mult < ceiling )는 그대로 유지. V_RECOVERY 예외 없음(1.2 자체가 방어 상한).
    """
    try:
        m = float(mult)
    except (TypeError, ValueError):
        return 1.0
    ceil_f = float(ceiling)
    if m <= ceil_f:
        return m
    if not is_defensive_regime(regime_key):
        return m
    return min(m, ceil_f)


BEAR_GRACE_BLOCK_REGIMES = frozenset({"BEAR"})  # P0-4: recovery grace never in BEAR (Architect spec)


def recovery_grace_blocked_by_regime(regime_key: Any) -> bool:
    """P0-4: BEAR 국면 — FACTORY_RECOVERY_GRACE / PIL relaxed 유예 절대 불가."""
    return normalize_regime_for_guard(regime_key) in BEAR_GRACE_BLOCK_REGIMES


def allow_prebuy_advantage_boost(regime_key: Any) -> bool:
    """
    P0-3: BEAR/HIGH_VOL → ROTATION/SPILLOVER advantage Kelly 가산 무시.

    frontrun_gate 와 별도 — advantage 플래그가 ON 이어도 방어 국면에서는 ×2/×1.5 차단.
    """
    return not is_defensive_regime(regime_key)


def resolve_meta_regime_key(sys_config: Optional[Dict[str, Any]] = None) -> str:
    """META_REGIME_KEY SSOT → config fallback."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        from meta_governor_consumer import load_meta_state_resolved
        from meta_state_store import normalize_regime_key, resolve_config_regime_key

        rk = normalize_regime_key(load_meta_state_resolved().get("META_REGIME_KEY"))
        if rk not in ("", "UNKNOWN"):
            return rk
        return resolve_config_regime_key(cfg)
    except Exception:
        return str(cfg.get("CURRENT_REGIME_KEY") or "UNKNOWN").strip().upper()
