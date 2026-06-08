"""
Graceful Kelly fail-safe — 국면 UNKNOWN·config 분열 시 즉시 1% 락업 대신 완만한 폴백.

- 최근 N일 유효 Kelly 이동평균 (REGIME_KELLY_SNAPSHOT)
- NEUTRAL(SIDEWAYS) ACTION_BY_REGIME 기본 kelly_cap × DYNAMIC_KELLY_RISK 스케일
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

KELLY_SNAPSHOT_KEY = "REGIME_KELLY_SNAPSHOT"
DEFAULT_LOOKBACK_DAYS = 3
DEFAULT_UNKNOWN_FLOOR = 0.005
DEFAULT_UNKNOWN_CAP = 0.022
NEUTRAL_REGIME_KEY = "SIDEWAYS"
BULLISH_REGIME_KEYS = frozenset({"BULL", "RISK_ON", "GOLDILOCKS"})


def _coerce_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _neutral_kelly_cap() -> float:
    return _regime_kelly_cap(NEUTRAL_REGIME_KEY)


def _regime_kelly_cap(
    regime_key: str,
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    """META_REGIME_ACTION.kelly_cap 우선, 없으면 ACTION_BY_REGIME 템플릿."""
    rk = str(regime_key or "").strip().upper()
    m = meta if isinstance(meta, dict) else {}
    ra = m.get("META_REGIME_ACTION")
    if isinstance(ra, dict) and ra.get("kelly_cap") is not None:
        cap = _coerce_float(ra.get("kelly_cap"), -1.0)
        if cap > 0:
            return cap
    from meta_governor import ACTION_BY_REGIME

    tpl = ACTION_BY_REGIME.get(rk) or ACTION_BY_REGIME.get("UNKNOWN") or {}
    return _coerce_float(tpl.get("kelly_cap"), 0.018)


def record_kelly_snapshot(
    effective_kelly: float,
    regime_key: str,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> None:
    """리포트·관제탑 직후 호출 — 최근 정상 Kelly 기록."""
    eff = max(0.0, float(effective_kelly))
    rk = str(regime_key or "").strip().upper()
    if rk in ("", "UNKNOWN") or eff <= 0.0:
        return
    try:
        from config_manager import get_config_value, set_config_value

        raw = get_config_value(KELLY_SNAPSHOT_KEY)
        hist: List[Dict[str, Any]] = list(raw) if isinstance(raw, list) else []
        today = datetime.now().strftime("%Y-%m-%d")
        hist = [h for h in hist if isinstance(h, dict) and str(h.get("date", "")) != today]
        hist.append(
            {
                "date": today,
                "effective_kelly": round(eff, 6),
                "regime_key": rk,
            }
        )
        hist = hist[-max(1, int(lookback_days)) :]
        set_config_value(KELLY_SNAPSHOT_KEY, hist)
    except Exception as e:
        logger.debug("regime_kelly_failsafe: snapshot skip: %s", e)


def _kelly_ma_from_snapshots(
    sys_config: Optional[Dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Optional[float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    raw = cfg.get(KELLY_SNAPSHOT_KEY)
    if raw is None:
        try:
            from config_manager import get_config_value

            raw = get_config_value(KELLY_SNAPSHOT_KEY)
        except Exception:
            raw = None
    if not isinstance(raw, list) or not raw:
        return None
    vals: List[float] = []
    for row in raw[-lookback_days:]:
        if not isinstance(row, dict):
            continue
        rk = str(row.get("regime_key") or "").upper()
        if rk in ("", "UNKNOWN"):
            continue
        eff = _coerce_float(row.get("effective_kelly"), -1.0)
        if eff > 0:
            vals.append(eff)
    if not vals:
        return None
    return sum(vals) / len(vals)


def resolve_graceful_base_kelly(
    sys_config: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
    *,
    config_regime_unknown: bool = False,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Tuple[float, str]:
    """
    DYNAMIC_KELLY_RISK 대체/보정용 베이스 Kelly (비율 0–1).
    반환: (kelly, reason_code) — reason_code는 로그·리포트용.
    """
    c = sys_config if isinstance(sys_config, dict) else {}
    raw_base = _coerce_float(c.get("DYNAMIC_KELLY_RISK"), 0.01)

    from meta_state_store import normalize_regime_key, resolve_config_regime_key

    m = meta if isinstance(meta, dict) else {}
    rk_meta = normalize_regime_key(m.get("META_REGIME_KEY"))
    rk_cfg = resolve_config_regime_key(c)
    unknown_cfg = config_regime_unknown or rk_cfg in ("", "UNKNOWN")
    unknown_meta = rk_meta in ("", "UNKNOWN")

    if not unknown_cfg and not unknown_meta:
        rk = rk_meta if rk_meta not in ("", "UNKNOWN") else rk_cfg
        if rk not in ("", "UNKNOWN"):
            regime_cap = _regime_kelly_cap(rk, m)
            if regime_cap > raw_base:
                lifted = max(raw_base, min(regime_cap * 0.85, regime_cap))
                if lifted > raw_base + 1e-9:
                    return lifted, "regime_aligned_base"
        return raw_base, "config_ok"

    if not unknown_meta and unknown_cfg:
        # Meta는 확정, config만 미동기(SQLite lock 등) — 메모리상 Kelly 강제 상향
        cap = _regime_kelly_cap(rk_meta, m)
        blended = max(raw_base, min(cap * 0.85, cap))
        forced = max(DEFAULT_UNKNOWN_FLOOR, min(DEFAULT_UNKNOWN_CAP, blended))
        if rk_meta in BULLISH_REGIME_KEYS and raw_base <= 0.0105:
            # BULL + DYNAMIC_KELLY 1% 고착 → Regime cap 85% (예: 2.38%) 우선
            forced = max(forced, min(cap * 0.85, cap))
            logger.warning(
                "regime_kelly_failsafe: meta_led BULL unlock base %.4f → %.4f (cap=%.4f)",
                raw_base,
                forced,
                cap,
            )
            return forced, "meta_bull_forced_unlock"
        logger.warning(
            "regime_kelly_failsafe: meta_led_config_unknown rk=%s base %.4f → %.4f",
            rk_meta,
            raw_base,
            forced,
        )
        return forced, "meta_led_config_unknown"

    ma = _kelly_ma_from_snapshots(c, lookback_days=lookback_days)
    if ma is not None and ma > DEFAULT_UNKNOWN_FLOOR:
        clamped = max(DEFAULT_UNKNOWN_FLOOR, min(DEFAULT_UNKNOWN_CAP, ma))
        return clamped, "kelly_ma_fallback"

    neutral = _neutral_kelly_cap()
    baseline = max(raw_base, neutral * 0.75)
    baseline = max(DEFAULT_UNKNOWN_FLOOR, min(DEFAULT_UNKNOWN_CAP, baseline))
    return baseline, "neutral_regime_default"


def apply_graceful_kelly_to_effective(
    base_kelly: float,
    global_mult: float,
    cap: Optional[float],
    floor: Optional[float],
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    config_regime_unknown: bool = False,
) -> Tuple[float, float, str]:
    """
    리포트용 effective Kelly — UNKNOWN 시 base를 graceful 로 대체 후 cap/floor 클램프.
    반환: (effective, adjusted_base, reason).
    """
    adj_base, reason = resolve_graceful_base_kelly(
        sys_config,
        meta,
        config_regime_unknown=config_regime_unknown,
    )
    g = float(global_mult or 1.0)
    eff = adj_base * g
    if floor is not None:
        eff = max(eff, float(floor))
    if cap is not None:
        eff = min(eff, float(cap))
    eff = max(0.0, eff)

    # config 미동기 + Meta BULL: 리포트 eff_k가 1%로 떨어지지 않도록 메모리 floor
    if meta and reason in ("meta_led_config_unknown", "meta_bull_forced_unlock"):
        rk_meta = str((meta or {}).get("META_REGIME_KEY") or "").strip().upper()
        if rk_meta in BULLISH_REGIME_KEYS:
            regime_cap = _regime_kelly_cap(rk_meta, meta)
            meta_floor = min(regime_cap * 0.85, regime_cap) * max(g, 0.01)
            if eff < meta_floor - 1e-9:
                logger.warning(
                    "regime_kelly_failsafe: eff_k floor lift %.4f → %.4f (meta=%s)",
                    eff,
                    meta_floor,
                    rk_meta,
                )
                eff = meta_floor
                adj_base = max(adj_base, meta_floor / g if g > 0 else meta_floor)

    return eff, adj_base, reason
