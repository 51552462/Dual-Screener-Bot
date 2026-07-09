"""
STRATEGY_LIFECYCLE — KR/US/BG 시장별 승격·강등·알파 TTL SSOT (system_config 병합).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

# 동적 섀도우 검증 — alpha_half_life 대비 Base Window 비율 구간 (Architect SSOT)
SHADOW_VERIFY_WINDOW_MIN_RATIO = 0.70
SHADOW_VERIFY_WINDOW_MAX_RATIO = 1.00
SHADOW_VERIFY_MIN_WINDOW_DAYS = 3

# 국면별 시간 압축(Regime Time Dilation) — 고변동·패닉 장세 데이터 밀도 반영
REGIME_TIME_DILATION_COMPRESS_FACTOR = 0.5
REGIME_TIME_DILATION_NEUTRAL_FACTOR = 1.0
REGIME_TIME_DILATION_COMPRESS_KEYS = frozenset({"HIGH_VOL", "BEAR_PANIC"})
REGIME_TIME_DILATION_NEUTRAL_KEYS = frozenset({"SIDEWAYS", "CHOP", "WHIPSAW"})

DEFAULT_STRATEGY_LIFECYCLE: Dict[str, Dict[str, Any]] = {
    "KR": {
        "candidate_min_wr": 0.45,
        "candidate_min_pf": 1.20,
        "candidate_min_trades": 15,
        "candidate_max_mdd_pct": -28.0,
        "live_min_wr": 0.50,
        "live_wr_mid_min": 0.45,
        "live_wr_mid_max": 0.499,
        "live_mid_min_pf": 1.50,
        "live_min_pf_if_wr_ok": 1.35,
        "promote_min_trades": 15,
        "alpha_half_life_days": 10,
        "shadow_verify_min_ratio": SHADOW_VERIFY_WINDOW_MIN_RATIO,
        "shadow_verify_max_ratio": SHADOW_VERIFY_WINDOW_MAX_RATIO,
        "cooloff_days": 3,
        "whipsaw_below_days": 2,
        "fast_track_enabled": True,
        "fast_track_min_trades": 10,
        "fast_track_min_pf": 2.0,
    },
    "US": {
        "candidate_min_wr": 0.42,
        "candidate_min_pf": 1.15,
        "candidate_min_trades": 8,
        "candidate_max_mdd_pct": -32.0,
        "live_min_wr": 0.48,
        "live_wr_mid_min": 0.42,
        "live_wr_mid_max": 0.479,
        "live_mid_min_pf": 1.45,
        "live_min_pf_if_wr_ok": 1.30,
        "promote_min_trades": 8,
        "alpha_half_life_days": 30,
        "shadow_verify_min_ratio": SHADOW_VERIFY_WINDOW_MIN_RATIO,
        "shadow_verify_max_ratio": SHADOW_VERIFY_WINDOW_MAX_RATIO,
        "cooloff_days": 7,
        "whipsaw_below_days": 3,
        "fast_track_enabled": True,
        "fast_track_min_trades": 8,
        "fast_track_min_pf": 2.0,
    },
    "BG": {
        "candidate_min_wr": 0.42,
        "candidate_min_pf": 1.15,
        "candidate_min_trades": 8,
        "candidate_max_mdd_pct": -32.0,
        "live_min_wr": 0.48,
        "live_wr_mid_min": 0.42,
        "live_wr_mid_max": 0.479,
        "live_mid_min_pf": 1.45,
        "live_min_pf_if_wr_ok": 1.30,
        "promote_min_trades": 8,
        "alpha_half_life_days": 21,
        "shadow_verify_min_ratio": SHADOW_VERIFY_WINDOW_MIN_RATIO,
        "shadow_verify_max_ratio": SHADOW_VERIFY_WINDOW_MAX_RATIO,
        "cooloff_days": 5,
        "whipsaw_below_days": 3,
        "fast_track_enabled": True,
        "fast_track_min_trades": 8,
        "fast_track_min_pf": 2.0,
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(dict(out[k]), v)
        else:
            out[k] = v
    return out


def load_strategy_lifecycle_config(system_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    """시장별 lifecycle 파라미터 (기본값 + STRATEGY_LIFECYCLE 덮어쓰기)."""
    merged = {m: dict(v) for m, v in DEFAULT_STRATEGY_LIFECYCLE.items()}
    if not isinstance(system_cfg, dict):
        return merged
    raw = system_cfg.get("STRATEGY_LIFECYCLE")
    if not isinstance(raw, dict):
        return merged
    for mkt, params in raw.items():
        mk = str(mkt or "").upper().strip()
        if mk not in merged or not isinstance(params, dict):
            continue
        merged[mk] = {**merged[mk], **params}
    return merged


def market_params(cfg: Dict[str, Dict[str, Any]], market: str) -> Dict[str, Any]:
    m = str(market or "KR").upper().strip()
    if m not in cfg:
        return dict(cfg.get("KR", DEFAULT_STRATEGY_LIFECYCLE["KR"]))
    return dict(cfg[m])


def _parse_iso_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


def compute_dynamic_shadow_base_window(
    market: str,
    *,
    system_cfg: Optional[Dict[str, Any]] = None,
    shadow_days_elapsed: Optional[int] = None,
    demoted_at_iso: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    시장별 alpha_half_life 기반 동적 섀도우 Base Window (반감기의 70~100%).

    window_ratio = min_ratio + (max_ratio - min_ratio) × tenure_fill
    tenure_fill  = clamp(shadow_days_elapsed / alpha_half_life, 0, 1)

    · 강등 직후(tenure=0) → 반감기 × 70% (빠른 초기 검증)
    · 섀도우 체류가 반감기에 도달 → 반감기 × 100% (풀 윈도우)
    """
    lc = load_strategy_lifecycle_config(system_cfg)
    mp = market_params(lc, str(market or "KR").upper())
    half_life = max(1, int(mp.get("alpha_half_life_days", 10)))

    try:
        min_ratio = float(mp.get("shadow_verify_min_ratio", SHADOW_VERIFY_WINDOW_MIN_RATIO))
    except (TypeError, ValueError):
        min_ratio = SHADOW_VERIFY_WINDOW_MIN_RATIO
    try:
        max_ratio = float(mp.get("shadow_verify_max_ratio", SHADOW_VERIFY_WINDOW_MAX_RATIO))
    except (TypeError, ValueError):
        max_ratio = SHADOW_VERIFY_WINDOW_MAX_RATIO

    min_ratio = max(0.5, min(min_ratio, max_ratio))
    max_ratio = max(min_ratio, max_ratio)

    elapsed = shadow_days_elapsed
    if elapsed is None and demoted_at_iso:
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        dem_dt = _parse_iso_dt(demoted_at_iso)
        if dem_dt is not None:
            elapsed = max(0, (now_dt - dem_dt.astimezone(timezone.utc)).days)

    if elapsed is None:
        # 강등 시각 미상 — 구간 중앙(85%)으로 보수적 기본값
        tenure_fill = 0.5
        elapsed = int(round(half_life * tenure_fill))
    else:
        tenure_fill = min(1.0, max(0.0, float(elapsed) / float(half_life)))

    window_ratio = min_ratio + (max_ratio - min_ratio) * tenure_fill
    base_window_days = max(SHADOW_VERIFY_MIN_WINDOW_DAYS, int(round(half_life * window_ratio)))

    return {
        "market": str(market or "KR").upper(),
        "alpha_half_life_days": half_life,
        "shadow_days_elapsed": int(elapsed),
        "tenure_fill": round(tenure_fill, 4),
        "window_ratio": round(window_ratio, 4),
        "window_min_ratio": round(min_ratio, 4),
        "window_max_ratio": round(max_ratio, 4),
        "base_window_days": base_window_days,
        "source": "alpha_half_life_dynamic",
    }


def resolve_effective_regime_key(
    meta: Optional[Mapping[str, Any]],
    *,
    market: Optional[str] = None,
) -> Tuple[str, str]:
    """
    섀도우 검증용 국면 키 — META_REGIME_KEY 우선, Treasury asymmetric 폴백.
    Returns (effective_key, raw_key). BEAR_PANIC 은 normalize 전 보존.
    """
    _ = market  # 향후 시장별 국면 분리 SSOT 예약
    raw = ""
    if isinstance(meta, Mapping):
        raw = str(meta.get("META_REGIME_KEY") or "").strip().upper()
        if not raw:
            asym = meta.get("META_TREASURY_ASYMMETRIC_WINDOW")
            if isinstance(asym, dict):
                raw = str(asym.get("regime_key") or "").strip().upper()
        if not raw:
            health = meta.get("META_STRATEGY_HEALTH")
            if isinstance(health, dict):
                hm = health.get("__meta__")
                if isinstance(hm, dict):
                    raw = str(hm.get("asymmetric_regime") or "").strip().upper()

    if raw in REGIME_TIME_DILATION_COMPRESS_KEYS or "PANIC" in raw:
        return raw if raw else "BEAR_PANIC", raw or "BEAR_PANIC"

    try:
        from meta_state_store import normalize_regime_key

        nk = normalize_regime_key(raw or "UNKNOWN")
    except Exception:
        nk = str(raw or "UNKNOWN").upper()
        if nk in ("CHOP", "WHIPSAW"):
            nk = "SIDEWAYS"
    return nk, raw or nk


def resolve_bear_stress_subphase(meta: Optional[Mapping[str, Any]]) -> Optional[str]:
    """META 위성·내재 bear sub-phase (BEAR_PANIC / BEAR_ACCEL 등)."""
    if not isinstance(meta, Mapping):
        return None
    for key in ("META_INTRINSIC_BEAR_PHASE", "META_BEAR_STRESS_PHASE"):
        val = meta.get(key)
        if val:
            return str(val).strip().upper()
    sat = meta.get("META_SATELLITE_INTEL")
    if isinstance(sat, dict):
        for block in (
            sat.get("hedge_engine"),
            sat.get("self_evolution_hedge"),
            sat,
        ):
            if isinstance(block, dict):
                bp = block.get("bear_phase")
                if bp:
                    return str(bp).strip().upper()
    return None


def regime_time_dilation_factor(
    regime_key: str,
    *,
    meta: Optional[Mapping[str, Any]] = None,
    raw_regime_key: str = "",
) -> Dict[str, Any]:
    """
    국면별 시간 압축 계수.

    · HIGH_VOL / BEAR_PANIC → ×0.5 (하루 데이터 밀도 높음 → 검증 기간 압축)
    · SIDEWAYS (CHOP/WHIPSAW 포함) → ×1.0 (시장 호흡 유지)
    · 그 외(BULL/BEAR/UNKNOWN) → ×1.0
    """
    rk = str(regime_key or "UNKNOWN").strip().upper()
    raw = str(raw_regime_key or rk).strip().upper()
    bear_sub = resolve_bear_stress_subphase(meta)

    is_compress = (
        rk in REGIME_TIME_DILATION_COMPRESS_KEYS
        or raw in REGIME_TIME_DILATION_COMPRESS_KEYS
        or bear_sub == "BEAR_PANIC"
        or "PANIC" in raw
    )
    if is_compress:
        return {
            "regime_key": rk,
            "raw_regime_key": raw,
            "bear_subphase": bear_sub,
            "dilation_factor": REGIME_TIME_DILATION_COMPRESS_FACTOR,
            "dilation_mode": "compress_high_density",
        }

    if rk in REGIME_TIME_DILATION_NEUTRAL_KEYS or raw in REGIME_TIME_DILATION_NEUTRAL_KEYS:
        return {
            "regime_key": rk,
            "raw_regime_key": raw,
            "bear_subphase": bear_sub,
            "dilation_factor": REGIME_TIME_DILATION_NEUTRAL_FACTOR,
            "dilation_mode": "neutral_sideways",
        }

    return {
        "regime_key": rk,
        "raw_regime_key": raw,
        "bear_subphase": bear_sub,
        "dilation_factor": REGIME_TIME_DILATION_NEUTRAL_FACTOR,
        "dilation_mode": "neutral_default",
    }


def apply_regime_time_dilation(
    base_window_days: int,
    *,
    regime_key: str,
    meta: Optional[Mapping[str, Any]] = None,
    raw_regime_key: str = "",
    min_window_days: int = SHADOW_VERIFY_MIN_WINDOW_DAYS,
) -> Dict[str, Any]:
    """Base Window × 국면 압축 → 최종 검증 윈도우."""
    dil = regime_time_dilation_factor(
        regime_key, meta=meta, raw_regime_key=raw_regime_key
    )
    factor = float(dil.get("dilation_factor") or REGIME_TIME_DILATION_NEUTRAL_FACTOR)
    base = max(1, int(base_window_days))
    final = max(int(min_window_days), int(round(base * factor)))
    return {
        **dil,
        "base_window_days": base,
        "final_window_days": final,
    }


def compute_dynamic_shadow_verification_window(
    market: str,
    *,
    system_cfg: Optional[Dict[str, Any]] = None,
    shadow_days_elapsed: Optional[int] = None,
    demoted_at_iso: Optional[str] = None,
    now: Optional[datetime] = None,
    meta: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    동적 섀도우 검증 윈도우 — alpha_half_life Base Window + 국면 Time Dilation.

    1) compute_dynamic_shadow_base_window (반감기 70~100%)
    2) apply_regime_time_dilation (HIGH_VOL/BEAR_PANIC ×0.5, SIDEWAYS ×1.0)
    """
    base = compute_dynamic_shadow_base_window(
        market,
        system_cfg=system_cfg,
        shadow_days_elapsed=shadow_days_elapsed,
        demoted_at_iso=demoted_at_iso,
        now=now,
    )
    rk, raw_rk = resolve_effective_regime_key(meta, market=market)
    dil = apply_regime_time_dilation(
        int(base["base_window_days"]),
        regime_key=rk,
        meta=meta,
        raw_regime_key=raw_rk,
    )
    out = {**base, **dil}
    out["verification_window_days"] = dil["final_window_days"]
    out["source"] = "alpha_half_life_dynamic+regime_dilation"
    return out
