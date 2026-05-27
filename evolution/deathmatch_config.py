"""
DEATHMATCH — 시장별 Battle Royal · Composite v2 · 절대 허들 · MDD 지수 패널티 SSOT.
"""
from __future__ import annotations

from typing import Any, Dict

DEFAULT_DEATHMATCH: Dict[str, Any] = {
    "composite_weights": {
        "ret": 0.22,
        "wr": 0.10,
        "pf": 0.10,
        "expectancy": 0.08,
        "kelly_sub": 0.05,
        "oos_bonus": 0.03,
        "mdd_penalty": 0.32,
        "vol_penalty": 0.10,
    },
    "mdd_soft_pct": -10.0,
    "mdd_exp_threshold_pct": -15.0,
    "mdd_exp_scale": 5.0,
    "mdd_exp_base": 1.45,
    "vol_penalty_scale": 0.12,
    "crash_market_mean_pct": -1.25,
    "relative_outperform_buffer_pp": 0.35,
    "absolute_hurdle_min_ret": 0.0,
    "absolute_outperform_buffer_pp": 0.25,
    "absolute_hurdle_z_bonus": 0.35,
    "absolute_hurdle_fail_penalty": 5.0,
    "meta_mult_disqualify_below": 0.05,
    "bottom_pct": 0.20,
    "allocation_standby_mult": 0.0,
    "allocation_hurdle_fail_mult": 0.0,
    "allocation_neutral_mult": 1.0,
    "allocation_top_boost_mult": 1.25,
    "allocation_champion_mult": 1.35,
    "allocation_max_group_mult": 1.5,
    "lookback_days_kr": 90,
    "lookback_days_us": 120,
}

MARKET_DEATHMATCH_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "KR": {
        "lookback_days": 90,
        "crash_market_mean_pct": -1.0,
        "mdd_exp_threshold_pct": -14.0,
        "absolute_outperform_buffer_pp": 0.20,
    },
    "US": {
        "lookback_days": 120,
        "crash_market_mean_pct": -1.5,
        "mdd_exp_threshold_pct": -16.0,
        "absolute_outperform_buffer_pp": 0.30,
    },
    "BG": {
        "lookback_days": 90,
        "mdd_exp_threshold_pct": -18.0,
    },
}


def load_deathmatch_config(sys_config: Dict[str, Any] | None) -> Dict[str, Any]:
    base = dict(DEFAULT_DEATHMATCH)
    cw = dict(base.get("composite_weights") or {})
    base["composite_weights"] = cw
    if not isinstance(sys_config, dict):
        return base
    raw = sys_config.get("DEATHMATCH")
    if not isinstance(raw, dict):
        return base
    for k, v in raw.items():
        if k == "composite_weights" and isinstance(v, dict):
            cw.update(v)
            base["composite_weights"] = cw
        else:
            base[k] = v
    return base


def market_deathmatch_params(cfg: Dict[str, Any], market: str) -> Dict[str, Any]:
    out = dict(cfg)
    mk = str(market or "KR").upper()
    ov = MARKET_DEATHMATCH_OVERRIDES.get(mk, {})
    out.update(ov)
    if mk == "KR":
        out.setdefault("lookback_days", int(cfg.get("lookback_days_kr", 90)))
    elif mk == "US":
        out.setdefault("lookback_days", int(cfg.get("lookback_days_us", 120)))
    else:
        out.setdefault("lookback_days", int(cfg.get("lookback_days_kr", 90)))
    return out
