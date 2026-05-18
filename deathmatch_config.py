"""
DEATHMATCH — 시장별 Battle Royal 채점·탈락·폭락 방어 SSOT.
"""
from __future__ import annotations

from typing import Any, Dict

DEFAULT_DEATHMATCH: Dict[str, Any] = {
    "composite_weights": {
        "ret": 0.38,
        "wr": 0.22,
        "pf": 0.20,
        "mdd_penalty": 0.20,
    },
    "mdd_soft_pct": -12.0,
    "mdd_penalty_scale": 8.0,
    "vol_penalty_scale": 0.15,
    "crash_market_mean_pct": -1.25,
    "relative_outperform_buffer_pp": 0.35,
    "bottom_pct": 0.20,
    "lookback_days_kr": 90,
    "lookback_days_us": 120,
}

MARKET_DEATHMATCH_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "KR": {"lookback_days": 90, "crash_market_mean_pct": -1.0},
    "US": {"lookback_days": 120, "crash_market_mean_pct": -1.5},
}


def load_deathmatch_config(sys_config: Dict[str, Any] | None) -> Dict[str, Any]:
    base = dict(DEFAULT_DEATHMATCH)
    if not isinstance(sys_config, dict):
        return base
    raw = sys_config.get("DEATHMATCH")
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k == "composite_weights" and isinstance(v, dict):
                cw = dict(base.get("composite_weights") or {})
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
    return out
