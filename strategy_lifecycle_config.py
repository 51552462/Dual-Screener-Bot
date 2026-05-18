"""
STRATEGY_LIFECYCLE — KR/US/BG 시장별 승격·강등·알파 TTL SSOT (system_config 병합).
"""
from __future__ import annotations

from typing import Any, Dict, Optional

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
        "cooloff_days": 3,
        "whipsaw_below_days": 2,
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
        "cooloff_days": 7,
        "whipsaw_below_days": 3,
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
        "cooloff_days": 5,
        "whipsaw_below_days": 3,
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
