"""
PIL — 시장·랭크별 프로필 (동적 Post-Mortem 윈도우 · Toxic · Vitality 임계).
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class PractitionerMarketProfile:
    market: str
    rank_tier: str
    post_mortem_window_days: int
    post_mortem_min_days: int
    winner_ret_pct: float
    loser_ret_pct: float
    vitality_lookback_days: int
    zombie_vitality_threshold: float
    min_closed_for_post_mortem: int
    toxic_config_key: str
    feature_priority: tuple = ()
    narrative_focus: str = ""


_DEFAULT_PROFILE = PractitionerMarketProfile(
    market="*",
    rank_tier="DEFAULT",
    post_mortem_window_days=30,
    post_mortem_min_days=14,
    winner_ret_pct=5.0,
    loser_ret_pct=-3.0,
    vitality_lookback_days=30,
    zombie_vitality_threshold=0.35,
    min_closed_for_post_mortem=3,
    toxic_config_key="KR",
    feature_priority=("dyn_cpv", "v_energy", "dyn_rs", "entry_breadth"),
    narrative_focus="범용",
)


_PROFILE_TABLE: List[PractitionerMarketProfile] = [
    PractitionerMarketProfile(
        market="KR",
        rank_tier="RANK_C",
        post_mortem_window_days=14,
        post_mortem_min_days=5,
        winner_ret_pct=4.0,
        loser_ret_pct=-2.5,
        vitality_lookback_days=30,
        zombie_vitality_threshold=0.38,
        min_closed_for_post_mortem=2,
        toxic_config_key="KR",
        feature_priority=("v_energy", "dyn_cpv", "sector", "entry_breadth"),
        narrative_focus="단기 테마·수급 교집합",
    ),
    PractitionerMarketProfile(
        market="KR",
        rank_tier="RANK_A",
        post_mortem_window_days=45,
        post_mortem_min_days=21,
        winner_ret_pct=5.0,
        loser_ret_pct=-3.0,
        vitality_lookback_days=45,
        zombie_vitality_threshold=0.35,
        min_closed_for_post_mortem=3,
        toxic_config_key="KR",
        feature_priority=("dyn_rs", "bars_held", "dyn_tb", "entry_cos_score"),
        narrative_focus="장기 매집·추세 지속",
    ),
    PractitionerMarketProfile(
        market="US",
        rank_tier="RANK_A",
        post_mortem_window_days=60,
        post_mortem_min_days=30,
        winner_ret_pct=4.0,
        loser_ret_pct=-2.5,
        vitality_lookback_days=60,
        zombie_vitality_threshold=0.35,
        min_closed_for_post_mortem=3,
        toxic_config_key="US",
        feature_priority=("dyn_rs", "bars_held", "entry_cos_score", "marcap_eok"),
        narrative_focus="US 장기 매집·추세",
    ),
    PractitionerMarketProfile(
        market="US",
        rank_tier="RANK_C",
        post_mortem_window_days=21,
        post_mortem_min_days=10,
        winner_ret_pct=4.5,
        loser_ret_pct=-2.5,
        vitality_lookback_days=30,
        zombie_vitality_threshold=0.38,
        min_closed_for_post_mortem=2,
        toxic_config_key="US",
        feature_priority=("v_energy", "dyn_cpv", "sector", "total_score"),
        narrative_focus="US 단기 모멘텀",
    ),
    PractitionerMarketProfile(
        market="KR",
        rank_tier="DEFAULT",
        post_mortem_window_days=30,
        post_mortem_min_days=14,
        winner_ret_pct=5.0,
        loser_ret_pct=-3.0,
        vitality_lookback_days=30,
        zombie_vitality_threshold=0.35,
        min_closed_for_post_mortem=3,
        toxic_config_key="KR",
        feature_priority=("dyn_cpv", "v_energy", "dyn_rs", "sector"),
        narrative_focus="KR 일반",
    ),
    PractitionerMarketProfile(
        market="BG",
        rank_tier="PRACT",
        post_mortem_window_days=14,
        post_mortem_min_days=7,
        winner_ret_pct=4.0,
        loser_ret_pct=-2.5,
        vitality_lookback_days=30,
        zombie_vitality_threshold=0.38,
        min_closed_for_post_mortem=2,
        toxic_config_key="US",
        feature_priority=("v_energy", "dyn_cpv", "dyn_rs", "total_score"),
        narrative_focus="Bitget PRACT 엔진",
    ),
    PractitionerMarketProfile(
        market="BG_SPOT",
        rank_tier="PRACT",
        post_mortem_window_days=14,
        post_mortem_min_days=7,
        winner_ret_pct=4.0,
        loser_ret_pct=-2.5,
        vitality_lookback_days=30,
        zombie_vitality_threshold=0.38,
        min_closed_for_post_mortem=2,
        toxic_config_key="US",
        feature_priority=("v_energy", "dyn_cpv", "dyn_rs", "total_score"),
        narrative_focus="Bitget Spot PRACT",
    ),
    PractitionerMarketProfile(
        market="BG_FUT",
        rank_tier="PRACT",
        post_mortem_window_days=21,
        post_mortem_min_days=10,
        winner_ret_pct=5.0,
        loser_ret_pct=-3.0,
        vitality_lookback_days=30,
        zombie_vitality_threshold=0.36,
        min_closed_for_post_mortem=2,
        toxic_config_key="US",
        feature_priority=("v_energy", "dyn_cpv", "dyn_rs", "bars_held"),
        narrative_focus="Bitget Futures PRACT",
    ),
    PractitionerMarketProfile(
        market="US",
        rank_tier="DEFAULT",
        post_mortem_window_days=45,
        post_mortem_min_days=21,
        winner_ret_pct=4.0,
        loser_ret_pct=-2.5,
        vitality_lookback_days=45,
        zombie_vitality_threshold=0.35,
        min_closed_for_post_mortem=3,
        toxic_config_key="US",
        feature_priority=("dyn_rs", "v_energy", "dyn_cpv", "bars_held"),
        narrative_focus="US 일반",
    ),
]


def extract_rank_tier(sig_type: object) -> str:
    s = str(sig_type or "").upper()
    for tier in ("RANK_C", "RANK_A", "RANK_B", "RANK_D"):
        if tier in s:
            return tier
    return "DEFAULT"


def resolve_practitioner_profile(
    market: str,
    rank_tier: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> PractitionerMarketProfile:
    mk = str(market or "KR").upper()
    tier = str(rank_tier or "DEFAULT").upper()
    base = None
    for p in _PROFILE_TABLE:
        if p.market == mk and p.rank_tier == tier:
            base = p
            break
    if base is None and tier != "DEFAULT":
        for p in _PROFILE_TABLE:
            if p.market == mk and p.rank_tier == "DEFAULT":
                base = p
                break
    if base is None and mk.startswith("BG_"):
        for p in _PROFILE_TABLE:
            if p.market == "BG" and p.rank_tier == tier:
                base = p
                break
    if base is None and mk.startswith("BG"):
        for p in _PROFILE_TABLE:
            if p.market == "BG" and p.rank_tier == "PRACT":
                base = p
                break
    if base is None:
        base = _DEFAULT_PROFILE

    if not isinstance(sys_config, dict):
        return base
    raw = sys_config.get("PRACTITIONER_MARKET_PROFILES")
    if not isinstance(raw, dict):
        return base
    key = f"{mk}_{tier}"
    ov = raw.get(key) or raw.get(mk)
    if not isinstance(ov, dict):
        return base
    allowed = {f.name for f in PractitionerMarketProfile.__dataclass_fields__.values()}
    patch = {k: v for k, v in ov.items() if k in allowed and v is not None}
    return replace(base, **patch) if patch else base


def toxic_rules_for_profile(
    profile: PractitionerMarketProfile,
    sys_config: Dict[str, Any],
) -> Dict[str, Any]:
    from toxic_antipattern_core import collect_merged_antipattern_rules

    cfg = dict(sys_config)
    if profile.toxic_config_key == "US":
        try:
            import json
            import os

            p = os.path.join(os.path.dirname(__file__), "us_toxic_ml_antipatterns.json")
            if os.path.isfile(p):
                with open(p, "r", encoding="utf-8") as f:
                    cfg["US_TOXIC_ML_ANTIPATTERNS"] = json.load(f)
        except Exception:
            pass
        merged = dict(collect_merged_antipattern_rules(cfg))
        us_only = cfg.get("US_TOXIC_ML_ANTIPATTERNS")
        if isinstance(us_only, dict):
            inner = us_only.get("rules") if isinstance(us_only.get("rules"), dict) else us_only
            if isinstance(inner, dict):
                merged.update({k: v for k, v in inner.items() if k != "_metadata"})
        return merged
    return collect_merged_antipattern_rules(cfg)
