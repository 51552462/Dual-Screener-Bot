"""
AceEvolution 일일 갱신 — 콜로세움/에이스 부검 직후 호출.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from evolution.ace_deathmatch_bridge import compute_t1_feedback_win_rate
from evolution.ace_evolution_store import load_playbook, revoke_playbook, save_playbook
from evolution.ace_evolution_synthesizer import synthesize_playbook_from_facts
from evolution.ace_evolution_ttl import evaluate_fast_decay_kr, evaluate_slow_decay_us, is_playbook_expired
from evolution.ace_fact_pack import build_ace_fact_pack
from evolution.ace_playbook_validator import stats_only_playbook

logger = logging.getLogger(__name__)


def _observe_only_flag(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: Optional[str] = None,
    logic_core: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """관측 전용 — 단, registry LIVE 승격 시 자동 해제."""
    if meta and market and logic_core:
        try:
            from strategy_promotion_engine import is_group_live_in_registry

            if is_group_live_in_registry(meta, market, logic_core):
                return False
        except Exception:
            pass
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not bool(cfg.get("ENABLE_ACE_EVOLUTION_WEIGHTING", False)):
        return True
    return bool(cfg.get("ACE_EVOLUTION_FORCE_OBSERVE", True))


def refresh_ace_evolution_for_market(
    *,
    market: str,
    logic_core: str,
    ace_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    feature_insights: List[Any],
    sector_summary: str,
    window_days: int,
    data_anchor: str,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    df_closed_all: Optional[pd.DataFrame] = None,
) -> Tuple[Dict[str, Any], str]:
    """
    단일 시장 Playbook 갱신 + KR T+1 fast-decay 선행 평가(전일 playbook 있을 때).
  Returns (playbook, notes).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    m = str(market).upper()
    observe = _observe_only_flag(
        cfg, market=m, logic_core=logic_core, meta=meta
    )

    # 전일 playbook fast-decay (KR) / slow (US)
    prev = load_playbook(m, cfg)
    if prev.get("logic_core") and not is_playbook_expired(prev, cfg) and df_closed_all is not None:
        as_of = str(prev.get("as_of_kst") or "")[:10]
        if m == "KR" and as_of:
            wr, n = compute_t1_feedback_win_rate(df_closed_all, market=m, as_of_kst=as_of, playbook=prev)
            revoke, reason = evaluate_fast_decay_kr(t1_win_rate_pct=wr, t1_n=n, sys_config=cfg)
            if revoke:
                revoke_playbook(m, reason=reason)
                logger.info("AceEvolution KR revoked: %s", reason)
        elif m == "US" and as_of:
            wr, n = compute_t1_feedback_win_rate(df_closed_all, market=m, as_of_kst=as_of, playbook=prev)
            revoke, reason = evaluate_slow_decay_us(t2_win_rate_pct=wr, t2_n=n, sys_config=cfg)
            if revoke:
                revoke_playbook(m, reason=reason)

    if ace_df is None or ace_df.empty or len(ace_df) < 3 or not logic_core:
        pb = stats_only_playbook(
            build_ace_fact_pack(
                market=m,
                logic_core=logic_core or "—",
                ace_df=ace_df if ace_df is not None else pd.DataFrame(),
                baseline_df=baseline_df if baseline_df is not None else pd.DataFrame(),
                feature_insights=feature_insights or [],
                sector_summary=sector_summary,
                window_days=window_days,
                data_anchor=data_anchor,
                meta=meta,
                sys_config=cfg,
            ),
            observe_only=True,
        )
        save_playbook(pb, validator_notes="insufficient_ace_sample")
        return pb, "insufficient_sample"

    fact = build_ace_fact_pack(
        market=m,
        logic_core=logic_core,
        ace_df=ace_df,
        baseline_df=baseline_df,
        feature_insights=feature_insights or [],
        sector_summary=sector_summary,
        window_days=window_days,
        data_anchor=data_anchor,
        meta=meta,
        sys_config=cfg,
    )

    pb, notes = synthesize_playbook_from_facts(fact, observe_only=observe)
    save_playbook(pb, validator_notes=notes)
    return pb, notes


def refresh_ace_evolution_from_colosseum_context(
    *,
    kr_logic: str,
    us_logic: str,
    kr_ace: pd.DataFrame,
    us_ace: pd.DataFrame,
    kr_baseline: pd.DataFrame,
    us_baseline: pd.DataFrame,
    kr_insights: List[Any],
    us_insights: List[Any],
    kr_sec: str,
    us_sec: str,
    kr_anchor: str,
    us_anchor: str,
    window_days: int,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    df_closed_all: Optional[pd.DataFrame] = None,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if kr_logic:
        pb, _ = refresh_ace_evolution_for_market(
            market="KR",
            logic_core=kr_logic,
            ace_df=kr_ace,
            baseline_df=kr_baseline,
            feature_insights=kr_insights,
            sector_summary=kr_sec,
            window_days=window_days,
            data_anchor=kr_anchor,
            sys_config=sys_config,
            meta=meta,
            df_closed_all=df_closed_all,
        )
        out["KR"] = pb
    if us_logic:
        pb, _ = refresh_ace_evolution_for_market(
            market="US",
            logic_core=us_logic,
            ace_df=us_ace,
            baseline_df=us_baseline,
            feature_insights=us_insights,
            sector_summary=us_sec,
            window_days=window_days,
            data_anchor=us_anchor,
            sys_config=sys_config,
            meta=meta,
            df_closed_all=df_closed_all,
        )
        out["US"] = pb
    return out
