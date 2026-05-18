"""
Registry 기반 N-Way Battle Royal — composite(MDD 패널티) · 상대평가 탈락 면제 · KR/US 챔피언.
"""
from __future__ import annotations

import html
import math
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from deathmatch_config import load_deathmatch_config, market_deathmatch_params
from deathmatch_report import (
    ArmDeathmatchRow,
    NWayDeathmatchResult,
    _effective_final_ret_pct,
    _profit_factor_from_ret,
    deathmatch_min_n_for_market,
    fmt_deathmatch_ret,
    nway_deathmatch_verdict,
)
from deathmatch_store import log_elimination_events, save_battle_royal_result
from strategy_promotion_engine import stable_strategy_id


def ledger_group_key(sig: str) -> str:
    raw = str(sig or "")
    if "[INCUBATOR_" in raw.upper():
        m = re.search(r"\[INCUBATOR_([^\]]+)\]", raw, flags=re.I)
        if m:
            return f"INCUBATOR_{m.group(1).strip()}"
    s = raw.replace("💀[기각/관찰용] ", "").replace("💀[기각] ", "")
    s = re.sub(r"^\[.*?\]\s*", "", s)
    return (s.split(" [")[0].strip() or "UNKNOWN")


def mdd_pct_from_returns(rets: List[float]) -> float:
    if len(rets) < 2:
        return 0.0
    eq = 1.0
    peak = 1.0
    worst_dd = 0.0
    for r in rets:
        eq *= 1.0 + float(r) / 100.0
        peak = max(peak, eq)
        if peak > 0:
            dd = (eq - peak) / peak * 100.0
            worst_dd = min(worst_dd, dd)
    return float(worst_dd)


def _zscore(vals: List[float]) -> List[float]:
    if len(vals) < 2:
        return [0.0] * len(vals)
    arr = np.asarray(vals, dtype=float)
    mu = float(np.nanmean(arr))
    sd = float(np.nanstd(arr))
    if sd < 1e-9:
        return [0.0] * len(vals)
    return [float((x - mu) / sd) for x in arr]


def _finite_or_none(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


@dataclass
class RegistryArmRow:
    arm_id: str
    label: str
    group_key: str
    registry_state: str
    arm_kind: str = "REGISTRY"
    n_closed: int = 0
    n_valid: int = 0
    mean_ret: Optional[float] = None
    win_rate_pct: Optional[float] = None
    profit_factor: Optional[float] = None
    mdd_pct: float = 0.0
    vol_pct: float = 0.0
    composite_score: float = 0.0
    rank: int = 0
    below_floor: bool = False
    relative_exempt: bool = False


@dataclass
class BattleRoyaleResult:
    market: str
    arms: List[RegistryArmRow] = field(default_factory=list)
    champion: Optional[RegistryArmRow] = None
    verdict: str = ""
    allocation_note: str = ""
    n_min: int = 5
    market_benchmark_ret: Optional[float] = None
    crash_defense_active: bool = False
    elimination_events: List[Dict[str, Any]] = field(default_factory=list)
    run_id: str = ""


def _arm_metrics_from_df(df_arm: pd.DataFrame) -> Tuple[int, int, Optional[float], Optional[float], Optional[float], float, float]:
    n_closed = int(len(df_arm))
    ret_s = _effective_final_ret_pct(df_arm)
    valid = ret_s.dropna()
    n_valid = int(len(valid))
    mean_ret = win_rate = pf = None
    mdd = vol = 0.0
    if n_valid > 0:
        m = float(valid.mean())
        if math.isfinite(m):
            mean_ret = m
        win_rate = float((valid > 0).sum() / n_valid * 100.0)
        pf = _profit_factor_from_ret(valid)
        rets = valid.tolist()
        mdd = mdd_pct_from_returns(rets)
        vol = float(valid.std()) if n_valid > 1 else 0.0
    return n_closed, n_valid, mean_ret, win_rate, pf, mdd, vol


def _compute_composite_scores(
    arms: List[RegistryArmRow],
    dmcfg: Dict[str, Any],
) -> None:
    weights = dmcfg.get("composite_weights") or {}
    w_ret = float(weights.get("ret", 0.38))
    w_wr = float(weights.get("wr", 0.22))
    w_pf = float(weights.get("pf", 0.20))
    w_mdd = float(weights.get("mdd_penalty", 0.20))
    mdd_soft = float(dmcfg.get("mdd_soft_pct", -12.0))
    mdd_scale = max(1.0, float(dmcfg.get("mdd_penalty_scale", 8.0)))
    vol_scale = float(dmcfg.get("vol_penalty_scale", 0.15))

    eligible = [a for a in arms if a.n_valid > 0 and a.mean_ret is not None and math.isfinite(a.mean_ret)]
    if not eligible:
        return

    rets = [float(a.mean_ret) for a in eligible]
    wrs = [float(a.win_rate_pct or 0) for a in eligible]
    pfs = [math.log1p(max(0, float(a.profit_factor or 0))) for a in eligible]

    z_ret = _zscore(rets)
    z_wr = _zscore(wrs)
    z_pf = _zscore(pfs)

    for i, a in enumerate(eligible):
        mdd_pen = 0.0
        if a.mdd_pct < mdd_soft:
            mdd_pen = (abs(a.mdd_pct) - abs(mdd_soft)) / mdd_scale
        vol_pen = (float(a.vol_pct) / 100.0) * vol_scale if a.vol_pct else 0.0
        a.composite_score = (
            w_ret * z_ret[i] + w_wr * z_wr[i] + w_pf * z_pf[i] - w_mdd * mdd_pen - vol_pen
        )


def _market_benchmark(df_closed: pd.DataFrame) -> Optional[float]:
    if df_closed is None or df_closed.empty:
        return None
    ret = _effective_final_ret_pct(df_closed).dropna()
    if ret.empty:
        return None
    m = float(ret.mean())
    return m if math.isfinite(m) else None


def _assign_ranks_and_elimination(
    arms: List[RegistryArmRow],
    *,
    n_min: int,
    dmcfg: Dict[str, Any],
    market_benchmark: Optional[float],
) -> Tuple[bool, List[Dict[str, Any]]]:
    bottom_pct = float(dmcfg.get("bottom_pct", 0.20))
    crash_thr = float(dmcfg.get("crash_market_mean_pct", -1.25))
    rel_buf = float(dmcfg.get("relative_outperform_buffer_pp", 0.35))

    crash_active = (
        market_benchmark is not None
        and math.isfinite(market_benchmark)
        and market_benchmark <= crash_thr
    )

    ranked = [
        a
        for a in arms
        if a.n_valid >= n_min and a.mean_ret is not None and math.isfinite(a.mean_ret)
    ]
    ranked.sort(key=lambda x: float(x.composite_score), reverse=True)
    for i, a in enumerate(ranked, start=1):
        a.rank = i

    unranked = [a for a in arms if a not in ranked]
    for a in unranked:
        a.rank = 999

    events: List[Dict[str, Any]] = []
    if len(ranked) < 2:
        return crash_active, events

    n_bottom = max(1, int(math.ceil(len(ranked) * bottom_pct)))
    bottom_set = set(id(a) for a in ranked[-n_bottom:])

    for a in ranked:
        a.below_floor = id(a) in bottom_set
        a.relative_exempt = False
        if a.below_floor and crash_active and market_benchmark is not None:
            if a.mean_ret is not None and float(a.mean_ret) >= float(market_benchmark) + rel_buf:
                a.relative_exempt = True
                a.below_floor = False
        if a.below_floor:
            events.append(
                {
                    "arm_id": a.arm_id,
                    "prior_rank": a.rank,
                    "reason": "bottom_quartile_composite",
                    "proposed_action": "STANDBY",
                    "relative_exempt": False,
                }
            )

    arms.sort(key=lambda x: (x.rank if x.rank < 999 else 9999, -float(x.composite_score)))
    return crash_active, events


def run_battle_royal(
    df_closed: pd.DataFrame,
    sys_config: Optional[dict] = None,
    *,
    market: str,
    lookback_days: Optional[int] = None,
    persist: bool = True,
) -> BattleRoyaleResult:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    dm_root = load_deathmatch_config(cfg)
    dmcfg = market_deathmatch_params(dm_root, market)
    mk = str(market or "KR").upper()
    run_id = str(uuid.uuid4())[:12]
    n_closed_total = len(df_closed) if df_closed is not None else 0
    n_min = deathmatch_min_n_for_market(cfg, mk, n_closed=n_closed_total)

    out = BattleRoyaleResult(market=mk, n_min=n_min, run_id=run_id)

    if df_closed is None or df_closed.empty:
        out.verdict = "청산 표본 없음 — Battle Royal 보류"
        return out

    work = df_closed.copy()
    lb = lookback_days if lookback_days is not None else int(dmcfg.get("lookback_days", 90))
    if lb > 0 and "exit_date" in work.columns:
        cutoff = (pd.Timestamp.now() - pd.Timedelta(days=lb)).strftime("%Y-%m-%d")
        work = work[work["exit_date"].astype(str) >= cutoff]

    bench = _market_benchmark(work)
    out.market_benchmark_ret = bench

    from strategy_registry_store import load_registry_rows

    reg_rows = load_registry_rows()
    reg_mkt = [r for r in reg_rows if str(r.get("market") or "KR").upper() == mk]

    by_arm: Dict[str, List[int]] = {}
    arm_meta: Dict[str, Dict[str, Any]] = {}

    for r in reg_mkt:
        gk = str(r.get("group_key") or "").strip()
        if not gk:
            continue
        sid = str(r.get("strategy_id") or stable_strategy_id(mk, gk))
        arm_meta[sid] = {
            "label": str(r.get("display_name") or gk),
            "group_key": gk,
            "registry_state": str(r.get("state") or "OBSERVING").upper(),
        }
        by_arm.setdefault(sid, [])

    if "sig_type" in work.columns:
        for idx, row in work.iterrows():
            gk = ledger_group_key(str(row.get("sig_type") or ""))
            sid = stable_strategy_id(mk, gk)
            if sid not in arm_meta:
                arm_meta[sid] = {
                    "label": gk,
                    "group_key": gk,
                    "registry_state": "UNREGISTERED",
                }
            by_arm.setdefault(sid, []).append(idx)

    arms: List[RegistryArmRow] = []
    for sid, meta in arm_meta.items():
        idxs = by_arm.get(sid, [])
        df_arm = work.loc[idxs] if idxs else pd.DataFrame()
        nc, nv, mr, wr, pf, mdd, vol = _arm_metrics_from_df(df_arm)
        arms.append(
            RegistryArmRow(
                arm_id=sid,
                label=str(meta["label"]),
                group_key=str(meta["group_key"]),
                registry_state=str(meta["registry_state"]),
                n_closed=nc,
                n_valid=nv,
                mean_ret=mr,
                win_rate_pct=wr,
                profit_factor=pf,
                mdd_pct=mdd,
                vol_pct=vol,
            )
        )

    if not arms:
        out.verdict = "Registry·청산 매핑 arm 0 — Discovery 후 재실행"
        return out

    _compute_composite_scores(arms, dmcfg)
    crash_active, elim_events = _assign_ranks_and_elimination(
        arms,
        n_min=n_min,
        dmcfg=dmcfg,
        market_benchmark=bench,
    )
    out.crash_defense_active = crash_active
    out.elimination_events = elim_events
    out.arms = arms

    ranked_ok = [a for a in arms if a.rank < 999]
    if ranked_ok:
        champ = min(ranked_ok, key=lambda x: x.rank)
        out.champion = champ

    legacy_rows = [
        ArmDeathmatchRow(
            label=a.label,
            n_closed=a.n_closed,
            n_valid=a.n_valid,
            mean_ret=a.mean_ret,
            win_rate_pct=a.win_rate_pct,
            profit_factor=a.profit_factor,
            rank=a.rank if a.rank < 999 else 0,
        )
        for a in (sorted(ranked_ok, key=lambda x: x.rank) if ranked_ok else arms[:8])
    ]
    out.verdict = _battle_royal_verdict(out, legacy_rows, n_min)
    out.allocation_note = _allocation_note(out, n_min, cfg)

    if persist:
        arm_dicts = [
            {
                "arm_id": a.arm_id,
                "arm_kind": a.arm_kind,
                "registry_state": a.registry_state,
                "label": a.label,
                "n_closed": a.n_closed,
                "n_valid": a.n_valid,
                "mean_ret": a.mean_ret,
                "win_rate_pct": a.win_rate_pct,
                "profit_factor": a.profit_factor,
                "mdd_pct": a.mdd_pct,
                "vol_pct": a.vol_pct,
                "composite_score": a.composite_score,
                "rank": a.rank,
                "below_floor": a.below_floor,
                "relative_exempt": a.relative_exempt,
            }
            for a in arms
        ]
        champ_d = None
        if out.champion:
            c = out.champion
            champ_d = {
                "arm_id": c.arm_id,
                "label": c.label,
                "registry_state": c.registry_state,
                "mean_ret": c.mean_ret,
                "win_rate_pct": c.win_rate_pct,
                "composite_score": c.composite_score,
                "n_valid": c.n_valid,
            }
        save_battle_royal_result(mk, arm_dicts, champ_d, run_id=run_id)
        log_elimination_events(mk, elim_events)

    return out


def _battle_royal_verdict(
    br: BattleRoyaleResult,
    legacy_arms: List[ArmDeathmatchRow],
    n_min: int,
) -> str:
    base = nway_deathmatch_verdict(legacy_arms, n_min)
    extra = []
    if br.champion:
        c = br.champion
        ret_s = fmt_deathmatch_ret(c.mean_ret, c.n_closed, n_valid=c.n_valid)
        extra.append(
            f"🏆 <b>{html.escape(br.market, quote=False)} 챔피언</b>: "
            f"{html.escape(c.label, quote=False)} ({c.registry_state}) "
            f"{ret_s} · score {c.composite_score:+.2f} · MDD {c.mdd_pct:.1f}%"
        )
    if br.crash_defense_active and br.market_benchmark_ret is not None:
        extra.append(
            f"🛡️ 폭락 방어: 시장 평균 {br.market_benchmark_ret:+.2f}% — "
            f"상대 선방 arm 탈락 면제 적용"
        )
    n_elim = sum(1 for a in br.arms if a.below_floor)
    n_exempt = sum(1 for a in br.arms if a.relative_exempt)
    if n_elim or n_exempt:
        extra.append(f"📉 탈락 후보 {n_elim} · 상대평가 면제 {n_exempt}")
    if extra:
        return base + " | " + " · ".join(extra)
    return base


def _allocation_note(br: BattleRoyaleResult, n_min: int, cfg: dict) -> str:
    from deathmatch_report import format_allocation_proposal_note

    legacy = [
        ArmDeathmatchRow(
            label=a.label,
            n_closed=a.n_closed,
            n_valid=a.n_valid,
            mean_ret=a.mean_ret,
            win_rate_pct=a.win_rate_pct,
            profit_factor=a.profit_factor,
            rank=a.rank,
        )
        for a in br.arms
        if a.rank < 999
    ]
    return format_allocation_proposal_note(legacy, n_min, cfg)


def battle_royal_to_nway(br: BattleRoyaleResult) -> NWayDeathmatchResult:
    """레거시 NWayDeathmatchResult 호환."""
    arms = [
        ArmDeathmatchRow(
            label=a.label,
            n_closed=a.n_closed,
            n_valid=a.n_valid,
            mean_ret=a.mean_ret,
            win_rate_pct=a.win_rate_pct,
            profit_factor=a.profit_factor,
            rank=a.rank if a.rank < 999 else len(br.arms) + 1,
        )
        for a in sorted(br.arms, key=lambda x: (x.rank, -x.composite_score))
    ]
    return NWayDeathmatchResult(
        arms=arms,
        verdict=br.verdict,
        allocation_note=br.allocation_note,
        n_min=br.n_min,
    )


def build_nway_deathmatch_registry(
    df_closed: pd.DataFrame,
    sys_config: Optional[dict] = None,
    *,
    lookback_days: Optional[int] = None,
    market: Optional[str] = None,
) -> Tuple[BattleRoyaleResult, NWayDeathmatchResult]:
    br = run_battle_royal(
        df_closed,
        sys_config,
        market=str(market or "KR").upper(),
        lookback_days=lookback_days,
        persist=True,
    )
    return br, battle_royal_to_nway(br)


def format_battle_royal_telegram(
    market_icon: str,
    br: BattleRoyaleResult,
    *,
    lookback_label: str = "전체 청산",
    ace_oneliner: str = "",
) -> str:
    mk_label = "KR" if br.market == "KR" else "US" if br.market == "US" else br.market
    lines = [
        f"{market_icon} <b>[9/9] 시스템 데스매치 결산 — {mk_label} Battle Royal</b>",
        f"📎 {html.escape(lookback_label, quote=False)} · arm당 유효 최소 <b>{br.n_min}</b>건",
    ]
    if br.market_benchmark_ret is not None and math.isfinite(br.market_benchmark_ret):
        lines.append(f"📊 시장 청산 벤치마크 평균: <b>{br.market_benchmark_ret:+.2f}%</b>")
    lines.append("")

    if br.champion:
        c = br.champion
        ret_s = fmt_deathmatch_ret(c.mean_ret, c.n_closed, n_valid=c.n_valid)
        wr_s = f"{c.win_rate_pct:.1f}%" if c.win_rate_pct is not None else "—"
        pf_s = f"{c.profit_factor:.2f}" if c.profit_factor is not None else "—"
        lines.append(
            f"🏆 <b>{mk_label} 챔피언 전략</b>: {html.escape(c.label, quote=False)} "
            f"<i>({html.escape(c.registry_state, quote=False)})</i>\n"
            f"   {ret_s} · 승률 {wr_s} · PF {pf_s} · "
            f"MDD {c.mdd_pct:.1f}% · score <b>{c.composite_score:+.2f}</b>"
        )
        lines.append("")

    lines.append("<b>📋 Registry N-Way 랭킹 (복합 점수 · MDD 패널티 반영)</b>")
    ranked = [a for a in br.arms if a.rank < 999]
    ranked.sort(key=lambda x: x.rank)
    if not ranked:
        lines.append(" ↳ 유효 표본 충족 arm 없음")
    else:
        for a in ranked[:12]:
            ret_s = fmt_deathmatch_ret(a.mean_ret, a.n_closed, n_valid=a.n_valid)
            wr_s = f"{a.win_rate_pct:.1f}%" if a.win_rate_pct is not None else "—"
            icon = "🥇" if a.rank == 1 else f"{a.rank}."
            flags = ""
            if a.below_floor:
                flags = " 📉탈락후보"
            if a.relative_exempt:
                flags = " 🛡️상대면제"
            lines.append(
                f" {icon} <b>{html.escape(a.label, quote=False)}</b>"
                f" <i>({a.registry_state})</i>{flags}\n"
                f"    {ret_s} · N={a.n_closed}(유효{a.n_valid}) · WR {wr_s} · "
                f"MDD {a.mdd_pct:.1f}% · <b>score {a.composite_score:+.2f}</b>"
            )

    observing = [a for a in br.arms if a.rank >= 999 and a.n_closed > 0]
    if observing:
        lines.append(f"\n<i>⚠️ 관망 {len(observing)} arm — 유효 수익률·표본 미달</i>")

    lines.append("")
    lines.append(f"💡 <b>결론:</b> {br.verdict}")
    if br.allocation_note:
        lines.append("")
        lines.append(br.allocation_note)
    if ace_oneliner:
        lines.append("")
        lines.append(ace_oneliner)
    return "\n".join(lines) + "\n"
