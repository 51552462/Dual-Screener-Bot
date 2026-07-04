"""
Registry N-Way Battle Royal — Scorecard L1~L4 · Composite v2 · 절대 허들 · KR/US 챔피언.
"""
from __future__ import annotations

import html
import math
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from evolution.deathmatch_config import load_deathmatch_config, market_deathmatch_params
from evolution.deathmatch_report import (
    ArmDeathmatchRow,
    NWayDeathmatchResult,
    _effective_final_ret_pct,
    deathmatch_min_n_for_market,
    fmt_deathmatch_ret,
    nway_deathmatch_verdict,
)
from evolution.deathmatch_scorecard import (
    ArmScorecard,
    attach_meta_health,
    attach_oos_from_mutants,
    build_arm_scorecard_from_df,
    compute_composite_v2,
    scorecard_to_dict,
)
from evolution.deathmatch_store import log_elimination_events, save_battle_royal_result
from strategy_promotion_engine import stable_strategy_id


def ledger_group_key(sig: str) -> str:
    raw = str(sig or "")
    if "[INCUBATOR_" in raw.upper():
        m = re.search(r"\[INCUBATOR_([^\]]+)\]", raw, flags=re.I)
        if m:
            return f"INCUBATOR_{m.group(1).strip()}"
    s = raw.replace("💀[기각/관찰용] ", "").replace("💀[기각] ", "")
    s = re.sub(r"^\[.*?\]\s*", "", s)
    base = (s.split(" [")[0].strip() or "UNKNOWN")
    # 선취매 버프 해시태그가 base 에 섞여 들어오는 엣지케이스 방어(브래킷 없는 시그널)
    base = (base.split(" #")[0].strip() or "UNKNOWN")

    # [선취매 Arm 데스매치 독립] 버프 시그널을 모(母)전략과 분리해, 데스매치/MAB 에서
    # "일반 로직" vs "선취매 버프 로직"이 별도 Arm 으로 경쟁하도록 접미사를 박제한다.
    #   · #순환매_선취매      → _PREBUY
    #   · [🌐스필오버 선취매]  → _SPILLOVER
    # 둘 다 보유 시 _PREBUY_SPILLOVER 로 완전 분리(결정적 순서).
    suffix = ""
    if "#순환매_선취매" in raw:
        suffix += "_PREBUY"
    if "스필오버 선취매" in raw:
        suffix += "_SPILLOVER"
    return base + suffix


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


@dataclass
class RegistryArmRow:
    """Telegram/레거시 호환 — 내부는 ArmScorecard."""
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
    sum_ret: Optional[float] = None
    expectancy: Optional[float] = None
    kelly_path_ret: Optional[float] = None
    meta_mult: float = 1.0
    tail_loss_streak: int = 0
    outperform_pp: Optional[float] = None
    hurdle_passed: bool = False
    hurdle_reason: str = ""
    champion_eligible: bool = False
    score_breakdown: Dict[str, float] = field(default_factory=dict)


def _scorecard_to_registry(sc: ArmScorecard) -> RegistryArmRow:
    return RegistryArmRow(
        arm_id=sc.arm_id,
        label=sc.label,
        group_key=sc.group_key,
        registry_state=sc.registry_state,
        n_closed=sc.n_closed,
        n_valid=sc.n_valid,
        mean_ret=sc.mean_ret,
        win_rate_pct=sc.win_rate_pct,
        profit_factor=sc.profit_factor,
        mdd_pct=sc.mdd_pct,
        vol_pct=sc.vol_pct,
        composite_score=sc.composite_score,
        rank=sc.rank,
        below_floor=sc.below_floor,
        relative_exempt=sc.relative_exempt,
        sum_ret=sc.sum_ret,
        expectancy=sc.expectancy,
        kelly_path_ret=sc.kelly_path_ret,
        meta_mult=sc.meta_mult,
        tail_loss_streak=sc.tail_loss_streak,
        outperform_pp=sc.outperform_pp,
        hurdle_passed=sc.hurdle_passed,
        hurdle_reason=sc.hurdle_reason,
        champion_eligible=sc.champion_eligible,
        score_breakdown=dict(sc.score_breakdown),
    )


def _mdd_for_df(df_arm: pd.DataFrame) -> float:
    if df_arm is None or df_arm.empty:
        return 0.0
    ret_s = _effective_final_ret_pct(df_arm).dropna()
    if ret_s.empty:
        return 0.0
    return mdd_pct_from_returns(ret_s.tolist())


def _market_benchmark(df_closed: pd.DataFrame) -> Optional[float]:
    if df_closed is None or df_closed.empty:
        return None
    ret = _effective_final_ret_pct(df_closed).dropna()
    if ret.empty:
        return None
    m = float(ret.mean())
    return m if math.isfinite(m) else None


def _load_meta_health(sys_config: Optional[dict]) -> Dict[str, Any]:
    try:
        from meta_governor_consumer import load_meta_state_resolved

        meta = load_meta_state_resolved()
        h = meta.get("META_STRATEGY_HEALTH")
        return h if isinstance(h, dict) else {}
    except Exception:
        return {}


def _assign_ranks_and_elimination(
    scorecards: List[ArmScorecard],
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
        for a in scorecards
        if a.n_valid >= n_min
        and a.mean_ret is not None
        and math.isfinite(float(a.mean_ret))
    ]
    ranked.sort(key=lambda x: float(x.composite_score), reverse=True)
    for i, a in enumerate(ranked, start=1):
        a.rank = i

    for a in scorecards:
        if a not in ranked:
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

    return crash_active, events


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
    n_hurdle_passed: int = 0
    n_champion_eligible: int = 0


def run_battle_royal(
    df_closed: pd.DataFrame,
    sys_config: Optional[dict] = None,
    *,
    market: str,
    lookback_days: Optional[int] = None,
    window_pre_sliced: bool = False,
    meta_health: Optional[Dict[str, Any]] = None,
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
    # DailyReportContext 슬라이스 입력 시 2차 now()-90d 컷 금지 (이중 잣대 제거)
    if not window_pre_sliced:
        lb = lookback_days if lookback_days is not None else int(dmcfg.get("lookback_days", 90))
        if lb > 0 and "exit_date" in work.columns:
            cutoff = (pd.Timestamp.now() - pd.Timedelta(days=lb)).strftime("%Y-%m-%d")
            work = work[work["exit_date"].astype(str) >= cutoff]

    bench = _market_benchmark(work)
    out.market_benchmark_ret = bench

    health = meta_health if isinstance(meta_health, dict) else _load_meta_health(cfg)

    from strategy_registry_store import load_registry_rows

    reg_rows = load_registry_rows()
    # [시장 키 동기화] Bitget 은 SPOT/FUT 서브마켓으로 데스매치를 돌리지만
    # MetaGovernor/strategy_registry 는 스팟+선물을 "BG" 로 통합 등록한다.
    # mk in ("SPOT","FUT") 인 경우 "BG" 로 저장된 레지스트리 행도 매칭시켜
    # registry_state 가 항상 UNREGISTERED 로만 보이는 불일치를 제거한다.
    _reg_mk_aliases = {mk, "BG"} if mk in ("SPOT", "FUT") else {mk}
    reg_mkt = [r for r in reg_rows if str(r.get("market") or "KR").upper() in _reg_mk_aliases]

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

    scorecards: List[ArmScorecard] = []
    for sid, meta in arm_meta.items():
        idxs = by_arm.get(sid, [])
        df_arm = work.loc[idxs] if idxs else pd.DataFrame()
        mdd = _mdd_for_df(df_arm)
        sc = build_arm_scorecard_from_df(
            sid,
            str(meta["label"]),
            str(meta["group_key"]),
            str(meta["registry_state"]),
            df_arm,
            mdd_pct=mdd,
        )
        scorecards.append(sc)

    if not scorecards:
        out.verdict = "Registry·청산 매핑 arm 0 — Discovery 후 재실행"
        return out

    attach_meta_health(scorecards, health, mk)
    attach_oos_from_mutants(scorecards, cfg)
    compute_composite_v2(scorecards, dmcfg, market_benchmark=bench)

    crash_active, elim_events = _assign_ranks_and_elimination(
        scorecards, n_min=n_min, dmcfg=dmcfg, market_benchmark=bench
    )
    out.crash_defense_active = crash_active
    out.elimination_events = elim_events
    out.n_hurdle_passed = sum(1 for s in scorecards if s.hurdle_passed)
    out.n_champion_eligible = sum(1 for s in scorecards if s.champion_eligible)

    arms = [_scorecard_to_registry(s) for s in scorecards]
    out.arms = arms

    eligible_champs = [
        s for s in scorecards if s.champion_eligible and s.rank < 999
    ]
    if eligible_champs:
        best = min(eligible_champs, key=lambda x: x.rank)
        out.champion = _scorecard_to_registry(best)
    else:
        ranked_ok = [s for s in scorecards if s.rank < 999]
        if ranked_ok and bench is not None and bench < 0:
            out.champion = None
        elif ranked_ok:
            best = min(ranked_ok, key=lambda x: x.rank)
            if best.hurdle_passed:
                out.champion = _scorecard_to_registry(best)

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
        for a in out.arms
        if a.rank < 999
    ]
    out.verdict = _battle_royal_verdict(out, legacy_rows, n_min)
    out.allocation_note = _allocation_note(out, n_min, cfg)

    if persist:
        arm_dicts = [scorecard_to_dict(s) for s in scorecards]
        for d, a in zip(arm_dicts, arms):
            d["arm_kind"] = "REGISTRY"
            d["below_floor"] = a.below_floor
            d["relative_exempt"] = a.relative_exempt
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
    elif br.n_champion_eligible == 0 and br.n_hurdle_passed == 0:
        extra.append(
            "⛔ <b>절대 허들 미통과</b> — 전 arm 마이너스·벤치 미달, 챔피언 공석"
        )
    if br.crash_defense_active and br.market_benchmark_ret is not None:
        extra.append(
            f"🛡️ 폭락 방어: 시장 평균 {br.market_benchmark_ret:+.2f}% · "
            f"허들통과 {br.n_hurdle_passed} · 챔피언후보 {br.n_champion_eligible}"
        )
    n_elim = sum(1 for a in br.arms if a.below_floor)
    n_exempt = sum(1 for a in br.arms if a.relative_exempt)
    if n_elim or n_exempt:
        extra.append(f"📉 탈락 후보 {n_elim} · 상대면제 {n_exempt}")
    if extra:
        return base + " | " + " · ".join(extra)
    return base


def _allocation_note(br: BattleRoyaleResult, n_min: int, cfg: dict) -> str:
    from evolution.deathmatch_report import format_allocation_proposal_note

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
    window_pre_sliced: bool = False,
    market: Optional[str] = None,
    meta_health: Optional[Dict[str, Any]] = None,
) -> Tuple[BattleRoyaleResult, NWayDeathmatchResult]:
    br = run_battle_royal(
        df_closed,
        sys_config,
        market=str(market or "KR").upper(),
        lookback_days=lookback_days,
        window_pre_sliced=window_pre_sliced,
        meta_health=meta_health,
        persist=True,
    )
    return br, battle_royal_to_nway(br)


def format_battle_royal_telegram(
    market_icon: str,
    br: BattleRoyaleResult,
    *,
    lookback_label: str = "전체 청산",
    ace_oneliner: str = "",
    include_title: bool = True,
    ranked_empty_note: Optional[str] = None,
) -> str:
    mk_label = "KR" if br.market == "KR" else "US" if br.market == "US" else br.market
    lines: list[str] = []
    if include_title:
        lines.append(
            f"{market_icon} <b>[9/9] 시스템 데스매치 — {mk_label} Full Scorecard</b>"
        )
    lines.extend([
        f"📎 {html.escape(lookback_label, quote=False)} · arm당 유효 최소 <b>{br.n_min}</b>건",
        "<i>Composite v2 · 지수 MDD · 절대허들(0%↑ 또는 벤치 초과) · Meta mult 연동</i>",
    ])
    if br.market_benchmark_ret is not None and math.isfinite(br.market_benchmark_ret):
        lines.append(f"📊 시장 벤치마크: <b>{br.market_benchmark_ret:+.2f}%</b>")
    lines.append(
        f"🧱 절대 허들: 통과 <b>{br.n_hurdle_passed}</b> · 챔피언 후보 <b>{br.n_champion_eligible}</b>"
    )
    lines.append("")

    if br.champion:
        c = br.champion
        ret_s = fmt_deathmatch_ret(c.mean_ret, c.n_closed, n_valid=c.n_valid)
        wr_s = f"{c.win_rate_pct:.1f}%" if c.win_rate_pct is not None else "—"
        pf_s = f"{c.profit_factor:.2f}" if c.profit_factor is not None else "—"
        exp_s = f"{c.expectancy:+.2f}" if c.expectancy is not None else "—"
        kelly_s = f"{c.kelly_path_ret:+.2f}%" if c.kelly_path_ret is not None else "—"
        op_s = f"{c.outperform_pp:+.2f}%p" if c.outperform_pp is not None else "—"
        bd = c.score_breakdown or {}
        mdd_exp = bd.get("mdd_exp_pen", 0)
        lines.append(
            f"🏆 <b>{mk_label} 챔피언</b>: {html.escape(c.label, quote=False)} "
            f"<i>({html.escape(c.registry_state, quote=False)})</i>\n"
            f"   {ret_s} · WR {wr_s} · PF {pf_s} · MDD {c.mdd_pct:.1f}% "
            f"(exp MDD−{mdd_exp:.2f})\n"
            f"   기대값 {exp_s} · Kelly경로 {kelly_s} · vs벤치 {op_s} · "
            f"meta×{c.meta_mult:.2f} · <b>score {c.composite_score:+.2f}</b>"
        )
        lines.append("")
    else:
        lines.append(
            f"🏆 <b>{mk_label} 챔피언</b>: <i>공석 — 절대 허들·meta mult 미충족</i>\n"
        )

    lines.append("<b>📋 N-Way Full Scorecard</b>")
    ranked = [a for a in br.arms if a.rank < 999]
    ranked.sort(key=lambda x: x.rank)
    if not ranked:
        lines.append(ranked_empty_note or " ↳ 유효 표본 충족 arm 없음")
    else:
        for a in ranked[:12]:
            ret_s = fmt_deathmatch_ret(a.mean_ret, a.n_closed, n_valid=a.n_valid)
            wr_s = f"{a.win_rate_pct:.1f}%" if a.win_rate_pct is not None else "—"
            icon = "🥇" if a.rank == 1 else f"{a.rank}."
            flags = ""
            if not a.hurdle_passed:
                flags += " ⛔허들"
            elif a.champion_eligible:
                flags += " ✅후보"
            if a.below_floor:
                flags += " 📉탈락"
            if a.relative_exempt:
                flags += " 🛡️면제"
            bd = a.score_breakdown or {}
            lines.append(
                f" {icon} <b>{html.escape(a.label, quote=False)}</b>"
                f" <i>({a.registry_state})</i>{flags}\n"
                f"    {ret_s} · WR {wr_s} · MDD {a.mdd_pct:.1f}% "
                f"(pen {bd.get('mdd_exp_pen', 0):.2f}) · meta×{a.meta_mult:.2f} · "
                f"<b>score {a.composite_score:+.2f}</b>"
            )

    observing = [a for a in br.arms if a.rank >= 999 and a.n_closed > 0]
    if observing:
        lines.append(f"\n<i>⚠️ 관망 {len(observing)} arm — 표본·유효 수익률 미달</i>")

    lines.append("")
    lines.append(
        "<i>※ [3/9] 자금관리 데스매치(고정 vs 켈리 경로)는 시스템 전체 비교, "
        "본 [9/9]은 전략 arm 결승입니다.</i>"
    )
    lines.append("")
    lines.append(f"💡 <b>결론:</b> {br.verdict}")
    if br.allocation_note:
        lines.append("")
        lines.append(br.allocation_note)
    if ace_oneliner:
        lines.append("")
        lines.append(ace_oneliner)
    return "\n".join(lines) + "\n"
