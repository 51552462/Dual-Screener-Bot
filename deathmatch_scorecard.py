"""
L1~L3 데스매치 Scorecard — 다축 집계 · 절대 허들 · Composite v2 · 지수 MDD 패널티.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from deathmatch_report import _effective_final_ret_pct, _profit_factor_from_ret


def exponential_mdd_penalty(
    mdd_pct: float,
    *,
    threshold_pct: float,
    scale: float,
    exp_base: float,
) -> float:
    """
    MDD(%)가 threshold(예: -15)보다 깊을수록 (exp_base)^(초과/scale) - 1 로 비선형 증가.
    mdd_pct, threshold_pct 모두 음수 또는 0 근처.
    """
    if mdd_pct >= threshold_pct:
        return 0.0
    excess = abs(float(mdd_pct)) - abs(float(threshold_pct))
    if excess <= 0:
        return 0.0
    sc = max(0.5, float(scale))
    base = max(1.01, float(exp_base))
    return float(math.pow(base, excess / sc) - 1.0)


def passes_absolute_hurdle(
    mean_ret: Optional[float],
    market_benchmark: Optional[float],
    *,
    min_ret: float = 0.0,
    outperform_buffer_pp: float = 0.25,
) -> Tuple[bool, str]:
    """
    챔피언·Z 가산 대상: 절대 수익 >= min_ret OR 벤치마크 대비 초과수익.
    """
    if mean_ret is None or not math.isfinite(float(mean_ret)):
        return False, "no_finite_ret"
    mr = float(mean_ret)
    if mr >= float(min_ret):
        return True, "absolute_positive"
    if market_benchmark is not None and math.isfinite(market_benchmark):
        if mr >= float(market_benchmark) + float(outperform_buffer_pp):
            return True, "relative_outperform"
    return False, "hurdle_fail"


def compute_expectancy(rets: List[float]) -> Optional[float]:
    if len(rets) < 3:
        return None
    wins = [float(x) for x in rets if float(x) > 0]
    losses = [float(x) for x in rets if float(x) <= 0]
    n = len(rets)
    wr = len(wins) / n if n else 0.0
    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = abs(float(np.mean(losses))) if losses else 0.1
    return float((wr * avg_win) - ((1.0 - wr) * avg_loss))


def kelly_path_mean_return(df_arm: pd.DataFrame) -> Optional[float]:
    """sim_kelly_invest × final_ret / 100 경로 평균 수익률 근사."""
    if df_arm is None or df_arm.empty:
        return None
    ret = _effective_final_ret_pct(df_arm)
    if "sim_kelly_invest" not in df_arm.columns:
        return float(ret.mean()) if ret.notna().any() else None
    inv = pd.to_numeric(df_arm["sim_kelly_invest"], errors="coerce").replace(0, np.nan)
    r = ret
    pnl_pct = r  # already %
    valid = inv.notna() & r.notna()
    if int(valid.sum()) < 2:
        return float(r.dropna().mean()) if r.notna().any() else None
    return float(r[valid].mean())


def _zscore(vals: List[float]) -> List[float]:
    if len(vals) < 2:
        return [0.0] * len(vals)
    arr = np.asarray(vals, dtype=float)
    mu = float(np.nanmean(arr))
    sd = float(np.nanstd(arr))
    if sd < 1e-9:
        return [0.0] * len(vals)
    return [float((x - mu) / sd) for x in arr]


@dataclass
class ArmScorecard:
    arm_id: str
    label: str
    group_key: str
    registry_state: str = "OBSERVING"
    n_closed: int = 0
    n_valid: int = 0
    mean_ret: Optional[float] = None
    sum_ret: Optional[float] = None
    win_rate_pct: Optional[float] = None
    profit_factor: Optional[float] = None
    expectancy: Optional[float] = None
    mdd_pct: float = 0.0
    vol_pct: float = 0.0
    kelly_path_ret: Optional[float] = None
    meta_mult: float = 1.0
    tail_loss_streak: int = 0
    meta_reason: str = ""
    oos_wr: Optional[float] = None
    oos_avg_ret: Optional[float] = None
    outperform_pp: Optional[float] = None
    hurdle_passed: bool = False
    hurdle_reason: str = ""
    champion_eligible: bool = False
    composite_score: float = 0.0
    score_breakdown: Dict[str, float] = field(default_factory=dict)
    rank: int = 999
    below_floor: bool = False
    relative_exempt: bool = False


def attach_meta_health(
    arms: List[ArmScorecard],
    health: Dict[str, Any],
    market: str,
) -> None:
    """META_STRATEGY_HEALTH → meta_mult, tail_loss_streak."""
    if not isinstance(health, dict):
        return
    mk = str(market or "KR").upper()
    for a in arms:
        gk = str(a.group_key or "").strip()
        key = f"{mk}|{gk}"
        hv = health.get(key)
        if not isinstance(hv, dict):
            for hk, v in health.items():
                if hk == "__meta__":
                    continue
                if isinstance(v, dict) and (hk.endswith("|" + gk) or hk == gk):
                    hv = v
                    break
        if not isinstance(hv, dict):
            continue
        try:
            a.meta_mult = float(hv.get("mult", 1.0) or 1.0)
        except (TypeError, ValueError):
            a.meta_mult = 1.0
        try:
            a.tail_loss_streak = int(hv.get("tail_loss_streak", 0) or 0)
        except (TypeError, ValueError):
            a.tail_loss_streak = 0
        a.meta_reason = str(hv.get("reason") or "")


def attach_oos_from_mutants(arms: List[ArmScorecard], sys_config: Optional[dict]) -> None:
    import os

    path = os.path.join(os.path.dirname(__file__), "validated_live_mutants.json")
    if not os.path.isfile(path):
        return
    try:
        import json as _json

        with open(path, "r", encoding="utf-8") as f:
            raw = _json.load(f)
        prom = raw.get("promoted") if isinstance(raw, dict) else []
        if not isinstance(prom, list):
            return
        by_name: Dict[str, Dict[str, Any]] = {}
        for p in prom:
            if isinstance(p, dict) and p.get("name"):
                by_name[str(p["name"]).strip().upper()] = p
        for a in arms:
            gk = str(a.group_key or "")
            if gk.upper().startswith("INCUBATOR_"):
                nm = gk.split("_", 1)[-1].upper()
                row = by_name.get(nm)
                if row:
                    try:
                        a.oos_wr = float(row.get("oos_win_rate"))
                    except (TypeError, ValueError):
                        pass
                    try:
                        a.oos_avg_ret = float(row.get("oos_avg_return"))
                    except (TypeError, ValueError):
                        pass
    except Exception:
        return


def build_arm_scorecard_from_df(
    arm_id: str,
    label: str,
    group_key: str,
    registry_state: str,
    df_arm: pd.DataFrame,
    *,
    mdd_pct: float,
) -> ArmScorecard:
    n_closed = int(len(df_arm))
    ret_s = _effective_final_ret_pct(df_arm) if not df_arm.empty else pd.Series(dtype=float)
    valid = ret_s.dropna()
    n_valid = int(len(valid))
    sc = ArmScorecard(
        arm_id=arm_id,
        label=label,
        group_key=group_key,
        registry_state=registry_state,
        n_closed=n_closed,
        n_valid=n_valid,
        mdd_pct=mdd_pct,
    )
    if n_valid > 0:
        rets = valid.tolist()
        m = float(valid.mean())
        if math.isfinite(m):
            sc.mean_ret = m
        sc.sum_ret = float(valid.sum())
        sc.win_rate_pct = float((valid > 0).sum() / n_valid * 100.0)
        sc.profit_factor = _profit_factor_from_ret(valid)
        sc.expectancy = compute_expectancy(rets)
        sc.vol_pct = float(valid.std()) if n_valid > 1 else 0.0
        sc.kelly_path_ret = kelly_path_mean_return(df_arm)
    return sc


def compute_composite_v2(
    arms: List[ArmScorecard],
    dmcfg: Dict[str, Any],
    *,
    market_benchmark: Optional[float],
) -> None:
    """L3: 절대 허들 → Z-score(허들 통과 arm) → 지수 MDD · 메타 실격."""
    weights = dmcfg.get("composite_weights") or {}
    w_ret = float(weights.get("ret", 0.22))
    w_wr = float(weights.get("wr", 0.10))
    w_pf = float(weights.get("pf", 0.10))
    w_exp = float(weights.get("expectancy", 0.08))
    w_kelly = float(weights.get("kelly_sub", 0.05))
    w_oos = float(weights.get("oos_bonus", 0.03))
    w_mdd = float(weights.get("mdd_penalty", 0.32))
    w_vol = float(weights.get("vol_penalty", 0.10))

    mdd_thr = float(dmcfg.get("mdd_exp_threshold_pct", -15.0))
    mdd_scale = float(dmcfg.get("mdd_exp_scale", 5.0))
    mdd_exp_base = float(dmcfg.get("mdd_exp_base", 1.45))
    vol_scale = float(dmcfg.get("vol_penalty_scale", 0.12))

    min_ret = float(dmcfg.get("absolute_hurdle_min_ret", 0.0))
    outperf_buf = float(dmcfg.get("absolute_outperform_buffer_pp", 0.25))
    z_bonus = float(dmcfg.get("absolute_hurdle_z_bonus", 0.35))
    fail_pen = float(dmcfg.get("absolute_hurdle_fail_penalty", 5.0))
    meta_dq = float(dmcfg.get("meta_mult_disqualify_below", 0.05))

    pool = [
        a
        for a in arms
        if a.n_valid > 0 and a.mean_ret is not None and math.isfinite(float(a.mean_ret))
    ]
    if not pool:
        return

    bench = market_benchmark
    hurdle_ok: List[ArmScorecard] = []
    for a in pool:
        if bench is not None and math.isfinite(bench):
            a.outperform_pp = float(a.mean_ret) - float(bench)  # type: ignore
        ok, reason = passes_absolute_hurdle(
            a.mean_ret, bench, min_ret=min_ret, outperform_buffer_pp=outperf_buf
        )
        a.hurdle_passed = ok
        a.hurdle_reason = reason
        a.champion_eligible = ok and a.meta_mult > meta_dq
        if ok:
            hurdle_ok.append(a)

    z_pool = hurdle_ok if len(hurdle_ok) >= 2 else pool

    def _vals(attr: str, transform=None) -> List[float]:
        out_v: List[float] = []
        for x in z_pool:
            v = getattr(x, attr, None)
            if v is None:
                out_v.append(0.0)
            elif transform:
                out_v.append(transform(v))
            else:
                out_v.append(float(v))
        return out_v

    z_ret = _zscore(_vals("mean_ret"))
    z_wr = _zscore(_vals("win_rate_pct"))
    z_pf = _zscore(_vals("profit_factor", lambda v: math.log1p(max(0, float(v or 0)))))
    z_exp = _zscore(_vals("expectancy", lambda v: float(v or 0)))
    z_kelly = _zscore(_vals("kelly_path_ret", lambda v: float(v or 0)))

    z_map = {a.arm_id: i for i, a in enumerate(z_pool)}

    for a in pool:
        mdd_pen = exponential_mdd_penalty(
            float(a.mdd_pct),
            threshold_pct=mdd_thr,
            scale=mdd_scale,
            exp_base=mdd_exp_base,
        )
        vol_pen = (float(a.vol_pct) / 100.0) * vol_scale if a.vol_pct else 0.0

        oos_b = 0.0
        if a.oos_wr is not None and a.oos_wr >= 0.5:
            oos_b = w_oos
        if a.oos_avg_ret is not None and a.oos_avg_ret > 0:
            oos_b = max(oos_b, w_oos * 0.5)

        atk = 0.0
        if a.arm_id in z_map:
            i = z_map[a.arm_id]
            atk = (
                w_ret * z_ret[i]
                + w_wr * z_wr[i]
                + w_pf * z_pf[i]
                + w_exp * z_exp[i]
                + w_kelly * z_kelly[i]
            )
            if a.hurdle_passed:
                atk += z_bonus
        else:
            atk = -fail_pen

        if not a.hurdle_passed:
            atk -= fail_pen

        if a.meta_mult <= meta_dq:
            a.champion_eligible = False
            atk -= fail_pen * 0.5

        composite = atk - w_mdd * mdd_pen - vol_pen + oos_b
        a.composite_score = float(composite)
        a.score_breakdown = {
            "attack": round(atk, 4),
            "mdd_exp_pen": round(mdd_pen, 4),
            "vol_pen": round(vol_pen, 4),
            "oos_bonus": round(oos_b, 4),
            "hurdle_bonus": round(z_bonus if a.hurdle_passed else 0.0, 4),
            "hurdle_fail_pen": round(0.0 if a.hurdle_passed else fail_pen, 4),
        }


def scorecard_to_dict(a: ArmScorecard) -> Dict[str, Any]:
    return {
        "arm_id": a.arm_id,
        "arm_kind": "REGISTRY",
        "label": a.label,
        "group_key": a.group_key,
        "registry_state": a.registry_state,
        "n_closed": a.n_closed,
        "n_valid": a.n_valid,
        "mean_ret": a.mean_ret,
        "sum_ret": a.sum_ret,
        "win_rate_pct": a.win_rate_pct,
        "profit_factor": a.profit_factor,
        "expectancy": a.expectancy,
        "mdd_pct": a.mdd_pct,
        "vol_pct": a.vol_pct,
        "kelly_path_ret": a.kelly_path_ret,
        "meta_mult": a.meta_mult,
        "tail_loss_streak": a.tail_loss_streak,
        "oos_wr": a.oos_wr,
        "outperform_pp": a.outperform_pp,
        "hurdle_passed": a.hurdle_passed,
        "hurdle_reason": a.hurdle_reason,
        "champion_eligible": a.champion_eligible,
        "composite_score": a.composite_score,
        "score_breakdown": json.dumps(a.score_breakdown, ensure_ascii=False),
        "rank": a.rank,
        "below_floor": a.below_floor,
        "relative_exempt": a.relative_exempt,
    }
