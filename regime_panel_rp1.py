"""
RP-1 + C-1: 15구간 레짐 패널 — baseline + 조건부 섹터 A/B.

Stage 1: supernova time_machine + Phase A tier replay (no config_kv write).
Stage 2: C-1 sector spillover boost — auto-branched from Stage 1 verdict.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from performance_budget_governor import (
    DEFAULT_MDD_CAP_PCT,
    _band_for_exhaustion,
    resolve_mdd_cap_pct,
    resolve_position_quota_regime_map,
)
from time_machine_backtester import (
    LOOKAHEAD_BIAS_WARNING_HTML,
    REGIME_PERIODS,
    _summarize_trade_results,
)

RP1_MIN_TRADES_AUTO_VERDICT = 20
RP1_METRICS_METHOD = "daily_equal_weight_v2.3.3_a3_quota_regime_kelly"
RP1_KELLY_CAP_BASELINE_BUCKET = "SIDEWAYS"
RP1_CAGR_MEASUREMENT_FLOOR_PCT = -50.0
RP1_MAX_POSITIONS_PER_DAY = 20
RP1_LOOKAHEAD_NOTICE = (
    "상한선 추정치 — v1 오늘 뇌 템플릿 lookahead. Pass≠실전보장."
)
C1_SECTOR_BOOST_PCT = 5.0  # +5% final_ret when spillover aligns (C-1 A/B only)


def resolve_rp1_max_positions_per_day(
    bucket: str,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> Tuple[int, str]:
    """
    A-3 POSITION_QUOTA_REGIME_MAP for RP-1 historical bucket proxy.
    BULL=20, SIDEWAYS=15, BEAR=8 (HIGH_VOL unused in 15-panel buckets).
    """
    quota_map = resolve_position_quota_regime_map(sys_config)
    bk = str(bucket or "").strip().upper()
    if bk in quota_map:
        return int(quota_map[bk]), bk
    default = int(quota_map.get("DEFAULT", RP1_MAX_POSITIONS_PER_DAY))
    return default, "DEFAULT"


def resolve_rp1_regime_kelly_cap(
    bucket: str,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> Tuple[float, str]:
    """CAT-F ACTION_BY_REGIME kelly_cap for RP-1 historical bucket proxy."""
    del sys_config  # RP-1 uses module SSOT; config_kv override reserved for later
    from meta_governor import ACTION_BY_REGIME

    bk = str(bucket or "").strip().upper()
    tpl = ACTION_BY_REGIME.get(bk) or ACTION_BY_REGIME.get("UNKNOWN") or {}
    cap = float(tpl.get("kelly_cap", 0.015))
    return cap, bk


def resolve_rp1_regime_kelly_mult(
    bucket: str,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> Tuple[float, float, float, str]:
    """
    Regime kelly scale vs SIDEWAYS baseline (0.018 SSOT).
    BEAR 0.010 → ~0.556×, BULL 0.028 → ~1.556× tier exposure.
    """
    cap, key = resolve_rp1_regime_kelly_cap(bucket, sys_config)
    baseline_cap, _ = resolve_rp1_regime_kelly_cap(RP1_KELLY_CAP_BASELINE_BUCKET, sys_config)
    if baseline_cap <= 0:
        baseline_cap = 0.018
    return cap / baseline_cap, cap, baseline_cap, key


def nav_to_period_return_pct(nav_end: float, *, base: float = 100.0) -> float:
    """Period total return (%) from ending NAV — pairs with CAGR for short-window reads."""
    if base <= 0:
        return 0.0
    return ((float(nav_end) / base) - 1.0) * 100.0

def replay_tier_overlay_on_returns(
    trade_returns_pct: Sequence[float],
    *,
    mdd_cap_pct: float = DEFAULT_MDD_CAP_PCT,
    base_position_pct: float = 1.0,
) -> Tuple[List[float], List[Dict[str, Any]]]:
    """
    Legacy per-trade sequential replay (unit tests only).
    Live RP-1 metrics use replay_tier_overlay_on_trades (daily EOD compounding).
    """
    nav = 100.0
    peak = 100.0
    adjusted: List[float] = []
    tier_log: List[Dict[str, Any]] = []

    for i, raw_ret in enumerate(trade_returns_pct):
        dd_pct = 0.0 if peak <= 0 else max(0.0, (peak - nav) / peak * 100.0)
        exhaustion = (dd_pct / mdd_cap_pct * 100.0) if mdd_cap_pct > 0 else 0.0
        band = _band_for_exhaustion(exhaustion)
        k_mult = float(band["kelly_throttle_mult"])
        q_mult = float(band["position_quota_mult"])
        if band.get("block_new_entries"):
            k_mult = 0.0
            q_mult = 0.0

        weight = base_position_pct * k_mult * q_mult
        applied = float(raw_ret) * weight
        nav *= 1.0 + applied / 100.0
        if nav > peak:
            peak = nav
        adjusted.append(applied)
        tier_log.append(
            {
                "trade_idx": i,
                "exhaustion_pct": round(exhaustion, 4),
                "band": band["band"],
                "kelly_throttle_mult": k_mult,
                "position_quota_mult": q_mult,
                "nav_after": round(nav, 6),
            }
        )

    return adjusted, tier_log


def replay_tier_overlay_on_trades(
    ordered_trades: Sequence[Dict[str, Any]],
    *,
    mdd_cap_pct: float = DEFAULT_MDD_CAP_PCT,
    max_positions_per_day: int = RP1_MAX_POSITIONS_PER_DAY,
    regime_kelly_mult: float = 1.0,
) -> Tuple[List[float], List[Dict[str, Any]]]:
    """
    Tier replay aligned with daily equal-weight portfolio NAV.
    Exhaustion band is fixed at day open; intraday trades share the band; NAV compounds EOD.
    """
    nav = 100.0
    peak = 100.0
    adjusted: List[float] = []
    tier_log: List[Dict[str, Any]] = []
    n = len(ordered_trades)
    i = 0

    while i < n:
        day = str(ordered_trades[i].get("date") or "")[:10]
        if not day:
            adjusted.append(0.0)
            i += 1
            continue

        day_trades: List[Dict[str, Any]] = []
        while i < n and str(ordered_trades[i].get("date") or "")[:10] == day:
            day_trades.append(ordered_trades[i])
            i += 1

        dd_pct = 0.0 if peak <= 0 else max(0.0, (peak - nav) / peak * 100.0)
        exhaustion = (dd_pct / mdd_cap_pct * 100.0) if mdd_cap_pct > 0 else 0.0
        band = _band_for_exhaustion(exhaustion)
        k_mult = float(band["kelly_throttle_mult"])
        q_mult = float(band["position_quota_mult"])
        if band.get("block_new_entries"):
            k_mult = 0.0
            q_mult = 0.0
        weight = k_mult * q_mult * float(regime_kelly_mult)

        capped = day_trades[:max_positions_per_day]
        day_applied: List[float] = []
        for t in capped:
            applied = float(t.get("final_ret", 0.0)) * weight
            adjusted.append(applied)
            day_applied.append(applied)
        for _ in day_trades[len(capped) :]:
            adjusted.append(0.0)

        daily_ret = sum(day_applied) / len(day_applied) if day_applied else 0.0
        nav *= 1.0 + daily_ret / 100.0
        if nav > peak:
            peak = nav
        tier_log.append(
            {
                "date": day,
                "exhaustion_pct": round(exhaustion, 4),
                "band": band["band"],
                "kelly_throttle_mult": k_mult,
                "position_quota_mult": q_mult,
                "regime_kelly_mult": round(float(regime_kelly_mult), 6),
                "daily_tier_return_pct": round(daily_ret, 6),
                "nav_after_eod": round(nav, 6),
                "trades_in_day": len(capped),
            }
        )

    return adjusted, tier_log


def compute_equity_metrics(
    trade_returns_pct: Sequence[float],
    start_dt: str,
    end_dt: str,
) -> Dict[str, float]:
    """CAGR (annualized) and MDD from sequential trade PnL (%)."""
    if not trade_returns_pct:
        return {"cagr_pct": 0.0, "mdd_pct": 0.0, "nav_end": 100.0}

    nav = 100.0
    peak = 100.0
    mdd = 0.0
    for r in trade_returns_pct:
        nav *= 1.0 + float(r) / 100.0
        if nav > peak:
            peak = nav
        dd = (peak - nav) / peak * 100.0 if peak > 0 else 0.0
        mdd = max(mdd, dd)

    start = datetime.strptime(start_dt[:10], "%Y-%m-%d")
    end = datetime.strptime(end_dt[:10], "%Y-%m-%d")
    days = max(1, (end - start).days)
    years = days / 365.25
    cagr = ((nav / 100.0) ** (1.0 / years) - 1.0) * 100.0 if years > 0 else 0.0
    return {"cagr_pct": float(cagr), "mdd_pct": float(mdd), "nav_end": float(nav)}


def sort_trades_by_date(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(trades, key=lambda t: (str(t.get("date") or "")[:10], str(t.get("code") or "")))


def trades_to_daily_returns(
    trades: Sequence[Dict[str, Any]],
    *,
    max_positions_per_day: int = RP1_MAX_POSITIONS_PER_DAY,
) -> Tuple[List[str], List[float]]:
    """
    Equal-weight daily portfolio return (%).
    Multiple same-day entries share NAV (mean return), capped at max_positions_per_day.
    """
    by_date: Dict[str, List[float]] = {}
    for t in trades:
        day = str(t.get("date") or "")[:10]
        if not day:
            continue
        by_date.setdefault(day, []).append(float(t.get("final_ret", 0.0)))

    dates = sorted(by_date.keys())
    daily: List[float] = []
    for day in dates:
        # Chronological cap (trade order within day) — never sort-by-return (was picking worst N).
        rets = by_date[day]
        if len(rets) > max_positions_per_day:
            rets = rets[:max_positions_per_day]
        daily.append(sum(rets) / len(rets))
    return dates, daily


def _sample_tier_log(tier_log: Sequence[Dict[str, Any]], *, samples: int = 5) -> List[Dict[str, Any]]:
    """Spread samples across the daily tier trace."""
    n = len(tier_log)
    if n <= samples:
        return list(tier_log)
    idxs = sorted({int(round(i * (n - 1) / (samples - 1))) for i in range(samples)})
    return [dict(tier_log[i]) for i in idxs]


def compute_period_portfolio_metrics(
    trades: Sequence[Dict[str, Any]],
    start_dt: str,
    end_dt: str,
    *,
    mdd_cap_pct: float = DEFAULT_MDD_CAP_PCT,
    max_positions_per_day: int = RP1_MAX_POSITIONS_PER_DAY,
    regime_kelly_mult: float = 1.0,
    regime_kelly_cap: Optional[float] = None,
) -> Dict[str, Any]:
    """RCA v2.3: daily equal-weight raw + tier; single NAV path for CAGR/MDD/tier_log."""
    ordered = sort_trades_by_date(list(trades))
    dates, daily_raw = trades_to_daily_returns(ordered, max_positions_per_day=max_positions_per_day)

    if not daily_raw:
        return {
            "cagr_pct": 0.0,
            "cagr_pct_raw": 0.0,
            "mdd_pct": 0.0,
            "mdd_pct_raw": 0.0,
            "mdd_pct_tier": 0.0,
            "nav_end": 100.0,
            "nav_end_raw": 100.0,
            "period_return_pct": 0.0,
            "period_return_pct_raw": 0.0,
            "trading_days": 0,
            "trades_per_day": 0.0,
            "metrics_method": RP1_METRICS_METHOD,
            "max_positions_per_day": max_positions_per_day,
            "regime_kelly_mult": regime_kelly_mult,
            "regime_kelly_cap": regime_kelly_cap,
            "tier_events": 0,
            "tier_log_sample": [],
            "tier_replay_unit": "daily_equal_weight",
        }

    raw_metrics = compute_equity_metrics(daily_raw, start_dt, end_dt)
    adj_trade, tier_log = replay_tier_overlay_on_trades(
        ordered,
        mdd_cap_pct=mdd_cap_pct,
        max_positions_per_day=max_positions_per_day,
        regime_kelly_mult=regime_kelly_mult,
    )
    adj_trades = [{**t, "final_ret": ar} for t, ar in zip(ordered, adj_trade)]
    _, daily_tier = trades_to_daily_returns(adj_trades, max_positions_per_day=max_positions_per_day)
    tier_daily_metrics = compute_equity_metrics(daily_tier, start_dt, end_dt)

    return {
        "cagr_pct": tier_daily_metrics["cagr_pct"],
        "cagr_pct_raw": raw_metrics["cagr_pct"],
        "mdd_pct": tier_daily_metrics["mdd_pct"],
        "mdd_pct_raw": raw_metrics["mdd_pct"],
        "mdd_pct_tier": tier_daily_metrics["mdd_pct"],
        "nav_end": tier_daily_metrics["nav_end"],
        "nav_end_raw": raw_metrics["nav_end"],
        "period_return_pct": nav_to_period_return_pct(tier_daily_metrics["nav_end"]),
        "period_return_pct_raw": nav_to_period_return_pct(raw_metrics["nav_end"]),
        "trading_days": len(dates),
        "trades_per_day": round(len(ordered) / max(len(dates), 1), 2),
        "metrics_method": RP1_METRICS_METHOD,
        "max_positions_per_day": max_positions_per_day,
        "regime_kelly_mult": round(float(regime_kelly_mult), 6),
        "regime_kelly_cap": regime_kelly_cap,
        "tier_events": len(tier_log),
        "tier_log_sample": _sample_tier_log(tier_log),
        "tier_replay_unit": "daily_equal_weight",
    }


def tag_fail_cause(
    *,
    total_trades: int,
    mdd_pct: float,
    cagr_pct: float,
    bucket: str,
) -> Optional[str]:
    if total_trades < 3:
        return "A"
    if mdd_pct > 10.0:
        return "C"
    if bucket == "BULL" and cagr_pct < 20.0 and total_trades >= RP1_MIN_TRADES_AUTO_VERDICT:
        return "B"
    if bucket in ("SIDEWAYS", "BEAR") and cagr_pct < 0 and total_trades >= RP1_MIN_TRADES_AUTO_VERDICT:
        return "B"
    return None


def judge_period_verdict(
    *,
    bucket: str,
    total_trades: int,
    cagr_pct: float,
    mdd_pct: float,
    pf: float,
) -> str:
    if total_trades < RP1_MIN_TRADES_AUTO_VERDICT:
        return "SKIP_LOW_N"

    if bucket == "BULL":
        if cagr_pct < RP1_CAGR_MEASUREMENT_FLOOR_PCT:
            return "INCONCLUSIVE"
        if cagr_pct >= 40.0 or pf > 1.3:
            return "PASS"
        if 25.0 <= cagr_pct < 40.0 and mdd_pct <= 10.0:
            return "NEAR_MISS"
        return "FAIL"

    if bucket == "SIDEWAYS":
        if mdd_pct <= 10.0 and pf >= 1.0:
            return "PASS"
        if mdd_pct <= 10.0 and 0.9 <= pf < 1.0:
            return "NEAR_MISS"
        return "FAIL"

    # BEAR
    if mdd_pct <= 10.0:
        return "PASS" if pf >= 0.95 else "NEAR_MISS"
    if 7.0 <= mdd_pct <= 10.5:
        return "NEAR_MISS"
    return "FAIL"


def judge_bucket_summary(period_rows: List[Dict[str, Any]], bucket: str) -> Dict[str, Any]:
    eligible = [r for r in period_rows if r.get("bucket") == bucket and r.get("verdict") != "SKIP_LOW_N"]
    passes = sum(1 for r in eligible if r.get("verdict") == "PASS")
    fails = sum(1 for r in eligible if r.get("verdict") == "FAIL")
    near = sum(1 for r in eligible if r.get("verdict") == "NEAR_MISS")
    skipped = sum(1 for r in period_rows if r.get("bucket") == bucket and r.get("verdict") == "SKIP_LOW_N")

    if bucket == "BULL":
        bucket_pass = passes >= 3
    elif bucket == "SIDEWAYS":
        bucket_pass = fails <= 1 and all(r.get("mdd_pct", 99) <= 10.0 for r in eligible)
    else:
        bucket_pass = fails <= 1 and all(r.get("mdd_pct", 99) <= 10.0 for r in eligible)

    return {
        "bucket": bucket,
        "pass_count": passes,
        "fail_count": fails,
        "near_miss_count": near,
        "skipped_low_n": skipped,
        "bucket_pass": bucket_pass,
    }


def mdd_crosscheck_badge(period_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """MDD>10% anywhere → Near-miss 강등 배지 (헌법 우선)."""
    violations = [r["regime_name"] for r in period_rows if r.get("mdd_pct", 0) > 10.0]
    return {
        "mdd_cap_violation": bool(violations),
        "violating_regimes": violations,
        "badge": "NEAR_MISS_DEMOTE" if violations else "MDD_OK",
    }


def decide_stage2_c1(stage1_report: Dict[str, Any]) -> Dict[str, Any]:
    """Stage 1 → Stage 2 C-1 branch (no director round-trip)."""
    periods = stage1_report.get("periods", [])
    causes = [r.get("fail_cause") for r in periods if r.get("verdict") == "FAIL"]
    overall = stage1_report.get("overall_verdict", "FAIL")

    if overall == "INCONCLUSIVE":
        return {"action": "SKIP", "reason": "무결론: 전 구간 n<20 — RP-1 No-Go (원인 A)"}

    if "A" in causes:
        return {"action": "SKIP", "reason": "C-1 스킵: 원인 A (신호부족)"}
    if "C" in causes:
        return {"action": "SKIP", "reason": "C-1 스킵: 원인 C (MDD구조)"}

    if overall == "PASS":
        return {"action": "OPTIONAL_SKIP", "reason": "Stage1 Pass — C-1 우선순위 낮음, 스킵 가능"}

    if overall == "NEAR_MISS":
        return {"action": "FULL_AB", "reason": "Near-miss — 15구간 전체 C-1 A/B", "regime_filter": None}

    if "B" in causes:
        b_names = [r["regime_name"] for r in periods if r.get("fail_cause") == "B"]
        return {
            "action": "REDUCED_AB",
            "reason": "Fail 원인 B — 태깅 구간만 C-1 A/B",
            "regime_filter": b_names,
        }

    return {"action": "SKIP", "reason": "C-1 스킵: Stage1 Fail (분류 불명)"}


def apply_c1_sector_boost(
    trades: List[Dict[str, Any]],
    *,
    boost_pct: float = C1_SECTOR_BOOST_PCT,
    boost_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
) -> List[Dict[str, Any]]:
    """C-1: multiply final_ret when sector spillover aligns (default +boost_pct%)."""
    out: List[Dict[str, Any]] = []
    for t in trades:
        row = dict(t)
        mult = 1.0
        if boost_fn is not None:
            mult = 1.0 + boost_fn(row) * (boost_pct / 100.0)
        elif row.get("sector_boost_eligible"):
            mult = 1.0 + boost_pct / 100.0
        row["final_ret"] = float(row.get("final_ret", 0.0)) * mult
        row["c1_boost_applied"] = mult != 1.0
        out.append(row)
    return out


def _run_one_regime_period(
    regime_name: str,
    meta: Dict[str, Any],
    stock_list: Sequence[str],
    *,
    run_backtest_fn: Callable[..., Dict[str, Any]],
    c1_boost: bool = False,
    boost_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
    sys_config: Optional[Mapping[str, Any]] = None,
    mdd_cap_pct: float = DEFAULT_MDD_CAP_PCT,
) -> Dict[str, Any]:
    start_dt, end_dt = meta["start"], meta["end"]
    bucket = meta.get("bucket", "UNKNOWN")
    substitution_log: List[str] = []

    def _collect(period_meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        raw = run_backtest_fn(
            regime_name,
            list(stock_list),
            period_meta["start"],
            period_meta["end"],
        )
        trades = raw.get("trades", [])
        stats = _summarize_trade_results(trades)
        return trades, stats

    trades, stats = _collect(meta)
    if stats["total_trades"] == 0 and meta.get("backup"):
        backup = meta["backup"]
        substitution_log.append(
            f"{regime_name}: primary 0 trades → backup {backup['start']}~{backup['end']}"
        )
        start_dt, end_dt = backup["start"], backup["end"]
        trades, stats = _collect({**meta, **backup})

    if c1_boost and trades:
        trades = apply_c1_sector_boost(trades, boost_fn=boost_fn)

    max_pos, quota_key = resolve_rp1_max_positions_per_day(bucket, sys_config)
    kelly_mult, kelly_cap, kelly_baseline, kelly_key = resolve_rp1_regime_kelly_mult(
        bucket, sys_config
    )
    metrics = compute_period_portfolio_metrics(
        trades,
        start_dt,
        end_dt,
        mdd_cap_pct=mdd_cap_pct,
        max_positions_per_day=max_pos,
        regime_kelly_mult=kelly_mult,
        regime_kelly_cap=kelly_cap,
    )

    verdict = judge_period_verdict(
        bucket=bucket,
        total_trades=stats["total_trades"],
        cagr_pct=metrics["cagr_pct"],
        mdd_pct=metrics["mdd_pct"],
        pf=stats["pf"],
    )
    fail_cause = None
    near_miss_cause = None
    if verdict == "FAIL":
        fail_cause = tag_fail_cause(
            total_trades=stats["total_trades"],
            mdd_pct=metrics["mdd_pct"],
            cagr_pct=metrics["cagr_pct"],
            bucket=bucket,
        )
    elif verdict == "NEAR_MISS":
        near_miss_cause = tag_fail_cause(
            total_trades=stats["total_trades"],
            mdd_pct=metrics["mdd_pct"],
            cagr_pct=metrics["cagr_pct"],
            bucket=bucket,
        )
        if near_miss_cause is None and stats["total_trades"] >= RP1_MIN_TRADES_AUTO_VERDICT:
            near_miss_cause = "B"

    return {
        "regime_name": regime_name,
        "bucket": bucket,
        "regime": meta.get("regime"),
        "start": start_dt,
        "end": end_dt,
        "substitution_log": substitution_log,
        "total_trades": stats["total_trades"],
        "win_rate": stats["win_rate"],
        "pf": stats["pf"],
        "avg_pnl": stats["avg_pnl"],
        "cagr_pct": round(metrics["cagr_pct"], 4),
        "cagr_pct_raw": round(metrics.get("cagr_pct_raw", metrics["cagr_pct"]), 4),
        "period_return_pct": round(metrics.get("period_return_pct", 0.0), 4),
        "period_return_pct_raw": round(metrics.get("period_return_pct_raw", 0.0), 4),
        "mdd_pct": round(metrics["mdd_pct"], 4),
        "mdd_pct_raw": round(metrics["mdd_pct_raw"], 4),
        "mdd_pct_tier": round(metrics["mdd_pct_tier"], 4),
        "trades_per_day": metrics["trades_per_day"],
        "trading_days": metrics["trading_days"],
        "metrics_method": metrics["metrics_method"],
        "max_positions_per_day": metrics["max_positions_per_day"],
        "position_quota_regime_key": quota_key,
        "regime_kelly_cap": round(kelly_cap, 6),
        "regime_kelly_mult": round(kelly_mult, 6),
        "regime_kelly_baseline_cap": round(kelly_baseline, 6),
        "regime_kelly_regime_key": kelly_key,
        "zero_entries": stats["total_trades"] == 0,
        "verdict": verdict,
        "fail_cause": fail_cause,
        "near_miss_cause": near_miss_cause,
        "tier_log_sample": metrics["tier_log_sample"],
        "tier_events": metrics["tier_events"],
        "c1_boost": c1_boost,
    }


def _tier_mdd_uniform_suspect(period_rows: List[Dict[str, Any]], *, min_periods: int = 3) -> bool:
    """True when tier MDD is identical across periods — measurement artifact guard."""
    tier_mdds = [
        round(float(r.get("mdd_pct_tier", 0.0)), 4)
        for r in period_rows
        if int(r.get("tier_events") or 0) > 0
    ]
    if len(tier_mdds) < min_periods:
        return False
    return len(set(tier_mdds)) == 1


def build_stage1_report(
    period_rows: List[Dict[str, Any]],
    *,
    mdd_cap_pct: float = DEFAULT_MDD_CAP_PCT,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    mdd_badge = mdd_crosscheck_badge(period_rows)
    buckets = {
        b: judge_bucket_summary(period_rows, b) for b in ("BULL", "SIDEWAYS", "BEAR")
    }
    all_bucket_pass = all(buckets[b]["bucket_pass"] for b in buckets)

    eligible_cagrs = [
        r.get("cagr_pct", 0.0) for r in period_rows
        if r.get("verdict") != "SKIP_LOW_N" and r.get("bucket") == "BULL"
    ]
    avg_bull_cagr = float(np.mean(eligible_cagrs)) if eligible_cagrs else 0.0

    all_skip = bool(period_rows) and all(
        r.get("verdict") == "SKIP_LOW_N" for r in period_rows
    )

    if all_skip:
        overall = "INCONCLUSIVE"
    elif any(r.get("verdict") == "INCONCLUSIVE" for r in period_rows):
        overall = "INCONCLUSIVE"
    elif _tier_mdd_uniform_suspect(period_rows):
        overall = "INCONCLUSIVE"
    elif mdd_badge["mdd_cap_violation"]:
        overall = "NEAR_MISS"
    elif all_bucket_pass and avg_bull_cagr >= 35.0:
        overall = "PASS"
    elif any(r.get("verdict") == "NEAR_MISS" for r in period_rows) or mdd_badge["mdd_cap_violation"]:
        overall = "NEAR_MISS"
    elif all(r.get("verdict") in ("PASS", "SKIP_LOW_N") for r in period_rows):
        overall = "PASS"
    else:
        overall = "FAIL"

    return {
        "schema": "regime_panel_rp1.v2.3.3",
        "metrics_method": RP1_METRICS_METHOD,
        "position_quota_regime_map": resolve_position_quota_regime_map(
            sys_config if isinstance(sys_config, dict) else None
        ),
        "regime_kelly_cap_map": {
            b: resolve_rp1_regime_kelly_cap(b, sys_config)[0]
            for b in ("BULL", "SIDEWAYS", "BEAR")
        },
        "regime_kelly_baseline_bucket": RP1_KELLY_CAP_BASELINE_BUCKET,
        "regime_kelly_baseline_cap": resolve_rp1_regime_kelly_cap(
            RP1_KELLY_CAP_BASELINE_BUCKET, sys_config
        )[0],
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "lookahead_notice": RP1_LOOKAHEAD_NOTICE,
        "lookahead_html": LOOKAHEAD_BIAS_WARNING_HTML,
        "mdd_cap_pct": mdd_cap_pct,
        "mdd_crosscheck": mdd_badge,
        "tier_mdd_uniform_suspect": _tier_mdd_uniform_suspect(period_rows),
        "min_trades_auto_verdict": RP1_MIN_TRADES_AUTO_VERDICT,
        "periods": period_rows,
        "bucket_summary": buckets,
        "avg_bull_cagr_pct": round(avg_bull_cagr, 4),
        "overall_verdict": overall,
    }


def run_regime_panel_rp1(
    stock_list: Sequence[str],
    *,
    run_backtest_fn: Optional[Callable[..., Dict[str, Any]]] = None,
    output_dir: Optional[str] = None,
    run_stage2: bool = True,
    boost_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
) -> Dict[str, Any]:
    """
    Stage 1 RP-1 baseline + optional Stage 2 C-1 A/B (auto-branched).
    Writes reports/regime_panel/rp1_{date}.json
    """
    from regime_panel_rp1_runner import (
        clear_rp1_matrix_cache,
        default_run_backtest_for_period,
        load_rp1_brain_cached,
        prime_rp1_matrix_cache,
        resolve_rp1_use_matrix_cache,
    )

    if run_backtest_fn is None:
        run_backtest_fn = default_run_backtest_for_period

    brain = load_rp1_brain_cached(force_reload=True)
    ml_n = len(brain.get("LIVE_CLUSTER_TEMPLATES") or {})
    ud_n = len(brain.get("UNDERDOG_CLUSTER_TEMPLATES") or {})
    if run_backtest_fn is default_run_backtest_for_period and ml_n + ud_n == 0:
        raise RuntimeError(
            "RP-1 aborted: LIVE_CLUSTER_TEMPLATES empty. "
            "Server uses config_kv (SQLite) — ensure load_system_config path, not JSON-only."
        )

    mdd_cap = resolve_mdd_cap_pct(brain)
    period_rows: List[Dict[str, Any]] = []
    n_periods = len(REGIME_PERIODS)

    matrix_meta: Optional[Dict[str, Any]] = None
    if run_backtest_fn is default_run_backtest_for_period and resolve_rp1_use_matrix_cache():
        matrix_meta = prime_rp1_matrix_cache(list(stock_list))

    for idx, (regime_name, meta) in enumerate(REGIME_PERIODS.items(), 1):
        from regime_panel_rp1_runner import log_rp1

        log_rp1(
            f"[RP-1] period {idx}/{n_periods} {regime_name} "
            f"({meta['start']}~{meta['end']}) bucket={meta.get('bucket')}"
        )
        row = _run_one_regime_period(
            regime_name,
            meta,
            stock_list,
            run_backtest_fn=run_backtest_fn,
            sys_config=brain,
            mdd_cap_pct=mdd_cap,
        )
        log_rp1(
            f"  -> trades={row['total_trades']} verdict={row['verdict']} "
            f"CAGR={row['cagr_pct']}% period_ret={row.get('period_return_pct')}% "
            f"(raw_cagr={row.get('cagr_pct_raw')}) "
            f"MDD={row['mdd_pct']}% (raw={row.get('mdd_pct_raw')})"
        )
        period_rows.append(row)

    stage1 = build_stage1_report(period_rows, mdd_cap_pct=mdd_cap, sys_config=brain)
    stage2_plan = decide_stage2_c1(stage1)

    stage2_result: Optional[Dict[str, Any]] = None
    if run_stage2 and stage2_plan["action"] in ("FULL_AB", "REDUCED_AB"):
        filt = stage2_plan.get("regime_filter")
        names = list(REGIME_PERIODS.keys()) if not filt else list(filt)
        baseline_rows: List[Dict[str, Any]] = []
        c1_rows: List[Dict[str, Any]] = []
        for rn in names:
            if rn not in REGIME_PERIODS:
                continue
            meta = REGIME_PERIODS[rn]
            baseline_rows.append(
                _run_one_regime_period(
                    rn,
                    meta,
                    stock_list,
                    run_backtest_fn=run_backtest_fn,
                    c1_boost=False,
                    sys_config=brain,
                    mdd_cap_pct=mdd_cap,
                )
            )
            c1_rows.append(
                _run_one_regime_period(
                    rn,
                    meta,
                    stock_list,
                    run_backtest_fn=run_backtest_fn,
                    c1_boost=True,
                    boost_fn=boost_fn,
                    sys_config=brain,
                    mdd_cap_pct=mdd_cap,
                )
            )
        stage2_result = {
            "plan": stage2_plan,
            "baseline": baseline_rows,
            "c1_boost": c1_rows,
            "delta_avg_cagr": round(
                float(np.mean([r["cagr_pct"] for r in c1_rows]))
                - float(np.mean([r["cagr_pct"] for r in baseline_rows])),
                4,
            ) if c1_rows else 0.0,
        }
    else:
        stage2_result = {"plan": stage2_plan, "skipped": True}

    report = {
        "stage1": stage1,
        "stage2": stage2_result,
    }

    out_dir = output_dir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "reports",
        "regime_panel",
    )
    os.makedirs(out_dir, exist_ok=True)
    date_tag = datetime.now().strftime("%Y%m%d")
    out_path = os.path.join(out_dir, f"rp1_{date_tag}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    report["output_path"] = out_path
    if matrix_meta is not None:
        clear_rp1_matrix_cache()
    return report


def count_regime_periods() -> Dict[str, int]:
    buckets: Dict[str, int] = {"BULL": 0, "SIDEWAYS": 0, "BEAR": 0}
    for meta in REGIME_PERIODS.values():
        b = meta.get("bucket")
        if b in buckets:
            buckets[b] += 1
    return {"total": len(REGIME_PERIODS), **buckets}
