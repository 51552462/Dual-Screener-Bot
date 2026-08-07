"""
RP-1 + C-1: 15구간 레짐 패널 — baseline + 조건부 섹터 A/B.

Stage 1: supernova time_machine + Phase A tier replay (no config_kv write).
Stage 2: C-1 sector spillover boost — auto-branched from Stage 1 verdict.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from performance_budget_governor import (
    DEFAULT_MDD_CAP_PCT,
    _band_for_exhaustion,
    resolve_mdd_cap_pct,
)
from time_machine_backtester import (
    LOOKAHEAD_BIAS_WARNING_HTML,
    REGIME_PERIODS,
    _summarize_trade_results,
    load_factory_brain_readonly,
)

RP1_MIN_TRADES_AUTO_VERDICT = 20
RP1_LOOKAHEAD_NOTICE = (
    "상한선 추정치 — v1 오늘 뇌 템플릿 lookahead. Pass≠실전보장."
)
C1_SECTOR_BOOST_PCT = 5.0  # +5% final_ret when spillover aligns (C-1 A/B only)


def replay_tier_overlay_on_returns(
    trade_returns_pct: Sequence[float],
    *,
    mdd_cap_pct: float = DEFAULT_MDD_CAP_PCT,
    base_position_pct: float = 1.0,
) -> Tuple[List[float], List[Dict[str, Any]]]:
    """
  Peak-to-trough exhaustion → DEFAULT_BUDGET_BANDS replay (read-only, no config write).
  Each trade sized by kelly_throttle_mult * position_quota_mult at entry.
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

    raw_returns = [float(t["final_ret"]) for t in trades]
    adj_returns, tier_log = replay_tier_overlay_on_returns(raw_returns)
    metrics = compute_equity_metrics(adj_returns, start_dt, end_dt)

    verdict = judge_period_verdict(
        bucket=bucket,
        total_trades=stats["total_trades"],
        cagr_pct=metrics["cagr_pct"],
        mdd_pct=metrics["mdd_pct"],
        pf=stats["pf"],
    )
    fail_cause = None
    if verdict == "FAIL":
        fail_cause = tag_fail_cause(
            total_trades=stats["total_trades"],
            mdd_pct=metrics["mdd_pct"],
            cagr_pct=metrics["cagr_pct"],
            bucket=bucket,
        )

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
        "mdd_pct": round(metrics["mdd_pct"], 4),
        "zero_entries": stats["total_trades"] == 0,
        "verdict": verdict,
        "fail_cause": fail_cause,
        "tier_log_sample": tier_log[:5],
        "tier_events": len(tier_log),
        "c1_boost": c1_boost,
    }


def build_stage1_report(
    period_rows: List[Dict[str, Any]],
    *,
    mdd_cap_pct: float = DEFAULT_MDD_CAP_PCT,
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
        "schema": "regime_panel_rp1.v1",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "lookahead_notice": RP1_LOOKAHEAD_NOTICE,
        "lookahead_html": LOOKAHEAD_BIAS_WARNING_HTML,
        "mdd_cap_pct": mdd_cap_pct,
        "mdd_crosscheck": mdd_badge,
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
    if run_backtest_fn is None:
        from regime_panel_rp1_runner import default_run_backtest_for_period

        run_backtest_fn = default_run_backtest_for_period

    mdd_cap = resolve_mdd_cap_pct(load_factory_brain_readonly())
    period_rows: List[Dict[str, Any]] = []

    for regime_name, meta in REGIME_PERIODS.items():
        period_rows.append(
            _run_one_regime_period(
                regime_name, meta, stock_list, run_backtest_fn=run_backtest_fn
            )
        )

    stage1 = build_stage1_report(period_rows, mdd_cap_pct=mdd_cap)
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
                _run_one_regime_period(rn, meta, stock_list, run_backtest_fn=run_backtest_fn, c1_boost=False)
            )
            c1_rows.append(
                _run_one_regime_period(
                    rn, meta, stock_list, run_backtest_fn=run_backtest_fn, c1_boost=True, boost_fn=boost_fn
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
    return report


def count_regime_periods() -> Dict[str, int]:
    buckets: Dict[str, int] = {"BULL": 0, "SIDEWAYS": 0, "BEAR": 0}
    for meta in REGIME_PERIODS.values():
        b = meta.get("bucket")
        if b in buckets:
            buckets[b] += 1
    return {"total": len(REGIME_PERIODS), **buckets}
