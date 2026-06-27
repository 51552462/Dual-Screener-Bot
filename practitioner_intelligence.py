"""
Practitioner Intelligence Layer (PIL) — 실무자 리포트 SSOT.

Post-Mortem(동적 윈도우) · Vitality(좀비) · LLM 브리핑 · 메타 페널티 입력.
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

from evolution.deathmatch_battle_royale import ledger_group_key
from reports.forward_report_scalar import col_series, prepare_forward_trades_df, row_scalar, scalar_float
from practitioner_llm import build_practitioner_llm_summary, format_llm_html_line
from practitioner_market_profiles import (
    PractitionerMarketProfile,
    extract_rank_tier,
    resolve_practitioner_profile,
    toxic_rules_for_profile,
)
from reports.report_feature_analyzer import ReportFeatureAnalyzer


@dataclass
class PractitionerBrief:
    market: str
    group_key: str
    rank_tier: str
    profile: PractitionerMarketProfile
    market_icon: str = "🇰🇷"
    today_win: int = 0
    today_loss: int = 0
    today_flat: int = 0
    n_today_closed: int = 0
    compound_seed: float = 0.0
    open_cnt: int = 0
    rolling_wr_pct: Optional[float] = None
    n_closed_window: int = 0
    wr_trend_pp: Optional[float] = None
    active_days: int = 0
    turnover_30d: float = 0.0
    stale_hold_ratio: float = 0.0
    vitality_score: float = 0.5
    is_zombie: bool = False
    vitality_status: str = "ACTIVE"
    post_mortem_window_days: int = 30
    post_mortem_stats: str = ""
    sector_line: str = ""
    toxic_line: str = ""
    llm_summary: str = ""
    penalty_action: str = ""
    today_closes_html: str = ""
    currency_suffix: str = "원"
    venue_label: str = ""
    zombie_streak_days: int = 0
    zombie_retire_after_days: int = 5
    force_retired: bool = False
    timekeeper_header: str = ""
    staleness_banner: str = ""
    session_anchor: str = ""


def _exit_day_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty or "exit_date" not in df.columns:
        return pd.Series(dtype=str)
    return df["exit_date"].astype(str).str.slice(0, 10)


def _compress_sector(raw: object) -> str:
    s = str(raw or "").strip()
    if not s or s.lower() in ("none", "nan"):
        return ""
    return s.split(",")[0].strip()[:24]


def _sector_top_line(df: pd.DataFrame, ret_col: str = "final_ret") -> str:
    if df is None or df.empty or "sector" not in df.columns:
        return ""
    r = pd.to_numeric(df.get(ret_col), errors="coerce").fillna(0.0)
    sub = df.copy()
    sub["_r"] = r
    wins = sub[sub["_r"] > 0]
    if wins.empty:
        return ""
    sectors = [_compress_sector(x) for x in wins["sector"] if _compress_sector(x)]
    if not sectors:
        return ""
    from collections import Counter

    top, cnt = Counter(sectors).most_common(1)[0]
    return f"승자 테마 최빈: {top} ({cnt}건)"


def _toxic_hit_line(
    losers: pd.DataFrame,
    rules: Dict[str, Any],
    sys_config: dict,
) -> str:
    if losers is None or losers.empty or not rules:
        return ""
    from toxic_antipattern_core import any_toxic_rule_matches

    hits = 0
    n = 0
    for _, row in losers.iterrows():
        facts = {
            "dyn_cpv": row_scalar(row, "dyn_cpv"),
            "dyn_tb": row_scalar(row, "dyn_tb"),
            "v_energy": row_scalar(row, "v_energy"),
            "dyn_rs": row_scalar(row, "dyn_rs"),
        }
        sec = _compress_sector(row.get("sector")) or "유망섹터"
        if any_toxic_rule_matches(sys_config, facts, sec):
            hits += 1
        n += 1
    if n == 0:
        return ""
    pct = hits / n * 100.0
    return f"패자 오답노트 bbox 적중 {pct:.0f}% ({hits}/{n})"


def compute_vitality(
    g_all: pd.DataFrame,
    g_closed: pd.DataFrame,
    *,
    profile: PractitionerMarketProfile,
    tz_name: str,
    valid_open_mask: Optional[pd.Series] = None,
) -> Tuple[float, bool, str, Optional[float], int, float, float]:
    """vitality_score, is_zombie, status, wr_trend_pp, active_days, turnover, stale_ratio."""
    lookback = int(profile.vitality_lookback_days)
    tz = pytz.timezone(tz_name)
    today = datetime.now(tz).date()
    cutoff = (today - timedelta(days=lookback)).strftime("%Y-%m-%d")

    closed = g_closed.copy() if g_closed is not None else pd.DataFrame()
    if not closed.empty:
        closed["_xd"] = _exit_day_series(closed)
        closed = closed[closed["_xd"] >= cutoff]

    active_days = 0
    wr_trend_pp: Optional[float] = None
    rolling_wr: Optional[float] = None
    n_closed_w = int(len(closed))

    if n_closed_w > 0:
        ret = pd.to_numeric(col_series(closed, "final_ret"), errors="coerce").fillna(0.0)
        rolling_wr = float((ret > 0).sum() / len(ret) * 100.0)
        active_days = int(closed["_xd"].nunique())
        weeks: List[float] = []
        for w in range(4):
            w_end = today - timedelta(days=w * 7)
            w_start = w_end - timedelta(days=6)
            ws, we = w_start.strftime("%Y-%m-%d"), w_end.strftime("%Y-%m-%d")
            chunk = closed[(closed["_xd"] >= ws) & (closed["_xd"] <= we)]
            if len(chunk) >= 1:
                cr = pd.to_numeric(col_series(chunk, "final_ret"), errors="coerce").fillna(0.0)
                weeks.append(float((cr > 0).mean() * 100.0))
        if len(weeks) >= 2:
            wr_trend_pp = float(weeks[0] - weeks[-1])

    open_cnt = int(valid_open_mask.sum()) if valid_open_mask is not None else 0
    entries = 0
    if "entry_date" in g_all.columns:
        ent = g_all.copy()
        ent["_ed"] = ent["entry_date"].astype(str).str.slice(0, 10)
        entries = int((ent["_ed"] >= cutoff).sum())
    exits = n_closed_w
    turnover = float((entries + exits) / max(open_cnt, 1)) if (entries + exits) > 0 else 0.0

    stale_ratio = 0.0
    if valid_open_mask is not None and open_cnt > 0:
        open_df = g_all[valid_open_mask]
        if "bars_held" in open_df.columns:
            bh = pd.to_numeric(open_df["bars_held"], errors="coerce").fillna(0)
            stale_ratio = float((bh > 40).sum() / max(open_cnt, 1))
        elif "entry_date" in open_df.columns:
            stale_ratio = 0.3 if active_days < 3 and open_cnt >= 5 else 0.0

    score = 0.5
    if rolling_wr is not None:
        score += 0.25 * min(1.0, rolling_wr / 55.0)
    score += 0.2 * min(1.0, active_days / max(lookback * 0.5, 5))
    score += 0.15 * min(1.0, turnover / 0.5)
    if wr_trend_pp is not None and wr_trend_pp > 0:
        score += 0.1
    score -= 0.25 * stale_ratio
    score = float(max(0.0, min(1.0, score)))

    thr = float(profile.zombie_vitality_threshold)
    is_zombie = score < thr or (
        active_days < max(2, lookback // 15) and open_cnt >= 8 and turnover < 0.04
    )
    status = "ZOMBIE" if is_zombie else ("ACTIVE" if score >= 0.55 else "WATCH")
    return score, is_zombie, status, wr_trend_pp, active_days, turnover, stale_ratio


def build_post_mortem(
    g_closed: pd.DataFrame,
    *,
    profile: PractitionerMarketProfile,
    sys_config: dict,
    meta: Optional[dict],
    tz_name: str,
) -> Tuple[str, str, str]:
    """post_mortem_stats, sector_line, toxic_line (plain)."""
    tz = pytz.timezone(tz_name)
    today = datetime.now(tz).date()
    win_days = int(profile.post_mortem_window_days)
    cutoff = (today - timedelta(days=win_days)).strftime("%Y-%m-%d")

    closed = prepare_forward_trades_df(g_closed, context="build_post_mortem")
    if closed.empty or "exit_date" not in closed.columns:
        return (
            f"최근 {win_days}일 청산 없음 (프로필 {profile.rank_tier})",
            "",
            "",
        )
    closed["_xd"] = _exit_day_series(closed)
    windowed = closed[closed["_xd"] >= cutoff].copy()
    n_w = len(windowed)
    if n_w < profile.min_closed_for_post_mortem:
        return (
            f"동적 윈도우 {win_days}일 · 청산 {n_w}건 — "
            f"Post-Mortem 최소 {profile.min_closed_for_post_mortem}건 미달",
            "",
            "",
        )

    fr = pd.to_numeric(col_series(windowed, "final_ret"), errors="coerce").fillna(0.0)
    winners = windowed[fr >= float(profile.winner_ret_pct)]
    losers = windowed[fr <= float(profile.loser_ret_pct)]

    sector_line = _sector_top_line(winners)
    rules = toxic_rules_for_profile(profile, sys_config)
    toxic_line = _toxic_hit_line(losers, rules, sys_config)

    stats = (
        f"윈도우 {win_days}일({profile.post_mortem_min_days}~{win_days}) · "
        f"청산 {n_w} · 대박 {len(winners)} · 참사 {len(losers)}"
    )

    if len(winners) < 2 or len(losers) < 2:
        return stats, sector_line, toxic_line

    try:
        rfa = ReportFeatureAnalyzer(sys_config=sys_config, meta=meta)
        lines, ok, insights = rfa.build_winner_loser_dna_contrast(
            winners_df=winners,
            losers_df=losers,
            top_n=2,
            min_per_group=2,
        )
        if ok and insights:
            plain = re.sub(r"<[^>]+>", "", "".join(lines))
            plain = re.sub(r"\s+", " ", plain).strip()[:500]
            stats = plain or stats
    except Exception:
        pass

    return stats, sector_line, toxic_line


def build_practitioner_brief(
    *,
    market: str,
    group_key: str,
    sample_sig: str,
    g_all: pd.DataFrame,
    g_closed: pd.DataFrame,
    g_today_closed: pd.DataFrame,
    sys_config: dict,
    meta: Optional[dict],
    base_seed: float,
    market_icon: str,
    mkt_today_str: str,
    session_anchor: str = "",
    timekeeper_header: str = "",
    staleness_banner: str = "",
    valid_open_mask: pd.Series,
    format_exit_reason,
    safe_ret_fn,
    win_loss_fn,
) -> PractitionerBrief:
    rank_tier = extract_rank_tier(sample_sig)
    if str(market).upper().startswith("BG"):
        rank_tier = "PRACT" if "PRACT" in str(sample_sig).upper() else rank_tier
    profile = resolve_practitioner_profile(market, rank_tier, sys_config)
    g_all = prepare_forward_trades_df(g_all, context=f"PIL:{market}:{group_key}:all")
    g_closed = prepare_forward_trades_df(g_closed, context=f"PIL:{market}:{group_key}:closed")
    g_today_closed = prepare_forward_trades_df(
        g_today_closed, context=f"PIL:{market}:{group_key}:today"
    )

    mk_u = str(market).upper()
    if mk_u.startswith("BG"):
        tz_name = "UTC"
    elif mk_u == "KR":
        tz_name = "Asia/Seoul"
    else:
        tz_name = "America/New_York"

    if "final_ret" in g_today_closed.columns:
        g_today_closed = g_today_closed.copy()
        g_today_closed["_ret_pct"] = g_today_closed["final_ret"].map(safe_ret_fn)
    else:
        g_today_closed = g_today_closed.copy()
        g_today_closed["_ret_pct"] = 0.0
    win_cnt, loss_cnt, flat_cnt = win_loss_fn(g_today_closed["_ret_pct"])

    cum_pnl = 0.0
    if "sim_kelly_invest" in g_closed.columns and "final_ret" in g_closed.columns:
        inv = pd.to_numeric(g_closed["sim_kelly_invest"], errors="coerce").fillna(400000).replace(0, 400000)
        ret_c = pd.to_numeric(g_closed["final_ret"], errors="coerce").fillna(0.0)
        cum_pnl = scalar_float((inv * ret_c / 100.0).sum())
    compound_seed = scalar_float(float(base_seed) + cum_pnl)
    open_cnt = int(valid_open_mask.sum())

    vit = compute_vitality(
        g_all,
        g_closed,
        profile=profile,
        tz_name=tz_name,
        valid_open_mask=valid_open_mask,
    )
    score, is_zombie, v_status, wr_trend, active_d, turnover, stale = vit

    closed_for_pm = g_closed[g_closed["status"].astype(str).str.contains("CLOSED", na=False)]
    pm_stats, sector_line, toxic_line = build_post_mortem(
        closed_for_pm,
        profile=profile,
        sys_config=sys_config,
        meta=meta,
        tz_name=tz_name,
    )

    tz = pytz.timezone(tz_name)
    today = datetime.now(tz).date()
    cutoff = (today - timedelta(days=profile.post_mortem_window_days)).strftime("%Y-%m-%d")
    closed_w = closed_for_pm.copy()
    if not closed_w.empty and "exit_date" in closed_w.columns:
        closed_w["_xd"] = _exit_day_series(closed_w)
        closed_w = closed_w[closed_w["_xd"] >= cutoff]
    n_closed_window = len(closed_w)
    rolling_wr = None
    if n_closed_window > 0:
        r = pd.to_numeric(col_series(closed_w, "final_ret"), errors="coerce").fillna(0.0)
        rolling_wr = float((r > 0).sum() / len(r) * 100.0)

    llm_facts = {
        "market": market,
        "group_key": group_key,
        "rank_tier": rank_tier,
        "post_mortem_window": profile.post_mortem_window_days,
        "vitality_lookback": profile.vitality_lookback_days,
        "wr_trend_pp": f"{wr_trend:+.1f}" if wr_trend is not None else "—",
        "vitality_score": f"{score:.2f}",
        "is_zombie": is_zombie,
        "post_mortem_stats": pm_stats,
        "sector_line": sector_line,
        "toxic_line": toxic_line,
        "rolling_wr": f"{rolling_wr:.1f}%" if rolling_wr is not None else "—",
        "n_closed_window": n_closed_window,
        "vitality_line": (
            f"활력 {score:.2f} · 추세 {wr_trend:+.1f}%p · 회전 {turnover:.2f} · "
            f"상태 {v_status}"
            if wr_trend is not None
            else f"활력 {score:.2f} · 회전 {turnover:.2f} · 상태 {v_status}"
        ),
    }
    llm_summary = build_practitioner_llm_summary(llm_facts, force=True)

    penalty_action = "none"
    if is_zombie:
        penalty_action = "KELLY_MULT→0 · COOLED/퇴역 후보 (MetaGovernor·데스매치 연동)"

    closes_html = ""
    if not g_today_closed.empty:
        for _, row in g_today_closed.iterrows():
            name = html.escape(str(row.get("name", "-")), quote=False)
            reason = html.escape(format_exit_reason(row.get("exit_reason")), quote=False)
            ret = row_scalar(row, "_ret_pct", safe_ret_fn(row_scalar(row, "final_ret")))
            closes_html += f" - {name} ({ret:+.2f}%) / {reason}\n"
    else:
        closes_html = " - 없음\n"

    return PractitionerBrief(
        market=market,
        group_key=group_key,
        rank_tier=rank_tier,
        profile=profile,
        market_icon=market_icon,
        today_win=win_cnt,
        today_loss=loss_cnt,
        today_flat=flat_cnt,
        n_today_closed=int(len(g_today_closed)),
        compound_seed=compound_seed,
        open_cnt=open_cnt,
        rolling_wr_pct=rolling_wr,
        n_closed_window=n_closed_window,
        wr_trend_pp=wr_trend,
        active_days=active_d,
        turnover_30d=turnover,
        stale_hold_ratio=stale,
        vitality_score=score,
        is_zombie=is_zombie,
        vitality_status=v_status,
        post_mortem_window_days=profile.post_mortem_window_days,
        post_mortem_stats=pm_stats,
        sector_line=sector_line,
        toxic_line=toxic_line,
        llm_summary=llm_summary,
        penalty_action=penalty_action,
        today_closes_html=closes_html,
        timekeeper_header=timekeeper_header,
        staleness_banner=staleness_banner,
        session_anchor=session_anchor or mkt_today_str,
    )


def format_practitioner_brief_html(b: PractitionerBrief) -> str:
    venue = f" · {html.escape(b.venue_label, quote=False)}" if b.venue_label else ""
    anchor_s = html.escape(b.session_anchor or "—", quote=False)
    lines = [
        f"{b.market_icon} <b>[{b.market} 실무자 리포트 · PIL{venue}]</b> "
        f"{html.escape(b.group_key, quote=False)} "
        f"<i>({html.escape(b.rank_tier, quote=False)} · {html.escape(b.profile.narrative_focus, quote=False)})</i>\n",
    ]
    if b.timekeeper_header:
        lines.append(b.timekeeper_header)
    if b.staleness_banner:
        lines.append(b.staleness_banner)
    lines.append(
        f"📅 앵커일 <b>{anchor_s}</b> 청산: <b>{b.today_win}승 {b.today_loss}패</b>",
    )
    if b.today_flat:
        lines[-1] += f" · 무{b.today_flat}"
    lines[-1] += f" (청산 <b>{b.n_today_closed}</b>건)\n"

    wr_s = f"{b.rolling_wr_pct:.1f}%" if b.rolling_wr_pct is not None else "—"
    trend_s = f"{b.wr_trend_pp:+.1f}%p" if b.wr_trend_pp is not None else "—"
    icon_v = "🧟" if b.is_zombie else ("🔥" if b.vitality_status == "ACTIVE" else "👀")
    lines.append(
        f"{icon_v} <b>활력</b> {b.vitality_score:.2f} · "
        f"30일 승률 <b>{wr_s}</b> · 추세 <b>{trend_s}</b> · "
        f"활성일 <b>{b.active_days}</b> · 회전율 <b>{b.turnover_30d:.2f}</b> · "
        f"<b>{b.vitality_status}</b>\n"
    )
    if b.is_zombie:
        lines.append(
            f"⛔ <b>시스템 페널티:</b> {html.escape(b.penalty_action, quote=False)}\n"
        )
        if b.zombie_streak_days > 0:
            lines.append(
                f" ⏱️ 좀비 연속 <b>{b.zombie_streak_days}</b>/{b.zombie_retire_after_days}일 "
                f"{'→ <b>RETIRED</b>' if b.force_retired else '(N일 시 강제 퇴역)'}\n"
            )
    if b.force_retired:
        lines.append(" ☠️ <b>무자비 퇴역:</b> Kelly=0 · registry <b>RETIRED</b>\n")

    cur = b.currency_suffix or "원"
    lines.append(f"💰 누적 복리 시드: <b>{b.compound_seed:,.0f}{cur}</b>\n")
    lines.append(f"📦 유효 보유: <b>{b.open_cnt}개</b>\n")
    lines.append(
        f"🔬 <b>Post-Mortem</b> ({b.post_mortem_window_days}일 윈도우 · "
        f"청산 {b.n_closed_window}건)\n"
    )
    if b.sector_line:
        lines.append(f" ▪ {html.escape(b.sector_line, quote=False)}\n")
    if b.toxic_line:
        lines.append(f" ▪ {html.escape(b.toxic_line, quote=False)}\n")
    lines.append(format_llm_html_line(b.llm_summary))
    lines.append("📌 오늘 청산:\n")
    lines.append(b.today_closes_html)
    return "".join(lines)


def parse_group_from_sig(sig_type: object) -> str:
    return ledger_group_key(str(sig_type or ""))
