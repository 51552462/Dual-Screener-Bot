"""
주간 Flow 롤업 — UniversalDnaBlock · FlowTagReportSnapshot 일일 엔진 재사용(7일 slice).
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from forward_flow_tag_deep_dive import FlowTagBlock, build_flow_tag_snapshot
from forward_score_bucket_deep_dive import (
    UniversalDnaBlock,
    _assemble_universal_governor_insight,
    build_universal_dna_block,
)
from report_feature_analyzer import ReportFeatureAnalyzer


@dataclass(frozen=True)
class WeeklyDnaRollup:
    market: str
    n_total: int
    n_winners: int
    n_losers: int
    dominant_features: Tuple[str, ...]
    hall_name: Optional[str]
    hall_ret: Optional[float]
    shame_name: Optional[str]
    shame_ret: Optional[float]
    summary_plain: str


@dataclass(frozen=True)
class WeeklyFlowTagRollup:
    market: str
    dominant_tag: Optional[str]
    dominant_tag_n: int
    best_pnl_tag: Optional[str]
    best_pnl_cum_ret: float
    toxic_tag: Optional[str]
    toxic_cum_ret: Optional[float]
    toxic_reason: str
    penalty_mult: Optional[float]
    top_tags_lines: Tuple[str, ...]


def _strip_html(s: str) -> str:
    t = re.sub(r"<[^>]+>", "", s)
    return re.sub(r"\s+", " ", t).strip()


def _empty_dna_rollup(market: str) -> WeeklyDnaRollup:
    return WeeklyDnaRollup(
        market=str(market).upper(),
        n_total=0,
        n_winners=0,
        n_losers=0,
        dominant_features=(),
        hall_name=None,
        hall_ret=None,
        shame_name=None,
        shame_ret=None,
        summary_plain="표본 없음",
    )


def _empty_tag_rollup(market: str) -> WeeklyFlowTagRollup:
    return WeeklyFlowTagRollup(
        market=str(market).upper(),
        dominant_tag=None,
        dominant_tag_n=0,
        best_pnl_tag=None,
        best_pnl_cum_ret=0.0,
        toxic_tag=None,
        toxic_cum_ret=None,
        toxic_reason="",
        penalty_mult=None,
        top_tags_lines=(),
    )


def build_weekly_dna_rollup(
    df: pd.DataFrame,
    *,
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> WeeklyDnaRollup:
    """7일 청산 표본 → UniversalDnaBlock 1회."""
    mkt = str(market).upper()
    if df is None or df.empty:
        return _empty_dna_rollup(mkt)
    try:
        rfa = ReportFeatureAnalyzer(sys_config=sys_config, meta=meta)
        block: UniversalDnaBlock = build_universal_dna_block(
            df,
            analyzer=rfa,
            sys_config=sys_config,
            meta=meta,
        )
        feats = tuple(ins.label for ins in block.insights[:3])
        summary_plain = _strip_html(_assemble_universal_governor_insight(block))
        return WeeklyDnaRollup(
            market=mkt,
            n_total=block.n_total,
            n_winners=block.n_winners,
            n_losers=block.n_losers,
            dominant_features=feats,
            hall_name=block.top_hall_name,
            hall_ret=block.top_hall_ret,
            shame_name=block.top_shame_name,
            shame_ret=block.top_shame_ret,
            summary_plain=summary_plain,
        )
    except Exception:
        return _empty_dna_rollup(mkt)


def build_weekly_flow_tag_rollup(
    df: pd.DataFrame,
    *,
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
    today_str: Optional[str] = None,
) -> WeeklyFlowTagRollup:
    """7일 청산 표본 → FlowTagReportSnapshot (주간은 registry 미기록)."""
    mkt = str(market).upper()
    if df is None or df.empty:
        return _empty_tag_rollup(mkt)
    try:
        return _build_weekly_flow_tag_rollup_inner(
            df, market=mkt, sys_config=sys_config, today_str=today_str
        )
    except Exception:
        return _empty_tag_rollup(mkt)


def _build_weekly_flow_tag_rollup_inner(
    df: pd.DataFrame,
    *,
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
    today_str: Optional[str] = None,
) -> WeeklyFlowTagRollup:
    snap = build_flow_tag_snapshot(
        df,
        sys_config=sys_config,
        market=market,
        today_str=today_str,
        persist_toxic=False,
    )
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        penalty_mult = float(cfg.get("FLOW_TAG_TOXIC_DEFAULT_MULT", 0.85))
    except (TypeError, ValueError):
        penalty_mult = 0.85
    reg = cfg.get("FLOW_TAG_PENALTY_MULT")
    if isinstance(reg, dict) and snap.toxic is not None:
        try:
            penalty_mult = float(reg.get(snap.toxic.tag, penalty_mult))
        except (TypeError, ValueError):
            pass

    blocks: Tuple[FlowTagBlock, ...] = snap.blocks
    dominant_tag: Optional[str] = None
    dominant_n = 0
    best_pnl_tag: Optional[str] = None
    best_cum = 0.0
    lines: List[str] = []

    if blocks:
        by_n = max(blocks, key=lambda b: b.n)
        dominant_tag = by_n.tag
        dominant_n = int(by_n.n)
        by_pnl = max(blocks, key=lambda b: b.cum_ret_pct)
        best_pnl_tag = by_pnl.tag
        best_cum = float(by_pnl.cum_ret_pct)
        for b in blocks[:5]:
            lines.append(
                f"{b.tag}(n={b.n}, cum={b.cum_ret_pct:+.1f}%, WR={b.win_rate_pct:.0f}%)"
            )

    toxic = snap.toxic
    return WeeklyFlowTagRollup(
        market=str(market).upper(),
        dominant_tag=dominant_tag,
        dominant_tag_n=dominant_n,
        best_pnl_tag=best_pnl_tag,
        best_pnl_cum_ret=best_cum,
        toxic_tag=toxic.tag if toxic else None,
        toxic_cum_ret=float(toxic.cum_ret_pct) if toxic else None,
        toxic_reason=toxic.toxic_reason if toxic else "",
        penalty_mult=penalty_mult if toxic else None,
        top_tags_lines=tuple(lines),
    )


def format_weekly_dna_rollup_html(rollup: WeeklyDnaRollup) -> str:
    icon = "🇰🇷" if rollup.market == "KR" else "🇺🇸"
    out = f"\n🧬 <b>[{icon} 주간 Universal DNA]</b>\n"
    out += (
        f" 표본 <b>{rollup.n_total}</b>건 "
        f"(대박군 {rollup.n_winners} · 참사군 {rollup.n_losers})\n"
    )
    if rollup.dominant_features:
        feats = " · ".join(html.escape(f, quote=False) for f in rollup.dominant_features)
        out += f" 🔑 <b>지배 팩터:</b> {feats}\n"
    else:
        out += " 🔑 <b>지배 팩터:</b> <i>통계 분리 미달</i>\n"
    if rollup.hall_name:
        hr = f"({rollup.hall_ret:+.0f}%)" if rollup.hall_ret is not None else ""
        out += f" 🏆 캐리 1위: <b>{html.escape(rollup.hall_name, quote=False)}</b>{hr}\n"
    if rollup.shame_name:
        sr = f"({rollup.shame_ret:+.0f}%)" if rollup.shame_ret is not None else ""
        out += f" 💀 출혈 1위: <b>{html.escape(rollup.shame_name, quote=False)}</b>{sr}\n"
    if rollup.summary_plain:
        out += f" <i>{html.escape(rollup.summary_plain[:280], quote=False)}</i>\n"
    return out


def format_weekly_flow_tag_rollup_html(rollup: WeeklyFlowTagRollup) -> str:
    icon = "🇰🇷" if rollup.market == "KR" else "🇺🇸"
    out = f"\n🏷️ <b>[{icon} 주간 Flow 태그]</b>\n"
    if rollup.dominant_tag:
        out += (
            f" 📊 최다 노출: <code>{html.escape(rollup.dominant_tag, quote=False)}</code> "
            f"({rollup.dominant_tag_n}건)\n"
        )
    if rollup.best_pnl_tag:
        out += (
            f" 💰 태그 중 최고 cum: <code>{html.escape(rollup.best_pnl_tag, quote=False)}</code> "
            f"({rollup.best_pnl_cum_ret:+.1f}%)\n"
        )
    if rollup.toxic_tag:
        out += (
            f" ☠️ <b>주간 독성 1위:</b> <code>{html.escape(rollup.toxic_tag, quote=False)}</code> "
            f"(cum {rollup.toxic_cum_ret:+.1f}% · {html.escape(rollup.toxic_reason[:80], quote=False)})\n"
        )
        if rollup.penalty_mult is not None:
            out += f" ↳ 페널티 배수 적용·권고: <b>×{rollup.penalty_mult:.2f}</b>\n"
    elif rollup.top_tags_lines:
        out += " ☠️ <b>독성:</b> <i>주간 임계 미충족</i>\n"
    else:
        out += " ↳ flow_tags 표본 없음\n"
    if rollup.top_tags_lines:
        out += " · " + html.escape(" | ".join(rollup.top_tags_lines[:4]), quote=False) + "\n"
    return out
