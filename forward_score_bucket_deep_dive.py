"""
점수대(bucket)별 포워드 딥 다이브 — 조인 1회·pd.cut 벡터화 후 groupby 슬라이스만 분석.
"""
from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from reports.forward_report_scalar import col_series, prepare_forward_trades_df, scalar_float
from reports.report_feature_analyzer import ContrastInsight, ReportFeatureAnalyzer


@dataclass(frozen=True)
class BucketBlock:
    """점수 구간 1개에 대한 텔레그램 요약용 뷰 모델 (SSOT)."""

    bucket_label: str
    n_rows: int
    win_rate_pct: float
    profit_factor: float
    dominant_sector: str
    top_stocks_html: str
    exit_date_min: Optional[str]
    exit_date_max: Optional[str]
    key_drivers_html: str
    dna_compact_html: str
    dna_contrast_lines: Tuple[str, ...]


@dataclass(frozen=True)
class DualTrackBucketBlock:
    """당일 실전 vs 과거 기준(Sim) — 점수대 1쌍."""

    bucket_label: str
    live: Optional[BucketBlock]
    hist: Optional[BucketBlock]
    drift_comment: str


@dataclass(frozen=True)
class UniversalDnaBlock:
    """점수대 무관 전체 시장 Universal DNA SSOT."""

    n_total: int
    n_winners: int
    n_losers: int
    exit_date_min: Optional[str]
    exit_date_max: Optional[str]
    feature_brackets_html: str
    insights: Tuple[ContrastInsight, ...]
    hall_of_fame_html: str
    wall_of_shame_html: str
    top_hall_name: Optional[str]
    top_hall_ret: Optional[float]
    top_shame_name: Optional[str]
    top_shame_ret: Optional[float]


def _resolve_stock_name(row: pd.Series) -> str:
    for k in ("name", "stock_name", "ticker", "code"):
        if k not in row.index:
            continue
        v = row.get(k)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        t = str(v).strip()
        if t and t.lower() not in ("nan", "none"):
            return t
    return "—"


def _sector_ok_for_mode(val: Any) -> bool:
    t = str(val).strip()
    if not t or t.lower() in ("nan", "none"):
        return False
    if "유망" in t or "포착" in t:
        return False
    if t == "기타/혼합":
        return False
    return True


def _dominant_sector_from_bucket(t_df: pd.DataFrame) -> str:
    if "sector" not in t_df.columns or t_df.empty:
        return "—"
    mask = t_df["sector"].apply(_sector_ok_for_mode)
    valid = t_df.loc[mask, "sector"]
    if valid.empty:
        return "—"
    vc = valid.astype(str).str.strip().value_counts()
    if vc.empty:
        return "—"
    top = str(vc.index[0])[:20]
    return top


def _exit_date_span(t_df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    if "exit_date" not in t_df.columns or t_df.empty:
        return None, None
    dt = pd.to_datetime(t_df["exit_date"], errors="coerce").dropna()
    if dt.empty:
        return None, None
    dmin = dt.min()
    dmax = dt.max()
    fmt = lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)[:10]
    return fmt(dmin), fmt(dmax)


def _format_key_drivers_nlargest(
    t_df: pd.DataFrame, *, k: int = 3, ret_col: str = "final_ret"
) -> str:
    if t_df.empty or ret_col not in t_df.columns:
        return "—"
    fr = pd.to_numeric(t_df[ret_col], errors="coerce")
    work = t_df.assign(_fr=fr).dropna(subset=["_fr"])
    if work.empty:
        return "—"
    pos = work[work["_fr"] > 0]
    if len(pos) > 0:
        pick = pos.nlargest(min(k, len(pos)), "_fr")
    else:
        pick = work.nlargest(min(k, len(work)), "_fr")
    parts: List[str] = []
    for _, row in pick.iterrows():
        nm = html.escape(_resolve_stock_name(row), quote=False)
        r = scalar_float(row["_fr"])
        parts.append(f"{nm}({r:+.0f}%)")
    return ", ".join(parts) if parts else "—"


def _format_ranked_extremes(
    t_df: pd.DataFrame,
    *,
    k: int,
    largest: bool,
    ret_col: str = "final_ret",
) -> Tuple[str, Optional[str], Optional[float]]:
    """명예의 전당(largest) / 수치의 전당(nsmallest). (HTML, 1등 종목명, 1등 수익률)"""
    if t_df.empty or ret_col not in t_df.columns:
        return "—", None, None
    fr = pd.to_numeric(t_df[ret_col], errors="coerce")
    work = t_df.assign(_fr=fr).dropna(subset=["_fr"])
    if work.empty:
        return "—", None, None
    if largest:
        pick = work.nlargest(min(k, len(work)), "_fr")
    else:
        pick = work.nsmallest(min(k, len(work)), "_fr")
    parts: List[str] = []
    first_name: Optional[str] = None
    first_ret: Optional[float] = None
    for i, (_, row) in enumerate(pick.iterrows()):
        nm_plain = _resolve_stock_name(row)
        r = scalar_float(row["_fr"])
        if i == 0:
            first_name = nm_plain
            first_ret = r
        parts.append(f"{html.escape(nm_plain, quote=False)}({r:+.0f}%)")
    joined = ", ".join(parts) if parts else "—"
    return joined, first_name, first_ret


def _dna_brackets_from_insights(insights: Sequence[ContrastInsight]) -> str:
    if not insights:
        return "<i>표본 부족 (대박 DNA 판별 인사이트 0건)으로 Micro-DNA 대조 생략</i>"
    chunks = [f"[<b>{html.escape(ins.label, quote=False)}</b>]" for ins in insights]
    return " ".join(chunks)


def _universal_feature_brackets_detailed(insights: Sequence[ContrastInsight]) -> str:
    """Universal용 — 이진은 승자 활성 비율, 연속은 분리 방향을 대괄호에 압축."""
    if not insights:
        return "<i>판별력 Top 피처 없음</i>"
    parts: List[str] = []
    for ins in insights:
        lab = html.escape(ins.label, quote=False)
        if ins.kind == "binary":
            if ins.direction == "higher_in_winners":
                parts.append(f"[<b>{lab}</b> · {ins.winner_line}]")
            else:
                parts.append(f"[<b>{lab}</b> · 패자군 우세 · {ins.loser_line}]")
        else:
            dir_ko = "높은 값대" if ins.direction == "higher_in_winners" else "낮은 값대"
            parts.append(f"[<b>{lab}</b> · 승자군 {dir_ko}]")
    return " ".join(parts)


def _assemble_universal_governor_insight(block: UniversalDnaBlock) -> str:
    if block.n_winners < 5 or block.n_losers < 5:
        return (
            f"전체 통합 Universal 법칙 산출에 표본이 부족합니다 "
            f"(대박 <b>{block.n_winners}</b>건 · 참사 <b>{block.n_losers}</b>건, 각 5건 이상 필요). "
            "청산 표본이 더 쌓이면 재추정하십시오."
        )
    if not block.insights:
        hall = html.escape(block.top_hall_name or "—", quote=False)
        hall_r = (
            f"({block.top_hall_ret:+.0f}%)" if block.top_hall_ret is not None else ""
        )
        shame = html.escape(block.top_shame_name or "—", quote=False)
        shame_r = (
            f"({block.top_shame_ret:+.0f}%)" if block.top_shame_ret is not None else ""
        )
        return (
            f"수치 DNA 판별력은 약하나, 롤링 윈도우에서 계좌를 끌어올린 1위는 "
            f"<b>{hall}</b>{hall_r} (대박군 <b>{block.n_winners}</b>건 중 상위 캐리)이며, "
            f"출혈 1위는 <b>{shame}</b>{shame_r}입니다."
        )

    feat_labels = " · ".join(
        f"<b>{html.escape(ins.label, quote=False)}</b>" for ins in block.insights[:2]
    )
    hall = html.escape(block.top_hall_name or "—", quote=False)
    hall_r = f"({block.top_hall_ret:+.0f}%)" if block.top_hall_ret is not None else ""
    shame = html.escape(block.top_shame_name or "—", quote=False)
    shame_r = f"({block.top_shame_ret:+.0f}%)" if block.top_shame_ret is not None else ""

    return (
        f"대박군은 {feat_labels} 축에서 패자군과 통계적으로 분리되었고, "
        f"계좌를 끌어올린 실명 1위는 <b>{hall}</b>{hall_r}이며 "
        f"동일 윈도우 대박 표본 <b>{block.n_winners}</b>건 중 상위 캐리 라인입니다. "
        f"출혈 1위는 <b>{shame}</b>{shame_r}입니다."
    )


def build_universal_dna_block(
    df: pd.DataFrame,
    *,
    analyzer: Optional[ReportFeatureAnalyzer] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    jackpot_threshold: float = 5.0,
    disaster_threshold: float = -3.0,
    min_per_group: int = 5,
    top_n_features: int = 2,
    top_n_extremes: int = 3,
) -> UniversalDnaBlock:
    """
    전체 청산 표본 1회 — Universal 판별 피처 + 명예/수치의 전당 + 청산일 알리바이.
    """
    rfa = analyzer or ReportFeatureAnalyzer(sys_config=sys_config, meta=meta)
    n_total = len(df) if df is not None else 0
    exit_min, exit_max = _exit_date_span(df) if n_total else (None, None)

    if df is None or df.empty or "final_ret" not in df.columns:
        empty_ins: Tuple[ContrastInsight, ...] = ()
        return UniversalDnaBlock(
            n_total=n_total,
            n_winners=0,
            n_losers=0,
            exit_date_min=exit_min,
            exit_date_max=exit_max,
            feature_brackets_html="<i>표본 없음</i>",
            insights=empty_ins,
            hall_of_fame_html="—",
            wall_of_shame_html="—",
            top_hall_name=None,
            top_hall_ret=None,
            top_shame_name=None,
            top_shame_ret=None,
        )

    fr = pd.to_numeric(col_series(df, "final_ret"), errors="coerce").fillna(0.0)
    j_th = scalar_float(jackpot_threshold, 5.0)
    d_th = scalar_float(disaster_threshold, -3.0)
    winners = df.loc[fr >= j_th]
    losers = df.loc[fr <= d_th]
    nw, nl = len(winners), len(losers)

    insights, err_lines, _, _ = rfa._collect_winner_loser_contrast_insights(
        winners,
        losers,
        top_n=int(top_n_features),
        min_per_group=int(min_per_group),
    )
    if insights:
        brackets = _universal_feature_brackets_detailed(insights)
    else:
        brackets = (
            " ".join(err_lines).strip()
            or f"<i>표본 부족 (대박 <b>{nw}</b>건 · 참사 <b>{nl}</b>건)으로 Universal DNA 대조 생략</i>"
        )

    hall_html, hall_name, hall_ret = _format_ranked_extremes(
        df, k=int(top_n_extremes), largest=True
    )
    shame_html, shame_name, shame_ret = _format_ranked_extremes(
        df, k=int(top_n_extremes), largest=False
    )

    return UniversalDnaBlock(
        n_total=n_total,
        n_winners=nw,
        n_losers=nl,
        exit_date_min=exit_min,
        exit_date_max=exit_max,
        feature_brackets_html=brackets,
        insights=tuple(insights),
        hall_of_fame_html=hall_html,
        wall_of_shame_html=shame_html,
        top_hall_name=hall_name,
        top_hall_ret=hall_ret,
        top_shame_name=shame_name,
        top_shame_ret=shame_ret,
    )


def format_universal_dna_html(
    block: UniversalDnaBlock,
    *,
    market: str,
    rolling_days: int,
    today_str: str,
) -> str:
    m_esc = html.escape(str(market), quote=False)
    today_esc = html.escape(today_str, quote=False)

    out = "🌍 <b>[전체 티어 통합: Universal DNA]</b>\n"
    out += (
        f"📎 <i>{m_esc}장 · 최근 <b>{rolling_days}</b>일 청산 롤링 · "
        f"리포트일 KST <b>{today_esc}</b> · 전체 <b>{block.n_total}</b>건 · "
        f"대박(≥5%) <b>{block.n_winners}</b> · 참사(≤-3%) <b>{block.n_losers}</b></i>\n"
    )
    if block.exit_date_min and block.exit_date_max:
        out += (
            f"📅 청산 알리바이: <b>{html.escape(block.exit_date_min, quote=False)}</b>"
            f"~<b>{html.escape(block.exit_date_max, quote=False)}</b> (KST)\n"
        )
    else:
        out += "📅 <i>청산일 알리바이: exit_date 미기록</i>\n"

    out += f"✅ <b>전체 대박주 절대 공통점</b> ➔ {block.feature_brackets_html}\n"
    out += f"🏆 <b>명예의 전당 Top 3:</b> {block.hall_of_fame_html}\n"
    out += f"💀 <b>수치의 전당 Worst 3:</b> {block.wall_of_shame_html}\n\n"
    out += "🗣️ <b>[관제탑 · Universal]</b> "
    out += _assemble_universal_governor_insight(block) + "\n\n"
    return out


def _exit_span_html(b: BucketBlock) -> str:
    if b.exit_date_min and b.exit_date_max:
        return (
            f"청산 KST <b>{html.escape(b.exit_date_min, quote=False)}</b>"
            f"~<b>{html.escape(b.exit_date_max, quote=False)}</b>"
        )
    return "<i>청산일 미기록</i>"


def _sector_tag_html(sector: str) -> str:
    if not sector or sector == "—":
        return "[테마 미분류]"
    return f"[{html.escape(sector, quote=False)} 주도]"


def _format_champion_receipt(
    b: BucketBlock,
    *,
    emphasize: str,
) -> str:
    """emphasize: 'wr' | 'pf' | 'both'"""
    lbl = html.escape(b.bucket_label, quote=False)
    core = f"<b>{lbl}</b> (표본 <b>{b.n_rows}</b>건"
    if emphasize in ("wr", "both"):
        core += f" · 승률 <b>{b.win_rate_pct:.1f}%</b>"
    if emphasize in ("pf", "both"):
        core += f" · PF <b>{b.profit_factor:.2f}</b>"
    core += f") ➔ {_sector_tag_html(b.dominant_sector)} {b.top_stocks_html}"
    core += f" · {_exit_span_html(b)}"
    return core


class ForwardScoreBucketDeepDive:
    def __init__(
        self,
        *,
        sys_config: Optional[Dict[str, Any]] = None,
        meta: Optional[Dict[str, Any]] = None,
        analyzer: Optional[ReportFeatureAnalyzer] = None,
        min_bucket_rows: int = 5,
        min_contrast_per_group: int = 2,
        top_n_dna: int = 2,
        top_n_drivers: int = 3,
        top_n_summary_stocks: int = 2,
        jackpot_threshold: float = 5.0,
        disaster_threshold: float = -3.0,
    ) -> None:
        self._analyzer = analyzer or ReportFeatureAnalyzer(
            sys_config=sys_config, meta=meta
        )
        self._min_bucket = int(min_bucket_rows)
        self._min_contrast = int(min_contrast_per_group)
        self._top_dna = int(top_n_dna)
        self._top_drv = int(top_n_drivers)
        self._top_summary_stocks = int(top_n_summary_stocks)
        self._th_j = float(jackpot_threshold)
        self._th_d = float(disaster_threshold)

    @staticmethod
    def assign_score_buckets(df: pd.DataFrame) -> pd.DataFrame:
        """
        total_score가 있으면 pd.cut으로 10~90점대 라벨을 벡터 할당.
        없으면 기존 `tier` 열을 `_score_bucket`으로 복사.
        """
        out = df
        score_src = None
        if "total_score" in df.columns and pd.to_numeric(df["total_score"], errors="coerce").notna().any():
            score_src = pd.to_numeric(df["total_score"], errors="coerce")
        elif "final_score" in df.columns and pd.to_numeric(df["final_score"], errors="coerce").notna().any():
            score_src = pd.to_numeric(df["final_score"], errors="coerce")
        if score_src is not None:
            out = df.copy()
            labels = [f"{t}점대" for t in range(10, 100, 10)]
            edges = [-np.inf] + list(range(20, 100, 10)) + [np.inf]
            out["_score_bucket"] = pd.cut(score_src, bins=edges, labels=labels, right=False)
        elif "tier" in df.columns:
            out = df.copy()
            out["_score_bucket"] = out["tier"]
        else:
            out = df.copy()
            out["_score_bucket"] = np.nan
        return out

    def build_bucket_blocks(self, df: pd.DataFrame) -> List[BucketBlock]:
        prep = self.assign_score_buckets(df)
        sub = prep.dropna(subset=["_score_bucket"])
        if sub.empty:
            return []

        blocks: List[BucketBlock] = []
        grp = sub.groupby("_score_bucket", observed=True, sort=True)
        for bucket_label, t_df in grp:
            blabel = str(bucket_label)
            if len(t_df) < self._min_bucket:
                continue

            fr_b = pd.to_numeric(col_series(t_df, "final_ret"), errors="coerce").fillna(0.0)
            wins_mask = fr_b > 0
            wins_count = int(wins_mask.sum())
            t_wr = (wins_count / len(t_df)) * 100.0 if len(t_df) else 0.0
            gross_profit = scalar_float(fr_b.loc[wins_mask].sum())
            gross_loss = abs(scalar_float(fr_b.loc[~wins_mask].sum()))
            t_pf = scalar_float(gross_profit / (gross_loss + 0.1), 1.0)

            sector = _dominant_sector_from_bucket(t_df)
            exit_min, exit_max = _exit_date_span(t_df)
            top2 = _format_key_drivers_nlargest(t_df, k=self._top_summary_stocks)
            drivers = _format_key_drivers_nlargest(t_df, k=self._top_drv)

            winners = t_df.loc[fr_b >= self._th_j].copy()
            losers = t_df.loc[fr_b <= self._th_d].copy()

            lines, ok, ins = self._analyzer.build_winner_loser_dna_contrast(
                winners_df=winners,
                losers_df=losers,
                top_n=self._top_dna,
                min_per_group=self._min_contrast,
            )
            if ok and ins:
                compact = _dna_brackets_from_insights(ins)
            else:
                compact = "".join(lines).strip() or "<i>DNA 대조 생략</i>"

            blocks.append(
                BucketBlock(
                    bucket_label=blabel,
                    n_rows=len(t_df),
                    win_rate_pct=t_wr,
                    profit_factor=t_pf,
                    dominant_sector=sector,
                    top_stocks_html=top2,
                    exit_date_min=exit_min,
                    exit_date_max=exit_max,
                    key_drivers_html=drivers,
                    dna_compact_html=compact,
                    dna_contrast_lines=tuple(lines),
                )
            )

        return blocks


def format_bucket_blocks_telegram_html(blocks: Sequence[BucketBlock]) -> str:
    """점수대별 1줄 요약(HTML) — 레거시 단일 트랙."""
    lines: List[str] = []
    for b in blocks:
        lines.append(
            f"📌 <b>[{html.escape(b.bucket_label, quote=False)} 구간]</b> "
            f"승률 {b.win_rate_pct:.1f}% | PF {b.profit_factor:.2f} | "
            f"주도주: {b.key_drivers_html} | 대박 DNA: {b.dna_compact_html}\n"
        )
    return "".join(lines)


def _format_track_line(
    b: Optional[BucketBlock],
    *,
    prefix: str,
    empty_label: str,
) -> str:
    if b is None or b.n_rows <= 0:
        return f"{prefix}{empty_label}\n"
    return (
        f"{prefix}승률 <b>{b.win_rate_pct:.1f}%</b> | PF <b>{b.profit_factor:.2f}</b> | "
        f"주도주: {b.key_drivers_html}"
        f" <i>(n={b.n_rows})</i>\n"
    )


def _compute_drift_comment(
    live: Optional[BucketBlock],
    hist: Optional[BucketBlock],
) -> str:
    if live is None or live.n_rows <= 0:
        return "당일 실전 표본 없음 — DNA Drift 판정 보류."
    if hist is None or hist.n_rows < 5:
        return "과거 기준 표본 부족 — Drift 대조 불가."
    wr_gap = abs(live.win_rate_pct - hist.win_rate_pct)
    live_dna = (live.dna_compact_html or "").strip()
    hist_dna = (hist.dna_compact_html or "").strip()
    if wr_gap <= 8.0 and live_dna and hist_dna and live_dna == hist_dna:
        return "당일 폼이 과거 기준 DNA 궤적과 <b>정합</b> (Drift 낮음)."
    if wr_gap >= 18.0:
        direction = "강세" if live.win_rate_pct > hist.win_rate_pct else "약세"
        return (
            f"승률 괴리 <b>{wr_gap:.0f}%p</b> — 당일 실전이 과거 기준 대비 <b>{direction}</b> "
            f"(Drift <b>주의</b>)."
        )
    if live_dna and hist_dna and live_dna != hist_dna:
        return "동적 피처 DNA 브라켓이 과거 기준과 <b>불일치</b> (Drift 감지)."
    return "승률·DNA가 과거 기준과 <b>근접</b> (Drift 보통)."


def build_dual_track_bucket_blocks(
    live_df: pd.DataFrame,
    hist_df: pd.DataFrame,
    dive: "ForwardScoreBucketDeepDive",
) -> List[DualTrackBucketBlock]:
    """LIVE·HIST 각각 버킷 후 라벨 합집합으로 Dual-Track 블록 생성."""
    live_blocks = {b.bucket_label: b for b in dive.build_bucket_blocks(live_df)}
    hist_blocks = {b.bucket_label: b for b in dive.build_bucket_blocks(hist_df)}
    labels = sorted(
        set(live_blocks) | set(hist_blocks),
        key=lambda x: int(str(x).replace("점대", "").strip() or "0"),
    )
    out: List[DualTrackBucketBlock] = []
    for lbl in labels:
        live_b = live_blocks.get(lbl)
        hist_b = hist_blocks.get(lbl)
        if live_b is None and hist_b is None:
            continue
        out.append(
            DualTrackBucketBlock(
                bucket_label=lbl,
                live=live_b,
                hist=hist_b,
                drift_comment=_compute_drift_comment(live_b, hist_b),
            )
        )
    return out


def format_dual_track_micro_dna_html(
    blocks: Sequence[DualTrackBucketBlock],
    *,
    staleness_banner: str = "",
    anchor_day: str = "",
    anchor_label: str = "세션 앵커",
    meta_line: str = "",
) -> str:
    """듀얼 트랙 Micro-DNA 텔레그램 HTML."""
    out = "👑 <b>[점수대별 Micro-DNA · 동적 피처 · Dual-Track]</b>\n"
    if meta_line:
        out += f"📎 <i>{meta_line}</i>\n"
    if anchor_day:
        out += (
            f"🗓️ {html.escape(anchor_label, quote=False)}: "
            f"<b>{html.escape(anchor_day, quote=False)}</b>\n"
        )
    if staleness_banner:
        out += staleness_banner + "\n"
    if not blocks:
        out += (
            "<i>표본 부족 — LIVE·HIST 모두 점수 버킷 통과 구간 없음 "
            "(total_score/tier 확인).</i>\n"
        )
        return out

    for dt in blocks:
        lbl = html.escape(dt.bucket_label, quote=False)
        out += f"📌 <b>[{lbl} 구간]</b>\n"
        out += _format_track_line(
            dt.live,
            prefix="- 🟢 <b>당일 실전:</b> ",
            empty_label="<i>청산 0건</i>",
        )
        if dt.hist and dt.hist.n_rows > 0:
            out += (
                f"- 🏛️ <b>과거 기준(Sim):</b> 기대 승률 <b>{dt.hist.win_rate_pct:.1f}%</b> | "
                f"PF <b>{dt.hist.profit_factor:.2f}</b> | "
                f"역대 주도주: {dt.hist.key_drivers_html} | "
                f"DNA: {dt.hist.dna_compact_html}\n"
            )
        else:
            out += "- 🏛️ <b>과거 기준(Sim):</b> <i>롤링 표본 부족</i>\n"
        out += (
            f"- 🧬 <b>DNA 대조:</b> {dt.drift_comment}\n"
        )
    out += "\n"
    return out


def _champion_insight_tail(best_wr: BucketBlock, best_pf: BucketBlock) -> str:
    out = "💡 <b>[관제탑 딥다이브 통찰 및 시너지 지침]</b>\n"
    if best_wr.win_rate_pct < 40.0 or best_pf.profit_factor < 1.0:
        out += (
            "🚨 <b>[시스템 비상]</b> 최우수 구간의 성적조차 승률 40% 미만이거나 손익비가 박살 난 상태입니다. "
            "이는 특정 로직의 문제가 아닌 시장 전반의 수급 붕괴(Systemic Risk)를 의미합니다. "
            "관제탑은 즉각 모든 로직의 켈리 비중을 최소치(0.2%)로 동결하고 보수적 관망을 지시합니다.\n"
        )
    elif best_wr.win_rate_pct >= 50.0 and best_pf.profit_factor >= 1.5:
        out += (
            "🔥 <b>[엣지 확인]</b> 시스템의 득점 모델이 시장과 완벽히 동기화되어 통계적 우위(Edge)를 증명했습니다. "
            f"내일은 <b>{html.escape(best_wr.bucket_label, quote=False)}</b>"
        )
        if best_wr.dominant_sector and best_wr.dominant_sector != "—":
            out += f" · <b>{html.escape(best_wr.dominant_sector, quote=False)}</b> 테마 캐리 라인에 가중을 검토하십시오.\n"
        else:
            out += " 구간 캐리 라인에 가중을 검토하십시오.\n"
    else:
        out += (
            "⚖️ <b>[혼조세]</b> 최우수 구간의 성적이 압도적이지 않습니다. "
            "방어적인 익절/손절(Hybrid) 라인을 유지하며 시장 방향성이 결정될 때까지 자본을 보존하십시오.\n"
        )
    return out + "\n"


def _format_champion_track_lines(
    blocks: Sequence[BucketBlock],
    *,
    emoji: str,
    title: str,
) -> str:
    if not blocks:
        return f"- {emoji} <b>{html.escape(title, quote=False)}:</b> <i>표본 부족 (버킷 통과 0)</i>\n"
    best_wr = max(blocks, key=lambda b: b.win_rate_pct)
    best_pf = max(blocks, key=lambda b: b.profit_factor)
    if best_wr.bucket_label == best_pf.bucket_label:
        return (
            f"- {emoji} <b>{html.escape(title, quote=False)}:</b> "
            f"{_format_champion_receipt(best_wr, emphasize='both')}\n"
        )
    return (
        f"- {emoji} <b>{html.escape(title, quote=False)}:</b> "
        f"{_format_champion_receipt(best_wr, emphasize='wr')}\n"
        f"   💎 PF 1위: {_format_champion_receipt(best_pf, emphasize='pf')}\n"
    )


def format_dual_track_tier_champion_summary_html(
    live_blocks: Sequence[BucketBlock],
    hist_blocks: Sequence[BucketBlock],
    *,
    market: str,
    rolling_days: int,
    session_anchor: str,
    calendar_today_kst: str,
    db_watermark: Optional[str],
    anchor_label: str,
    read_source: str = "MAIN",
    staleness_grade: str = "GREEN",
) -> str:
    """최우수 성적표 — LIVE(당일 실전) vs HIST(과거 롤링) 듀얼 트랙."""
    m_esc = html.escape(str(market), quote=False)
    out = "🏆 <b>[점수 구간별 최우수 성적표 요약 · Dual-Track]</b>\n"
    out += (
        f"📎 <i>{m_esc}장 · 롤링 <b>{rolling_days}</b>일 · "
        f"리포트일 KST <b>{html.escape(calendar_today_kst, quote=False)}</b> · "
        f"세션앵커({html.escape(anchor_label, quote=False)}) "
        f"<b>{html.escape(session_anchor, quote=False)}</b> · "
        f"DB청산 <b>{html.escape(str(db_watermark or '—'), quote=False)}</b> · "
        f"읽기 <b>{html.escape(read_source, quote=False)}</b> · "
        f"Staleness <b>{html.escape(staleness_grade, quote=False)}</b></i>\n"
    )
    out += _format_champion_track_lines(
        live_blocks, emoji="🟢", title="당일 실전(LIVE) 최우수"
    )
    out += _format_champion_track_lines(
        hist_blocks, emoji="🏛️", title="과거 기준(HIST) 최우수"
    )

    insight_blocks = hist_blocks if hist_blocks else live_blocks
    if insight_blocks:
        best_wr = max(insight_blocks, key=lambda b: b.win_rate_pct)
        best_pf = max(insight_blocks, key=lambda b: b.profit_factor)
        out += (
            " ◽ <i>영수증 청산 구간은 각 트랙 LIVE/HIST 쿼리 윈도우를 따릅니다. "
            "타 시장·기간 tier 절대값 비교 금지.</i>\n\n"
        )
        out += _champion_insight_tail(best_wr, best_pf)
    else:
        out += " ◽ <i>듀얼 트랙 모두 표본 부족 — 최우수 구간 산출 불가.</i>\n\n"
    return out


def format_tier_champion_summary_html(
    blocks: Sequence[BucketBlock],
    *,
    market: str,
    rolling_days: int,
    today_str: str,
) -> str:
    """레거시 단일 트랙 (내부 호환)."""
    if not blocks:
        return ""
    live_blocks = list(blocks)
    return format_dual_track_tier_champion_summary_html(
        live_blocks,
        live_blocks,
        market=market,
        rolling_days=rolling_days,
        session_anchor=today_str,
        calendar_today_kst=today_str,
        db_watermark=None,
        anchor_label="legacy",
        read_source="MAIN",
    )
