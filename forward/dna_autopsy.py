"""
[6/9] 대박주/참사주 DNA 부검 — DailyReportContext 슬라이스 · 3단 Fallback · 임계 SSOT.
"""
from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd

from daily_report_context import DailyReportContext
from practitioner_market_profiles import resolve_practitioner_profile


@dataclass(frozen=True)
class DnaAutopsySlice:
    """윈도우 내 청산 + 대박/참사 분류 결과."""

    market: str
    df_closed: pd.DataFrame
    winners: pd.DataFrame
    losers: pd.DataFrame
    n_closed: int
    n_winners: int
    n_losers: int
    jackpot_threshold: float
    disaster_threshold: float
    min_per_group: int
    window_label: str


def resolve_dna_thresholds(
    market: str,
    sys_config: Optional[dict[str, Any]] = None,
) -> tuple[float, float, int]:
    """
    대박/참사 임계 — config 키 우선, 없으면 PractitionerMarketProfile DEFAULT.
    FORWARD_DNA_JACKPOT_PCT_{KR|US}, FORWARD_DNA_DISASTER_PCT_{KR|US}
    """
    mk = str(market or "KR").upper()
    cfg = sys_config if isinstance(sys_config, dict) else {}
    prof = resolve_practitioner_profile(mk, "DEFAULT", cfg)

    def _f(key: str, default: float) -> float:
        v = cfg.get(key)
        if v is None:
            return float(default)
        try:
            return float(v)
        except (TypeError, ValueError):
            return float(default)

    jackpot = _f(f"FORWARD_DNA_JACKPOT_PCT_{mk}", prof.winner_ret_pct)
    disaster = _f(f"FORWARD_DNA_DISASTER_PCT_{mk}", prof.loser_ret_pct)
    try:
        min_pg = int(cfg.get("FORWARD_DNA_MIN_PER_GROUP", 2))
    except (TypeError, ValueError):
        min_pg = 2
    return jackpot, disaster, max(1, min_pg)


def build_dna_autopsy_slice(
    ctx: DailyReportContext,
    market: str,
    df_closed: pd.DataFrame,
    *,
    sys_config: Optional[dict[str, Any]] = None,
) -> DnaAutopsySlice:
    """DailyReportContext 윈도우 메타 + 시장별 임계로 승·패 DataFrame 구성."""
    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    window_label = f"{tk.rolling_cutoff}~{tk.session_anchor}"
    jackpot, disaster, min_pg = resolve_dna_thresholds(mk, sys_config)

    empty = DnaAutopsySlice(
        market=mk,
        df_closed=pd.DataFrame(),
        winners=pd.DataFrame(),
        losers=pd.DataFrame(),
        n_closed=0,
        n_winners=0,
        n_losers=0,
        jackpot_threshold=jackpot,
        disaster_threshold=disaster,
        min_per_group=min_pg,
        window_label=window_label,
    )
    if df_closed is None or df_closed.empty:
        return empty

    closed = df_closed.copy()
    ret = pd.to_numeric(closed.get("final_ret"), errors="coerce")
    winners = closed.loc[ret >= jackpot].head(50).copy()
    losers = closed.loc[ret <= disaster].head(50).copy()
    return DnaAutopsySlice(
        market=mk,
        df_closed=closed,
        winners=winners,
        losers=losers,
        n_closed=int(len(closed)),
        n_winners=int(len(winners)),
        n_losers=int(len(losers)),
        jackpot_threshold=jackpot,
        disaster_threshold=disaster,
        min_per_group=min_pg,
        window_label=window_label,
    )


def format_dna_autopsy_section(
    slice_: DnaAutopsySlice,
    *,
    ctx: DailyReportContext,
    n_real: int,
    n_open: int,
    sys_config: Optional[dict[str, Any]] = None,
    meta: Optional[dict[str, Any]] = None,
) -> str:
    """Tier A/B/C Fallback 또는 ReportFeatureAnalyzer 본문."""
    mk = slice_.market
    tk = ctx.timekeeper_for(mk)
    wm = tk.db_watermark_exit or "—"
    lag = ctx.lag_for(mk)
    wl = html.escape(slice_.window_label)
    j = slice_.jackpot_threshold
    d = slice_.disaster_threshold

    # Tier A — 윈도우 내 청산 0건
    if slice_.n_closed == 0:
        out = (
            f"<i>⚠️ [6/9] {mk} DNA 부검 — 롤링 윈도우 (<b>{wl}</b>) 내 "
            f"청산(CLOSED) <b>0</b>건.</i>\n"
            f"표본 실거래 <b>{n_real}</b>건 · 유효OPEN <b>{n_open}</b>건 · "
            f"DB청산 워터마크 <b>{html.escape(str(wm))}</b> · lag <b>{lag}</b>일 · "
            f"롤링 <b>{ctx.window_days}</b>일.\n"
            f"<i>→ 최근 {ctx.window_days}일 안에 exit_date가 앵커까지 기록된 청산이 없습니다. "
            f"(OPEN만 있거나 exit_date 미기록 CLOSED는 이 섹션에 포함되지 않습니다.)</i>\n"
        )
        if n_real == 0:
            out += (
                "<i>▪ 실거래 표본도 0건 — 포워드 장부 적재·시장 필터를 점검하세요.</i>\n"
            )
        return out

    # Tier B — 청산은 있으나 극단 수익률 0건
    if slice_.n_winners == 0 and slice_.n_losers == 0:
        return (
            f"<i>ℹ️ {mk} 롤링 윈도우 (<b>{wl}</b>) 내 청산 <b>{slice_.n_closed}</b>건 — "
            f"대박(≥{j:g}%)·참사(≤{d:g}%) 기준에 부합하는 종목이 없습니다.</i>\n"
            f"<i>중간 손익 구간만 존재합니다 (횡보·소폭 변동). "
            f"임계: 프로필·<code>FORWARD_DNA_JACKPOT_PCT_{mk}</code> / "
            f"<code>FORWARD_DNA_DISASTER_PCT_{mk}</code></i>\n"
        )

    # Analyzer 경로 (성공 또는 Tier C)
    try:
        from report_feature_analyzer import ReportFeatureAnalyzer

        dna_an = ReportFeatureAnalyzer(sys_config=sys_config or {}, meta=meta)
        dna_lines, ok, _ins = dna_an.build_winner_loser_dna_contrast(
            winners_df=slice_.winners,
            losers_df=slice_.losers,
            top_n=2,
            min_per_group=slice_.min_per_group,
        )
        body = "".join(dna_lines)
        if ok:
            hdr = (
                f"📎 임계 대박 ≥{j:g}% · 참사 ≤{d:g}% · "
                f"윈도우 <b>{wl}</b>\n"
            )
            return hdr + body

        # Tier C — 극단 표본은 있으나 그룹당 min_per_group 미달
        return (
            body
            + f"<i>ℹ️ 대박 <b>{slice_.n_winners}</b>건 · 참사 <b>{slice_.n_losers}</b>건 — "
            f"DNA 대조에는 각 최소 <b>{slice_.min_per_group}</b>건 필요.</i>\n"
            f"<i>임계 조정: <code>FORWARD_DNA_JACKPOT_PCT_{mk}</code> / "
            f"<code>FORWARD_DNA_DISASTER_PCT_{mk}</code> · "
            f"<code>FORWARD_DNA_MIN_PER_GROUP</code></i>\n"
        )
    except Exception as ex:
        return (
            f"<i>⚠️ [6/9] DNA 대조 예외: {html.escape(str(ex)[:120], quote=False)}</i>\n"
        )
