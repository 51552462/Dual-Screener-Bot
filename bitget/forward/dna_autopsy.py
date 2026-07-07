"""
[6/9] 대박/참사 DNA 부검 — BitgetReportContext 슬라이스 (주식 dna_autopsy 이식).
"""
from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd

from bitget.infra.market_keys import to_deathmatch_key as _market_key, to_pil_key
from bitget.reports.bitget_report_context import BitgetReportContext
from practitioner_market_profiles import resolve_practitioner_profile


@dataclass(frozen=True)
class DnaAutopsySlice:
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
    market_type: str,
    sys_config: Optional[dict[str, Any]] = None,
) -> tuple[float, float, int]:
    mk = _market_key(market_type)
    pil_mk = to_pil_key(market_type)
    cfg = sys_config if isinstance(sys_config, dict) else {}
    prof = resolve_practitioner_profile(pil_mk, "DEFAULT", cfg)

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
    ctx: BitgetReportContext,
    market_type: str,
    df_closed: pd.DataFrame,
    *,
    sys_config: Optional[dict[str, Any]] = None,
) -> DnaAutopsySlice:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    window_label = f"{tk.rolling_cutoff}~{tk.session_anchor}"
    jackpot, disaster, min_pg = resolve_dna_thresholds(market_type, sys_config)

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
    ctx: BitgetReportContext,
    market_type: str,
    n_real: int,
    n_open: int,
    sys_config: Optional[dict[str, Any]] = None,
    meta: Optional[dict[str, Any]] = None,
) -> str:
    mk = slice_.market
    tk = ctx.timekeeper_for(market_type)
    wm = tk.db_watermark_exit or "—"
    lag = ctx.lag_for(market_type)
    wl = html.escape(slice_.window_label)
    j = slice_.jackpot_threshold
    d = slice_.disaster_threshold

    if slice_.n_closed == 0:
        out = (
            f"<i>⚠️ [6/9] {mk} DNA 부검 — 롤링 윈도우 (<b>{wl}</b>) 내 "
            f"청산(CLOSED) <b>0</b>건.</i>\n"
            f"표본 실거래 <b>{n_real}</b>건 · 유효OPEN <b>{n_open}</b>건 · "
            f"DB청산 워터마크 <b>{html.escape(str(wm))}</b> · lag <b>{lag}</b>d · "
            f"롤링 <b>{ctx.window_days}</b>일.\n"
        )
        if n_real == 0:
            out += "<i>▪ 실거래 표본 0건 — 포워드 장부 적재·시장 필터를 점검하세요.</i>\n"
        return out

    if slice_.n_winners == 0 and slice_.n_losers == 0:
        return (
            f"<i>ℹ️ {mk} 롤링 윈도우 (<b>{wl}</b>) 내 청산 <b>{slice_.n_closed}</b>건 — "
            f"대박(≥{j:g}%)·참사(≤{d:g}%) 기준에 부합하는 종목이 없습니다.</i>\n"
            f"<i>임계: <code>FORWARD_DNA_JACKPOT_PCT_{mk}</code> / "
            f"<code>FORWARD_DNA_DISASTER_PCT_{mk}</code></i>\n"
        )

    try:
        from reports.report_feature_analyzer import ReportFeatureAnalyzer

        dna_an = ReportFeatureAnalyzer(sys_config=sys_config or {}, meta=meta)
        dna_lines, ok, _ins = dna_an.build_winner_loser_dna_contrast(
            winners_df=slice_.winners,
            losers_df=slice_.losers,
            top_n=2,
            min_per_group=slice_.min_per_group,
        )
        body = "".join(dna_lines)
        if ok:
            hdr = f"📎 임계 대박 ≥{j:g}% · 참사 ≤{d:g}% · 윈도우 <b>{wl}</b>\n"
            return hdr + body

        return (
            body
            + f"<i>ℹ️ 대박 <b>{slice_.n_winners}</b>건 · 참사 <b>{slice_.n_losers}</b>건 — "
            f"DNA 대조에는 각 최소 <b>{slice_.min_per_group}</b>건 필요.</i>\n"
        )
    except Exception as ex:
        return (
            f"<i>⚠️ [6/9] DNA 대조 예외: {html.escape(str(ex)[:120], quote=False)}</i>\n"
        )
