"""
[9/9] 시스템 데스매치 — DailyReportContext · Tier DM-A/B/C Fallback.
"""
from __future__ import annotations

import html
from typing import Any, Optional

import pandas as pd

from reports.daily_report_context import DailyReportContext, DailyReportMarketSlice


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


def _tier_dm_a(
    ctx: DailyReportContext,
    market: str,
    *,
    n_real: int,
    n_open: int,
    n_min: int,
) -> str:
    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    wm = tk.db_watermark_exit or "—"
    lag = ctx.lag_for(mk)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    out = (
        f"<i>⚠️ [9/9] {mk} 데스매치 — 롤링 윈도우 (<b>{wl}</b>) 내 청산(CLOSED) <b>0</b>건.</i>\n"
        f"표본 실거래 <b>{n_real}</b> · 유효OPEN <b>{n_open}</b> · "
        f"DB청산 워터마크 <b>{_esc(wm)}</b> · lag <b>{lag}</b>일 · "
        f"arm 최소 <b>{n_min}</b>건 · 롤링 <b>{ctx.window_days}</b>일.\n"
        f"<i>→ exit_date가 앵커까지 기록된 CLOSED가 없어 Battle Royal을 보류합니다. "
        f"(OPEN만 있거나 exit_date 미기록 CLOSED는 포함되지 않습니다.)</i>\n"
    )
    if n_real == 0:
        out += "<i>▪ 실거래 표본도 0건 — 포워드 장부 적재·시장 필터를 점검하세요.</i>\n"
    return out


def _tier_dm_b(ctx: DailyReportContext, market: str, *, n_closed: int) -> str:
    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 청산 <b>{n_closed}</b>건 · 윈도우 <b>{wl}</b> — "
        f"Registry↔sig_type 매핑 arm <b>0</b>.</i>\n"
        f"<i>strategy_registry·청산 sig_type 정합을 점검하세요.</i>\n"
    )


def _tier_dm_c(
    ctx: DailyReportContext,
    market: str,
    *,
    n_closed: int,
    n_min: int,
    n_observing: int,
    n_ranked: int,
) -> str:
    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 청산 <b>{n_closed}</b>건 · 유효 순위 arm <b>{n_ranked}</b> · "
        f"관망 <b>{n_observing}</b> — arm당 최소 <b>{n_min}</b>건 미달.</i>\n"
        f"<i>윈도우 <b>{wl}</b></i>\n"
    )


def build_deathmatch_section(
    ctx: DailyReportContext,
    market: str,
    df_closed: pd.DataFrame,
    mkt_slice: DailyReportMarketSlice,
    *,
    sys_config: dict,
    meta: Optional[dict[str, Any]],
    market_icon: str,
    apply_deathmatch_allocation: bool = True,
) -> str:
    """[9/9] 전문 — ctx 윈도우 슬라이스·3단 Fallback."""
    from evolution.ace_deathmatch_bridge import (
        build_ace_deathmatch_comparison,
        format_ace_evolution_oneliner,
    )
    from evolution.ace_evolution_store import load_playbook
    from evolution.deathmatch_battle_royale import (
        build_nway_deathmatch_registry,
        format_battle_royal_telegram,
    )
    from evolution.deathmatch_report import maybe_apply_deathmatch_allocation

    mk = str(market).upper()
    n_closed = int(len(df_closed)) if df_closed is not None else 0
    n_real = int(len(mkt_slice.df_real))
    n_open = int(mkt_slice.n_open_valid)
    tk = ctx.timekeeper_for(mk)

    meta_h = None
    if isinstance(meta, dict):
        meta_h = meta.get("META_STRATEGY_HEALTH")

    br, dm = build_nway_deathmatch_registry(
        df_closed,
        sys_config,
        market=mk,
        lookback_days=0,
        window_pre_sliced=True,
        meta_health=meta_h if isinstance(meta_h, dict) else None,
    )

    if apply_deathmatch_allocation:
        _cfg_dm = dict(sys_config)
        _cfg_dm["DEATHMATCH_APPLY_ALLOCATION"] = 1
        maybe_apply_deathmatch_allocation(
            dm, _cfg_dm, battle_royale=br, market=mk
        )

    hdr = ctx.market_window_header_html(
        mk, n_real=n_real, n_closed=n_closed, n_open=n_open
    )
    lookback_label = (
        f"{mk} 윈도우 {tk.rolling_cutoff}~{tk.session_anchor} · "
        f"청산 {n_closed}건 · Registry Battle Royal"
    )

    ace_line = ""
    try:
        _ace_pb = load_playbook(mk, sys_config)
        _ace_dm = build_ace_deathmatch_comparison(
            df_closed, market=mk, playbook=_ace_pb
        )
        ace_line = format_ace_evolution_oneliner(_ace_dm)
    except Exception as ex:
        ace_line = f"<i>⚠️ [진화] 스킵: {_esc(str(ex)[:72])}</i>"

    ranked = [a for a in br.arms if a.rank < 999]
    observing = [a for a in br.arms if a.rank >= 999 and a.n_closed > 0]

    msg = f"{market_icon} <b>[9/9] 시스템 데스매치 — {mk} Full Scorecard</b>\n"
    msg += hdr

    if n_closed == 0:
        try:
            from proprietary_friction_store import insert_regime_friction_event

            insert_regime_friction_event(
                date=ctx.calendar_today_kst,
                market=mk,
                event_type="DM_A_ZERO_CLOSED",
            )
        except Exception:
            pass
        msg += _tier_dm_a(ctx, mk, n_real=n_real, n_open=n_open, n_min=br.n_min)
        if ace_line:
            msg += f"\n{ace_line}"
        return msg

    if n_closed > 0 and not br.arms:
        msg += _tier_dm_b(ctx, mk, n_closed=n_closed)
        if ace_line:
            msg += f"\n{ace_line}"
        return msg

    if n_closed > 0 and not ranked:
        msg += _tier_dm_c(
            ctx,
            mk,
            n_closed=n_closed,
            n_min=br.n_min,
            n_observing=len(observing),
            n_ranked=0,
        )
        if observing:
            msg += (
                f"<i>관망 arm 예: {len(observing)}개 "
                f"(표본·유효 수익률 미달)</i>\n"
            )
        if ace_line:
            msg += f"\n{ace_line}"
        return msg

    body = format_battle_royal_telegram(
        market_icon,
        br,
        lookback_label=lookback_label,
        ace_oneliner=ace_line,
        include_title=False,
    )

    # ── [부가] 국면(Regime)별 로직 수익·방어 교차 랭킹 ──────────────────────
    #   글로벌 데스매치는 위 본문 그대로 유지하고, 그 아래에 상승/횡보/하락(+고변동)
    #   국면별 리더보드를 분리 출력한다. Composite v2 재활용 + 베이지안 수축.
    #   메인 매매·메타 파이프라인 미접촉(관측 전용), 실패 시 안전 스킵.
    regime_xrank = ""
    if str(sys_config.get("REGIME_XRANK_ENABLED", "1")).strip().lower() in (
        "1", "true", "yes", "on"
    ):
        try:
            from evolution.regime_logic_crossmatrix import (
                compute_regime_leaderboards,
                format_regime_leaderboards_telegram,
            )

            _boards = compute_regime_leaderboards(df_closed, sys_config, market=mk)
            if _boards:
                regime_xrank = "\n" + format_regime_leaderboards_telegram(
                    market_icon, mk, _boards, lookback_label=lookback_label
                )
        except Exception as ex:
            regime_xrank = f"\n<i>⚠️ [국면별 교차랭킹] 스킵: {_esc(str(ex)[:72])}</i>"

    # ── [전조현상] 챔피언/독성 로직의 점화일(T0) 역추적 → 30일 전 환경 벡터 박제 ──
    #   읽기 전용(RO URI) + 신규 테이블(champion_precursor_genesis)에만 기록한다.
    #   데스매치/앙상블/NAV 경로 미접촉. 실패해도 리포트에 영향 0(안전 스킵).
    if str(sys_config.get("GENESIS_PRECURSOR_ENABLED", "1")).strip().lower() in (
        "1", "true", "yes", "on"
    ):
        try:
            from evolution.champion_genesis import capture_champion_precursors

            _g = capture_champion_precursors(br, sys_config, market=mk)
            if not _g.get("skipped") and (_g.get("captured") or _g.get("toxic")):
                regime_xrank += (
                    f"\n<i>🧬 [전조 축적] 챔피언 {_g.get('captured', 0)} · "
                    f"독성 {_g.get('toxic', 0)} 전조 박제</i>"
                )
        except Exception as ex:
            regime_xrank += f"\n<i>⚠️ [전조 축적] 스킵: {_esc(str(ex)[:64])}</i>"

    # ── [P4-3] 초신성 DNA 그룹별 전용 수익 기여도 서브테이블 ──────────────
    #   RANK_A~D(학습 코호트)·MFE_진화형_황금타점(엔진8 EMA 수렴)·BEAST 등으로
    #   세분화해, 자가진화 루프(엔진6/8/9/10)가 실제로 어느 DNA를 밀어주고
    #   있고 그게 돈을 벌고 있는지 관측한다. 읽기 전용 집계 — 매매/사이징
    #   경로 미접촉, 실패해도 본문에 영향 0(안전 스킵).
    sn_dna_section = ""
    if str(sys_config.get("SUPERNOVA_DNA_SUBTABLE_ENABLED", "1")).strip().lower() in (
        "1", "true", "yes", "on"
    ):
        try:
            from evolution.deathmatch_report import (
                build_supernova_dna_subtable,
                format_supernova_dna_subtable_telegram,
            )

            _sn_dna = build_supernova_dna_subtable(df_closed, sys_config, market=mk)
            if _sn_dna.arms:
                sn_dna_section = "\n" + format_supernova_dna_subtable_telegram(
                    market_icon, _sn_dna, lookback_label=lookback_label
                )
        except Exception as ex:
            sn_dna_section = f"\n<i>⚠️ [초신성 DNA 서브테이블] 스킵: {_esc(str(ex)[:72])}</i>"

    return msg + body + regime_xrank + sn_dna_section

