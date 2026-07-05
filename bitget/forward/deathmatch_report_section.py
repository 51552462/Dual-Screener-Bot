"""
[9/9] 시스템 데스매치 — BitgetReportContext · Tier DM-A/B/C Fallback.
"""
from __future__ import annotations

import html
from typing import Any, Optional

import pandas as pd

from bitget.reports.bitget_report_context import BitgetReportContext, BitgetReportMarketSlice


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


def _market_key(market_type: str) -> str:
    m = str(market_type or "spot").strip().lower()
    return "FUT" if m in ("futures", "fut", "future") else "SPOT"


def _tier_dm_a(
    ctx: BitgetReportContext,
    market_type: str,
    *,
    n_real: int,
    n_open: int,
    n_min: int,
) -> str:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    wm = tk.db_watermark_exit or "—"
    lag = ctx.lag_for(market_type)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    out = (
        f"<i>⚠️ [9/9] {mk} 데스매치 — 롤링 윈도우 (<b>{wl}</b>) 내 청산(CLOSED) <b>0</b>건.</i>\n"
        f"표본 실거래 <b>{n_real}</b> · 유효OPEN <b>{n_open}</b> · "
        f"DB청산 워터마크 <b>{_esc(wm)}</b> · lag <b>{lag}</b>d · "
        f"arm 최소 <b>{n_min}</b>건 · 롤링 <b>{ctx.window_days}</b>일.\n"
    )
    if n_real == 0:
        out += "<i>▪ 실거래 표본 0건 — 포워드 장부 적재·시장 필터를 점검하세요.</i>\n"
    return out


def _tier_dm_b(ctx: BitgetReportContext, market_type: str, *, n_closed: int) -> str:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 청산 <b>{n_closed}</b>건 · 윈도우 <b>{wl}</b> — "
        f"Registry↔sig_type 매핑 arm <b>0</b>.</i>\n"
        f"<i>strategy_registry·청산 sig_type 정합을 점검하세요.</i>\n"
    )


def _tier_dm_c(
    ctx: BitgetReportContext,
    market_type: str,
    *,
    n_closed: int,
    n_min: int,
    n_observing: int,
    n_ranked: int,
) -> str:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 청산 <b>{n_closed}</b>건 · 유효 순위 arm <b>{n_ranked}</b> · "
        f"관망 <b>{n_observing}</b> — arm당 최소 <b>{n_min}</b>건 미달.</i>\n"
        f"<i>윈도우 <b>{wl}</b></i>\n"
    )


def build_deathmatch_section(
    ctx: BitgetReportContext,
    market_type: str,
    df_closed: pd.DataFrame,
    mkt_slice: BitgetReportMarketSlice,
    *,
    sys_config: dict,
    meta: Optional[dict[str, Any]],
    market_icon: str,
    apply_deathmatch_allocation: bool = True,
) -> str:
    from evolution.ace_deathmatch_bridge import (
        build_ace_deathmatch_comparison,
        format_ace_evolution_oneliner,
    )
    from evolution.ace_evolution_store import load_playbook
    from evolution.deathmatch_battle_royale import (
        battle_royal_to_nway,
        format_battle_royal_telegram,
        run_battle_royal,
    )

    mk = _market_key(market_type)
    n_closed = int(len(df_closed)) if df_closed is not None else 0
    n_real = int(len(mkt_slice.df_real))
    n_open = int(mkt_slice.n_open_valid)
    tk = ctx.timekeeper_for(market_type)

    meta_h = None
    if isinstance(meta, dict):
        meta_h = meta.get("META_STRATEGY_HEALTH")

    # persist=False: evolution.deathmatch_store/deathmatch_report의 저장 경로는
    # 전부 주식(factory) SSOT(meta_governor_state.json·system_config.json·
    # market_data.sqlite)를 대상으로 하드코딩돼 있어 BITGET_DB_STORAGE_PATH를
    # 인식하지 못한다. 코인 자체 Kelly 사이징은 이 값을 전혀 읽지 않으므로(항상
    # bitget.governance.meta_consumer 경유) 여기서 persist해도 아무 효과가 없고,
    # 주식·코인이 같은 서버/체크아웃을 공유하는 상황(예: 단일 서버 운영, 로컬
    # 테스트)에서는 코인 데스매치 결과가 주식 실거래 META_GROUP_KELLY_MULT를
    # 덮어쓸 위험만 남는다. 표시용 랭킹은 in-memory 결과만으로 충분하므로 저장은
    # 완전히 건너뛴다. (apply_deathmatch_allocation 파라미터는 하위 호환용으로
    # 시그니처에 남겨두되 더 이상 아무 것도 적용하지 않는다.)
    br = run_battle_royal(
        df_closed,
        sys_config,
        market=mk,
        lookback_days=0,
        window_pre_sliced=True,
        meta_health=meta_h if isinstance(meta_h, dict) else None,
        persist=False,
    )
    dm = battle_royal_to_nway(br)

    hdr = ctx.market_window_header_html(
        market_type, n_real=n_real, n_closed=n_closed, n_open=n_open
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
        msg += _tier_dm_a(ctx, market_type, n_real=n_real, n_open=n_open, n_min=br.n_min)
        if ace_line:
            msg += f"\n{ace_line}"
        return msg

    if n_closed > 0 and not br.arms:
        msg += _tier_dm_b(ctx, market_type, n_closed=n_closed)
        if ace_line:
            msg += f"\n{ace_line}"
        return msg

    if n_closed > 0 and not ranked:
        msg += _tier_dm_c(
            ctx,
            market_type,
            n_closed=n_closed,
            n_min=br.n_min,
            n_observing=len(observing),
            n_ranked=0,
        )
        if observing:
            msg += f"<i>관망 arm 예: {len(observing)}개</i>\n"
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
    return msg + body
