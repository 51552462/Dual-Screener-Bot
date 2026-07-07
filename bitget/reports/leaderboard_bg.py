"""Bitget [2/9] logic leaderboard — Live NAV + realized PnL SSOT."""
from __future__ import annotations

import html
from typing import Any, Optional

import pandas as pd

from bitget.forward.gates import _extract_core_group
from bitget.live_nav_manager import base_capital_for, live_nav
from bitget.reports.bitget_report_context import BitgetReportContext
from reports.forward_report_scalar import profit_factor_from_returns, scalar_float


def _realized_pnl_usdt(row) -> float:
    try:
        inv = float(row.get("sim_kelly_invest", 0) or 0)
        ret = float(row.get("final_ret", 0) or 0)
        return inv * ret / 100.0
    except (TypeError, ValueError):
        return 0.0


def build_logic_leaderboard_html(
    *,
    market_type: str,
    market_icon: str,
    ctx: BitgetReportContext,
    mkt_slice: Any,
    df_all: pd.DataFrame,
    sys_config: Optional[dict] = None,
) -> str:
    hdr = ctx.market_window_header_html(
        market_type,
        n_real=len(mkt_slice.df_real),
        n_closed=mkt_slice.n_closed_window,
        n_open=mkt_slice.n_open_valid,
    )
    lines = [f"{market_icon} <b>[2/9] 로직별 복리 생존 리더보드</b>", hdr]
    base_seed = live_nav(market_type)
    ref_base = base_capital_for(market_type, sys_config)

    if df_all is None or df_all.empty:
        lines.append("표본 부족 — 아직 로직별 진입 기록이 없습니다.\n")
        return "\n".join(lines)

    df = df_all.copy()
    df["group"] = df["sig_type"].apply(_extract_core_group)
    df_open = df[df["status"].astype(str).str.upper().str.startswith("OPEN")]
    leaderboard = []
    for group in df["group"].unique():
        g_df = df[df["group"] == group]
        g_closed = g_df[g_df["status"].astype(str).str.contains("CLOSED", na=False)]
        pnl = sum(_realized_pnl_usdt(r) for _, r in g_closed.iterrows())
        wr = (
            (len(g_closed[g_closed["final_ret"].astype(float) > 0]) / len(g_closed)) * 100.0
            if len(g_closed) > 0
            else 0.0
        )
        pf = profit_factor_from_returns(g_closed["final_ret"]) if len(g_closed) else 0.0
        n_open = int((df_open["group"] == group).sum()) if not df_open.empty else 0
        bal = ref_base + pnl
        leaderboard.append(
            {
                "g": group,
                "bal": bal,
                "wr": wr,
                "op": n_open,
                "tot": len(g_closed),
                "pf": pf,
            }
        )

    leaderboard.sort(key=lambda x: x["bal"], reverse=True)
    if not leaderboard:
        lines.append("표본 부족 — 청산 실적이 있는 로직이 없습니다.\n")
        return "\n".join(lines)

    for i, e in enumerate(leaderboard[:15]):
        medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏃"
        if e["bal"] < ref_base * 0.8:
            medal = "📉"
        if e["bal"] < ref_base * 0.5:
            medal = "💀"
        lines.append(
            f"{medal} <b>{html.escape(str(e['g']), quote=False)}</b>: "
            f"{e['bal']:,.2f} USDT"
        )
        lines.append(
            f"   ↳ 승률 {scalar_float(e['wr']):.0f}% (PF {scalar_float(e['pf']):.2f}) · "
            f"누적 {e['tot']}전 · OPEN {e['op']}"
        )
    lines.append(f"\n<i>Live NAV 기준 {base_seed:,.2f} USDT · 복리 잔고는 청산 PnL 누적 추정</i>\n")
    return "\n".join(lines)
