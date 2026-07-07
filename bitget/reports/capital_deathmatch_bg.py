"""Bitget [3/9] CapitalDeathmatch — USDT formatting wrapper."""
from __future__ import annotations

import html
from typing import Optional

import pandas as pd

from capital_deathmatch import CapitalDeathmatchAnalyzer, CapitalDeathmatchBlock
from bitget.live_nav_manager import live_notional, live_nav


def _fmt_usdt(value: float, *, signed: bool = False) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        v = 0.0
    return f"{v:+,.2f} USDT" if signed else f"{v:,.2f} USDT"


def _fmt_pct(value: float, *, signed: bool = True) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        v = 0.0
    if signed:
        return f"{v:+.2f}%"
    return f"{v:.2f}%"


def analyze_capital_deathmatch(
    df_closed: pd.DataFrame,
    *,
    market_type: str,
    sys_config: Optional[dict] = None,
    meta: Optional[dict] = None,
) -> CapitalDeathmatchBlock:
    ref = live_nav(market_type)
    fb = live_notional(market_type, sys_config, meta)
    analyzer = CapitalDeathmatchAnalyzer(
        reference_capital=float(ref),
        zero_invest_fallback=float(fb),
    )
    if "invest_amount" not in df_closed.columns and "margin_used" in df_closed.columns:
        work = df_closed.copy()
        work["invest_amount"] = work["margin_used"]
        return analyzer.analyze(work)
    return analyzer.analyze(df_closed)


def format_capital_deathmatch_telegram(
    *,
    market_icon: str,
    block: CapitalDeathmatchBlock,
    subtitle: str = "(Bitget 가상매매 청산 기준)",
) -> str:
    b = block
    fk = b.fixed
    kk = b.kelly
    lines = [
        f"{market_icon} <b>[3/9] 자금 관리 전략 데스매치</b> <i>{html.escape(subtitle, quote=False)}</i>",
        (
            f"📊 표본: 총 {b.n_trades}전 {b.win_trades}승 "
            f"(승률 {b.overall_win_rate_pct:.1f}%) · 정렬: "
            f"<code>{html.escape(b.sort_key_used, quote=False)}</code>"
        ),
        f"⚓ 기준 NAV R={_fmt_usdt(b.reference_capital)}",
        "",
        (
            f"🛡️ <b>[고정 비중]</b> 누적 <b>{_fmt_usdt(fk.cum_pnl, signed=True)}</b> | "
            f"총수익률 {_fmt_pct(fk.total_return_pct)} | MDD <b>{_fmt_pct(fk.mdd_pct, signed=False)}</b> | "
            f"평균 투입 {fk.avg_deploy_pct:.2f}% | 최대 연패 <b>{fk.max_losing_streak}</b>회"
        ),
        (
            f"💰 <b>[동적 켈리]</b> 누적 <b>{_fmt_usdt(kk.cum_pnl, signed=True)}</b> | "
            f"총수익률 {_fmt_pct(kk.total_return_pct)} | MDD <b>{_fmt_pct(kk.mdd_pct, signed=False)}</b> | "
            f"평균 투입 {kk.avg_deploy_pct:.2f}% | 최대 연패 <b>{kk.max_losing_streak}</b>회"
        ),
        "",
        f"🏁 우위: <b>{'동적 켈리' if kk.cum_pnl > fk.cum_pnl else '고정 비중'}</b> "
        f"(Δ {_fmt_usdt(kk.cum_pnl - fk.cum_pnl, signed=True)})",
        "",
    ]
    return "\n".join(lines)
