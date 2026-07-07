"""Bitget [1/9] macro/treasury section — Live NAV + meta regime (USDT)."""
from __future__ import annotations

import html
from datetime import datetime
from typing import Any, Optional

from bitget.governance.meta_consumer import resolve_trading_kelly_base
from bitget.infra.market_keys import normalize_market_type, to_report_label
from bitget.live_nav_manager import get_market_state, live_nav, resolve_effective_kelly
from bitget.reports.bitget_report_context import BitgetReportContext


def build_bitget_macro_section_html(
    *,
    market_type: str,
    market_icon: str,
    ctx: BitgetReportContext,
    mkt_slice: Any,
    sys_config: dict,
    meta: Optional[dict],
    integrity_html: str = "",
) -> str:
    mkt = normalize_market_type(market_type)
    mk_label = to_report_label(market_type)
    treasury_key = "TREASURY_SPOT_USDT" if mkt == "spot" else "TREASURY_FUTURES_USDT"
    treasury_cash = float(sys_config.get(treasury_key, 0.0) or 0.0)
    regime = str(meta.get("META_REGIME_KEY") if isinstance(meta, dict) else sys_config.get("CURRENT_REGIME_KEY", "UNKNOWN"))
    conf = None
    if isinstance(meta, dict):
        ra = meta.get("META_REGIME_ACTION")
        if isinstance(ra, dict):
            conf = ra.get("confidence")
    eff_k = resolve_trading_kelly_base(sys_config, meta or {}) * 100.0
    g_mult = float((meta or {}).get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    nav_st = get_market_state(market_type)
    nav = live_nav(market_type)
    hwm = float(nav_st.get("hwm", nav) or nav)
    mdd = float(nav_st.get("mdd_pct", 0.0) or 0.0)
    base = float(nav_st.get("base_capital", nav) or nav)
    b_status = str(sys_config.get("CRYPTO_BREADTH_STATUS", "NEUTRAL"))
    fresh = str(sys_config.get("MACRO_DAILY_FRESHNESS") or sys_config.get("BITGET_MACRO_FRESHNESS") or "")

    lines = [
        f"{market_icon} <b>[1/9] {mk_label} 국면/국고 현황</b>",
        ctx.market_window_header_html(
            market_type,
            n_real=len(mkt_slice.df_real),
            n_closed=mkt_slice.n_closed_window,
            n_open=mkt_slice.n_open_valid,
        ),
    ]
    if integrity_html:
        lines.append(integrity_html.rstrip())
    lines.append(f"📅 {datetime.utcnow().strftime('%Y-%m-%d')} UTC")
    lines.append(f"🌐 국면: <b>{html.escape(regime, quote=False)}</b>" + (f" (conf {conf})" if conf is not None else ""))
    lines.append(f"💎 Live NAV: <b>{nav:,.2f} USDT</b> · HWM {hwm:,.2f} · MDD {mdd:.2f}% · 기준 {base:,.2f}")
    lines.append(f"🏦 잔여 국고(현금): <b>{treasury_cash:,.2f} USDT</b>")
    lines.append(f"⚖️ 유효 켈리: {eff_k:.3f}% · META_GLOBAL ×{g_mult:.2f}")
    lines.append(f"🌊 Breadth: {html.escape(b_status, quote=False)}")
    if fresh:
        lines.append(f"🛰️ 매크로 신선도: {html.escape(fresh, quote=False)}")
    damp = sys_config.get("DOOMSDAY_DEFCON")
    if damp is not None:
        lines.append(f"☢️ DEFCON: <b>{html.escape(str(damp), quote=False)}</b>")
    return "\n".join(lines) + "\n"
