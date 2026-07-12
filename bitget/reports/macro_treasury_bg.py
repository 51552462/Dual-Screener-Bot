"""Bitget [1/9] macro/treasury section — ReportStateBinder SSOT wrapper."""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.clock import utc_date_key
from bitget.infra.market_keys import normalize_market_type, to_report_label
from bitget.reports.bitget_report_context import BitgetReportContext
from bitget.reports.report_state_binder_bg import (
    build_macro_treasury_block,
    format_macro_treasury_section_html,
)


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
    treasury_key = "TREASURY_SPOT_USDT" if mkt == "spot" else "TREASURY_FUTURES_USDT"
    block = build_macro_treasury_block(
        market_type=market_type,
        meta=meta,
        sys_config=sys_config,
        df_closed_real=getattr(mkt_slice, "df_closed", None),
        treasury_config_key=treasury_key,
    )
    lead = ctx.market_window_header_html(
        market_type,
        n_real=len(mkt_slice.df_real),
        n_closed=mkt_slice.n_closed_window,
        n_open=mkt_slice.n_open_valid,
    )
    if integrity_html:
        lead = lead.rstrip() + "\n" + integrity_html.rstrip() + "\n"
    today = utc_date_key()
    return format_macro_treasury_section_html(
        block,
        display_label=to_report_label(market_type),
        market_icon=market_icon,
        today_str=today,
        lead_in_html=lead,
    )
