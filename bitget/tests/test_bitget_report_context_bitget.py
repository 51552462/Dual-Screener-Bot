"""Bitget report context — timekeeper SSOT smoke tests."""
from __future__ import annotations

import bitget.forward.forward_trade_identity  # noqa: F401 — stabilize import graph

from datetime import date

from bitget.infra.clock import utc_date_days_ago_str, utc_date_key, utc_now
from bitget.reports.bitget_report_context import BitgetReportContext, BitgetReportTimekeeper


def test_timekeeper_for_market_uses_clock_ssot():
    tk = BitgetReportTimekeeper.for_market("spot", rolling_days=90, db_watermark_exit="2026-07-10")
    now = utc_now()
    assert tk.session_anchor == utc_date_key(anchor=now)
    assert tk.rolling_cutoff == utc_date_days_ago_str(90, anchor=now)
    assert tk.market == "SPOT"


def test_lag_for_watermark():
    tk = BitgetReportTimekeeper.for_market(
        "futures",
        rolling_days=90,
        db_watermark_exit="2026-07-08",
    )
    ctx = BitgetReportContext(
        tk_spot=tk,
        tk_futures=tk,
        db_read_path=":memory:",
        window_days=90,
        calendar_today_utc=tk.session_anchor,
    )
    lag = ctx.lag_for("futures")
    anchor = date.fromisoformat(tk.session_anchor[:10])
    wm = date.fromisoformat("2026-07-08")
    assert lag == max(0, (anchor - wm).days)


def test_timekeeper_module_avoids_raw_datetime_now():
    import inspect

    from bitget.reports import bitget_report_context as brc

    src = inspect.getsource(brc)
    assert "datetime.now(timezone.utc)" not in src
    assert "datetime.utcnow()" not in src
    assert "utc_now" in src
    assert "utc_date_str" in src
