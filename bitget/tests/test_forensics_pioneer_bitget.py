"""bitget.forensics_pioneer — PUMP_DNA preemptive shadow log Clock SSOT."""
from __future__ import annotations

from unittest import mock

import pandas as pd


def test_forensics_pioneer_module_uses_clock_ssot():
    import inspect

    from bitget import forensics_pioneer as fp

    src = inspect.getsource(fp)
    assert "datetime.utcnow()" not in src
    assert "datetime.now()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src


def test_run_forensics_pioneer_logs_utc_timestamp():
    from bitget import forensics_pioneer as fp
    from bitget.pump_forensics import PATTERN_KEYS

    cfg = {
        "PUMP_DNA": {
            "GLOBAL": {
                "pre_emptive_rule": {k: True for k in PATTERN_KEYS},
            }
        }
    }
    ohlcv = pd.DataFrame(
        {
            "Date": [f"2026-07-{d:02d}" for d in range(1, 31)],
            "Open": [1.0] * 30,
            "High": [1.1] * 30,
            "Low": [0.9] * 30,
            "Close": [1.0] * 30,
            "Volume": [1000.0] * 30,
        }
    )
    captured: dict = {}

    class _Cur:
        def execute(self, *args, **kwargs):
            return None

        def commit(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            return None

        def close(self):
            return None

    def _insert(cur, market, code, name, price, sig, tags, logged_at, **kw):
        captured["logged_at"] = logged_at

    with mock.patch("bitget.forensics_pioneer.load_config", return_value=cfg), mock.patch(
        "bitget.forensics_pioneer.get_connection", return_value=_Conn()
    ), mock.patch(
        "bitget.forensics_pioneer._load_scan_tables", return_value=["BITGET_SPOT_SOL_USDT_1D"]
    ), mock.patch("bitget.forensics_pioneer.pd.read_sql", return_value=ohlcv), mock.patch(
        "bitget.forensics_pioneer._extract_flags",
        return_value={k: True for k in PATTERN_KEYS},
    ), mock.patch(
        "bitget.forensics_pioneer.bitget_shadow_tracking.insert_virtual_trade_row",
        side_effect=_insert,
    ), mock.patch(
        "bitget.forensics_pioneer.utc_datetime_str", return_value="2026-07-11 04:00:00"
    ), mock.patch("bitget.forensics_pioneer.flush_gc"):
        fp.run_forensics_pioneer()
    assert captured["logged_at"] == "2026-07-11 04:00:00"


def test_run_forensics_pioneer_skips_without_rules():
    from bitget import forensics_pioneer as fp

    with mock.patch("bitget.forensics_pioneer.load_config", return_value={"PUMP_DNA": {}}), mock.patch(
        "bitget.forensics_pioneer.get_connection"
    ) as conn_mock:
        fp.run_forensics_pioneer()
    conn_mock.assert_not_called()
