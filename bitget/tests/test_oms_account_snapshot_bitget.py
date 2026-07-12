"""Private WS account equity → OMS balance (freshness-gated)."""
from __future__ import annotations

from unittest import mock

from bitget.data.stream_buffer import PrivateStreamBuffer
from bitget.trading.account_snapshot import (
    fetch_usdt_balance,
    try_private_ws_usdt_total,
    usdt_total_from_account_row,
)
from bitget.trading.oms_source_stats import OmsSourceCounters


def test_usdt_total_prefers_usdt_equity():
    assert usdt_total_from_account_row({"usdtEquity": "100.5", "available": "1"}) == 100.5
    assert usdt_total_from_account_row({"equity": "90", "available": "1"}) == 90.0
    assert usdt_total_from_account_row({"available": "10", "frozen": "2.5"}) == 12.5
    assert usdt_total_from_account_row({"available": "0", "frozen": "0"}) == 0.0
    assert usdt_total_from_account_row({}) is None
    assert usdt_total_from_account_row(None) is None


def test_try_ws_none_when_never_initialized():
    buf = PrivateStreamBuffer(max_events=16)
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert try_private_ws_usdt_total() is None


def test_try_ws_none_on_empty_touch_without_row():
    """Empty account snapshot touches channel but must not invent flat 0."""
    buf = PrivateStreamBuffer(max_events=16)
    buf.touch_channel("account")
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert try_private_ws_usdt_total() is None


def test_try_ws_true_zero_equity_is_valid():
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_account("USDT-FUTURES", {"usdtEquity": "0", "available": "0"})
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert try_private_ws_usdt_total() == 0.0


def test_try_ws_stale_falls_through():
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_account("USDT-FUTURES", {"usdtEquity": "50"})
    # Force stale channel mono
    buf._channel_mono["account"] = 0.0  # noqa: SLF001 — test stale invariant
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert try_private_ws_usdt_total() is None


def test_fetch_balance_prefers_ws_and_records_source():
    c = OmsSourceCounters()
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_account("USDT-FUTURES", {"usdtEquity": "123.4"})

    class _Ex:
        def fetch_balance(self):
            raise AssertionError("should not REST when WS fresh")

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.account_snapshot.record_oms_source", side_effect=c.record
    ):
        val = fetch_usdt_balance(_Ex(), market_type="futures", prefer_ws=True)
    assert val == 123.4
    assert c.lifetime_snapshot()["fetch_balance.private_ws"] == 1


def test_fetch_balance_force_rest_post_fill():
    c = OmsSourceCounters()
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_account("USDT-FUTURES", {"usdtEquity": "999"})

    class _Ex:
        def fetch_balance(self):
            return {"total": {"USDT": 88.0}}

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.account_snapshot.record_oms_source", side_effect=c.record
    ), mock.patch(
        "bitget.trading.account_snapshot.call_with_retry",
        side_effect=lambda fn, **k: fn(),
    ):
        val = fetch_usdt_balance(_Ex(), market_type="futures", prefer_ws=False)
    assert val == 88.0
    assert c.lifetime_snapshot()["fetch_balance.rest"] == 1
    assert c.lifetime_snapshot()["fetch_balance.private_ws"] == 0


def test_fetch_balance_spot_always_rest():
    c = OmsSourceCounters()
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_account("USDT-FUTURES", {"usdtEquity": "999"})

    class _Ex:
        def fetch_balance(self):
            return {"total": {"USDT": 40.0}}

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.account_snapshot.record_oms_source", side_effect=c.record
    ), mock.patch(
        "bitget.trading.account_snapshot.call_with_retry",
        side_effect=lambda fn, **k: fn(),
    ):
        val = fetch_usdt_balance(_Ex(), market_type="spot", prefer_ws=True)
    assert val == 40.0
    assert c.lifetime_snapshot()["fetch_balance.rest"] == 1


def test_executor_wires_prefer_ws_split():
    import inspect

    from bitget import executor as ex

    src = inspect.getsource(ex.execute_real_order)
    assert "prefer_ws=True" in src
    assert "prefer_ws=False" in src
    assert "fetch_usdt_balance" in inspect.getsource(ex._fetch_total_usdt)


def test_heartbeat_includes_bal_keys():
    from bitget.trading.oms_source_stats import oms_source_heartbeat_snapshot

    c = OmsSourceCounters()
    c.record("fetch_balance", "private_ws")
    c.record("fetch_balance", "rest")
    with mock.patch(
        "bitget.trading.oms_source_stats.get_oms_source_counters", return_value=c
    ):
        snap = oms_source_heartbeat_snapshot()
    assert snap["bal_ws"] == 1
    assert snap["bal_rest"] == 1
