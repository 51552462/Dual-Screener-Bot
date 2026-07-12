"""OMS open-order snapshot — orders-channel freshness + REST fallback."""
from __future__ import annotations

import time
from unittest import mock

from bitget.data.stream_buffer import PrivateStreamBuffer
from bitget.data import ws_private_producer as prod
from bitget.trading import order_snapshot as osnap


def test_is_open_order_status():
    assert osnap.is_open_order_status("live") is True
    assert osnap.is_open_order_status("partially_filled") is True
    assert osnap.is_open_order_status("filled") is False
    assert osnap.is_open_order_status("canceled") is False


def test_empty_orders_snapshot_marks_channel_fresh():
    buf = PrivateStreamBuffer(max_events=32)
    n = prod.handle_private_ws_message(
        {
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "orders", "instId": "default"},
            "data": [],
        },
        buffer=buf,
    )
    assert n == 0
    assert buf.channel_age_sec("orders") < 5.0
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        out = osnap.try_private_ws_open_orders(max_age_sec=20.0)
    assert out == []


def test_account_tick_does_not_freshen_orders():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_account("USDT-FUTURES", {"available": "1"})
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert osnap.try_private_ws_open_orders(max_age_sec=20.0) is None


def test_filters_closed_keeps_live():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_order(
        "USDT-FUTURES",
        "oid-live",
        {"orderId": "oid-live", "instId": "BTCUSDT", "status": "live", "side": "buy", "size": "1"},
    )
    buf.update_order(
        "USDT-FUTURES",
        "oid-done",
        {"orderId": "oid-done", "instId": "ETHUSDT", "status": "filled", "side": "sell", "size": "2"},
    )
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        out = osnap.try_private_ws_open_orders(max_age_sec=20.0)
    assert out is not None
    assert len(out) == 1
    assert out[0]["id"] == "oid-live"
    assert out[0]["symbol"] == "BTC/USDT:USDT"


def test_list_open_orders_prefers_ws():
    buf = PrivateStreamBuffer(max_events=32)
    buf.touch_channel("orders")

    class _Ex:
        def fetch_open_orders(self):
            raise AssertionError("REST must not run when orders channel fresh")

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        out = osnap.list_open_orders(_Ex(), prefer_private_ws=True)
    assert out == []


def test_list_open_orders_rest_fallback():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_order(
        "USDT-FUTURES",
        "old",
        {"orderId": "old", "instId": "BTCUSDT", "status": "live"},
    )
    with buf._lock:
        buf._channel_mono["orders"] = time.monotonic() - 100.0

    class _Ex:
        def fetch_open_orders(self):
            return [{"id": "rest1", "symbol": "SOL/USDT:USDT", "status": "open", "side": "buy"}]

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch("bitget.infra.network_retry.time.sleep"):
        out = osnap.list_open_orders(_Ex(), prefer_private_ws=True, max_private_age_sec=20.0)
    assert out is not None
    assert out[0]["id"] == "rest1"


def test_recon_uses_list_open_orders_ssot():
    import inspect

    from bitget.trading import reconciliation as recon

    src = inspect.getsource(recon)
    assert "list_open_orders" in src
    assert 'op="oms.fetch_open_orders"' not in src
