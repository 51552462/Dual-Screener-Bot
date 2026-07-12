"""OMS hydrate — private WS order lookup + REST fallback."""
from __future__ import annotations

import time
from unittest import mock

from bitget.data.stream_buffer import PrivateStreamBuffer
from bitget.trading import order_snapshot as osnap


def test_try_private_ws_order_includes_filled():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_order(
        "USDT-FUTURES",
        "oid-f",
        {
            "orderId": "oid-f",
            "instId": "BTCUSDT",
            "status": "filled",
            "size": "2",
            "filledQty": "2",
            "priceAvg": "65000",
        },
    )
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        od = osnap.try_private_ws_order("oid-f", max_age_sec=20.0)
    assert od is not None
    assert od["status"] == "filled"
    assert od["filled"] == 2.0
    assert od["average"] == 65000.0
    assert od["_source"] == "private_ws"


def test_try_private_ws_order_stale_returns_none():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_order(
        "USDT-FUTURES",
        "oid-old",
        {"orderId": "oid-old", "instId": "ETHUSDT", "status": "filled", "filledQty": "1"},
    )
    with buf._lock:
        row = buf._orders[next(iter(buf._orders))]
        row["ts_mono"] = time.monotonic() - 100.0
        buf._channel_mono["orders"] = time.monotonic() - 100.0
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert osnap.try_private_ws_order("oid-old", max_age_sec=20.0) is None


def test_fetch_order_snapshot_prefers_ws():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_order(
        "USDT-FUTURES",
        "oid1",
        {
            "orderId": "oid1",
            "instId": "BTCUSDT",
            "status": "filled",
            "size": "1",
            "filledQty": "1",
            "priceAvg": "100",
        },
    )

    class _Ex:
        def fetch_order(self, *a, **k):
            raise AssertionError("REST must not run when WS hit")

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        od = osnap.fetch_order_snapshot(_Ex(), "oid1", "BTC/USDT:USDT", market_type="futures")
    assert od is not None
    assert od["_source"] == "private_ws"
    assert od["average"] == 100.0


def test_fetch_order_snapshot_spot_always_rest():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_order(
        "USDT-FUTURES",
        "oid1",
        {"orderId": "oid1", "instId": "BTCUSDT", "status": "filled", "filledQty": "1"},
    )

    class _Ex:
        def fetch_order(self, oid, sym):
            return {"id": oid, "symbol": sym, "status": "closed", "filled": 1.0, "average": 50.0}

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch("bitget.infra.network_retry.time.sleep"):
        od = osnap.fetch_order_snapshot(_Ex(), "oid1", "BTC/USDT", market_type="spot")
    assert od is not None
    assert od["_source"] == "rest"
    assert od["average"] == 50.0


def test_hydrate_uses_fetch_order_snapshot_ssot():
    import inspect

    from bitget.trading import reconciliation as recon

    src = inspect.getsource(recon._hydrate_recent_executions)
    assert "fetch_order_snapshot" in src
    assert "ex.fetch_order" not in src
    assert "import sqlite3" in inspect.getsource(recon)
