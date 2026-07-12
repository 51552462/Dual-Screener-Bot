"""OMS position index — private WS freshness gate + REST fallback."""
from __future__ import annotations

import time
from unittest import mock

import pytest

from bitget.data.stream_buffer import PrivateStreamBuffer
from bitget.trading import position_manager as pm


def test_private_inst_id_to_ccxt_futures():
    assert pm.private_inst_id_to_ccxt_futures("BTCUSDT") == "BTC/USDT:USDT"
    assert pm.private_inst_id_to_ccxt_futures("ETH/USDT") == "ETH/USDT:USDT"


def test_try_private_ws_index_none_when_never_updated():
    buf = PrivateStreamBuffer(max_events=32)
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert pm.try_private_ws_position_index(max_age_sec=20.0) is None


def test_try_private_ws_index_fresh_empty_is_valid():
    buf = PrivateStreamBuffer(max_events=32)
    # Positions channel empty snapshot marks channel fresh (flat book)
    buf.touch_channel("positions")
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        out = pm.try_private_ws_position_index(max_age_sec=20.0)
    assert out == {}


def test_try_private_ws_index_account_only_is_not_positions_fresh():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_account("USDT-FUTURES", {"marginCoin": "USDT", "available": "1"})
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert pm.try_private_ws_position_index(max_age_sec=20.0) is None


def test_try_private_ws_index_maps_positions():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_position(
        "USDT-FUTURES",
        "ETHUSDT:short",
        {"instId": "ETHUSDT", "holdSide": "short", "total": "2.5"},
    )
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        out = pm.try_private_ws_position_index(max_age_sec=20.0)
    assert out[("ETH/USDT:USDT", "SHORT")] == pytest.approx(2.5)


def test_try_private_ws_index_stale_returns_none():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT:long",
        {"instId": "BTCUSDT", "holdSide": "long", "total": "1"},
    )
    # Force stale positions channel
    with buf._lock:
        for row in buf._positions.values():
            row["ts_mono"] = time.monotonic() - 100.0
        buf._channel_mono["positions"] = time.monotonic() - 100.0
        buf._last_update_mono = time.monotonic() - 100.0
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert pm.try_private_ws_position_index(max_age_sec=20.0) is None


def test_build_open_position_index_prefers_fresh_ws():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT:long",
        {"instId": "BTCUSDT", "holdSide": "long", "total": "1.0"},
    )

    class _Ex:
        def fetch_positions(self):
            raise AssertionError("REST must not be called when WS fresh")

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        out = pm.build_open_position_index(_Ex(), prefer_private_ws=True)
    assert out[("BTC/USDT:USDT", "LONG")] == pytest.approx(1.0)


def test_build_open_position_index_rest_fallback_when_stale():
    buf = PrivateStreamBuffer(max_events=32)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT:long",
        {"instId": "BTCUSDT", "holdSide": "long", "total": "1"},
    )
    with buf._lock:
        buf._channel_mono["positions"] = time.monotonic() - 100.0
        buf._last_update_mono = time.monotonic() - 100.0

    class _Ex:
        def fetch_positions(self):
            return [{"symbol": "SOL/USDT:USDT", "side": "long", "contracts": 3.0}]

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch("bitget.infra.network_retry.time.sleep"):
        out = pm.build_open_position_index(_Ex(), prefer_private_ws=True, max_private_age_sec=20.0)
    assert out[("SOL/USDT:USDT", "LONG")] == pytest.approx(3.0)


def test_memory_policy_private_pos_age_constant():
    from bitget.infra.memory_policy import PRIVATE_POS_INDEX_MAX_AGE_SEC

    assert PRIVATE_POS_INDEX_MAX_AGE_SEC == 20.0
