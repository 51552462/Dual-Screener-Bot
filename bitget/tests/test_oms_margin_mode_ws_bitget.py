"""Margin mode — private WS positions row with REST fallback."""
from __future__ import annotations

from unittest import mock

from bitget.data.stream_buffer import PrivateStreamBuffer
from bitget.trading import leverage_manager as lm
from bitget.trading.oms_source_stats import OmsSourceCounters


def test_normalize_margin_mode_token():
    assert lm.normalize_margin_mode_token("crossed") == "cross"
    assert lm.normalize_margin_mode_token("isolated") == "isolated"
    assert lm.normalize_margin_mode_token("isol") == "isolated"
    assert lm.normalize_margin_mode_token("") is None
    assert lm.normalize_margin_mode_token(None) is None


def test_try_ws_none_when_never_initialized():
    buf = PrivateStreamBuffer(max_events=16)
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert lm.try_private_ws_margin_mode("BTC/USDT:USDT") is None


def test_try_ws_none_on_fresh_flat_without_symbol_row():
    """Virgin symbol: fresh empty positions must not invent a mode."""
    buf = PrivateStreamBuffer(max_events=16)
    buf.touch_channel("positions")
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert lm.try_private_ws_margin_mode("BTC/USDT:USDT") is None


def test_try_ws_reads_margin_mode_from_matching_row():
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT",
        {
            "instId": "BTCUSDT",
            "holdSide": "long",
            "total": "1",
            "marginMode": "crossed",
        },
    )
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert lm.try_private_ws_margin_mode("BTC/USDT:USDT") == "cross"


def test_try_ws_ignores_pos_mode_as_margin():
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT",
        {"instId": "BTCUSDT", "total": "1", "posMode": "hedge_mode"},
    )
    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ):
        assert lm.try_private_ws_margin_mode("BTC/USDT:USDT") is None


def test_current_prefers_ws_and_records():
    c = OmsSourceCounters()
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_position(
        "USDT-FUTURES",
        "ETHUSDT",
        {"instId": "ETHUSDT", "total": "2", "marginMode": "isolated"},
    )

    class _Ex:
        def fetch_positions(self, *a, **k):
            raise AssertionError("should not REST")

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.leverage_manager.record_oms_source", side_effect=c.record
    ):
        mode = lm.current_margin_mode_from_exchange(
            _Ex(), "ETH/USDT:USDT", prefer_ws=True
        )
    assert mode == "isolated"
    assert c.lifetime_snapshot()["margin_mode.private_ws"] == 1


def test_current_force_rest_post_mutation():
    c = OmsSourceCounters()
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT",
        {"instId": "BTCUSDT", "total": "1", "marginMode": "cross"},
    )

    class _Ex:
        def fetch_positions(self, symbols=None):
            return [{"symbol": "BTC/USDT:USDT", "marginMode": "isolated"}]

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.leverage_manager.record_oms_source", side_effect=c.record
    ), mock.patch(
        "bitget.trading.leverage_manager.call_with_retry",
        side_effect=lambda fn, **k: fn(),
    ):
        mode = lm.current_margin_mode_from_exchange(
            _Ex(), "BTC/USDT:USDT", prefer_ws=False
        )
    assert mode == "isolated"
    assert c.lifetime_snapshot()["margin_mode.rest"] == 1
    assert c.lifetime_snapshot()["margin_mode.private_ws"] == 0


def test_enforce_skips_second_fetch_when_already_aligned():
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT",
        {"instId": "BTCUSDT", "total": "1", "marginMode": "cross"},
    )
    calls = {"fetch": 0, "set": 0}

    class _Ex:
        def fetch_positions(self, *a, **k):
            calls["fetch"] += 1
            return [{"symbol": "BTC/USDT:USDT", "marginMode": "cross"}]

        def set_margin_mode(self, *a, **k):
            calls["set"] += 1
            return True

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch("bitget.trading.leverage_manager.record_oms_source"):
        ok, want, cur = lm.enforce_margin_mode(_Ex(), "BTC/USDT:USDT", "cross")
    assert ok is True
    assert want == "cross"
    assert cur == "cross"
    assert calls["set"] == 0
    assert calls["fetch"] == 0  # WS satisfied; no REST verify needed


def test_enforce_sets_then_rest_verify():
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_position(
        "USDT-FUTURES",
        "BTCUSDT",
        {"instId": "BTCUSDT", "total": "1", "marginMode": "isolated"},
    )
    sets = {"n": 0}

    class _Ex:
        def fetch_positions(self, *a, **k):
            return [{"symbol": "BTC/USDT:USDT", "marginMode": "cross"}]

        def set_margin_mode(self, mode, symbol):
            sets["n"] += 1
            assert mode == "cross"
            return True

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch("bitget.trading.leverage_manager.record_oms_source"), mock.patch(
        "bitget.trading.leverage_manager.call_with_retry",
        side_effect=lambda fn, **k: fn(),
    ):
        ok, want, cur = lm.enforce_margin_mode(_Ex(), "BTC/USDT:USDT", "cross")
    assert ok is True
    assert cur == "cross"
    assert sets["n"] == 1


def test_heartbeat_includes_mm_keys():
    from bitget.trading.oms_source_stats import oms_source_heartbeat_snapshot

    c = OmsSourceCounters()
    c.record("margin_mode", "private_ws")
    c.record("margin_mode", "rest")
    with mock.patch(
        "bitget.trading.oms_source_stats.get_oms_source_counters", return_value=c
    ):
        snap = oms_source_heartbeat_snapshot()
    assert snap["mm_ws"] == 1
    assert snap["mm_rest"] == 1
