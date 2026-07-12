"""Public StreamBuffer ref price for order normalize + slippage instId SSOT."""
from __future__ import annotations

from unittest import mock

from bitget.data.stream_buffer import StreamBuffer
from bitget.data.ws_stream_producer import normalize_inst_id
from bitget.trading.market_price_snapshot import (
    fetch_ref_price,
    try_public_ws_ref_price,
)
from bitget.trading.oms_source_stats import OmsSourceCounters
from bitget.trading.slippage_guard import estimate_slippage_bps


def test_normalize_inst_id_ccxt_futures():
    assert normalize_inst_id("BTC/USDT:USDT") == "BTCUSDT"
    assert normalize_inst_id("ETH_USDT") == "ETHUSDT"


def test_try_ws_none_when_empty():
    buf = StreamBuffer(max_symbols=8)
    with mock.patch("bitget.data.stream_buffer.get_stream_buffer", return_value=buf):
        assert try_public_ws_ref_price("BTC/USDT:USDT", market_type="futures") is None


def test_try_ws_fresh_last():
    buf = StreamBuffer(max_symbols=8)
    buf.update_ticker("BTCUSDT", last=65000.0, bid=64990.0, ask=65010.0, inst_type="USDT-FUTURES")
    with mock.patch("bitget.data.stream_buffer.get_stream_buffer", return_value=buf):
        assert try_public_ws_ref_price("BTC/USDT:USDT", market_type="futures") == 65000.0


def test_try_ws_stale_returns_none():
    buf = StreamBuffer(max_symbols=8)
    buf.update_ticker("BTCUSDT", last=65000.0, inst_type="USDT-FUTURES")
    # Force stale ticker mono
    key = "USDT-FUTURES:BTCUSDT"
    with buf._lock:  # noqa: SLF001
        buf._tickers[key]["ts_mono"] = 0.0
    with mock.patch("bitget.data.stream_buffer.get_stream_buffer", return_value=buf):
        assert try_public_ws_ref_price("BTC/USDT:USDT", market_type="futures") is None


def test_fetch_ref_prefers_ws_and_records():
    c = OmsSourceCounters()
    buf = StreamBuffer(max_symbols=8)
    buf.update_ticker("ETHUSDT", last=3000.0, inst_type="USDT-FUTURES")

    class _Ex:
        def fetch_ticker(self, *a, **k):
            raise AssertionError("should not REST")

    with mock.patch(
        "bitget.data.stream_buffer.get_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.market_price_snapshot.record_oms_source", side_effect=c.record
    ):
        px = fetch_ref_price(_Ex(), "ETH/USDT:USDT", market_type="futures", prefer_ws=True)
    assert px == 3000.0
    assert c.lifetime_snapshot()["fetch_ticker.public_ws"] == 1


def test_fetch_ref_rest_fallback():
    c = OmsSourceCounters()
    buf = StreamBuffer(max_symbols=8)

    class _Ex:
        def fetch_ticker(self, symbol):
            return {"last": 111.0}

    with mock.patch(
        "bitget.data.stream_buffer.get_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.market_price_snapshot.record_oms_source", side_effect=c.record
    ), mock.patch(
        "bitget.trading.market_price_snapshot.call_with_retry",
        side_effect=lambda fn, **k: fn(),
    ):
        px = fetch_ref_price(_Ex(), "BTC/USDT:USDT", market_type="futures", prefer_ws=True)
    assert px == 111.0
    assert c.lifetime_snapshot()["fetch_ticker.rest"] == 1


def test_explicit_price_skips_ws_and_rest():
    class _Ex:
        def fetch_ticker(self, *a, **k):
            raise AssertionError("no fetch")

    px = fetch_ref_price(
        _Ex(), "BTC/USDT:USDT", market_type="futures", prefer_ws=True, explicit=42.5
    )
    assert px == 42.5


def test_slippage_resolves_ccxt_futures_symbol():
    """Regression: BTC/USDT:USDT must map to BTCUSDT buffer key."""
    buf = StreamBuffer(max_symbols=8)
    buf.update_orderbook(
        "BTCUSDT",
        bids=[[100.0, 1]],
        asks=[[100.1, 1]],
        inst_type="USDT-FUTURES",
    )
    with mock.patch("bitget.trading.slippage_guard.get_stream_buffer", return_value=buf):
        ok, spread, reason = estimate_slippage_bps(
            "BTC/USDT:USDT", market_type="futures", max_spread_bps=30.0, max_stale_sec=30.0
        )
    assert ok is True
    assert spread is not None
    assert reason.startswith("ok_")


def test_executor_uses_fetch_ref_price():
    import inspect

    from bitget import executor as ex

    src = inspect.getsource(ex._normalize_order_from_markets)
    assert "fetch_ref_price" in src


def test_heartbeat_includes_tk_keys():
    from bitget.trading.oms_source_stats import oms_source_heartbeat_snapshot

    c = OmsSourceCounters()
    c.record("fetch_ticker", "public_ws")
    c.record("fetch_ticker", "rest")
    with mock.patch(
        "bitget.trading.oms_source_stats.get_oms_source_counters", return_value=c
    ):
        snap = oms_source_heartbeat_snapshot()
    assert snap["tk_ws"] == 1
    assert snap["tk_rest"] == 1
