"""Chapter 6 — WebSocket frame → StreamBuffer producer."""
from __future__ import annotations

from bitget.data.stream_buffer import StreamBuffer
from bitget.data import ws_stream_producer as prod
from bitget.infra.websocket_client import BitgetPublicWsClient, FakeWsTransport
from bitget.trading.slippage_guard import estimate_slippage_bps


def test_normalize_inst_id_and_type():
    assert prod.normalize_inst_id("BTC_USDT") == "BTCUSDT"
    assert prod.normalize_inst_id("BTC/USDT:USDT") == "BTCUSDT"
    assert prod.normalize_inst_type("swap") == "USDT-FUTURES"
    assert prod.normalize_inst_type("SPOT") == "SPOT"


def test_build_market_subscribe_args_caps_channels():
    symbols = [f"COIN{i}_USDT" for i in range(100)]
    args = prod.build_market_subscribe_args(symbols, include_books=True)
    assert len(args) <= 50
    assert any(a["channel"] == "ticker" for a in args)
    assert any(a["channel"] == "books5" for a in args)


def test_handle_ticker_updates_buffer_and_spread_gate():
    buf = StreamBuffer(max_symbols=32)
    n = prod.handle_ws_message(
        {
            "action": "snapshot",
            "arg": {"instType": "SPOT", "channel": "ticker", "instId": "ETHUSDT"},
            "data": [
                {
                    "instId": "ETHUSDT",
                    "lastPr": "2200.10",
                    "bidPr": "2199.5",
                    "askPr": "2200.5",
                    "quoteVolume": "1234567.0",
                }
            ],
        },
        buffer=buf,
    )
    assert n == 1
    row = buf.get_ticker("ETHUSDT", "SPOT")
    assert row is not None
    assert row["last"] == 2200.10
    assert row["quote_volume_24h"] == 1234567.0

    # Populate global buffer used by slippage_guard
    prod.handle_ws_message(
        {
            "action": "snapshot",
            "arg": {"instType": "SPOT", "channel": "ticker", "instId": "ETHUSDT"},
            "data": [
                {
                    "instId": "ETHUSDT",
                    "lastPr": "2200.10",
                    "bidPr": "2199.5",
                    "askPr": "2200.5",
                }
            ],
        }
    )
    ok, spread, reason = estimate_slippage_bps("ETH_USDT", market_type="spot", max_spread_bps=30.0)
    assert ok is True
    assert spread is not None and spread < 30.0


def test_handle_books_updates_orderbook_spread():
    buf = StreamBuffer(max_symbols=16)
    prod.handle_ws_message(
        {
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "books5", "instId": "BTCUSDT"},
            "data": [
                {
                    "instId": "BTCUSDT",
                    "bids": [["100.0", "1"], ["99.9", "2"]],
                    "asks": [["100.1", "1"], ["100.2", "2"]],
                }
            ],
        },
        buffer=buf,
    )
    bps = buf.orderbook_spread_bps("BTCUSDT", "USDT-FUTURES")
    assert bps is not None
    assert 0.0 < bps < 20.0


def test_producer_with_fake_ws_client_end_to_end():
    buf = StreamBuffer(max_symbols=16)
    producer = prod.StreamBufferProducer(buffer=buf)
    transport = FakeWsTransport()
    client = BitgetPublicWsClient(
        transport=transport,
        on_message=producer.on_message,
        ping_interval_sec=60.0,
        pong_timeout_sec=30.0,
        stale_recv_sec=120.0,
    )
    client.set_subscriptions(
        prod.build_ticker_subscribe_args(["BTC_USDT"], inst_type="SPOT")
    )
    client.start()
    import time

    deadline = time.time() + 2.0
    while time.time() < deadline and not any('"op":"subscribe"' in s for s in transport.sent):
        time.sleep(0.02)
    transport.push(
        '{"action":"snapshot","arg":{"instType":"SPOT","channel":"ticker","instId":"BTCUSDT"},'
        '"data":[{"instId":"BTCUSDT","lastPr":"65000","bidPr":"64999","askPr":"65001"}]}'
    )
    deadline = time.time() + 2.0
    while time.time() < deadline and producer.updates < 1:
        time.sleep(0.02)
    client.stop(join_timeout=2.0)

    assert producer.updates >= 1
    assert buf.get_last_price("BTCUSDT", "SPOT") == 65000.0
