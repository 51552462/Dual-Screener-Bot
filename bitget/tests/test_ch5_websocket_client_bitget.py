"""Chapter 5 — Bitget WebSocket client SSOT (ping/pong + reconnect)."""
from __future__ import annotations

import time
from unittest import mock

import pytest

from bitget.infra import websocket_client as ws
from bitget.infra.memory_policy import (
    WS_MAX_CHANNELS_PER_CONN,
    WS_PING_INTERVAL_SEC,
    WS_PUBLIC_URL,
)


def test_memory_policy_ws_constants():
    assert WS_PUBLIC_URL.startswith("wss://")
    assert WS_PING_INTERVAL_SEC == 30.0
    assert WS_MAX_CHANNELS_PER_CONN == 50


def test_chunk_subscribe_args_and_cap():
    args = [{"instType": "SPOT", "channel": "ticker", "instId": f"S{i}"} for i in range(45)]
    batches = ws.chunk_subscribe_args(args, batch_size=20)
    assert len(batches) == 3
    assert sum(len(b) for b in batches) == 45

    too_many = args + [{"instType": "SPOT", "channel": "ticker", "instId": "X"}]
    with pytest.raises(ValueError):
        ws.chunk_subscribe_args(too_many, max_channels=45)


def test_reconnect_backoff_grows_and_caps():
    with mock.patch("bitget.infra.websocket_client.random.uniform", return_value=0.0):
        assert ws.compute_ws_reconnect_sec(0) == pytest.approx(1.0)
        assert ws.compute_ws_reconnect_sec(1) == pytest.approx(2.0)
        assert ws.compute_ws_reconnect_sec(2) == pytest.approx(4.0)
        assert ws.compute_ws_reconnect_sec(10) == pytest.approx(60.0)


def test_client_subscribes_and_handles_pong_heartbeat():
    transport = ws.FakeWsTransport()
    messages: list = []

    client = ws.BitgetPublicWsClient(
        transport=transport,
        on_message=lambda m: messages.append(m),
        ping_interval_sec=0.05,
        pong_timeout_sec=1.0,
        stale_recv_sec=30.0,
    )
    client.set_subscriptions(
        [{"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"}]
    )
    client.start()
    deadline = time.time() + 2.0
    while time.time() < deadline and not any(s == "ping" for s in transport.sent):
        time.sleep(0.02)
    # Inject a market payload
    transport.push('{"arg":{"channel":"ticker"},"data":[{"lastPr":"1"}]}')
    deadline = time.time() + 2.0
    while time.time() < deadline and not messages:
        time.sleep(0.02)
    client.stop(join_timeout=2.0)

    assert any('"op":"subscribe"' in s for s in transport.sent)
    assert any(s == "ping" for s in transport.sent)
    assert messages
    assert client.state == ws.WsState.STOPPED


def test_missed_pong_forces_reconnect_session():
    class NoPongTransport(ws.FakeWsTransport):
        def send(self, data: str) -> None:
            self.sent.append(data)
            # deliberately no auto-pong

    transport = NoPongTransport()
    states: list[ws.WsState] = []

    client = ws.BitgetPublicWsClient(
        transport=transport,
        on_state=lambda st: states.append(st),
        ping_interval_sec=0.02,
        pong_timeout_sec=0.05,
        stale_recv_sec=30.0,
    )
    client.set_subscriptions([])
    client.start()
    deadline = time.time() + 3.0
    while time.time() < deadline and ws.WsState.RECONNECTING not in states:
        time.sleep(0.02)
    client.stop(join_timeout=2.0)

    assert ws.WsState.CONNECTED in states
    assert ws.WsState.RECONNECTING in states or transport.closed


def test_set_subscriptions_rejects_over_cap():
    client = ws.BitgetPublicWsClient(transport=ws.FakeWsTransport())
    args = [{"instType": "SPOT", "channel": "ticker", "instId": str(i)} for i in range(51)]
    with pytest.raises(ValueError):
        client.set_subscriptions(args)
