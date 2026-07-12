"""Private WS — login SSOT, producer projection, opt-in service."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from unittest import mock

from bitget.data.stream_buffer import PrivateStreamBuffer
from bitget.data import ws_private_producer as prod
from bitget.data import ws_private_service as svc
from bitget.infra import websocket_client as ws
from bitget.infra.websocket_client import BitgetPrivateWsClient, FakeWsTransport


def test_ws_login_sign_matches_hmac_base64():
    ts = "1538054050"
    secret = "22582BD0CFF14C41EDBF1AB98506286D"
    got = ws.build_ws_login_sign(secret, timestamp=ts)
    expect = base64.b64encode(
        hmac.new(
            secret.encode("utf-8"),
            f"{ts}GET/user/verify".encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    assert got == expect


def test_login_payload_shape_no_secret_in_outer_keys():
    raw = ws.build_ws_login_payload("key", "secret", "pass", timestamp="100")
    body = json.loads(raw)
    assert body["op"] == "login"
    arg = body["args"][0]
    assert arg["apiKey"] == "key"
    assert arg["passphrase"] == "pass"
    assert arg["timestamp"] == "100"
    assert "sign" in arg
    assert "secret" not in raw  # secret only used for HMAC, not embedded as field


def test_private_client_login_then_subscribe():
    transport = FakeWsTransport(auto_login_ok=True)
    states: list[ws.WsState] = []
    messages: list = []

    client = BitgetPrivateWsClient(
        credentials_provider=lambda: ("k", "s", "p"),
        transport=transport,
        on_message=lambda m: messages.append(m),
        on_state=lambda st: states.append(st),
        ping_interval_sec=60.0,
        stale_recv_sec=120.0,
        login_timeout_sec=2.0,
    )
    client.set_subscriptions(prod.build_private_subscribe_args())
    client.start()
    deadline = time.time() + 3.0
    while time.time() < deadline and not any('"op":"subscribe"' in s for s in transport.sent):
        time.sleep(0.02)
    transport.push(
        '{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"positions","instId":"default"},'
        '"data":[{"instId":"BTCUSDT","holdSide":"long","total":"0.1","unrealizedPL":"1.2"}]}'
    )
    deadline = time.time() + 2.0
    while time.time() < deadline and not messages:
        time.sleep(0.02)
    client.stop(join_timeout=2.0)

    assert any('"op":"login"' in s for s in transport.sent)
    assert any('"op":"subscribe"' in s for s in transport.sent)
    # login must precede subscribe
    login_i = next(i for i, s in enumerate(transport.sent) if '"op":"login"' in s)
    sub_i = next(i for i, s in enumerate(transport.sent) if '"op":"subscribe"' in s)
    assert login_i < sub_i
    assert ws.WsState.AUTHENTICATED in states
    assert messages


def test_private_login_failure_reconnects_without_subscribe():
    transport = FakeWsTransport(auto_login_ok=False)
    transport.push('{"event":"error","code":"30005","msg":"bad"}')

    client = BitgetPrivateWsClient(
        credentials_provider=lambda: ("k", "s", "p"),
        transport=transport,
        ping_interval_sec=60.0,
        stale_recv_sec=120.0,
        login_timeout_sec=0.8,
    )
    client.set_subscriptions(prod.build_private_subscribe_args())
    client.start()
    time.sleep(1.2)
    client.stop(join_timeout=2.0)

    assert any('"op":"login"' in s for s in transport.sent)
    assert not any('"op":"subscribe"' in s for s in transport.sent)


def test_producer_projects_positions_orders_account():
    buf = PrivateStreamBuffer(max_events=64)
    n = prod.handle_private_ws_message(
        {
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "positions", "instId": "default"},
            "data": [
                {
                    "instId": "ETHUSDT",
                    "holdSide": "short",
                    "total": "2",
                    "unrealizedPL": "-3",
                    "noise": "drop_me",
                }
            ],
        },
        buffer=buf,
    )
    assert n == 1
    row = buf.get_position("ETHUSDT:short", "USDT-FUTURES")
    assert row is not None
    assert row["total"] == "2"
    assert "noise" not in row

    prod.handle_private_ws_message(
        {
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "orders", "instId": "default"},
            "data": [{"orderId": "oid1", "instId": "ETHUSDT", "status": "live", "noise": "x"}],
        },
        buffer=buf,
    )
    od = buf.get_order("oid1", "USDT-FUTURES")
    assert od is not None and od["status"] == "live" and "noise" not in od

    prod.handle_private_ws_message(
        {
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "account", "coin": "default"},
            "data": [{"marginCoin": "USDT", "available": "100", "equity": "100", "noise": "y"}],
        },
        buffer=buf,
    )
    assert buf.stats()["accounts"] == 1


def test_private_service_opt_in_and_soft_fail():
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("BITGET_DAEMON_PRIVATE_WS", None)
        assert svc.private_ws_daemon_enabled() is False
        assert svc.heartbeat_private_ws_snapshot() == {"enabled": False}

    service = svc.PrivateWsMarketService()
    with mock.patch.object(svc, "live_ws_transport_available", return_value=False):
        assert service.start() is False


def test_private_service_end_to_end_fake():
    buf = PrivateStreamBuffer(max_events=32)
    transport = FakeWsTransport()

    def _factory(*, on_message):
        return BitgetPrivateWsClient(
            credentials_provider=lambda: ("k", "s", "p"),
            transport=transport,
            on_message=on_message,
            ping_interval_sec=60.0,
            stale_recv_sec=120.0,
            login_timeout_sec=2.0,
        )

    service = svc.PrivateWsMarketService(
        buffer=buf, client_factory=_factory, transport=transport
    )
    with mock.patch.dict(os.environ, {"BITGET_DAEMON_PRIVATE_WS": "1"}):
        assert service.start() is True
        deadline = time.time() + 3.0
        while time.time() < deadline and not any('"op":"subscribe"' in s for s in transport.sent):
            time.sleep(0.02)
        transport.push(
            '{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"account","coin":"default"},'
            '"data":[{"marginCoin":"USDT","available":"50","equity":"50"}]}'
        )
        deadline = time.time() + 2.0
        while time.time() < deadline and service.producer.updates < 1:
            time.sleep(0.02)
        with mock.patch.object(svc, "get_private_ws_market_service", return_value=service):
            snap = svc.heartbeat_private_ws_snapshot()
        service.stop(join_timeout=2.0)

    assert service.producer.updates >= 1
    assert snap["enabled"] is True
    assert snap["started"] is True
    assert "sign" not in snap and "apiKey" not in snap
    assert all(not isinstance(v, (dict, list)) for v in snap.values())


def test_architecture_private_ws_policy():
    from bitget.validation.architecture_checks import check_daemon_private_ws_policy

    r = check_daemon_private_ws_policy()
    assert r["ok"] is True
