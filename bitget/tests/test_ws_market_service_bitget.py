"""Public WS market service — opt-in daemon lifecycle + universe budget."""
from __future__ import annotations

import os
from unittest import mock

from bitget.data import ws_market_service as svc
from bitget.data.stream_buffer import StreamBuffer
from bitget.infra.websocket_client import BitgetPublicWsClient, FakeWsTransport


def test_public_ws_disabled_by_default():
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("BITGET_DAEMON_PUBLIC_WS", None)
        assert svc.public_ws_daemon_enabled() is False


def test_public_ws_enabled_opt_in():
    with mock.patch.dict(os.environ, {"BITGET_DAEMON_PUBLIC_WS": "1"}):
        assert svc.public_ws_daemon_enabled() is True


def test_resolve_universe_benchmarks_and_channel_budget():
    opens = [
        svc.WatchTarget("SOLUSDT", "USDT-FUTURES"),
        svc.WatchTarget("XRPUSDT", "USDT-FUTURES"),
    ]
    targets = svc.resolve_watch_universe(
        include_books=True,
        max_channels=10,  # budget = 5 targets
        open_targets=opens,
        extra_symbols=["DOGE_USDT", "AAA_USDT", "BBB_USDT"],
        benchmark_symbols=("BTCUSDT", "ETHUSDT"),
    )
    assert len(targets) == 5
    assert targets[0].inst_id == "BTCUSDT"
    assert targets[1].inst_id == "ETHUSDT"
    assert targets[2].inst_id == "SOLUSDT"
    args = svc.targets_to_subscribe_args(targets, include_books=True)
    assert len(args) <= 10
    assert any(a["channel"] == "ticker" for a in args)
    assert any(a["channel"] == "books5" for a in args)


def test_service_start_stop_with_fake_transport():
    buf = StreamBuffer(max_symbols=32)
    transport = FakeWsTransport()

    def _factory(*, on_message):
        return BitgetPublicWsClient(
            transport=transport,
            on_message=on_message,
            ping_interval_sec=60.0,
            pong_timeout_sec=30.0,
            stale_recv_sec=120.0,
        )

    service = svc.PublicWsMarketService(
        buffer=buf,
        transport=transport,
        include_books=False,
        refresh_sec=3600.0,
        client_factory=_factory,
    )
    with mock.patch.object(
        svc,
        "resolve_watch_universe",
        return_value=[svc.WatchTarget("BTCUSDT", "USDT-FUTURES")],
    ):
        assert service.start() is True
        assert service.started is True
        # Inject ticker while running
        import time

        deadline = time.time() + 2.0
        while time.time() < deadline and not any('"op":"subscribe"' in s for s in transport.sent):
            time.sleep(0.02)
        transport.push(
            '{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"ticker","instId":"BTCUSDT"},'
            '"data":[{"instId":"BTCUSDT","lastPr":"1","bidPr":"0.9","askPr":"1.1"}]}'
        )
        deadline = time.time() + 2.0
        while time.time() < deadline and service.producer.updates < 1:
            time.sleep(0.02)
        service.stop(join_timeout=2.0)

    assert service.producer.updates >= 1
    assert buf.get_last_price("BTCUSDT", "USDT-FUTURES") == 1.0
    assert service.started is False


def test_request_reconnect_on_universe_change():
    transport = FakeWsTransport()
    calls = {"reconnect": 0}

    class _Client(BitgetPublicWsClient):
        def request_reconnect(self):
            calls["reconnect"] += 1
            super().request_reconnect()

    def _factory(*, on_message):
        return _Client(
            transport=transport,
            on_message=on_message,
            ping_interval_sec=60.0,
            stale_recv_sec=120.0,
        )

    service = svc.PublicWsMarketService(
        buffer=StreamBuffer(max_symbols=8),
        include_books=False,
        refresh_sec=3600.0,
        client_factory=_factory,
    )
    u1 = [svc.WatchTarget("BTCUSDT", "USDT-FUTURES")]
    u2 = [
        svc.WatchTarget("BTCUSDT", "USDT-FUTURES"),
        svc.WatchTarget("ETHUSDT", "USDT-FUTURES"),
    ]
    with mock.patch.object(svc, "resolve_watch_universe", side_effect=[u1, u2, u2]):
        assert service.start() is True
        changed = service.refresh_universe_now()
        same = service.refresh_universe_now()
        service.stop(join_timeout=2.0)
    assert changed is True
    assert same is False
    assert calls["reconnect"] == 1


def test_start_soft_fails_without_websocket_package():
    service = svc.PublicWsMarketService(
        buffer=StreamBuffer(max_symbols=4),
        transport=None,
        client_factory=None,
        refresh_sec=3600.0,
    )
    with mock.patch.object(svc, "live_ws_transport_available", return_value=False):
        assert service.start() is False
    assert service.started is False
    from bitget.pipelines import bitget_auto_pilot as bap

    with mock.patch.dict(os.environ, {"BITGET_DAEMON_PUBLIC_WS": "1"}):
        assert bap._daemon_public_ws_enabled() is True
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("BITGET_DAEMON_PUBLIC_WS", None)
        assert bap._daemon_public_ws_enabled() is False


def test_heartbeat_snapshot_disabled_is_minimal():
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("BITGET_DAEMON_PUBLIC_WS", None)
        snap = svc.heartbeat_public_ws_snapshot()
    assert snap == {"enabled": False}


def test_heartbeat_snapshot_started_is_flat_scalars():
    buf = StreamBuffer(max_symbols=8)
    transport = FakeWsTransport()

    def _factory(*, on_message):
        return BitgetPublicWsClient(
            transport=transport,
            on_message=on_message,
            ping_interval_sec=60.0,
            stale_recv_sec=120.0,
        )

    service = svc.PublicWsMarketService(
        buffer=buf,
        include_books=False,
        refresh_sec=3600.0,
        client_factory=_factory,
    )
    with mock.patch.object(
        svc,
        "resolve_watch_universe",
        return_value=[svc.WatchTarget("BTCUSDT", "USDT-FUTURES")],
    ), mock.patch.dict(os.environ, {"BITGET_DAEMON_PUBLIC_WS": "1"}):
        assert service.start() is True
        # Point global singleton at this service for heartbeat helper
        with mock.patch.object(svc, "get_public_ws_market_service", return_value=service):
            snap = svc.heartbeat_public_ws_snapshot()
        service.stop(join_timeout=2.0)

    assert snap["enabled"] is True
    assert snap["started"] is True
    assert isinstance(snap["universe"], int)
    assert isinstance(snap["frames"], int)
    assert isinstance(snap["buf_age_sec"], float)
    # No nested buffer blob in heartbeat path
    assert "buffer" not in snap
    assert all(not isinstance(v, (dict, list)) for v in snap.values())


def test_daemon_heartbeat_tick_attaches_public_ws():
    from bitget.pipelines import bitget_auto_pilot as bap

    recorded: list[dict] = []
    stop = __import__("threading").Event()

    def _fake_hb(component, **kwargs):
        recorded.append({"component": component, **kwargs})
        stop.set()

    with mock.patch("bitget.infra.ops_logger.record_heartbeat", side_effect=_fake_hb), mock.patch(
        "bitget.data.ws_market_service.heartbeat_public_ws_snapshot",
        return_value={"enabled": False},
    ), mock.patch(
        "bitget.data.ws_private_service.heartbeat_private_ws_snapshot",
        return_value={"enabled": False},
    ):
        bap._heartbeat_loop(stop)

    assert recorded
    assert recorded[0]["component"] == bap.HEARTBEAT_COMPONENT
    assert recorded[0]["public_ws"] == {"enabled": False}
    assert recorded[0].get("private_ws") == {"enabled": False}
