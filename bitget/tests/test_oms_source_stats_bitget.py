"""OMS source telemetry — private_ws vs REST counters + health alerts."""
from __future__ import annotations

from unittest import mock

from bitget.infra.memory_policy import (
    OMS_REST_SHARE_ALERT_MIN_INTERVAL_SEC,
    OMS_REST_SHARE_MIN_SAMPLES,
    OMS_REST_SHARE_WARN,
)
from bitget.trading.oms_source_stats import (
    OmsSourceCounters,
    analyze_oms_book,
    maybe_warn_oms_rest_share,
    oms_source_heartbeat_snapshot,
    pick_latest_oms_heartbeat,
)
from bitget.trading import order_snapshot as osnap
from bitget.data.stream_buffer import PrivateStreamBuffer
import bitget.trading.oms_source_stats as oss


def test_counters_window_and_lifetime():
    c = OmsSourceCounters()
    c.record("position_index", "private_ws")
    c.record("position_index", "rest")
    life = c.lifetime_snapshot()
    assert life["position_index.private_ws"] == 1
    assert life["position_index.rest"] == 1

    c.begin_window()
    c.record("fetch_order", "private_ws")
    c.record("fetch_order", "private_ws")
    c.record("open_orders", "rest")
    win = c.end_window()
    assert win["fetch_order.private_ws"] == 2
    assert win["open_orders.rest"] == 1
    assert win["position_index.private_ws"] == 0  # window only
    # lifetime kept growing
    assert c.lifetime_snapshot()["fetch_order.private_ws"] == 2


def test_heartbeat_snapshot_flat_ints():
    c = OmsSourceCounters()
    c.record("position_index", "private_ws")
    c.record("open_orders", "rest")
    with mock.patch("bitget.trading.oms_source_stats.get_oms_source_counters", return_value=c):
        snap = oms_source_heartbeat_snapshot()
    assert snap["pos_ws"] == 1
    assert snap["oo_rest"] == 1
    assert all(isinstance(v, int) for k, v in snap.items() if k != "error")


def test_analyze_oms_book_rest_heavy_and_ok():
    assert float(OMS_REST_SHARE_WARN) > 0.5
    heavy = analyze_oms_book(
        {
            "pos_ws": 1,
            "pos_rest": 20,
            "oo_ws": 0,
            "oo_rest": 0,
            "fo_ws": 0,
            "fo_rest": 0,
        }
    )
    assert heavy["status"] == "rest_heavy"
    assert heavy["private_status"] == "rest_heavy"
    assert heavy["rest_share"] is not None and heavy["rest_share"] >= OMS_REST_SHARE_WARN

    ok = analyze_oms_book(
        {
            "pos_ws": 20,
            "pos_rest": 1,
            "oo_ws": 10,
            "oo_rest": 0,
            "fo_ws": 5,
            "fo_rest": 0,
        }
    )
    assert ok["status"] == "ok"
    assert analyze_oms_book(None)["status"] == "no_data"


def test_analyze_tk_does_not_contaminate_private_plane():
    """Public-off all-REST tickers must not mark private plane rest_heavy."""
    n = int(OMS_REST_SHARE_MIN_SAMPLES)
    book = {
        "pos_ws": 50,
        "pos_rest": 1,
        "oo_ws": 20,
        "oo_rest": 0,
        "fo_ws": 10,
        "fo_rest": 0,
        "bal_ws": 5,
        "bal_rest": 1,
        "mm_ws": 5,
        "mm_rest": 0,
        "tk_ws": 0,
        "tk_rest": n,
    }
    out = analyze_oms_book(book)
    assert out["private_status"] == "ok"
    assert out["status"] == "ok"
    assert out["public_status"] == "rest_heavy"
    assert out["combined_rest_share"] is not None
    # combined may look heavy; private alert plane must stay ok
    assert float(out["private_rest_share"] or 0) < float(OMS_REST_SHARE_WARN)


def test_maybe_warn_skips_when_private_ws_off():
    book = {
        "pos_ws": 0,
        "pos_rest": 50,
        "oo_ws": 0,
        "oo_rest": 50,
        "fo_ws": 0,
        "fo_rest": 50,
    }
    with mock.patch("bitget.governance.meta_alerts.send_meta_critical_alert") as alert:
        out = maybe_warn_oms_rest_share(book, private_ws_enabled=False)
    assert out["skip_reason"] == "private_ws_disabled"
    assert out["alerted"] is False
    alert.assert_not_called()


def test_maybe_warn_skips_below_min_samples():
    n = max(1, int(OMS_REST_SHARE_MIN_SAMPLES) - 1)
    book = {
        "pos_ws": 0,
        "pos_rest": n,
        "oo_ws": 0,
        "oo_rest": 0,
        "fo_ws": 0,
        "fo_rest": 0,
    }
    with mock.patch("bitget.governance.meta_alerts.send_meta_critical_alert") as alert:
        out = maybe_warn_oms_rest_share(book, private_ws_enabled=True)
    assert out["status"] == "rest_heavy"
    assert out["skip_reason"] == "below_min_samples"
    alert.assert_not_called()


def test_maybe_warn_alerts_then_throttles():
    n = int(OMS_REST_SHARE_MIN_SAMPLES)
    book = {
        "pos_ws": 0,
        "pos_rest": n,
        "oo_ws": 0,
        "oo_rest": 0,
        "fo_ws": 0,
        "fo_rest": 0,
    }
    oss._OMS_PRIVATE_REST_ALERT_MONO = 0.0
    oss._OMS_REST_ALERT_MONO = 0.0
    with mock.patch("bitget.governance.meta_alerts.send_meta_critical_alert") as alert, mock.patch(
        "bitget.infra.ops_logger.record_gauge_snapshot"
    ) as gauge:
        first = maybe_warn_oms_rest_share(book, private_ws_enabled=True)
        second = maybe_warn_oms_rest_share(book, private_ws_enabled=True)
    assert first["alerted"] is True
    assert second["alerted"] is False
    assert second.get("skip_reason") == "throttled"
    assert alert.call_count == 1
    assert gauge.call_count >= 1
    assert float(OMS_REST_SHARE_ALERT_MIN_INTERVAL_SEC) >= 60.0


def test_maybe_warn_public_plane_independent():
    n = int(OMS_REST_SHARE_MIN_SAMPLES)
    book = {
        "pos_ws": 40,
        "pos_rest": 1,
        "oo_ws": 10,
        "oo_rest": 0,
        "fo_ws": 0,
        "fo_rest": 0,
        "tk_ws": 0,
        "tk_rest": n,
    }
    oss._OMS_PUBLIC_REST_ALERT_MONO = 0.0
    with mock.patch("bitget.governance.meta_alerts.send_meta_critical_alert") as alert, mock.patch(
        "bitget.infra.ops_logger.record_gauge_snapshot"
    ):
        out = maybe_warn_oms_rest_share(
            book, private_ws_enabled=True, public_ws_enabled=True
        )
    assert out["private_status"] == "ok"
    assert out["alerted"] is False
    assert out["public_status"] == "rest_heavy"
    assert out["alerted_public"] is True
    assert alert.call_count == 1


def test_pick_latest_oms_heartbeat():
    rows = [
        {
            "component": "bitget_auto_pilot",
            "ts_utc": "2026-07-11T12:00:00+00:00",
            "payload": {
                "oms_book": {
                    "pos_ws": 10,
                    "pos_rest": 1,
                    "oo_ws": 0,
                    "oo_rest": 0,
                    "fo_ws": 0,
                    "fo_rest": 0,
                },
                "private_ws": {"enabled": True},
            },
        }
    ]
    picked = pick_latest_oms_heartbeat(rows)
    assert picked["analysis"]["status"] == "ok"
    assert picked["private_ws"]["enabled"] is True


def test_fetch_order_records_source():
    c = OmsSourceCounters()
    buf = PrivateStreamBuffer(max_events=16)
    buf.update_order(
        "USDT-FUTURES",
        "oid1",
        {
            "orderId": "oid1",
            "instId": "BTCUSDT",
            "status": "filled",
            "filledQty": "1",
            "priceAvg": "10",
        },
    )

    class _Ex:
        def fetch_order(self, *a, **k):
            raise AssertionError("should not REST")

    with mock.patch(
        "bitget.data.stream_buffer.get_private_stream_buffer", return_value=buf
    ), mock.patch(
        "bitget.trading.order_snapshot.record_oms_source", side_effect=c.record
    ):
        od = osnap.fetch_order_snapshot(_Ex(), "oid1", "BTC/USDT:USDT")
    assert od is not None
    assert c.lifetime_snapshot()["fetch_order.private_ws"] == 1


def test_recon_persists_source_counts_key():
    import inspect

    from bitget.trading import reconciliation as recon

    src = inspect.getsource(recon.run_scheduled_reconciliation)
    assert "begin_window" in src
    assert "source_counts" in src
    assert "LAST_OMS_SOURCE_COUNTS" in src


def test_heartbeat_loop_attaches_oms_book():
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
    ), mock.patch(
        "bitget.trading.oms_source_stats.oms_source_heartbeat_snapshot",
        return_value={"pos_ws": 0, "pos_rest": 1},
    ), mock.patch(
        "bitget.trading.oms_source_stats.maybe_warn_oms_rest_share",
        return_value={"status": "ok"},
    ) as warn:
        bap._heartbeat_loop(stop)

    assert recorded[0]["oms_book"]["pos_rest"] == 1
    warn.assert_called_once()


def test_dashboard_ops_panel_imports_oms_picker():
    import inspect

    from bitget import dashboard_ops_panel as dop

    src = inspect.getsource(dop)
    assert "pick_latest_oms_heartbeat" in src
    assert "_render_oms_book_panel" in src
