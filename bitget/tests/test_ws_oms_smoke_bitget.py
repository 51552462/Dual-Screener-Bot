"""Live WS/OMS smoke — observational readiness (no sockets / no orders)."""
from __future__ import annotations

from unittest import mock

from bitget.infra.clock import utc_now_iso
from bitget.validation.ws_oms_smoke import (
    HEARTBEAT_COMPONENT,
    check_ws_oms_smoke,
    run_ws_oms_smoke,
)


def _hb(
    *,
    public=None,
    private=None,
    oms=None,
    age_offset_sec: float = 0.0,
) -> dict:
    # Fresh by default: use current UTC; age computed at check time ≈ 0
    payload = {"kind": "liveness"}
    if public is not None:
        payload["public_ws"] = public
    if private is not None:
        payload["private_ws"] = private
    if oms is not None:
        payload["oms_book"] = oms
    return {
        "component": HEARTBEAT_COMPONENT,
        "ts_utc": utc_now_iso(),
        "payload": payload,
        "_age_offset_sec": age_offset_sec,
    }


def test_smoke_pass_when_ws_opt_in_off(monkeypatch):
    monkeypatch.delenv("BITGET_DAEMON_PUBLIC_WS", raising=False)
    monkeypatch.delenv("BITGET_DAEMON_PRIVATE_WS", raising=False)
    monkeypatch.delenv("BITGET_WS_OMS_SMOKE_STRICT", raising=False)
    report = check_ws_oms_smoke(heartbeat_row=None)
    assert report["passed"] is True
    assert report["public_ws_env"] is False
    assert report["private_ws_env"] is False
    assert report["checks"]["auto_pilot_heartbeat"]["ok"] is True


def test_smoke_fail_private_on_missing_creds_and_hb(monkeypatch):
    monkeypatch.setenv("BITGET_DAEMON_PRIVATE_WS", "1")
    monkeypatch.delenv("BITGET_DAEMON_PUBLIC_WS", raising=False)
    with mock.patch(
        "bitget.data.ws_private_service.credentials_available", return_value=False
    ), mock.patch(
        "bitget.data.ws_market_service.live_ws_transport_available", return_value=True
    ):
        report = check_ws_oms_smoke(heartbeat_row=None)
    assert report["passed"] is False
    assert "credentials" in report["hard_failures"]
    assert "auto_pilot_heartbeat" in report["hard_failures"]


def test_smoke_pass_private_live_healthy(monkeypatch):
    monkeypatch.setenv("BITGET_DAEMON_PRIVATE_WS", "1")
    monkeypatch.delenv("BITGET_DAEMON_PUBLIC_WS", raising=False)
    row = _hb(
        private={
            "enabled": True,
            "started": True,
            "ws_state": "authenticated",
            "buf_age_sec": 5.0,
            "frames": 10,
            "updates": 3,
        },
        oms={"pos_ws": 10, "pos_rest": 1, "oo_ws": 0, "oo_rest": 0, "fo_ws": 0, "fo_rest": 0},
    )
    with mock.patch(
        "bitget.data.ws_private_service.credentials_available", return_value=True
    ), mock.patch(
        "bitget.data.ws_market_service.live_ws_transport_available", return_value=True
    ):
        report = check_ws_oms_smoke(heartbeat_row=row)
    assert report["passed"] is True
    assert report["checks"]["private_ws_live"]["ok"] is True
    assert report["checks"]["oms_book_private"]["status"] == "ok"
    assert report["checks"]["oms_book"]["private_status"] == "ok"


def test_smoke_oms_rest_heavy_warn_not_fail(monkeypatch):
    monkeypatch.setenv("BITGET_DAEMON_PRIVATE_WS", "1")
    row = _hb(
        private={"enabled": True, "started": True, "ws_state": "connected", "buf_age_sec": 1.0},
        oms={
            "pos_ws": 0,
            "pos_rest": 30,
            "oo_ws": 0,
            "oo_rest": 0,
            "fo_ws": 0,
            "fo_rest": 0,
        },
    )
    with mock.patch(
        "bitget.data.ws_private_service.credentials_available", return_value=True
    ), mock.patch(
        "bitget.data.ws_market_service.live_ws_transport_available", return_value=True
    ):
        report = check_ws_oms_smoke(heartbeat_row=row, strict=False)
    assert report["passed"] is True
    assert "oms_book_private" in report["warnings"]

    with mock.patch(
        "bitget.data.ws_private_service.credentials_available", return_value=True
    ), mock.patch(
        "bitget.data.ws_market_service.live_ws_transport_available", return_value=True
    ):
        strict = check_ws_oms_smoke(heartbeat_row=row, strict=True)
    assert strict["passed"] is False
    assert "oms_book_private" in strict["hard_failures"]


def test_smoke_tk_rest_does_not_fail_private_plane(monkeypatch):
    monkeypatch.setenv("BITGET_DAEMON_PRIVATE_WS", "1")
    monkeypatch.delenv("BITGET_DAEMON_PUBLIC_WS", raising=False)
    row = _hb(
        private={"enabled": True, "started": True, "ws_state": "connected", "buf_age_sec": 1.0},
        oms={
            "pos_ws": 40,
            "pos_rest": 1,
            "oo_ws": 10,
            "oo_rest": 0,
            "fo_ws": 5,
            "fo_rest": 0,
            "tk_ws": 0,
            "tk_rest": 50,
        },
    )
    with mock.patch(
        "bitget.data.ws_private_service.credentials_available", return_value=True
    ), mock.patch(
        "bitget.data.ws_market_service.live_ws_transport_available", return_value=True
    ):
        report = check_ws_oms_smoke(heartbeat_row=row, strict=True)
    assert report["passed"] is True
    assert report["checks"]["oms_book_private"]["status"] == "ok"
    # public opt-in off → public plane check is informational
    assert report["checks"]["oms_book_public"]["ok"] is True


def test_smoke_public_not_started_fails(monkeypatch):
    monkeypatch.setenv("BITGET_DAEMON_PUBLIC_WS", "1")
    row = _hb(
        public={"enabled": True, "started": False, "ws_state": "disconnected"},
    )
    with mock.patch(
        "bitget.data.ws_market_service.live_ws_transport_available", return_value=True
    ):
        report = check_ws_oms_smoke(heartbeat_row=row)
    assert report["passed"] is False
    assert "public_ws_live" in report["hard_failures"]


def test_run_ws_oms_smoke_raise_on_fail(monkeypatch):
    monkeypatch.setenv("BITGET_DAEMON_PRIVATE_WS", "1")
    with mock.patch(
        "bitget.data.ws_private_service.credentials_available", return_value=False
    ), mock.patch(
        "bitget.data.ws_market_service.live_ws_transport_available", return_value=True
    ), mock.patch("bitget.infra.ops_logger.record_gauge_snapshot"):
        try:
            run_ws_oms_smoke(raise_on_fail=True, strict=False)
            raised = False
        except RuntimeError:
            raised = True
    assert raised is True


def test_pipeline_registers_ws_oms_smoke():
    from bitget.infra.runtime import BITGET_MODES
    from bitget.pipelines.bitget_pipelines import get_pipeline

    assert "ws_oms_smoke" in BITGET_MODES
    steps = get_pipeline("ws_oms_smoke")
    assert any(s.name == "ws_oms_smoke" for s in steps)
    health = get_pipeline("health")
    names = [s.name for s in health]
    assert names[0] == "infra_health"
    assert "ws_oms_smoke" in names
