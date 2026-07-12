"""bitget.live_nav_manager — SPOT/FUTURES USDT NAV Clock SSOT."""
from __future__ import annotations

import json
from unittest import mock


def test_live_nav_manager_module_uses_clock_ssot():
    import inspect

    from bitget import live_nav_manager as lnm

    src = inspect.getsource(lnm)
    assert "datetime.now()" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src


def test_save_treasury_state_stamps_utc(tmp_path, monkeypatch):
    from bitget import live_nav_manager as lnm

    path = tmp_path / "bitget_treasury_state.json"
    monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(path))
    with mock.patch("bitget.live_nav_manager.utc_datetime_str", return_value="2026-07-11 03:45:00"):
        lnm.apply_realized_pnl("spot", 100.0, exit_date="2026-07-11")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["updated_at"] == "2026-07-11 03:45:00"
    assert data["spot"]["last_exit_date"] == "2026-07-11"
    assert data["spot"]["n_closed"] == 1


def test_record_closure_mdd_tracks_drawdown(tmp_path, monkeypatch):
    from bitget import live_nav_manager as lnm

    path = tmp_path / "bitget_treasury_state.json"
    monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(path))
    base = lnm.base_capital_for("futures")
    lnm.apply_realized_pnl("futures", 1000.0)
    out = lnm.apply_realized_pnl("futures", -2000.0)
    assert out["hwm"] == base + 1000.0
    assert out["nav"] == base - 1000.0
    assert out["mdd_pct"] > 0.0
