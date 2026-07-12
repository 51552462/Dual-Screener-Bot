"""bitget doomsday + canary exporter Clock SSOT."""
from __future__ import annotations

from unittest import mock


def test_doomsday_bot_module_uses_clock_ssot():
    import inspect

    from bitget import doomsday_bot as db

    src = inspect.getsource(db)
    assert "datetime.now()" not in src
    assert "from datetime import" not in src
    assert "utc_hm_key" in src
    assert "print(" not in src
    assert "log_exception" in src


def test_doomsday_bridge_module_uses_clock_ssot():
    import inspect

    from bitget import doomsday_bridge as dbr

    src = inspect.getsource(dbr)
    assert "datetime.now(" not in src
    assert "from datetime import" not in src
    assert "utc_now_iso" in src
    assert "print(" not in src
    assert "get_logger" in src


def test_run_doomsday_radar_stamps_utc_hm():
    from bitget import doomsday_bot as db

    saved = {}

    def _save(cfg):
        saved.update(cfg)

    with mock.patch("bitget.doomsday_bot._fetch_global_crypto", return_value={"btc_dominance": 50.0, "market_cap_change_24h": 0.0}), mock.patch(
        "bitget.doomsday_bot._fetch_eth_btc_ratio", return_value=0.05
    ), mock.patch("bitget.doomsday_bot.load_config", return_value={}), mock.patch(
        "bitget.doomsday_bot.save_config", _save
    ), mock.patch("bitget.doomsday_bot.utc_hm_key", return_value="2026-07-11 04:30"):
        db.run_doomsday_radar()
    dd = saved["DOOMSDAY_DEFCON"]
    assert dd["updated_at"] == "2026-07-11 04:30"
    assert dd["level"] == 5
    assert "scores" in dd
    assert "Global_Contagion_Score" in dd["scores"]


def test_run_doomsday_radar_elevated_writes_floor_score():
    from bitget import doomsday_bot as db

    saved = {}

    def _save(cfg):
        saved.update(cfg)

    with mock.patch(
        "bitget.doomsday_bot._fetch_global_crypto",
        return_value={"btc_dominance": 60.0, "market_cap_change_24h": -7.0},
    ), mock.patch(
        "bitget.doomsday_bot._fetch_eth_btc_ratio", return_value=0.04
    ), mock.patch("bitget.doomsday_bot.load_config", return_value={}), mock.patch(
        "bitget.doomsday_bot.save_config", _save
    ), mock.patch("bitget.doomsday_bot.utc_hm_key", return_value="2026-07-11 04:30"):
        db.run_doomsday_radar()
    dd = saved["DOOMSDAY_DEFCON"]
    assert dd["level"] <= 2
    assert float(dd["scores"]["Global_Contagion_Score"]) >= 80.0


def test_canary_exporter_module_uses_clock_ssot():
    import inspect

    from bitget import canary_exporter as ce

    src = inspect.getsource(ce)
    assert "datetime.now(" not in src
    assert "from datetime import" not in src
    assert "utc_now_iso" in src
    assert "print(" not in src
    assert "_safe_log" in src
    assert "log_exception" in src

def test_compute_canary_state_updated_at_utc():
    from bitget import canary_exporter as ce

    with mock.patch("bitget.canary_exporter._top_alt_swaps", return_value=[]), mock.patch(
        "bitget.canary_exporter._last_prices", return_value={}
    ), mock.patch("bitget.canary_exporter._current_oi_total_usdt", return_value=None), mock.patch(
        "bitget.canary_exporter._oi_change_pct_24h", return_value=None
    ), mock.patch("bitget.canary_exporter._oi_change_via_history", return_value=(None, None)), mock.patch(
        "bitget.canary_exporter._avg_funding", return_value=None
    ), mock.patch("bitget.canary_exporter._btc_ret_3d", return_value=None), mock.patch(
        "bitget.canary_exporter._macro_up", return_value=(False, None)
    ), mock.patch("bitget.canary_exporter.utc_now_iso", return_value="2026-07-11T04:30:00+00:00"):
        state = ce.compute_canary_state()
    assert state["updated_at"] == "2026-07-11T04:30:00+00:00"
    assert state["schema"] == "bitget_canary.v1"
