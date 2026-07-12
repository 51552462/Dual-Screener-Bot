"""Bitget doomsday dampener — clock SSOT smoke tests."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from unittest import mock

from doomsday_dampener import GAMMA_KEY, STATE_KEY

from bitget.evolution.doomsday_dampener_bg import evolve_bitget_gamma
from bitget.infra.clock import utc_date_days_ago_str, utc_date_key


def test_evolve_bitget_gamma_window_uses_clock_ssot(tmp_path):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY, entry_date TEXT, exit_date TEXT,
            status TEXT, final_ret REAL, sig_type TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO bitget_forward_trades
        (entry_date, exit_date, status, final_ret, sig_type)
        VALUES ('2026-07-01', '2026-07-05', 'CLOSED_WIN', -2.5, '[SUPERNOVA]')
        """
    )
    conn.commit()
    conn.close()

    cfg = {
        GAMMA_KEY: 1.5,
        STATE_KEY: {
            "brake_log": [{"date": "2026-07-05", "mult": 0.75, "gamma": 1.5, "score": 72.0}],
            "history": [],
        },
    }
    anchor = datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc)
    with mock.patch("bitget.infra.config_manager.update_system_config") as upd:
        out = evolve_bitget_gamma(
            sys_config=cfg,
            db_path=str(db),
            persist=True,
            now=anchor,
        )
    assert out["reason"] == "defense_success"
    assert out["n_trades"] == 1
    assert utc_date_key(anchor=anchor) == "2026-07-08"
    assert utc_date_days_ago_str(7, anchor=anchor) == "2026-07-01"
    upd.assert_called_once()


def test_doomsday_dampener_module_uses_clock_ssot():
    import inspect

    from bitget.evolution import doomsday_dampener_bg as dd

    src = inspect.getsource(dd)
    assert "datetime.now()" not in src
    assert "timedelta" not in src
    assert "utc_now" in src
    assert "utc_date_days_ago_str" in src
    assert "utc_date_key" in src
