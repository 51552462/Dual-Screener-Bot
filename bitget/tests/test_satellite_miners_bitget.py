"""bitget satellite miners — underdog / pump / blackhole Clock SSOT + bounded reads."""
from __future__ import annotations

import sqlite3
from unittest import mock

import pandas as pd


def test_underdog_miner_module_uses_clock_ssot():
    import inspect

    from bitget import underdog_miner as ud

    src = inspect.getsource(ud)
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src
    assert "forward_underdog_miner_closed_sql" in src
    assert "print(" not in src
    assert "log_exception" in src


def test_underdog_miner_bounded_sql(tmp_path, monkeypatch):
    from bitget import underdog_miner as ud

    db_path = tmp_path / "m.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            market_type TEXT, position_side TEXT,
            dyn_cpv REAL, dyn_tb REAL, v_energy REAL, dyn_rs REAL,
            final_ret REAL, total_score REAL, status TEXT
        )
        """
    )
    for i in range(12):
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (i, "spot", "LONG", 0.5, 10.0, 5.0, 0.3, 20.0, 55.0, "CLOSED_TP"),
        )
    conn.commit()
    conn.close()

    monkeypatch.setattr(ud, "DB_PATH", str(db_path))
    monkeypatch.setattr(ud, "load_config", lambda: {})
    saved = {}

    def _save(cfg):
        saved.update(cfg)

    monkeypatch.setattr(ud, "save_config", _save)
    with mock.patch("bitget.underdog_miner.utc_date_key", return_value="2026-07-11"), mock.patch(
        "bitget.underdog_miner.utc_datetime_str", return_value="2026-07-11 03:00:00"
    ):
        ud.run_underdog_mining()
    templates = saved.get("UNDERDOG_CLUSTER_TEMPLATES", {})
    assert templates
    assert any(k.endswith("_260711") for k in templates)


def test_pump_forensics_module_uses_clock_ssot():
    import inspect

    from bitget import pump_forensics as pf

    src = inspect.getsource(pf)
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_hm_key" in src
    assert "sqlite_bitget_ohlcv_1d_tables_sql" in src
    assert "print(" not in src


def test_blackhole_hunter_module_uses_clock_ssot():
    import inspect

    from bitget import blackhole_hunter as bh

    src = inspect.getsource(bh)
    assert "datetime.utcnow()" not in src
    assert "date('now'" not in src
    assert "utc_hm_key" in src
    assert "utc_date_days_ago_str" in src
    assert "forward_blackhole_recent_closed_sql" in src
    assert "print(" not in src


def test_blackhole_hunter_uses_utc_window(tmp_path, monkeypatch):
    from bitget import blackhole_hunter as bh

    db_path = tmp_path / "m.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            symbol TEXT, final_ret REAL, dyn_cpv REAL, dyn_tb REAL,
            v_energy REAL, dyn_rs REAL, status TEXT, exit_date TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO bitget_forward_trades VALUES (1,'BTC_USDT', -10.0, 0.1,0.1,0.1,0.1,'CLOSED_SL','2026-07-10')"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(bh, "DB_PATH", str(db_path))
    monkeypatch.setattr(bh, "load_config", lambda: {"ANTI_PATTERNS": {"x": 1}})
    saved = {}

    def _save(cfg):
        saved.update(cfg)

    monkeypatch.setattr(bh, "save_config", _save)
    with mock.patch("bitget.blackhole_hunter.utc_date_days_ago_str", return_value="2026-06-27"), mock.patch(
        "bitget.blackhole_hunter.utc_hm_key", return_value="2026-07-11 03:00"
    ):
        bh.scan_blackhole_targets()
    assert saved["BLACKHOLE_TOXIC_COUNT"]["updated_at"] == "2026-07-11 03:00"
    assert saved["BLACKHOLE_TOXIC_COUNT"]["count"] >= 1
