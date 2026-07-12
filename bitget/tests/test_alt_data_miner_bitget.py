"""bitget.alt_data_miner — macro daily Clock SSOT + bounded last-row read."""
from __future__ import annotations

import sqlite3
from unittest import mock

import pytest


def test_alt_data_miner_module_uses_clock_ssot():
    import inspect

    from bitget import alt_data_miner as adm

    src = inspect.getsource(adm)
    assert "datetime.utcnow()" not in src
    assert "datetime.now()" not in src
    assert "from datetime import" not in src
    assert "utc_date_key" in src
    assert "macro_daily_last_row_sql" in src
    assert "SELECT *" not in src
    assert "print(" not in src
    assert "log_exception" in src


def test_run_once_uses_utc_date_key(tmp_path, monkeypatch):
    from bitget import alt_data_miner as adm

    db_path = tmp_path / "alt.sqlite"
    monkeypatch.setattr(adm, "alt_data_db_path", lambda: str(db_path))
    with mock.patch("bitget.alt_data_miner.utc_date_key", return_value="2026-07-11"), mock.patch(
        "bitget.alt_data_miner._fetch_global", return_value=(55.0, 1e12, 1.5)
    ), mock.patch("bitget.alt_data_miner._fetch_prices", return_value=(60000.0, 3000.0, 0.05)):
        row = adm.run_once()
    assert row is not None
    assert row["date"] == "2026-07-11"
    conn = sqlite3.connect(db_path)
    stored = conn.execute("SELECT date FROM macro_daily").fetchone()[0]
    conn.close()
    assert stored == "2026-07-11"


def test_load_last_row_uses_projected_columns(tmp_path, monkeypatch):
    from bitget import alt_data_miner as adm

    db_path = tmp_path / "alt.sqlite"
    monkeypatch.setattr(adm, "alt_data_db_path", lambda: str(db_path))
    adm.init_alt_db()
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO macro_daily
        (date, btc_dominance, eth_btc_ratio, total_market_cap_usd,
         market_cap_change_24h, btc_price_usd, eth_price_usd)
        VALUES (?,?,?,?,?,?,?)
        """,
        ("2026-07-10", 54.0, 0.04, 1e12, 0.5, 59000.0, 2900.0),
    )
    conn.commit()
    conn.close()
    row = adm._load_last_row()
    assert row is not None
    assert row["date"] == "2026-07-10"
    assert set(row.keys()) == {
        "date",
        "btc_dominance",
        "eth_btc_ratio",
        "total_market_cap_usd",
        "market_cap_change_24h",
        "btc_price_usd",
        "eth_price_usd",
    }
