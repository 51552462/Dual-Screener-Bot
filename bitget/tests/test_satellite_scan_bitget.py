"""Satellite OHLCV scanners — bounded table-list SSOT audit."""
from __future__ import annotations

import inspect
import sqlite3

from bitget import forensics_pioneer as fp
from bitget import pump_forensics as pf
from bitget import supernova_hunter as sn
from bitget.infra.bounded_reads import sqlite_bitget_ohlcv_1d_tables_sql
from bitget.infra.memory_policy import TIME_MACHINE_MAX_TABLES


def test_forensics_pioneer_uses_bounded_table_scan():
    src = inspect.getsource(fp)
    assert "sqlite_bitget_ohlcv_1d_tables_sql" in src
    assert "SELECT name FROM sqlite_master" not in src


def test_pump_forensics_uses_bounded_table_scan():
    src = inspect.getsource(pf)
    assert "sqlite_bitget_ohlcv_1d_tables_sql" in src
    assert "SELECT name FROM sqlite_master" not in src


def test_supernova_hunter_uses_bounded_table_scan():
    src = inspect.getsource(sn)
    assert "sqlite_bitget_ohlcv_tf_tables_sql" in src
    assert "sqlite_bitget_scan_tables_sql" in src
    assert 'conn.execute("SELECT name FROM sqlite_master' not in src


def test_sqlite_bitget_ohlcv_1d_tables_respects_limit(tmp_path):
    db = tmp_path / "m.sqlite"
    conn = sqlite3.connect(db)
    for i in range(400):
        conn.execute(
            f'CREATE TABLE "BITGET_SPOT_SYM{i}_USDT_1D" (Date TEXT, Close REAL)'
        )
    conn.commit()
    sql, params = sqlite_bitget_ohlcv_1d_tables_sql(limit=50)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    assert len(rows) == 50

    _, default_params = sqlite_bitget_ohlcv_1d_tables_sql()
    assert default_params == (TIME_MACHINE_MAX_TABLES,)
