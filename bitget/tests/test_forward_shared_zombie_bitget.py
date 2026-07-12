"""forward/shared — zombie reporter cleanup bounded batch SSOT."""
from __future__ import annotations

import inspect
import sqlite3

from bitget.forward import shared as fs
from bitget.infra.bounded_reads import (
    forward_zombie_fact_close_ids_sql,
    forward_zombie_zero_invest_ids_sql,
)
from bitget.infra.memory_policy import FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT


def test_shared_zombie_cleanup_uses_bounded_reads_ssot():
    src = inspect.getsource(fs.reporter_cleanup_zombie_forward_trades)
    assert "forward_zombie_zero_invest_ids_sql" in src
    assert "forward_zombie_fact_close_ids_sql" in src
    assert "SELECT id FROM bitget_forward_trades" not in src


def test_forward_shared_module_no_print():
    src = inspect.getsource(fs)
    assert "print(" not in src
    assert "log_exception" in src
    assert "get_logger" in src


def test_forward_zombie_zero_invest_ids_sql_bounded():
    sql, params = forward_zombie_zero_invest_ids_sql(limit=50)
    assert "LIMIT ?" in sql
    assert "OBSERVE_ONLY" in sql
    assert params == (50,)

    _, params_def = forward_zombie_zero_invest_ids_sql()
    assert params_def == (FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT,)


def test_forward_zombie_fact_close_ids_sql_bounded():
    sql, params = forward_zombie_fact_close_ids_sql(limit=80)
    assert "exit_date" in sql
    assert "LIMIT ?" in sql
    assert params == (80,)


def test_reporter_cleanup_zombie_batches(tmp_path, monkeypatch):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            status TEXT, quantity REAL, sim_kelly_invest REAL,
            margin_used REAL, sig_type TEXT, exit_date TEXT, final_ret REAL,
            exit_reason TEXT
        )
        """
    )
    for i in range(12):
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            (id, status, quantity, sim_kelly_invest, margin_used, sig_type)
            VALUES (?,?,?,?,?,?)
            """,
            (i + 1, "OPEN", 0.0, 0.0, 0.0, "ENGINE1"),
        )
    conn.commit()
    conn.close()

    monkeypatch.setattr(fs, "DB_PATH", str(db))
    monkeypatch.setattr(fs, "init_forward_db", lambda: None)
    monkeypatch.setattr(fs, "get_connection", lambda path: sqlite3.connect(str(db)))
    monkeypatch.setattr(fs, "FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT", 5)

    n = fs.reporter_cleanup_zombie_forward_trades()
    assert n == 12

    conn2 = sqlite3.connect(db)
    open_n = conn2.execute(
        "SELECT COUNT(*) FROM bitget_forward_trades WHERE status='OPEN'"
    ).fetchone()[0]
    zombie_n = conn2.execute(
        "SELECT COUNT(*) FROM bitget_forward_trades WHERE status='CLOSED_ZOMBIE'"
    ).fetchone()[0]
    conn2.close()
    assert open_n == 0
    assert zombie_n == 12
