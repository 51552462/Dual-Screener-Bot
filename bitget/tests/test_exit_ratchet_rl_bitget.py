"""Exit ratchet κ RL — bounded runner reads + clock SSOT audit."""
from __future__ import annotations

import inspect
import sqlite3

from bitget.evolution import exit_ratchet_rl_bg as rl
from bitget.infra.bounded_reads import exit_ratchet_runner_trades_sql
from bitget.infra.memory_policy import EXIT_RATCHET_RUNNER_LIMIT


def test_exit_ratchet_module_uses_bounded_reads_ssot():
    src = inspect.getsource(rl)
    assert "exit_ratchet_runner_trades_sql" in src
    assert "SELECT mfe, final_ret, exit_type" not in src


def test_exit_ratchet_runner_trades_sql_bounded():
    sql, params = exit_ratchet_runner_trades_sql(cutoff="2026-07-01", limit=80)
    assert "SELECT *" not in sql
    assert "free_runner=1 OR scaled_out_frac > 0" in sql
    assert "LIMIT ?" in sql
    assert "ORDER BY id DESC" in sql
    assert params == ("2026-07-01", 80)

    _, params_def = exit_ratchet_runner_trades_sql(cutoff="2026-07-01T00:00:00Z")
    assert params_def == ("2026-07-01", EXIT_RATCHET_RUNNER_LIMIT)


def test_read_runner_trades_respects_limit(tmp_path):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            entry_date TEXT, exit_date TEXT,
            market_type TEXT, status TEXT,
            mfe REAL, final_ret REAL,
            exit_type TEXT, bars_held INTEGER,
            free_runner INTEGER, scaled_out_frac REAL
        )
        """
    )
    for i in range(600):
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            (entry_date, exit_date, market_type, status, mfe, final_ret,
             exit_type, bars_held, free_runner, scaled_out_frac)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-07-01",
                "2026-07-05",
                "spot",
                "CLOSED_WIN",
                20.0,
                15.0,
                "RUNNER_TRAIL",
                8,
                1,
                0.5,
            ),
        )
    conn.commit()
    conn.close()

    rows = rl._read_runner_trades(str(db), "2026-06-01")
    assert len(rows) == EXIT_RATCHET_RUNNER_LIMIT

    sql, params = exit_ratchet_runner_trades_sql(cutoff="2026-06-01", limit=10)
    conn2 = sqlite3.connect(db)
    capped = conn2.execute(sql, params).fetchall()
    conn2.close()
    assert len(capped) == 10
