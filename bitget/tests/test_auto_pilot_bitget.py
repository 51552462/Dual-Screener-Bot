"""auto_pilot weekly flow — bounded CLOSED reads audit."""
from __future__ import annotations

import inspect
import sqlite3

import pandas as pd

from bitget import auto_pilot as ap
from bitget import system_auto_pilot as sap
from bitget.infra.bounded_reads import (
    forward_weekly_flow_closed_sql,
    forward_weekly_flow_tags_sql,
)
from bitget.infra.memory_policy import FORWARD_WEEKLY_TF_ROTATION_LIMIT


def test_auto_pilot_module_uses_weekly_flow_bounded_sql():
    src = inspect.getsource(ap)
    assert "forward_weekly_flow_closed_sql" in src
    assert "GROUP BY exit_date ORDER BY exit_date ASC" not in src
    assert "GROUP BY sig_type ORDER BY profit DESC LIMIT 3" not in src
    assert "print(" not in src
    assert "log_exception" in src


def test_system_auto_pilot_module_uses_weekly_flow_bounded_sql():
    src = inspect.getsource(sap)
    assert "forward_weekly_flow_closed_sql" in src
    assert "GROUP BY exit_date ORDER BY exit_date ASC" not in src
    assert "print(" not in src
    assert "get_logger" in src

def test_pipelines_module_uses_weekly_flow_tags_sql():
    from bitget.pipelines import bitget_pipelines as bp

    src = inspect.getsource(bp)
    assert "forward_weekly_flow_tags_sql" in src
    assert "SELECT flow_tags, final_ret FROM bitget_forward_trades" not in src


def test_reconciliation_orphan_detect_uses_bounded_recon_sql():
    from bitget.trading import reconciliation as recon

    src = inspect.getsource(recon.detect_orphan_positions)
    assert "forward_open_recon_futures_sql" in src
    assert "SELECT symbol, position_side FROM bitget_forward_trades" not in src


def test_forward_weekly_flow_closed_sql_bounded():
    sql, params = forward_weekly_flow_closed_sql(
        market_type="spot",
        since_date="2026-07-04",
        limit=150,
    )
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    assert "exit_date" in sql
    assert params == ("spot", "2026-07-04", 150)

    _, params_def = forward_weekly_flow_closed_sql(
        market_type="futures",
        since_date="2026-07-04T00:00:00Z",
    )
    assert params_def == ("futures", "2026-07-04", FORWARD_WEEKLY_TF_ROTATION_LIMIT)


def test_forward_weekly_flow_tags_sql_bounded():
    sql, params = forward_weekly_flow_tags_sql(since_date="2026-07-04", limit=120)
    assert "flow_tags" in sql
    assert "LIMIT ?" in sql
    assert params == ("2026-07-04", 120)


def test_weekly_flow_aggregation_from_bounded_sample(tmp_path):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            market_type TEXT, exit_date TEXT, sig_type TEXT,
            status TEXT, final_ret REAL, sim_kelly_invest REAL
        )
        """
    )
    for i in range(30):
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            (market_type, exit_date, sig_type, status, final_ret, sim_kelly_invest)
            VALUES (?,?,?,?,?,?)
            """,
            ("spot", "2026-07-10", "ENGINE1", "CLOSED_TP", 2.0, 1000.0),
        )
    conn.commit()
    q, p = forward_weekly_flow_closed_sql(market_type="spot", since_date="2026-07-01")
    df = pd.read_sql(q, conn, params=p)
    conn.close()
    assert len(df) == 30
    invest = pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0.0)
    ret = pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0)
    pnl = float((invest * ret / 100.0).sum())
    assert pnl == 600.0
