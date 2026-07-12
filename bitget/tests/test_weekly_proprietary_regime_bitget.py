"""Weekly Shadow PRI — bounded funnel/friction reads + clock SSOT audit."""
from __future__ import annotations

import inspect
import sqlite3

import pandas as pd

from bitget.evolution import weekly_proprietary_regime_bg as pri
from bitget.infra.bounded_reads import pri_funnel_week_sql, pri_friction_week_sql
from bitget.infra.memory_policy import PRI_FUNNEL_WEEK_LIMIT, PRI_FRICTION_WEEK_LIMIT


def test_weekly_pri_module_uses_bounded_funnel_friction_sql():
    src = inspect.getsource(pri)
    assert "pri_funnel_week_sql" in src
    assert "pri_friction_week_sql" in src
    assert "SELECT ts, market, universe_size" not in src


def test_pri_funnel_week_sql_bounded():
    sql, params = pri_funnel_week_sql(
        market="SPOT",
        week_start="2026-07-01",
        week_end="2026-07-07",
        limit=50,
    )
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    assert "scan_funnel_snapshot" in sql
    assert params == ("SPOT", "2026-07-01", "2026-07-07", 50)

    _, params_def = pri_funnel_week_sql(
        market="FUT",
        week_start="2026-07-01",
        week_end="2026-07-07",
    )
    assert params_def == ("FUT", "2026-07-01", "2026-07-07", PRI_FUNNEL_WEEK_LIMIT)


def test_pri_friction_week_sql_bounded():
    sql, params = pri_friction_week_sql(
        market="SPOT",
        week_start="2026-07-01",
        week_end="2026-07-07",
        limit=30,
    )
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    assert "regime_friction_event" in sql
    assert params == ("SPOT", "2026-07-01", "2026-07-07", 30)

    _, params_def = pri_friction_week_sql(
        market="SPOT",
        week_start="2026-07-01",
        week_end="2026-07-07",
    )
    assert params_def == ("SPOT", "2026-07-01", "2026-07-07", PRI_FRICTION_WEEK_LIMIT)


def test_load_funnel_week_respects_limit(tmp_path):
    db = tmp_path / "m.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE scan_funnel_snapshot (
            ts TEXT, market TEXT, universe_size INTEGER,
            survivors INTEGER, pass_rate_pct REAL
        )
        """
    )
    rows = [
        (f"2026-07-0{(i % 7) + 1} 12:00:00", "SPOT", 100, 10 + i, 10.0 + i)
        for i in range(500)
    ]
    conn.executemany(
        "INSERT INTO scan_funnel_snapshot VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    df = pri._load_funnel_week(conn, "spot", "2026-07-01", "2026-07-07")
    conn.close()
    assert len(df) <= PRI_FUNNEL_WEEK_LIMIT
