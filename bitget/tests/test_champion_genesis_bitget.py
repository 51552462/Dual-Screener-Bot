"""Champion genesis — bounded CLOSED/arm/backfill reads audit."""
from __future__ import annotations

import inspect
import sqlite3

from bitget.evolution import champion_genesis_bg as cg
from bitget.infra.bounded_reads import (
    GENESIS_LABEL_TRADE_COLUMNS,
    genesis_arm_snapshot_sql,
    genesis_closed_trades_sql,
    genesis_market_energy_closed_sql,
    genesis_pending_champions_sql,
    genesis_unresolved_predictions_sql,
)
from bitget.infra.memory_policy import (
    GENESIS_ARM_SNAPSHOT_LIMIT,
    GENESIS_CLOSED_TRADES_LIMIT,
    GENESIS_PENDING_BACKFILL_LIMIT,
    GENESIS_UNRESOLVED_PREDICTION_LIMIT,
)


def test_champion_genesis_module_uses_bounded_reads_ssot():
    src = inspect.getsource(cg)
    assert "genesis_closed_trades_sql" in src
    assert "genesis_market_energy_closed_sql" in src
    assert "genesis_arm_snapshot_sql" in src
    assert "genesis_pending_champions_sql" in src
    assert "genesis_unresolved_predictions_sql" in src
    assert "ORDER BY entry_date ASC" not in src
    assert "FROM bitget_forward_trades\n            WHERE market_type" not in src


def test_genesis_closed_trades_sql_bounded():
    sql, params = genesis_closed_trades_sql(market_type="spot", limit=200)
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    assert "ORDER BY entry_date DESC" in sql
    for col in GENESIS_LABEL_TRADE_COLUMNS.split(","):
        assert col.strip() in sql
    assert params == ("spot", 200)

    _, params_def = genesis_closed_trades_sql(market_type="futures")
    assert params_def == ("futures", GENESIS_CLOSED_TRADES_LIMIT)


def test_genesis_market_energy_sql_projection():
    sql, params = genesis_market_energy_closed_sql(market_type="spot", limit=100)
    assert "v_energy" in sql
    assert "sig_type" not in sql
    assert "LIMIT ?" in sql
    assert params == ("spot", 100)


def test_genesis_arm_snapshot_sql_bounded():
    sql, params = genesis_arm_snapshot_sql(market="spot", label="CORE_A", limit=40)
    assert "deathmatch_arm_snapshot" in sql
    assert "LIMIT ?" in sql
    assert params == ("SPOT", "CORE_A", "CORE_A", 40)

    _, params_def = genesis_arm_snapshot_sql(market="futures", label="X")
    assert params_def == ("FUT", "X", "X", GENESIS_ARM_SNAPSHOT_LIMIT)


def test_genesis_backfill_sql_limits():
    pend_sql, pend_params = genesis_pending_champions_sql(
        market="spot", cutoff="2026-06-01", limit=50
    )
    assert "champion_precursor_genesis" in pend_sql
    assert "LIMIT ?" in pend_sql
    assert pend_params == ("spot", "2026-06-01", 50)

    pred_sql, pred_params = genesis_unresolved_predictions_sql(
        market="spot", cutoff="2026-06-01"
    )
    assert "precursor_prediction_log" in pred_sql
    assert "LIMIT ?" in pred_sql
    assert pred_params == ("spot", "2026-06-01", GENESIS_UNRESOLVED_PREDICTION_LIMIT)


def test_fetch_label_trades_bounded_and_chronological(tmp_path):
    db = tmp_path / "t.sqlite"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            entry_date TEXT, exit_date TEXT, symbol TEXT, sig_type TEXT,
            status TEXT, final_ret REAL, dyn_cpv REAL, dyn_tb REAL,
            v_energy REAL, entry_breadth REAL, flow_tags TEXT, market_type TEXT
        )
        """
    )
    for i in range(120):
        conn.execute(
            """
            INSERT INTO bitget_forward_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                f"2026-01-{i+1:02d}",
                f"2026-01-{i+1:02d}",
                "BTC_USDT",
                "ENGINE1",
                "CLOSED_TP",
                1.0,
                0.5,
                1.0,
                2.0,
                1.0,
                "",
                "spot",
            ),
        )
    conn.commit()
    rows = cg._fetch_label_trades(conn, "spot", "ENGINE1")
    conn.close()
    assert len(rows) == 120
    dates = [str(r["entry_date"]) for r in rows]
    assert dates == sorted(dates)
