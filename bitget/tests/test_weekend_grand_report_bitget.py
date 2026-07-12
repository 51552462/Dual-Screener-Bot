"""bitget.weekend_grand_report — Tier-2 bounded reads + UTC period SSOT."""
from __future__ import annotations

import sqlite3
from unittest import mock

import pandas as pd


def test_weekend_grand_report_module_uses_bounded_reads():
    import inspect

    from bitget import weekend_grand_report as wgr

    src = inspect.getsource(wgr)
    assert "forward_grand_report_closed_sql" in src
    assert "grand_report_genesis_sql" in src
    assert "grand_report_deathmatch_champion_sql" in src
    assert "grand_report_elimination_events_sql" in src
    assert "grand_report_strategy_registry_sql" in src
    assert "utc_now" in src
    assert "datetime.now(" not in src
    assert "SELECT market, champion_label" not in src
    assert "print(" not in src
    assert "log_exception" in src


def test_evolution_block_uses_bounded_sql(tmp_path):
    from bitget import weekend_grand_report as wgr

    db = tmp_path / "m.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE champion_precursor_genesis (
            market TEXT,
            champion_label TEXT,
            kind TEXT,
            status TEXT,
            realized_fwd_ret REAL,
            crowned_date TEXT,
            resolved_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE deathmatch_champion (
            market TEXT,
            champion_label TEXT,
            composite_score REAL,
            win_rate REAL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO champion_precursor_genesis VALUES
        ('spot', 'C1', 'k', 'confirmed', 5.0, '2026-07-10', '2026-07-10 12:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO deathmatch_champion VALUES ('spot', 'Alpha', 0.9, 0.6)
        """
    )
    conn.commit()

    captured: list[tuple[str, tuple]] = []

    def _fake_read_sql(sql, connection, params=()):
        captured.append((sql, params))
        if "champion_precursor_genesis" in sql:
            return pd.read_sql(sql, connection, params=params)
        if "deathmatch_champion" in sql:
            return pd.read_sql(sql, connection, params=params)
        return pd.DataFrame()

    with mock.patch("bitget.weekend_grand_report.pd.read_sql", side_effect=_fake_read_sql):
        text = wgr._evolution_block(conn, "2026-07-01", "2026-07-11", detailed=False)

    conn.close()
    assert "챔피언 전조 검증" in text
    assert any("LIMIT ?" in q for q, _ in captured)
    assert not any("SELECT *" in q for q, _ in captured)


def test_grand_report_genesis_sql_bounded():
    from bitget.infra.bounded_reads import (
        GRAND_REPORT_GENESIS_COLUMNS,
        grand_report_genesis_sql,
    )
    from bitget.infra.memory_policy import GRAND_REPORT_GENESIS_LIMIT

    sql, params = grand_report_genesis_sql(start="2026-07-01", end="2026-07-11", limit=120)
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    for col in GRAND_REPORT_GENESIS_COLUMNS.split(","):
        assert col.strip() in sql
    assert params == ("2026-07-01", "2026-07-11 23:59:59", 120)

    _, params_def = grand_report_genesis_sql(start="2026-07-01", end="2026-07-07")
    assert params_def == ("2026-07-01", "2026-07-07 23:59:59", GRAND_REPORT_GENESIS_LIMIT)


def test_grand_report_strategy_registry_sql_no_id_column():
    from bitget.infra.bounded_reads import grand_report_strategy_registry_sql

    sql, params = grand_report_strategy_registry_sql(limit=100)
    assert "ORDER BY id" not in sql
    assert "updated_at" in sql
    assert params == (100,)
