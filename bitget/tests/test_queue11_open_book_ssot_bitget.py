"""Queue 11 — OPEN book / identity / shadow / time_machine bounded SQL SSOT."""
from __future__ import annotations

import inspect


def test_forward_open_pnl_parity_sql_bounded():
    from bitget.infra.bounded_reads import forward_open_pnl_parity_sql
    from bitget.infra.memory_policy import FORWARD_OPEN_MAX_SAFETY

    sql, params = forward_open_pnl_parity_sql()
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    assert "status='OPEN'" in sql.replace(" ", "")
    assert params == (FORWARD_OPEN_MAX_SAFETY,)


def test_pnl_parity_module_uses_open_ssot():
    from bitget.validation import pnl_parity as pp

    src = inspect.getsource(pp)
    assert "forward_open_pnl_parity_sql" in src
    assert "warn_if_open_exceeds_safety" in src
    assert "utc_datetime_str_tz" in src
    assert "datetime.now" not in src
    assert "FROM bitget_forward_trades\n            WHERE status='OPEN'" not in src


def test_forward_shadow_defended_match_sql():
    from bitget.infra.bounded_reads import forward_shadow_defended_match_sql

    sql = forward_shadow_defended_match_sql()
    assert "LIMIT 1" in sql
    assert "final_ret" in sql


def test_shadow_tracker_uses_defended_match_ssot():
    from bitget import shadow_performance_tracker as spt

    src = inspect.getsource(spt)
    assert "forward_shadow_defended_match_sql" in src
    assert "SELECT final_ret, leverage, position_side" not in src


def test_identity_blank_symbol_ids_sql_bounded():
    from bitget.infra.bounded_reads import forward_identity_blank_symbol_ids_sql
    from bitget.infra.memory_policy import FORWARD_IDENTITY_BLANK_REPAIR_BATCH_LIMIT

    sql, params = forward_identity_blank_symbol_ids_sql(
        market_where="LOWER(IFNULL(market_type,'')) = 'spot'"
    )
    assert "LIMIT ?" in sql
    assert "TRIM(symbol)" in sql
    assert params == (FORWARD_IDENTITY_BLANK_REPAIR_BATCH_LIMIT,)


def test_identity_repair_uses_blank_batch_ssot():
    from bitget.forward import forward_trade_identity as fti

    src = inspect.getsource(fti)
    assert "forward_identity_blank_symbol_ids_sql" in src
    assert "FORWARD_IDENTITY_BLANK_REPAIR_BATCH_LIMIT" in src


def test_time_machine_uses_bar_ceiling():
    from bitget import time_machine_backtester as tmb

    src = inspect.getsource(tmb)
    assert "TIME_MACHINE_MAX_BARS_PER_TABLE" in src
    assert "bar_limit=" in src
