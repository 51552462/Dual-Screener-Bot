"""Bitget Tier-2 bounded reads — unit tests."""
from __future__ import annotations

import sqlite3

import pandas as pd

from bitget.infra.bounded_reads import (
    overseer_daily_closed_sql,
    real_execution_leaderboard_sql,
)


def test_real_execution_leaderboard_sql_spot():
    sql, params = real_execution_leaderboard_sql(market_type="spot", limit=100)
    assert "LIMIT ?" in sql
    assert "SELECT" in sql and "practitioner_key" in sql
    assert params == ["spot", 100]


def test_real_execution_leaderboard_sql_all():
    sql, params = real_execution_leaderboard_sql(market_type="all")
    assert "WHERE" not in sql
    assert len(params) == 1


def test_overseer_daily_closed_filters_exit_date_in_sql():
    sql, params = overseer_daily_closed_sql(today="2026-07-11")
    assert "exit_date" in sql
    assert " IN (?, ?)" in sql
    assert params[0] == "2026-07-11"
    assert params[1] == "2026-07-10"
    assert params[2] == "2026-07-11"


def test_overseer_daily_closed_bounded_query(tmp_path):
    db = tmp_path / "m.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            entry_date TEXT,
            exit_date TEXT,
            final_ret REAL,
            position_side TEXT,
            sig_type TEXT,
            status TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO bitget_forward_trades VALUES (?,?,?,?,?,?,?)",
        [
            (1, "2026-07-11", "2026-07-11", 1.0, "LONG", "S1", "CLOSED"),
            (2, "2026-07-10", "2026-07-11", -0.5, "SHORT", "S2", "CLOSED"),
            (3, "2026-07-11", "2026-07-10", 2.0, "LONG", "S3", "CLOSED"),
        ],
    )
    conn.commit()
    q, p = overseer_daily_closed_sql(today="2026-07-11", limit=10)
    df = pd.read_sql(q, conn, params=p)
    conn.close()
    assert len(df) == 3
    ids = set(int(x) for x in df["id"].tolist())
    assert ids == {1, 2, 3}


def test_forward_open_track_sql_projection():
    from bitget.infra.bounded_reads import forward_open_track_sql
    from bitget.infra.memory_policy import FORWARD_OPEN_MAX_SAFETY

    sql, params = forward_open_track_sql(market_type="futures")
    assert "SELECT" in sql
    assert "status='OPEN'" in sql.replace(" ", "")
    assert "entry_atr" in sql
    assert "LIMIT ?" in sql
    assert params == ("futures", FORWARD_OPEN_MAX_SAFETY)


def test_forward_dashboard_closed_bounded():
    from bitget.infra.bounded_reads import forward_dashboard_closed_sql

    sql, params = forward_dashboard_closed_sql(limit=500)
    assert "CLOSED" in sql
    assert params == (500,)


def test_forward_pil_trades_preserves_open():
    from bitget.infra.bounded_reads import forward_pil_trades_sql

    sql, _params = forward_pil_trades_sql(market_type="spot")
    assert "NOT LIKE 'CLOSED%'" in sql
    assert "market_type" in sql


def test_forward_brain_tune_sql_projection():
    from bitget.infra.bounded_reads import forward_brain_tune_closed_sql

    sql, params = forward_brain_tune_closed_sql(limit=120)
    assert "final_ret" in sql
    assert "dyn_cpv" in sql
    assert "SELECT *" not in sql
    assert params == (120,)


def test_forward_pri_week_sql_bounded():
    from bitget.infra.bounded_reads import forward_pri_closed_week_sql

    sql, params = forward_pri_closed_week_sql(
        market_type="spot",
        week_start="2026-07-01",
        week_end="2026-07-07",
        limit=1000,
    )
    assert "INCUBATOR" in sql
    assert "LIMIT ?" in sql
    assert params == ("spot", "2026-07-01", "2026-07-07", 1000)


def test_forward_autopilot_analysis_sql_projection():
    from bitget.infra.bounded_reads import (
        FORWARD_AUTOPILOT_CLOSED_COLUMNS,
        forward_autopilot_analysis_closed_sql,
    )

    sql, params = forward_autopilot_analysis_closed_sql(limit=500)
    assert "SELECT *" not in sql
    assert "live_a_ret" in sql
    assert "champ_c_ret" in sql
    for col in FORWARD_AUTOPILOT_CLOSED_COLUMNS.split(","):
        assert col.strip() in sql
    assert params == (500,)


def test_forward_incubator_judge_sql():
    from bitget.infra.bounded_reads import forward_incubator_judge_closed_sql

    sql, params = forward_incubator_judge_closed_sql(limit=200)
    assert "INCUBATOR" in sql
    assert "sig_type" in sql
    assert "SELECT *" not in sql
    assert params == (200,)


def test_forward_weekly_tf_rotation_sql():
    from bitget.infra.bounded_reads import forward_weekly_tf_rotation_sql

    sql, params = forward_weekly_tf_rotation_sql(
        market_type="futures",
        since_date="2026-07-01",
        limit=1500,
    )
    assert "entry_date" in sql
    assert "timeframe" in sql
    assert "LIMIT ?" in sql
    assert params == ("futures", "2026-07-01", 1500)


def test_task_queue_claim_next_sql():
    from bitget.infra.bounded_reads import task_queue_claim_next_sql

    sql = task_queue_claim_next_sql()
    assert "SELECT *" not in sql
    assert "LIMIT 1" in sql
    assert "engine, mode, payload" in sql
    assert "status='PENDING'" in sql


def test_forward_exploration_budget_closed_sql():
    from bitget.infra.bounded_reads import forward_exploration_budget_closed_sql

    sql, params = forward_exploration_budget_closed_sql(since_date="2026-07-04", limit=800)
    assert "SELECT *" not in sql
    assert "sig_type, final_ret, sim_kelly_invest" in sql
    assert "LIMIT ?" in sql
    assert "INCUBATOR" in sql
    assert params == ("2026-07-04", 800)


def test_macro_daily_last_row_sql():
    from bitget.infra.bounded_reads import macro_daily_last_row_sql

    sql = macro_daily_last_row_sql()
    assert "SELECT *" not in sql
    assert "btc_dominance" in sql
    assert "LIMIT 1" in sql


def test_macro_daily_lookback_sql():
    from bitget.infra.bounded_reads import macro_daily_lookback_sql

    sql, params = macro_daily_lookback_sql(max_rows=120)
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    assert params == (120,)


def test_shadow_blocked_history_sql():
    from bitget.infra.bounded_reads import shadow_blocked_history_sql

    sql, params = shadow_blocked_history_sql(limit=300)
    assert "SELECT *" not in sql
    assert "blocked_at" in sql
    assert "LIMIT ?" in sql
    assert params == (300,)


def test_forward_underdog_miner_closed_sql():
    from bitget.infra.bounded_reads import forward_underdog_miner_closed_sql

    sql, params = forward_underdog_miner_closed_sql(limit=500)
    assert "SELECT *" not in sql
    assert "total_score <= 60" in sql
    assert "LIMIT ?" in sql
    assert params == (500,)


def test_forward_blackhole_recent_closed_sql():
    from bitget.infra.bounded_reads import forward_blackhole_recent_closed_sql

    sql, params = forward_blackhole_recent_closed_sql(since_date="2026-06-27", limit=900)
    assert "SELECT *" not in sql
    assert "exit_date" in sql
    assert "LIMIT ?" in sql
    assert params == ("2026-06-27", 900)


def test_forward_grand_report_closed_sql():
    from bitget.infra.bounded_reads import forward_grand_report_closed_sql

    sql, params = forward_grand_report_closed_sql(
        market_type="spot",
        start="2026-07-01",
        end="2026-07-07",
        limit=4000,
    )
    assert "SELECT *" not in sql
    assert "sim_kelly_invest" in sql
    assert "INCUBATOR" in sql
    assert params == ("spot", "2026-07-01", "2026-07-07 23:59:59", 4000)


def test_forward_identity_trades_sql_preserves_open_and_bounds_closed():
    from bitget.infra.bounded_reads import (
        FORWARD_IDENTITY_COLUMNS,
        forward_identity_trades_sql,
    )

    sql, params = forward_identity_trades_sql(
        market_type="spot",
        rolling_cutoff="2026-04-01",
        session_anchor="2026-07-11",
        closed_limit=500,
    )
    assert "SELECT *" not in sql
    assert "OPEN" in sql
    assert "CLOSED" in sql
    assert "LIMIT ?" in sql
    for col in FORWARD_IDENTITY_COLUMNS.split(","):
        assert col.strip() in sql
    assert params == ("2026-04-01", "2026-07-11", "2026-04-01", "2026-07-11", 500)


def test_forward_toxic_graveyard_closed_sql_bounded():
    from bitget.infra.bounded_reads import (
        TOXIC_GRAVEYARD_COLUMNS,
        forward_toxic_graveyard_closed_sql,
    )
    from bitget.infra.memory_policy import TOXIC_GRAVEYARD_CLOSED_LIMIT

    sql, params = forward_toxic_graveyard_closed_sql(limit=800)
    assert "SELECT *" not in sql
    assert "CLOSED" in sql
    assert "LIMIT ?" in sql
    for col in TOXIC_GRAVEYARD_COLUMNS.split(","):
        assert col.strip() in sql
    assert params == (800,)

    sql_def, params_def = forward_toxic_graveyard_closed_sql()
    assert params_def == (TOXIC_GRAVEYARD_CLOSED_LIMIT,)


def test_forward_data_miner_mfe_winners_sql_bounded():
    from bitget.infra.bounded_reads import (
        DATA_MINER_MFE_WINNER_COLUMNS,
        forward_data_miner_mfe_winners_sql,
    )
    from bitget.infra.memory_policy import DATA_MINER_MFE_WINNERS_LIMIT

    sql, params = forward_data_miner_mfe_winners_sql(timeframe="1d", mfe_min=8.0, limit=120)
    assert "SELECT *" not in sql
    assert "LIMIT ?" in sql
    for col in DATA_MINER_MFE_WINNER_COLUMNS.split(","):
        assert col.strip() in sql
    assert params == ("1D", 8.0, 120)

    _, params_def = forward_data_miner_mfe_winners_sql(timeframe="4H", mfe_min=5.0)
    assert params_def == ("4H", 5.0, DATA_MINER_MFE_WINNERS_LIMIT)


def test_forward_data_miner_mfe_training_sql_utc_since():
    from bitget.infra.bounded_reads import (
        DATA_MINER_MFE_TRAINING_COLUMNS,
        forward_data_miner_mfe_training_sql,
    )
    from bitget.infra.memory_policy import DATA_MINER_MFE_TRAINING_LIMIT

    sql, params = forward_data_miner_mfe_training_sql(
        timeframe="1H",
        since_date="2026-06-11",
        limit=80,
    )
    assert "SELECT *" not in sql
    assert "DATE('now'" not in sql
    assert "IFNULL(entry_date" in sql
    for col in DATA_MINER_MFE_TRAINING_COLUMNS.split(","):
        assert col.strip() in sql
    assert params == ("1H", "2026-06-11", 80)

    _, params_def = forward_data_miner_mfe_training_sql(
        timeframe="2H",
        since_date="2026-07-01T12:00:00+00:00",
    )
    assert params_def == ("2H", "2026-07-01", DATA_MINER_MFE_TRAINING_LIMIT)


def test_forward_cluster_mining_symbols_sql_bounded():
    from bitget.infra.bounded_reads import forward_cluster_mining_symbols_sql
    from bitget.infra.memory_policy import SUPERNOVA_CLUSTER_FORWARD_SYMBOL_LIMIT

    sql, params = forward_cluster_mining_symbols_sql(since_date="2026-06-11", limit=120)
    assert "SELECT *" not in sql
    assert "GROUP BY market_type, symbol, timeframe" in sql
    assert "LIMIT ?" in sql
    assert "CLOSED" in sql
    assert params == ("2026-06-11", 120)

    _, params_def = forward_cluster_mining_symbols_sql(since_date="2026-07-01")
    assert params_def == ("2026-07-01", SUPERNOVA_CLUSTER_FORWARD_SYMBOL_LIMIT)


def test_sqlite_bitget_ohlcv_tables_sql_bounded():
    from bitget.infra.bounded_reads import sqlite_bitget_ohlcv_tables_sql
    from bitget.infra.memory_policy import SUPERNOVA_CLUSTER_MAX_TABLES

    sql, params = sqlite_bitget_ohlcv_tables_sql(limit=100)
    assert "SELECT *" not in sql
    assert "BITGET_%" in sql
    assert "__tmp" in sql
    assert params == (100,)

    _, params_def = sqlite_bitget_ohlcv_tables_sql()
    assert params_def == (SUPERNOVA_CLUSTER_MAX_TABLES,)


def test_forward_identity_trades_sql_spot_filter():
    from bitget.infra.bounded_reads import forward_identity_trades_sql

    sql, params = forward_identity_trades_sql(
        market_type="futures",
        rolling_cutoff="2026-07-01",
        session_anchor="2026-07-11",
    )
    assert "futures" in sql
    assert params[-1] == 8000  # default FORWARD_IDENTITY_CLOSED_LIMIT


def test_gap_healer_throttle_and_reuse_buffers():
    from bitget.data import gap_healer as gh

    gh._heal_gate._last_mono = 0.0
    gh._stale_symbols_buf.clear()
    health = gh.assess_buffer_health(symbols=["BTCUSDT"], max_age_sec=0.01)
    assert isinstance(health["stale_symbols"], list)
    out = gh.heal_if_stale(max_age_sec=0.01, force=False)
    assert out["reason"] in (
        "throttled",
        "mtf_update_ok:global_stale",
        "mtf_update_ok:symbol_stale",
        "mtf_update_failed:global_stale",
        "mtf_update_failed:symbol_stale",
        "ok",
    )


def test_pri_funnel_friction_week_sql_defaults():
    from bitget.infra.bounded_reads import pri_funnel_week_sql, pri_friction_week_sql
    from bitget.infra.memory_policy import PRI_FUNNEL_WEEK_LIMIT, PRI_FRICTION_WEEK_LIMIT

    _, funnel_params = pri_funnel_week_sql(
        market="SPOT", week_start="2026-07-01", week_end="2026-07-07"
    )
    assert funnel_params[-1] == PRI_FUNNEL_WEEK_LIMIT

    _, friction_params = pri_friction_week_sql(
        market="SPOT", week_start="2026-07-01", week_end="2026-07-07"
    )
    assert friction_params[-1] == PRI_FRICTION_WEEK_LIMIT


def test_genesis_bounded_sql_defaults():
    from bitget.infra.bounded_reads import (
        genesis_arm_snapshot_sql,
        genesis_closed_trades_sql,
        genesis_pending_champions_sql,
        genesis_unresolved_predictions_sql,
    )
    from bitget.infra.memory_policy import (
        GENESIS_ARM_SNAPSHOT_LIMIT,
        GENESIS_CLOSED_TRADES_LIMIT,
        GENESIS_PENDING_BACKFILL_LIMIT,
        GENESIS_UNRESOLVED_PREDICTION_LIMIT,
    )

    _, closed_params = genesis_closed_trades_sql(market_type="spot")
    assert closed_params == ("spot", GENESIS_CLOSED_TRADES_LIMIT)

    _, arm_params = genesis_arm_snapshot_sql(market="spot", label="X")
    assert arm_params == ("SPOT", "X", "X", GENESIS_ARM_SNAPSHOT_LIMIT)

    _, pend_params = genesis_pending_champions_sql(market="spot", cutoff="2026-07-01")
    assert pend_params == ("spot", "2026-07-01", GENESIS_PENDING_BACKFILL_LIMIT)

    _, pred_params = genesis_unresolved_predictions_sql(market="spot", cutoff="2026-07-01")
    assert pred_params == ("spot", "2026-07-01", GENESIS_UNRESOLVED_PREDICTION_LIMIT)


def test_elastic_and_ratchet_bounded_sql_defaults():
    from bitget.infra.bounded_reads import (
        elastic_vol_closed_rets_sql,
        exit_ratchet_runner_trades_sql,
        forward_pri_open_metrics_sql,
    )
    from bitget.infra.memory_policy import (
        ELASTIC_VOL_CLOSED_LIMIT,
        EXIT_RATCHET_RUNNER_LIMIT,
        FORWARD_OPEN_MAX_SAFETY,
    )

    _, open_params = forward_pri_open_metrics_sql(market_type="spot")
    assert open_params == ("spot", FORWARD_OPEN_MAX_SAFETY)

    _, closed_params = elastic_vol_closed_rets_sql(
        market_type="spot",
        since_date="2026-07-01",
    )
    assert closed_params == ("spot", "2026-07-01", ELASTIC_VOL_CLOSED_LIMIT)

    _, runner_params = exit_ratchet_runner_trades_sql(cutoff="2026-07-01")
    assert runner_params == ("2026-07-01", EXIT_RATCHET_RUNNER_LIMIT)


def test_open_and_ohlcv_table_scan_sql_bounded():
    from bitget.infra.bounded_reads import (
        forward_open_float_pnl_sql,
        forward_open_recon_futures_sql,
        sqlite_bitget_ohlcv_1d_tables_sql,
        sqlite_bitget_ohlcv_tf_tables_sql,
        sqlite_bitget_scan_tables_sql,
    )
    from bitget.infra.memory_policy import (
        FORWARD_OPEN_MAX_SAFETY,
        SUPERNOVA_CLUSTER_MAX_TABLES,
        TIME_MACHINE_MAX_TABLES,
    )

    _, open_pnl_params = forward_open_float_pnl_sql()
    assert open_pnl_params == (FORWARD_OPEN_MAX_SAFETY,)

    _, recon_params = forward_open_recon_futures_sql(limit=200)
    assert recon_params == (200,)

    _, d1_params = sqlite_bitget_ohlcv_1d_tables_sql()
    assert d1_params == (TIME_MACHINE_MAX_TABLES,)

    _, tf_params = sqlite_bitget_ohlcv_tf_tables_sql(timeframe="4H")
    assert tf_params == ("%_4H", SUPERNOVA_CLUSTER_MAX_TABLES)

    _, scan_params = sqlite_bitget_scan_tables_sql(market_type="spot", timeframe="1D")
    assert scan_params == ("BITGET_SPOT_%", "%_1D", SUPERNOVA_CLUSTER_MAX_TABLES)


def test_weekly_flow_sql_defaults():
    from bitget.infra.bounded_reads import (
        forward_weekly_flow_closed_sql,
        forward_weekly_flow_tags_sql,
    )
    from bitget.infra.memory_policy import FORWARD_WEEKLY_TF_ROTATION_LIMIT

    _, closed_params = forward_weekly_flow_closed_sql(
        market_type="spot",
        since_date="2026-07-01",
    )
    assert closed_params == ("spot", "2026-07-01", FORWARD_WEEKLY_TF_ROTATION_LIMIT)

    _, tag_params = forward_weekly_flow_tags_sql(since_date="2026-07-01")
    assert tag_params == ("2026-07-01", FORWARD_WEEKLY_TF_ROTATION_LIMIT)


def test_zombie_cleanup_sql_defaults():
    from bitget.infra.bounded_reads import (
        forward_zombie_fact_close_ids_sql,
        forward_zombie_zero_invest_ids_sql,
    )
    from bitget.infra.memory_policy import FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT

    _, z_params = forward_zombie_zero_invest_ids_sql()
    assert z_params == (FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT,)

    _, f_params = forward_zombie_fact_close_ids_sql()
    assert f_params == (FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT,)
