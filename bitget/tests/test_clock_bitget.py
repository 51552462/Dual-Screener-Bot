"""Bitget UTC clock SSOT — unit tests."""
from __future__ import annotations

from datetime import timedelta

from bitget.infra.clock import (
    utc_date_days_ago_str,
    utc_date_key,
    utc_date_str,
    utc_datetime_str,
    utc_datetime_str_tz,
    utc_hm_key,
    utc_hours_ago_iso,
    utc_now,
    utc_now_iso,
    parse_utc_iso,
)


def test_utc_now_is_timezone_aware():
    now = utc_now()
    assert now.tzinfo is not None
    assert len(utc_date_str()) == 10
    assert len(utc_datetime_str()) >= 19
    assert utc_datetime_str_tz().endswith("UTC")


def test_utc_date_days_ago_str():
    days = 7
    expected = (utc_now() - timedelta(days=days)).strftime("%Y-%m-%d")
    assert utc_date_days_ago_str(days) == expected
    assert len(utc_date_days_ago_str(1)) == 10
    anchor = utc_now()
    assert utc_date_days_ago_str(3, anchor=anchor) == (
        anchor - timedelta(days=3)
    ).strftime("%Y-%m-%d")


def test_utc_hm_key():
    now = utc_now()
    assert utc_hm_key(anchor=now) == now.strftime("%Y-%m-%d %H:%M")
    assert len(utc_hm_key()) >= 16


def test_utc_date_key():
    now = utc_now()
    assert utc_date_key() == utc_date_str()
    assert utc_date_key(anchor=now) == now.strftime("%Y-%m-%d")


def test_utc_now_iso_is_timezone_aware():
    iso = utc_now_iso()
    assert "+" in iso or iso.endswith("Z") or "+00:00" in iso
    parsed = utc_now()
    assert parsed.tzinfo is not None


def test_utc_hours_ago_iso():
    from datetime import timedelta

    now = utc_now()
    since = utc_hours_ago_iso(2.0, anchor=now)
    expected = (now - timedelta(hours=2.0)).isoformat()
    assert since == expected


def test_parse_utc_iso():
    parsed = parse_utc_iso("2026-07-11T04:00:00+00:00")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parse_utc_iso("2026-07-11T04:00:00Z") == parsed
    assert parse_utc_iso("") is None


def test_reconciliation_module_uses_clock_ssot():
    """OMS reconciliation must not call deprecated naive UTC helpers."""
    import inspect

    from bitget.trading import reconciliation as recon

    src = inspect.getsource(recon)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "utc_date_str" in src
    assert "utc_datetime_str_tz" in src
    assert "forward_open_recon_futures_sql" in src


def test_data_miner_module_uses_clock_ssot():
    import inspect

    from bitget import data_miner

    src = inspect.getsource(data_miner)
    assert "datetime.utcnow()" not in src
    assert "DATE('now'" not in src
    assert "utc_datetime_str" in src
    assert "utc_date_days_ago_str" in src
    assert "forward_data_miner_mfe_winners_sql" in src
    assert "_resolve_cluster_mining_tables" in src
    assert "SUPERNOVA_CLUSTER_MAX_TABLES" in src


def test_supernova_hunter_module_uses_clock_ssot():
    """Alpha evolution + DNA template mining must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import supernova_hunter

    src = inspect.getsource(supernova_hunter)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src
    assert "sqlite_bitget_scan_tables_sql" in src
    assert "SUPERNOVA_SCAN_MAX_WORKERS" in src


def test_elastic_threshold_module_uses_clock_ssot():
    """Starvation/vol proxy lookback must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import elastic_threshold_bg as et

    src = inspect.getsource(et)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import" not in src
    assert "utc_date_days_ago_str" in src
    assert "elastic_vol_closed_rets_sql" in src
    assert "forward_pri_open_metrics_sql" in src


def test_exit_ratchet_rl_bg_module_uses_clock_ssot():
    """Weekly κ RL lookback must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import exit_ratchet_rl_bg as rl

    src = inspect.getsource(rl)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "timedelta" not in src
    assert "utc_date_days_ago_str" in src
    assert "utc_now" in src
    assert "exit_ratchet_runner_trades_sql" in src


def test_weekly_proprietary_regime_module_uses_clock_ssot():
    """Shadow PRI week window must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import weekly_proprietary_regime_bg as pri

    src = inspect.getsource(pri)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import" not in src
    assert "utc_now" in src
    assert "utc_date_days_ago_str" in src
    assert "pri_funnel_week_sql" in src
    assert "pri_friction_week_sql" in src


def test_master_scanner_module_uses_clock_ssot():
    """MTF scanner dedup + funnel snapshot must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import master_scanner

    src = inspect.getsource(master_scanner)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "datetime_now_utc_date" not in src
    assert "utc_date_str" in src
    assert "utc_hm_key" in src


def test_mtf_data_updater_module_uses_clock_ssot():
    """OHLCV batch start stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import mtf_data_updater

    src = inspect.getsource(mtf_data_updater)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str_tz" in src


def test_executor_module_uses_clock_ssot():
    """Live order executor must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import executor

    src = inspect.getsource(executor)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str_tz" in src


def test_daemon_loop_utc_tick_uses_clock_ssot():
    """24/7 daemon UtcTick must not call raw datetime.now(timezone.utc)."""
    import inspect

    from bitget.infra import daemon_loop

    src = inspect.getsource(daemon_loop)
    assert "datetime.now(timezone.utc)" not in src
    assert "utc_now" in src
    assert "utc_date_key" in src
    assert "utc_hm_key" in src


def test_ops_logger_module_uses_clock_ssot():
    """Ops events append/query must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.infra import ops_logger

    src = inspect.getsource(ops_logger)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import" not in src
    assert "utc_now_iso" in src
    assert "utc_hours_ago_iso" in src


def test_watchdog_module_uses_clock_ssot():
    """Heartbeat/circuit breaker timestamps must use clock SSOT."""
    import inspect

    from bitget import watchdog

    src = inspect.getsource(watchdog)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import datetime, timezone" not in src
    assert "parse_utc_iso" in src
    assert "utc_now_iso" in src
    assert "utc_now()" in src


def test_forward_trade_identity_module_uses_clock_ssot():
    """Identity diagnostic report stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.forward import forward_trade_identity as fti

    src = inspect.getsource(fti)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str_tz" in src


def test_bitget_report_context_module_uses_clock_ssot():
    """Daily report timekeeper must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.reports import bitget_report_context as brc

    src = inspect.getsource(brc)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(timezone.utc)" not in src
    assert "from datetime import datetime" not in src
    assert "utc_now" in src
    assert "utc_date_str" in src
    assert "utc_date_days_ago_str" in src


def test_macro_treasury_bg_module_uses_clock_ssot():
    """Macro/treasury report header date must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.reports import macro_treasury_bg as mtb

    src = inspect.getsource(mtb)
    assert "datetime.now(timezone.utc)" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_date_key" in src


def test_report_state_binder_bg_module_uses_clock_ssot():
    """Lifecycle report block anchor must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.reports import report_state_binder_bg as rsb

    src = inspect.getsource(rsb)
    assert "datetime.now(timezone.utc)" not in src
    assert "datetime.utcnow()" not in src
    assert "utc_now" in src
    assert "parse_utc_iso" in src


def test_regime_analog_bg_module_uses_clock_ssot():
    """Regime analog scoring stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import regime_analog_bg as rab

    src = inspect.getsource(rab)
    assert "datetime.utcnow()" not in src
    assert "datetime.now()" not in src
    assert "utc_now" in src


def test_coin_regime_vector_module_uses_clock_ssot():
    """Vector history append stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import coin_regime_vector as crv

    src = inspect.getsource(crv)
    assert "datetime.now()" not in src
    assert "datetime.utcnow()" not in src
    assert "utc_now" in src


def test_weekly_regime_bg_module_uses_clock_ssot():
    """Weekly regime archive timestamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import weekly_regime_bg as wrb

    src = inspect.getsource(wrb)
    assert "datetime.now(timezone.utc)" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_now_iso" in src


def test_forward_reports_module_uses_clock_ssot():
    """Daily report lifecycle section must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.forward import reports as fr

    src = inspect.getsource(fr)
    assert "datetime.now(" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_date_key" in src
    assert "utc_datetime_str" in src


def test_deathmatch_report_section_module_uses_clock_ssot():
    """DM-A friction event date must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.forward import deathmatch_report_section as dmrs

    src = inspect.getsource(dmrs)
    assert "datetime.now(" not in src
    assert "datetime.utcnow()" not in src
    assert "utc_date_key" in src


def test_champion_genesis_bg_module_uses_clock_ssot():
    """Champion genesis wall-clock stamps must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import champion_genesis_bg as cgb

    src = inspect.getsource(cgb)
    assert "datetime.now()" not in src
    assert "datetime.utcnow()" not in src
    assert "utc_datetime_str" in src
    assert "utc_date_key" in src
    assert "utc_date_days_ago_str" in src
    assert "genesis_closed_trades_sql" in src
    assert "genesis_pending_champions_sql" in src


def test_forward_shared_module_uses_clock_ssot():
    """Forward ledger zombie cleanup exit_date must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.forward import shared as fs

    src = inspect.getsource(fs)
    assert "datetime.now()" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_date_key" in src
    assert "forward_zombie_zero_invest_ids_sql" in src


def test_system_auto_pilot_module_uses_clock_ssot():
    """Legacy auto_pilot compat layer must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import system_auto_pilot as sap

    src = inspect.getsource(sap)
    assert "datetime.utcnow()" not in src
    assert "datetime.now(" not in src
    assert "from datetime import" not in src
    assert "utc_now" in src
    assert "utc_date_days_ago_str" in src
    assert "forward_weekly_tf_rotation_sql" in src
    assert "forward_weekly_flow_closed_sql" in src


def test_task_orchestrator_module_uses_bounded_claim_sql():
    """Queue claim must use column projection SSOT, not SELECT *."""
    import inspect

    from bitget.infra import task_orchestrator as tq

    src = inspect.getsource(tq)
    assert "SELECT * FROM task_queue" not in src
    assert "task_queue_claim_next_sql" in src
    assert "utc_now_iso" in src
    assert "utc_now" in src


def test_meta_sync_module_uses_clock_ssot():
    """MetaGovernor age/staleness must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.governance import meta_sync as ms

    src = inspect.getsource(ms)
    assert "datetime.now(timezone.utc)" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "parse_utc_iso" in src
    assert "utc_now" in src
    assert "utc_now_iso" in src


def test_auto_pilot_module_uses_clock_ssot():
    """Live auto_pilot wall-clock windows must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import auto_pilot as ap

    src = inspect.getsource(ap)
    assert "datetime.now(" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_date_days_ago_str" in src
    assert "utc_now" in src
    assert "forward_weekly_flow_closed_sql" in src


def test_macro_hydrate_bg_module_uses_clock_ssot():
    """Macro lookback freshness cutoff must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import macro_hydrate_bg as mhb

    src = inspect.getsource(mhb)
    assert "datetime.utcnow()" not in src
    assert "datetime.now()" not in src
    assert "from datetime import" not in src
    assert "utc_date_key" in src


def test_alt_data_miner_module_uses_clock_ssot():
    """Alt-data live upsert date must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import alt_data_miner as adm

    src = inspect.getsource(adm)
    assert "datetime.utcnow()" not in src
    assert "utc_date_key" in src
    assert "macro_daily_last_row_sql" in src


def test_sentiment_miner_module_uses_clock_ssot():
    """Sentiment daily row date must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import sentiment_miner as sm

    src = inspect.getsource(sm)
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_date_key" in src


def test_live_nav_manager_module_uses_clock_ssot():
    """Treasury NAV state stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import live_nav_manager as lnm

    src = inspect.getsource(lnm)
    assert "datetime.now()" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src


def test_forensics_pioneer_module_uses_clock_ssot():
    """Shadow pioneer virtual trade log must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import forensics_pioneer as fp

    src = inspect.getsource(fp)
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src
    assert "sqlite_bitget_ohlcv_1d_tables_sql" in src


def test_shadow_tracking_module_uses_clock_ssot():
    """Blocked trade shadow log must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import shadow_tracking as st

    src = inspect.getsource(st)
    assert "datetime.now()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src


def test_shadow_performance_tracker_module_uses_clock_ssot():
    """Shadow defense evaluation stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import shadow_performance_tracker as spt

    src = inspect.getsource(spt)
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src
    assert "shadow_blocked_history_sql" in src


def test_doomsday_bot_module_uses_clock_ssot():
    """DEFCON radar stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import doomsday_bot as db

    src = inspect.getsource(db)
    assert "datetime.now()" not in src
    assert "utc_hm_key" in src


def test_doomsday_bridge_module_uses_clock_ssot():
    import inspect

    from bitget import doomsday_bridge as dbr

    src = inspect.getsource(dbr)
    assert "datetime.now(" not in src
    assert "utc_now_iso" in src


def test_canary_exporter_module_uses_clock_ssot():
    """Cross-market canary JSON stamp must use timezone-aware UTC SSOT."""
    import inspect

    from bitget import canary_exporter as ce

    src = inspect.getsource(ce)
    assert "datetime.now(" not in src
    assert "utc_now_iso" in src


def test_doomsday_dampener_bg_module_uses_clock_ssot():
    """Weekly gamma evolution must use timezone-aware UTC SSOT."""
    import inspect

    from bitget.evolution import doomsday_dampener_bg as dd

    src = inspect.getsource(dd)
    assert "datetime.now()" not in src
    assert "datetime.utcnow()" not in src
    assert "timedelta" not in src
    assert "utc_now" in src
    assert "utc_date_days_ago_str" in src


def test_synthetic_data_generator_module_uses_clock_ssot():
    import inspect

    from bitget import synthetic_data_generator as sdg

    src = inspect.getsource(sdg)
    assert "datetime.utcnow()" not in src
    assert "utc_datetime_str" in src


def test_dashboard_ops_panel_module_uses_clock_ssot():
    import inspect

    from bitget import dashboard_ops_panel as dop

    src = inspect.getsource(dop)
    assert "datetime.now(" not in src
    assert "utc_now" in src


def test_weekly_action_plan_snapshot_uses_clock_ssot():
    import inspect

    from bitget import weekly_action_plan as wap

    src = inspect.getsource(wap)
    assert "datetime.now(" not in src
    assert "utc_hm_key" in src


def test_validation_modules_use_clock_ssot():
    import inspect

    from bitget.validation import cutover, pnl_parity, signal_parity

    for mod in (signal_parity, pnl_parity, cutover):
        src = inspect.getsource(mod)
        assert "datetime.now(" not in src, mod.__name__


def test_toxic_graveyard_analyzer_module_uses_clock_ssot():
    import inspect

    from bitget import toxic_graveyard_analyzer as tga

    src = inspect.getsource(tga)
    assert "datetime.now()" not in src
    assert "utc_date_str" in src
    assert "forward_toxic_graveyard_closed_sql" in src


def test_bitget_pipelines_weekly_steps_use_clock_ssot():
    import inspect

    from bitget.pipelines import bitget_pipelines as bp

    src = inspect.getsource(bp)
    assert "datetime.now(timezone.utc)" not in src
    assert "utc_date_days_ago_str" in src


def test_institutional_db_backup_module_uses_clock_ssot():
    import inspect

    from bitget.scripts import institutional_db_backup as idb

    src = inspect.getsource(idb)
    assert "datetime.now(" not in src
    assert "utc_compact_key" in src


def test_ai_overseer_module_uses_kst_audit_and_bounded_reads():
    import inspect

    from bitget import ai_overseer as ao

    src = inspect.getsource(ao)
    assert "overseer_daily_closed_sql" in src
    assert "_kst_today_str" in src
    assert "datetime.now(timezone.utc)" not in src
    assert "pd.read_csv" not in src
