"""Factory daemon critical imports — prevents missing facade regressions."""


def test_auto_forward_tester_facade_exports():
    import auto_forward_tester as aft

    assert aft.DB_PATH
    assert callable(aft.init_forward_db)
    assert callable(aft.track_daily_positions)
    assert callable(aft.send_telegram_msg)


def test_system_auto_pilot_daemon_import_chain():
    from inverse_etf_sniper import run_inverse_etf_sniper_cycle

    assert callable(run_inverse_etf_sniper_cycle)
