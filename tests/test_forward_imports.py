"""Import integrity — forward/deep_dive private helpers bound after split."""
from __future__ import annotations

import importlib

import pytest

_DEEP_DIVE_PRIVATE_SYMBOLS = (
    "_open_market_db_ro",
    "_normalize_trade_market",
    "_reporter_cleanup_zombie_forward_trades",
    "_reporter_valid_holding_mask",
    "_reporter_deploy_fleet_mask",
    "_daily_report_trades_for_market",
    "_strategy_colosseum_brief",
    "_shadow_performance_brief",
    "_tier80_sync_effective_and_report_line",
    "_parse_mkt_group_key",
    "_exit_date_on_calendar",
    "_format_exit_reason_display",
    "_safe_final_ret_pct",
    "_win_loss_flat_counts",
    "_spillover_fallback_enabled",
    "_format_forward_ledger_error_html",
)


def test_deep_dive_module_imports_without_error():
    mod = importlib.reload(importlib.import_module("forward.deep_dive"))
    assert mod is not None
    mod._verify_deep_dive_private_bindings()


@pytest.mark.parametrize("name", _DEEP_DIVE_PRIVATE_SYMBOLS)
def test_deep_dive_private_symbols_bound(name: str):
    mod = importlib.import_module("forward.deep_dive")
    assert hasattr(mod, name), f"missing {name} on forward.deep_dive"
    assert callable(getattr(mod, name))


def test_deep_dive_public_report_entrypoints_bound():
    mod = importlib.import_module("forward.deep_dive")
    for name in (
        "send_comprehensive_daily_report",
        "send_group_practitioner_reports",
        "run_deep_dive_analysis",
    ):
        assert hasattr(mod, name), name
        assert callable(getattr(mod, name))
