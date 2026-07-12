"""Exploration budget registry market-key SSOT."""
from __future__ import annotations

from unittest import mock


def test_role_map_includes_futures_and_bg_futures_aliases():
    from bitget.governance import exploration_budget as eb

    rows = [
        {"market": "FUTURES", "group_key": "CORE_FUT", "state": "LIVE"},
        {"market": "BG_FUTURES", "group_key": "CORE_BG", "state": "CANDIDATE"},
        {"market": "SPOT", "group_key": "CORE_SP", "state": "OBSERVING"},
        {"market": "KR", "group_key": "STOCK_ONLY", "state": "LIVE"},
    ]
    eb._ROLE_CACHE["ts"] = 0.0
    eb._ROLE_CACHE["map"] = {}

    with mock.patch(
        "strategy_registry_store.load_registry_rows", return_value=rows
    ), mock.patch(
        "bitget.infra.data_paths.market_data_db_path", return_value=":memory:"
    ):
        role_map = eb._load_registry_role_map(force=True)

    assert role_map.get("CORE_FUT") == "CHAMPION"
    assert role_map.get("CORE_BG") == "EXPLORATION"
    assert role_map.get("CORE_SP") == "EXPLORATION"
    assert "STOCK_ONLY" not in role_map


def test_architecture_exploration_market_ssot():
    from bitget.validation.architecture_checks import check_exploration_budget_market_ssot

    r = check_exploration_budget_market_ssot()
    assert r.get("ok"), r
