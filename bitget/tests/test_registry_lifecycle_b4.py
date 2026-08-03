"""B-4 — registry lifecycle counts → MAB explore budget (config log only)."""
from __future__ import annotations

import os
import sqlite3
import tempfile
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import pytest


def _seed_registry(db_path: str, rows: list[tuple]) -> None:
    """Rows: (strategy_id, market, group_key, state)."""
    from strategy_registry_store import ensure_strategy_registry_schema

    ensure_strategy_registry_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        for sid, market, gk, state in rows:
            conn.execute(
                """
                INSERT INTO strategy_registry (
                    strategy_id, market, group_key, state, display_name,
                    capital_mult, source, updated_at
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (sid, market, gk, state, gk, 1.0, "test", "t0"),
            )
        conn.commit()
    finally:
        conn.close()


class TestCountLifecycleStates:
    def test_dedupes_bg_and_spot_same_group(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        db = tmp_path / "reg.sqlite"
        db.write_bytes(b"")
        _seed_registry(
            str(db),
            [
                ("s1", "BG", "ALPHA", "COOLED"),
                ("s2", "SPOT", "ALPHA", "COOLED"),
            ],
        )
        from bitget.evolution.registry_lifecycle_bg import count_lifecycle_states_bg

        counts = count_lifecycle_states_bg(str(db))
        assert counts["COOLED"] == 1
        assert counts["RETIRED"] == 0

    def test_separate_markets_count_separately(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        db = tmp_path / "reg.sqlite"
        db.write_bytes(b"")
        _seed_registry(
            str(db),
            [
                ("s1", "SPOT", "ALPHA", "LIVE"),
                ("s2", "FUT", "ALPHA", "COOLED"),
            ],
        )
        from bitget.evolution.registry_lifecycle_bg import count_lifecycle_states_bg

        counts = count_lifecycle_states_bg(str(db))
        assert counts["LIVE"] == 1
        assert counts["COOLED"] == 1


class TestComputeExploreBudget:
    def test_zero_when_no_churn(self):
        from bitget.evolution.registry_lifecycle_bg import compute_explore_budget_bg

        assert compute_explore_budget_bg(retired=0, cooled=0, live=5) == 0.0

    def test_positive_with_retired(self):
        from bitget.evolution.registry_lifecycle_bg import compute_explore_budget_bg

        ratio = compute_explore_budget_bg(retired=2, cooled=1, live=3)
        assert ratio > 0.0
        assert ratio <= 0.50


class TestPersistHook:
    def test_refresh_writes_config_kv(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LIFECYCLE_EXPLORE_BUDGET_ENABLED", "1")
        db = tmp_path / "reg.sqlite"
        db.write_bytes(b"")
        _seed_registry(
            str(db),
            [
                ("s1", "SPOT", "G1", "RETIRED"),
                ("s2", "SPOT", "G2", "COOLED"),
            ],
        )

        cfg: dict = {}

        def _set(k, v):
            cfg[k] = v

        def _get(k, default=None):
            return cfg.get(k, default)

        from bitget.evolution.registry_lifecycle_bg import (
            MAB_EXPLORE_BUDGET_KV,
            refresh_lifecycle_explore_budget_bg,
        )

        with mock.patch(
            "bitget.infra.config_manager.set_config_value", side_effect=_set
        ), mock.patch("bitget.infra.config_manager.get_config_value", side_effect=_get):
            out = refresh_lifecycle_explore_budget_bg(str(db))

        assert out["written"] is True
        assert MAB_EXPLORE_BUDGET_KV in cfg
        assert float(cfg[MAB_EXPLORE_BUDGET_KV]) > 0.0

    def test_kill_switch_skips_write(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LIFECYCLE_EXPLORE_BUDGET_ENABLED", "0")
        db = tmp_path / "reg.sqlite"
        db.write_bytes(b"")
        _seed_registry(str(db), [("s1", "SPOT", "G1", "RETIRED")])

        from bitget.evolution.registry_lifecycle_bg import refresh_lifecycle_explore_budget_bg

        with mock.patch("bitget.infra.config_manager.set_config_value") as set_mock:
            out = refresh_lifecycle_explore_budget_bg(str(db))
        assert out["enabled"] is False
        set_mock.assert_not_called()


def _base_try_add_cfg(**overrides) -> dict:
    cfg = {
        "GLOBAL_CIRCUIT_BREAKER": "OFF",
        "BITGET_MAX_OPEN_POSITIONS": 99,
        "PORTFOLIO_MDD_BREAKER_ENABLED": True,
        "PORTFOLIO_NAV_PEAK": 1000.0,
        "PORTFOLIO_MDD_REDUCE_PCT": 0.15,
        "PORTFOLIO_MDD_BLOCK_PCT": 0.20,
        "PORTFOLIO_MDD_HALT_PCT": 0.30,
        "PORTFOLIO_MDD_REDUCE_SIZE_MULT": 0.5,
        "ACCOUNT_SIZE_USDT": 100_000.0,
        "TREASURY_SPOT_USDT": 500.0,
        "TREASURY_FUTURES_USDT": 500.0,
        "DYNAMIC_KELLY_RISK": 0.01,
        "FIXED_RISK_PCT": 0.02,
        "ATR_SL_MULT": 2.0,
        "ANTI_PATTERNS": [],
        "WEIGHT_S1": 1.0,
        "WEIGHT_S4": 1.0,
        "FUTURES_LEVERAGE": 3.0,
        "MAX_LEVERAGE": 5,
    }
    cfg.update(overrides)
    return cfg


def _run_try_add_isolated(cfg: dict) -> float:
    from bitget.forward import ledger
    from bitget.forward.shared import _init_forward_db_schema

    hist = pd.DataFrame(
        {
            "Open": np.linspace(49_950, 50_050, 100),
            "High": np.linspace(50_030, 50_130, 100),
            "Low": np.linspace(49_870, 49_970, 100),
            "Close": np.linspace(49_950, 50_050, 100),
            "Volume": [1_000_000.0] * 100,
        }
    )

    with tempfile.TemporaryDirectory() as td:
        fwd_db = os.path.join(td, "forward.sqlite")
        conn = sqlite3.connect(fwd_db)
        _init_forward_db_schema(conn)
        conn.commit()
        conn.close()

        elastic_mock = mock.MagicMock()
        elastic_mock.apply_pair.side_effect = lambda cos, ml: SimpleNamespace(
            cos_cutoff=cos, ml_cutoff=ml
        )

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(ledger, "DB_PATH", fwd_db))
            stack.enter_context(mock.patch.object(ledger, "init_forward_db"))
            stack.enter_context(mock.patch.object(ledger, "load_system_config", return_value=cfg))
            stack.enter_context(
                mock.patch(
                    "bitget.trading.execution_safety._persist_portfolio_mdd_state",
                    return_value=True,
                )
            )
            stack.enter_context(mock.patch.object(ledger, "_load_hist", return_value=hist))
            stack.enter_context(mock.patch.object(ledger, "_calc_market_breadth", return_value=1.0))
            stack.enter_context(
                mock.patch(
                    "bitget.trading.slippage_guard.check_pre_scan_liquidity",
                    return_value=(True, ""),
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.trading.execution_safety.gross_entry_blocked",
                    return_value=False,
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.trading.tail_risk_gate.tail_risk_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.trading.doomsday_gate.doomsday_long_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.trading.concentration_gate.concentration_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.trading.price_sanity_gate.price_sanity_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(mock.patch.object(ledger, "fetch_funding_snapshot", return_value=None))
            stack.enter_context(
                mock.patch(
                    "bitget.governance.meta_consumer.load_meta_state_resolved",
                    return_value={},
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.governance.meta_consumer.effective_max_position_pct",
                    return_value=1.0,
                )
            )
            stack.enter_context(
                mock.patch.object(ledger, "get_exploration_role_scaler", return_value=(1.0, "LIVE"))
            )
            stack.enter_context(
                mock.patch.object(
                    ledger,
                    "_apply_thompson_kelly_multiplier",
                    side_effect=lambda c, tf, sig, k: k,
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.trading.regime_capital_relay.apply_regime_capital_to_kelly",
                    side_effect=lambda k, **kw: (k, {}),
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.evolution.elastic_threshold_bg.BitgetElasticThreshold",
                    return_value=elastic_mock,
                )
            )
            stack.enter_context(
                mock.patch.object(ledger, "compute_evolved_alpha_bonus_score", return_value=0.0)
            )
            stack.enter_context(mock.patch.object(ledger, "save_system_config"))
            stack.enter_context(
                mock.patch(
                    "bitget.evolution.regime_analog_bg.frontrun_gate",
                    return_value=(True, {}),
                )
            )
            ok, msg = ledger.try_add_virtual_position(
                "spot",
                "BTCUSDT",
                "4H",
                "[CORE_B]",
                85,
                50_000.0,
                {"ml_box_pass": True},
            )
        assert ok, msg
        read_conn = sqlite3.connect(fwd_db)
        try:
            row = read_conn.execute(
                "SELECT sim_kelly_invest FROM bitget_forward_trades WHERE status='OPEN'"
            ).fetchone()
        finally:
            read_conn.close()
        return float(row[0])


class TestProductionIsolation:
    def test_b4_on_off_identical_sim_kelly_and_incubator(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LIFECYCLE_EXPLORE_BUDGET_ENABLED", "1")
        db = tmp_path / "reg.sqlite"
        db.write_bytes(b"")
        _seed_registry(
            str(db),
            [("s1", "SPOT", "CORE_B", "RETIRED"), ("s2", "SPOT", "G2", "COOLED")],
        )

        cfg_store: dict = {}

        def _set(k, v):
            cfg_store[k] = v

        def _get(k, default=None):
            return cfg_store.get(k, default)

        from bitget import auto_pilot
        from bitget.evolution.registry_lifecycle_bg import refresh_lifecycle_explore_budget_bg

        incubator_cfg = {"INCUBATOR_TEMPLATES": {"alpha": {"dna": 1}}}
        df = pd.DataFrame(
            {
                "sig_type": ["INCUBATOR_alpha"] * 6,
                "final_ret": [0.1, 0.2, 0.1, 0.2, 0.1, 0.2],
            }
        )

        class _Conn:
            def close(self):
                pass

        with mock.patch(
            "bitget.infra.config_manager.set_config_value", side_effect=_set
        ), mock.patch("bitget.infra.config_manager.get_config_value", side_effect=_get):
            refresh_lifecycle_explore_budget_bg(str(db))

        with mock.patch.object(auto_pilot, "get_connection", return_value=_Conn()), mock.patch(
            "pandas.read_sql", return_value=df
        ):
            before_inc, _ = auto_pilot._judge_incubator_templates(dict(incubator_cfg))

        invest_on = _run_try_add_isolated(
            _base_try_add_cfg(MAB_EXPLORE_BUDGET_CURRENT=cfg_store.get("MAB_EXPLORE_BUDGET_CURRENT", 0.0))
        )

        monkeypatch.setenv("LIFECYCLE_EXPLORE_BUDGET_ENABLED", "0")
        invest_off = _run_try_add_isolated(_base_try_add_cfg())

        with mock.patch.object(auto_pilot, "get_connection", return_value=_Conn()), mock.patch(
            "pandas.read_sql", return_value=df
        ):
            after_inc, _ = auto_pilot._judge_incubator_templates(dict(incubator_cfg))

        assert before_inc.get("INCUBATOR_TEMPLATES") == after_inc.get("INCUBATOR_TEMPLATES")
        assert invest_on > 0.0
        assert invest_on == invest_off

    def test_shadow_tables_untouched(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LIFECYCLE_EXPLORE_BUDGET_ENABLED", "1")
        db = tmp_path / "market.sqlite"
        db.write_bytes(b"")
        conn = sqlite3.connect(str(db))
        conn.executescript(
            """
            CREATE TABLE bitget_deathmatch_alloc_shadow (id INTEGER PRIMARY KEY);
            CREATE TABLE bitget_walk_forward_shadow (id INTEGER PRIMARY KEY);
            INSERT INTO bitget_deathmatch_alloc_shadow DEFAULT VALUES;
            INSERT INTO bitget_walk_forward_shadow DEFAULT VALUES;
            """
        )
        from strategy_registry_store import ensure_strategy_registry_schema

        ensure_strategy_registry_schema(str(db))
        conn.execute(
            """
            INSERT INTO strategy_registry (
                strategy_id, market, group_key, state, display_name,
                capital_mult, source, updated_at
            ) VALUES ('s1','SPOT','G','RETIRED','G',1,'t','t')
            """
        )
        conn.commit()
        conn.close()

        from bitget.evolution.registry_lifecycle_bg import refresh_lifecycle_explore_budget_bg

        with mock.patch("bitget.infra.config_manager.set_config_value"), mock.patch(
            "bitget.infra.config_manager.get_config_value", return_value=None
        ):
            refresh_lifecycle_explore_budget_bg(str(db))

        conn = sqlite3.connect(str(db))
        try:
            dm = conn.execute("SELECT COUNT(*) FROM bitget_deathmatch_alloc_shadow").fetchone()[0]
            wf = conn.execute("SELECT COUNT(*) FROM bitget_walk_forward_shadow").fetchone()[0]
        finally:
            conn.close()
        assert dm == 1
        assert wf == 1
