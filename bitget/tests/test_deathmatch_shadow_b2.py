"""B-2 — deathmatch allocation shadow (4w log-only, production Kelly isolation)."""
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

from evolution.deathmatch_battle_royale import RegistryArmRow


def _synthetic_hist_df(rows: int = 100, price: float = 50_000.0) -> pd.DataFrame:
    close = price + np.linspace(-50, 50, rows)
    return pd.DataFrame(
        {
            "Open": close,
            "High": close + 80,
            "Low": close - 80,
            "Close": close,
            "Volume": [1_000_000.0] * rows,
        }
    )


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


def _make_arms_for_overlay() -> list[RegistryArmRow]:
    return [
        RegistryArmRow(
            arm_id="a1",
            label="CORE_A",
            group_key="CORE_A",
            registry_state="LIVE",
            n_closed=10,
            n_valid=10,
            composite_score=2.0,
            rank=1,
            hurdle_passed=True,
            champion_eligible=True,
            below_floor=False,
        ),
        RegistryArmRow(
            arm_id="a2",
            label="CORE_B",
            group_key="CORE_B",
            registry_state="LIVE",
            n_closed=10,
            n_valid=10,
            composite_score=0.5,
            rank=2,
            hurdle_passed=True,
            champion_eligible=False,
            below_floor=True,
        ),
    ]


def _run_try_add_isolated(cfg: dict, *, shadow_db: str) -> float:
    from bitget.forward import ledger
    from bitget.forward.shared import _init_forward_db_schema

    hist = _synthetic_hist_df()
    facts = {"ml_box_pass": True}

    with tempfile.TemporaryDirectory() as td:
        db_path = os.path.join(td, "forward.sqlite")
        conn = sqlite3.connect(db_path)
        _init_forward_db_schema(conn)
        conn.commit()
        conn.close()

        elastic_mock = mock.MagicMock()
        elastic_mock.apply_pair.side_effect = lambda cos, ml: SimpleNamespace(
            cos_cutoff=cos, ml_cutoff=ml
        )

        with ExitStack() as stack:
            stack.enter_context(patch.object(ledger, "DB_PATH", db_path))
            stack.enter_context(patch.object(ledger, "init_forward_db"))
            stack.enter_context(patch.object(ledger, "load_system_config", return_value=cfg))
            stack.enter_context(
                patch(
                    "bitget.trading.execution_safety._persist_portfolio_mdd_state",
                    return_value=True,
                )
            )
            stack.enter_context(patch.object(ledger, "_load_hist", return_value=hist))
            stack.enter_context(patch.object(ledger, "_calc_market_breadth", return_value=1.0))
            stack.enter_context(
                patch(
                    "bitget.trading.slippage_guard.check_pre_scan_liquidity",
                    return_value=(True, ""),
                )
            )
            stack.enter_context(
                patch(
                    "bitget.trading.execution_safety.gross_entry_blocked",
                    return_value=False,
                )
            )
            stack.enter_context(
                patch(
                    "bitget.trading.tail_risk_gate.tail_risk_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(
                patch(
                    "bitget.trading.doomsday_gate.doomsday_long_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(
                patch(
                    "bitget.trading.concentration_gate.concentration_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(
                patch(
                    "bitget.trading.price_sanity_gate.price_sanity_entry_blocked",
                    return_value=(False, {}),
                )
            )
            stack.enter_context(patch.object(ledger, "fetch_funding_snapshot", return_value=None))
            stack.enter_context(
                patch(
                    "bitget.governance.meta_consumer.load_meta_state_resolved",
                    return_value={},
                )
            )
            stack.enter_context(
                patch(
                    "bitget.governance.meta_consumer.effective_max_position_pct",
                    return_value=1.0,
                )
            )
            stack.enter_context(
                patch.object(ledger, "get_exploration_role_scaler", return_value=(1.0, "LIVE"))
            )
            stack.enter_context(
                patch.object(
                    ledger,
                    "_apply_thompson_kelly_multiplier",
                    side_effect=lambda c, tf, sig, k: k,
                )
            )
            stack.enter_context(
                patch(
                    "bitget.trading.regime_capital_relay.apply_regime_capital_to_kelly",
                    side_effect=lambda k, **kw: (k, {}),
                )
            )
            stack.enter_context(
                patch(
                    "bitget.evolution.elastic_threshold_bg.BitgetElasticThreshold",
                    return_value=elastic_mock,
                )
            )
            stack.enter_context(
                patch.object(ledger, "compute_evolved_alpha_bonus_score", return_value=0.0)
            )
            stack.enter_context(patch.object(ledger, "save_system_config"))
            stack.enter_context(
                patch(
                    "bitget.evolution.regime_analog_bg.frontrun_gate",
                    return_value=(True, {}),
                )
            )
            stack.enter_context(
                patch(
                    "bitget.infra.data_paths.market_data_db_path",
                    return_value=shadow_db,
                )
            )
            ok, msg = ledger.try_add_virtual_position(
                "spot",
                "BTCUSDT",
                "4H",
                "[CORE_B]",
                85,
                50_000.0,
                facts,
            )
        assert ok, msg
        read_conn = sqlite3.connect(db_path)
        try:
            row = read_conn.execute(
                "SELECT sim_kelly_invest FROM bitget_forward_trades WHERE status='OPEN'"
            ).fetchone()
        finally:
            read_conn.close()
        return float(row[0])


# patch alias for stack
patch = mock.patch


class TestShadowLoserMult:
    def test_loser_gets_zero_shadow_mult(self, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_ALLOCATION_SHADOW_ENABLED", "1")
        from bitget.evolution.deathmatch_allocation_shadow import build_shadow_allocation_proposal

        br = SimpleNamespace(
            champion=SimpleNamespace(group_key="CORE_A"),
            arms=_make_arms_for_overlay(),
        )
        out = build_shadow_allocation_proposal(br, market_type="spot", sys_config={})
        assert out is not None
        merged = out["merged_group_mult"]
        assert merged["CORE_A"] > 1.0
        assert merged["CORE_B"] == 0.0


class TestShadowLogWrite:
    def test_persist_shadow_rows_no_meta_write(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_ALLOCATION_SHADOW_ENABLED", "1")
        db = tmp_path / "shadow.sqlite"
        db.write_bytes(b"")

        from bitget.evolution.deathmatch_allocation_shadow import (
            record_deathmatch_shadow_from_battle_royale,
        )

        br = SimpleNamespace(
            champion=SimpleNamespace(group_key="CORE_A"),
            arms=_make_arms_for_overlay(),
        )
        with mock.patch(
            "bitget.infra.data_paths.market_data_db_path",
            return_value=str(db),
        ):
            out = record_deathmatch_shadow_from_battle_royale(
                br, market_type="futures", sys_config={}
            )
        assert out is not None
        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT group_key, merged_kelly_mult, source FROM bitget_deathmatch_alloc_shadow"
            ).fetchall()
        finally:
            conn.close()
        assert len(rows) >= 2
        assert all(r[2] == "deathmatch_run" for r in rows)
        by_gk = {r[0]: r[1] for r in rows}
        assert by_gk["CORE_B"] == 0.0


class TestProductionKellyIsolation:
    def test_shadow_on_off_identical_sim_kelly_invest(self, tmp_path, monkeypatch):
        """Mandatory B-2 gate — shadow must not change order sizing."""
        shadow_db = tmp_path / "market.sqlite"
        shadow_db.write_bytes(b"")
        cfg = _base_try_add_cfg(
            PORTFOLIO_NAV_PEAK=1000.0,
            TREASURY_SPOT_USDT=500.0,
            TREASURY_FUTURES_USDT=500.0,
        )

        monkeypatch.setenv("DEATHMATCH_ALLOCATION_SHADOW_ENABLED", "1")
        from bitget.evolution.deathmatch_allocation_shadow import (
            ensure_deathmatch_shadow_schema,
            persist_shadow_allocation_rows,
        )

        ensure_deathmatch_shadow_schema(str(shadow_db))
        persist_shadow_allocation_rows(
            {
                "market": "SPOT",
                "overlay": {"CORE_B": 0.0},
                "merged_group_mult": {"CORE_B": 0.0},
                "proposal": {"standby_groups": ["CORE_B"], "boost_groups": [], "eligible_n": 2},
            },
            db_path=str(shadow_db),
        )

        invest_shadow_on = _run_try_add_isolated(dict(cfg), shadow_db=str(shadow_db))

        monkeypatch.setenv("DEATHMATCH_ALLOCATION_SHADOW_ENABLED", "0")
        invest_shadow_off = _run_try_add_isolated(dict(cfg), shadow_db=str(shadow_db))

        assert invest_shadow_on > 0.0
        assert invest_shadow_on == invest_shadow_off

    def test_kelly_observe_writes_without_changing_return(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_ALLOCATION_SHADOW_ENABLED", "1")
        shadow_db = tmp_path / "market.sqlite"
        shadow_db.write_bytes(b"")

        from bitget.evolution.deathmatch_allocation_shadow import (
            observe_kelly_chain_shadow,
            persist_shadow_allocation_rows,
        )

        persist_shadow_allocation_rows(
            {
                "market": "SPOT",
                "overlay": {"CORE_B": 0.0},
                "merged_group_mult": {"CORE_B": 0.0},
                "proposal": {"eligible_n": 1},
            },
            db_path=str(shadow_db),
        )

        with mock.patch(
            "bitget.infra.data_paths.market_data_db_path",
            return_value=str(shadow_db),
        ):
            before = 0.0125
            after = observe_kelly_chain_shadow(
                before,
                core_group="CORE_B",
                market_type="spot",
                meta_state={},
                cfg={},
            )
        assert after == before

        conn = sqlite3.connect(str(shadow_db))
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM bitget_deathmatch_alloc_shadow WHERE source='kelly_observe'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert n == 1
