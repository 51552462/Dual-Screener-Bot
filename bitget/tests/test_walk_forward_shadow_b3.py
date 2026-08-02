"""B-3 — walk-forward shadow (log-only OOS judgment, production isolation)."""
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

from bitget.validation.walk_forward_bg import evaluate_oos_pass_from_returns


def _seed_forward_trades(
    db_path: str,
    *,
    market_type: str,
    sig_type: str,
    returns_pct: list[float],
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bitget_forward_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_date TEXT,
                exit_date TEXT,
                market_type TEXT,
                sig_type TEXT,
                status TEXT,
                final_ret REAL
            );
            """
        )
        for i, ret in enumerate(returns_pct):
            conn.execute(
                """
                INSERT INTO bitget_forward_trades (
                    entry_date, exit_date, market_type, sig_type, status, final_ret
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    f"2026-01-{i+1:02d}",
                    f"2026-02-{i+1:02d}",
                    market_type,
                    sig_type,
                    "CLOSED_WIN" if ret > 0 else "CLOSED_LOSS",
                    ret,
                ),
            )
        conn.commit()
    finally:
        conn.close()


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


def _run_try_add_isolated(cfg: dict, *, db_path: str) -> float:
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
            stack.enter_context(
                mock.patch(
                    "bitget.infra.data_paths.market_data_db_path",
                    return_value=db_path,
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


class TestOosJudgment:
    def test_oos_pass_when_recent_window_positive(self):
        returns = [0.5] * 25 + [2.0] * 5
        out = evaluate_oos_pass_from_returns(returns)
        assert out["pass"] is True
        assert out["reason"] == "oos_pass"
        assert out["oos_n"] >= 5

    def test_oos_fail_when_recent_window_negative(self):
        returns = [0.5] * 25 + [-3.0] * 5
        out = evaluate_oos_pass_from_returns(returns)
        assert out["pass"] is False
        assert out["reason"] == "oos_fail"

    def test_insufficient_data_below_min_trades(self):
        out = evaluate_oos_pass_from_returns([0.1] * 5)
        assert out["pass"] is False
        assert out["reason"] == "insufficient_data"


class TestNormalizedMarketKeys:
    def test_bg_market_type_resolves_to_spot_or_fut_not_bg(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        monkeypatch.setenv("WALK_FORWARD_SHADOW_ENABLED", "1")
        db = tmp_path / "market.sqlite"
        db.write_bytes(b"")
        _seed_forward_trades(
            str(db),
            market_type="BG",
            sig_type="CORE_A",
            returns_pct=[1.0] * 30,
        )
        _seed_forward_trades(
            str(db),
            market_type="futures",
            sig_type="CORE_B",
            returns_pct=[1.0] * 30,
        )

        from bitget.validation.walk_forward_shadow_bg import run_walk_forward_shadow_job

        with mock.patch(
            "bitget.infra.data_paths.market_data_db_path",
            return_value=str(db),
        ):
            out = run_walk_forward_shadow_job(forward_db_path=str(db), shadow_db_path=str(db))

        assert out is not None
        markets = {j["market"] for j in out["judgments"]}
        assert "BG" not in markets
        assert markets <= {"SPOT", "FUT"}

        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT DISTINCT market FROM bitget_walk_forward_shadow"
            ).fetchall()
        finally:
            conn.close()
        stored = {r[0] for r in rows}
        assert "BG" not in stored


class TestPipelinePlacement:
    def test_scan_pipelines_exclude_walk_forward_shadow(self):
        from bitget.pipelines import bitget_pipelines as bp

        for fn_name in ("_pipeline_scan_spot", "_pipeline_scan_futures", "_pipeline_scan_all"):
            steps = getattr(bp, fn_name)()
            names = [s.name for s in steps]
            assert "walk_forward_shadow" not in names

    def test_weekly_evolution_includes_walk_forward_shadow(self):
        from bitget.pipelines import bitget_pipelines as bp

        steps = bp._pipeline_weekly_evolution()
        names = [s.name for s in steps]
        assert "walk_forward_shadow" in names


class TestShadowPersistence:
    def test_shadow_rows_written_without_registry_touch(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WALK_FORWARD_SHADOW_ENABLED", "1")
        db = tmp_path / "market.sqlite"
        db.write_bytes(b"")
        _seed_forward_trades(
            str(db),
            market_type="spot",
            sig_type="WINNER",
            returns_pct=[2.0] * 30,
        )

        from strategy_registry_store import ensure_strategy_registry_schema, load_registry_rows

        ensure_strategy_registry_schema(str(db))
        conn = sqlite3.connect(str(db))
        conn.execute(
            """
            INSERT INTO strategy_registry (
                strategy_id, market, group_key, state, display_name, capital_mult,
                source, updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            ("s1", "SPOT", "WINNER", "LIVE", "WINNER", 1.0, "test", "t0"),
        )
        conn.commit()
        conn.close()
        before = load_registry_rows(str(db))

        from bitget.validation.walk_forward_shadow_bg import run_walk_forward_shadow_job

        with mock.patch(
            "bitget.infra.data_paths.market_data_db_path",
            return_value=str(db),
        ):
            run_walk_forward_shadow_job(forward_db_path=str(db), shadow_db_path=str(db))

        after = load_registry_rows(str(db))
        assert before == after

        conn = sqlite3.connect(str(db))
        try:
            n = conn.execute("SELECT COUNT(*) FROM bitget_walk_forward_shadow").fetchone()[0]
            row = conn.execute(
                "SELECT market, group_key, oos_pass FROM bitget_walk_forward_shadow LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        assert n >= 1
        assert row[0] == "SPOT"
        assert row[1] == "WINNER"
        assert row[2] == 1


class TestProductionIsolation:
    def test_shadow_on_off_identical_sim_kelly_invest(self, tmp_path, monkeypatch):
        shadow_db = tmp_path / "market.sqlite"
        shadow_db.write_bytes(b"")
        cfg = _base_try_add_cfg()

        _seed_forward_trades(
            str(shadow_db),
            market_type="spot",
            sig_type="CORE_B",
            returns_pct=[1.0] * 30,
        )

        monkeypatch.setenv("WALK_FORWARD_SHADOW_ENABLED", "1")
        from bitget.validation.walk_forward_shadow_bg import run_walk_forward_shadow_job

        with mock.patch(
            "bitget.infra.data_paths.market_data_db_path",
            return_value=str(shadow_db),
        ):
            run_walk_forward_shadow_job(
                forward_db_path=str(shadow_db),
                shadow_db_path=str(shadow_db),
            )

        invest_on = _run_try_add_isolated(dict(cfg), db_path=str(shadow_db))

        monkeypatch.setenv("WALK_FORWARD_SHADOW_ENABLED", "0")
        invest_off = _run_try_add_isolated(dict(cfg), db_path=str(shadow_db))

        assert invest_on > 0.0
        assert invest_on == invest_off

    def test_incubator_templates_unchanged_after_shadow_job(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WALK_FORWARD_SHADOW_ENABLED", "1")
        db = tmp_path / "market.sqlite"
        db.write_bytes(b"")
        _seed_forward_trades(
            str(db),
            market_type="futures",
            sig_type="INCUBATOR_alpha",
            returns_pct=[0.5] * 30,
        )

        from bitget import auto_pilot

        cfg = {"INCUBATOR_TEMPLATES": {"alpha": {"dna": 1}}}
        df = pd.DataFrame(
            {
                "sig_type": ["INCUBATOR_alpha"] * 6,
                "final_ret": [0.1, 0.2, 0.1, 0.2, 0.1, 0.2],
            }
        )

        class _Conn:
            def close(self):
                pass

        with mock.patch.object(auto_pilot, "get_connection", return_value=_Conn()), mock.patch(
            "pandas.read_sql", return_value=df
        ):
            before, _ = auto_pilot._judge_incubator_templates(dict(cfg))

        from bitget.validation.walk_forward_shadow_bg import run_walk_forward_shadow_job

        with mock.patch(
            "bitget.infra.data_paths.market_data_db_path",
            return_value=str(db),
        ):
            run_walk_forward_shadow_job(forward_db_path=str(db), shadow_db_path=str(db))

        with mock.patch.object(auto_pilot, "get_connection", return_value=_Conn()), mock.patch(
            "pandas.read_sql", return_value=df
        ):
            after, _ = auto_pilot._judge_incubator_templates(dict(cfg))

        assert before.get("INCUBATOR_TEMPLATES") == after.get("INCUBATOR_TEMPLATES")

    def test_promotion_block_flag_default_false(self, monkeypatch):
        monkeypatch.delenv("WALK_FORWARD_PROMOTION_BLOCK_ENABLED", raising=False)
        from bitget.validation.walk_forward_shadow_bg import walk_forward_promotion_block_enabled

        assert walk_forward_promotion_block_enabled() is False
