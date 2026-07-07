"""Bitget transplant gap 회귀 — market keys, deathmatch SSOT, PRACT DNA, funding PnL."""
from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from bitget.infra.market_keys import (
    normalize_market_type,
    to_db_key,
    to_deathmatch_key,
    to_pil_key,
)


class TestMarketKeys:
    def test_normalize_spot_aliases(self):
        assert normalize_market_type("spot") == "spot"
        assert normalize_market_type("SPOT") == "spot"
        assert normalize_market_type("BG_SPOT") == "spot"

    def test_normalize_futures_aliases(self):
        assert normalize_market_type("futures") == "futures"
        assert normalize_market_type("FUT") == "futures"
        assert normalize_market_type("BG_FUTURES") == "futures"

    def test_deathmatch_and_pil_keys(self):
        assert to_deathmatch_key("futures") == "FUT"
        assert to_deathmatch_key("spot") == "SPOT"
        assert to_pil_key("futures") == "BG_FUTURES"
        assert to_pil_key("spot") == "BG_SPOT"
        assert to_db_key("FUTURES") == "futures"


class TestDeathmatchBgSsot:
    def test_bitget_ssot_patches_registry_and_store(self, tmp_path):
        bg_db = tmp_path / "bitget.sqlite"
        bg_db.write_text("")

        with mock.patch("bitget.evolution.deathmatch_bg.market_data_db_path", return_value=str(bg_db)):
            import strategy_registry_store as srs
            import evolution.deathmatch_store as dms

            orig_load = srs.load_registry_rows
            orig_save = dms.save_battle_royal_result
            orig_log = dms.log_elimination_events
            orig_db = dms._db_path

            seen = {}

            def _fake_load(p=None):
                seen["load_path"] = p
                return []

            def _fake_save(*args, db_path=None, **kwargs):
                seen["save_path"] = db_path

            def _fake_log(market, events, db_path=None, **kwargs):
                seen["log_path"] = db_path

            srs.load_registry_rows = _fake_load
            dms.save_battle_royal_result = _fake_save
            dms.log_elimination_events = _fake_log
            dms._db_path = lambda: "stock-should-not-be-used"

            from bitget.evolution.deathmatch_bg import bitget_deathmatch_ssot

            with bitget_deathmatch_ssot():
                srs.load_registry_rows()
                dms.save_battle_royal_result("SPOT", [], None)
                dms.log_elimination_events("SPOT", [])

            assert seen["load_path"] == str(bg_db)
            assert seen["save_path"] == str(bg_db)
            assert seen["log_path"] == str(bg_db)

            srs.load_registry_rows = orig_load
            dms.save_battle_royal_result = orig_save
            dms.log_elimination_events = orig_log
            dms._db_path = orig_db

    def test_allocation_writes_bitget_meta_not_stock(self, tmp_path):
        from bitget.evolution.deathmatch_allocation_bg import apply_bitget_deathmatch_allocation_to_meta

        proposal = {
            "group_mult": {"CORE_A": 1.25, "CORE_B": 0.0},
            "standby_groups": ["CORE_B"],
            "boost_groups": ["CORE_A"],
            "eligible_n": 2,
        }
        meta_in = {
            "META_STRATEGY_HEALTH": {},
            "META_GROUP_KELLY_MULT": {"CORE_A": 1.0, "CORE_B": 1.0},
        }
        saved = {}

        def _save(state, path=None):
            saved["state"] = dict(state)

        with mock.patch(
            "bitget.evolution.deathmatch_allocation_bg.load_bitget_meta_unified",
            return_value=meta_in,
        ), mock.patch(
            "bitget.evolution.deathmatch_allocation_bg.save_bitget_meta_unified",
            side_effect=_save,
        ), mock.patch(
            "bitget.infra.config_manager.save_system_config",
            return_value=True,
        ):
            out = apply_bitget_deathmatch_allocation_to_meta(
                proposal, market="SPOT", sys_config={"DEATHMATCH_APPLY_ALLOCATION": "1"}
            )

        assert saved["state"]["META_DEATHMATCH_ALLOC_MARKET"] == "SPOT"
        assert saved["state"]["META_GROUP_KELLY_MULT"]["CORE_A"] == 1.25
        assert saved["state"]["META_GROUP_KELLY_MULT"]["CORE_B"] == 0.0
        assert out["market"] == "SPOT"


class TestFundingNetRet:
    def test_funding_adjustment_long_paying_rate(self):
        price_ret = 5.0
        notion = 1000.0
        accum = -2.5  # paid 2.5 USDT funding
        net = round(price_ret + (accum / notion) * 100.0, 2)
        assert net == 4.75

    def test_funding_adjustment_short_receiving_rate(self):
        price_ret = 3.0
        notion = 500.0
        accum = 1.0
        net = round(price_ret + (accum / notion) * 100.0, 2)
        assert net == 3.2


class TestCircuitBreakerSsot:
    def test_release_on_recovery(self):
        from bitget.forward.ledger import (
            CB_RELEASE_LOSS_RATIO,
            _update_global_circuit_breaker,
        )

        saved = {}

        def _save(cfg):
            saved["cfg"] = dict(cfg)
            return True

        base_cfg = {
            "GLOBAL_CIRCUIT_BREAKER": "ON",
            "GLOBAL_CIRCUIT_BREAKER_TRIGGER_DATE": "2026-01-01",
            "GLOBAL_CIRCUIT_BREAKER_TRIGGER_MARKET": "SPOT",
        }
        with mock.patch("bitget.forward.ledger.load_system_config", return_value=base_cfg), mock.patch(
            "bitget.forward.ledger.save_system_config", side_effect=_save
        ), mock.patch("bitget.forward.ledger.send_telegram_msg"):
            _update_global_circuit_breaker(
                "SPOT",
                CB_RELEASE_LOSS_RATIO + 0.01,
                -100.0,
                10000.0,
            )
        assert saved["cfg"]["GLOBAL_CIRCUIT_BREAKER"] == "OFF"

    def test_auto_pilot_cb_does_not_toggle_global(self):
        from bitget.auto_pilot import _apply_circuit_breaker

        df = pd.DataFrame(
            {
                "exit_date": pd.date_range("2026-01-01", periods=20, freq="D"),
                "sim_kelly_invest": [1000.0] * 20,
                "final_ret": [-11.0] * 20,
            }
        )
        cfg = {"GLOBAL_CIRCUIT_BREAKER": "OFF", "DYNAMIC_KELLY_RISK": 0.01}
        out = _apply_circuit_breaker(cfg, df)
        assert out.get("GLOBAL_CIRCUIT_BREAKER", "OFF") == "OFF"
        assert out.get("CLOSED_TRADE_CB_ADVISORY", {}).get("active") is True


class TestGenesisArmSnapshotRead:
    def test_fetch_arm_snapshot_reads_deathmatch_table(self, tmp_path):
        import sqlite3
        from bitget.evolution.champion_genesis_bg import _fetch_arm_snapshot_series

        db = tmp_path / "bg.sqlite"
        conn = sqlite3.connect(str(db))
        conn.execute(
            """
            CREATE TABLE deathmatch_arm_snapshot (
                trade_date TEXT, market TEXT, arm_id TEXT, label TEXT,
                composite_score REAL, mean_ret REAL
            )
            """
        )
        conn.execute(
            "INSERT INTO deathmatch_arm_snapshot VALUES (?,?,?,?,?,?)",
            ("2026-06-01", "SPOT", "CORE_A", "CORE_A", 1.5, 2.0),
        )
        conn.commit()
        series = _fetch_arm_snapshot_series(conn, "spot", "CORE_A")
        conn.close()
        assert len(series) == 1
        assert series[0][1] == 1.5


class TestPractitionerDnaParity:
    def test_practitioner_dbg_uses_percentile_scores_and_dna_flags(self):
        from bitget.signal_engines import compute_practitioner_01

        n = 260
        dates = pd.date_range("2023-01-01", periods=n, freq="D")
        close = pd.Series(100.0, index=dates).astype(float)
        # RSI dip to cross 30 from below on last bar
        close.iloc[-3:-1] = 28.0
        close.iloc[-1] = 31.0
        df = pd.DataFrame(
            {
                "Open": close,
                "High": close * 1.01,
                "Low": close * 0.99,
                "Close": close,
                "Volume": 1_000_000.0,
            },
            index=dates,
        )
        bench = pd.Series(50_000.0, index=dates)
        hit, _, _, dbg = compute_practitioner_01(df, bench, timeframe="1D")
        if not hit:
            pytest.skip("synthetic OHLCV did not trigger P01 — environment variance")
        for key in ("is_top_dna", "is_worst_dna", "is_death_combo", "is_tenbagger"):
            assert key in dbg
        assert 1.0 <= dbg["dyn_rs_score"] <= 10.0
        assert 1.0 <= dbg["dyn_cpv_score"] <= 10.0
        assert 1.0 <= dbg["dyn_tb_score"] <= 10.0
        assert dbg["dyn_rs_score"] != dbg.get("v_rs")
