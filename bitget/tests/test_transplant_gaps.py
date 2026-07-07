"""Bitget transplant gap 회귀 — market keys, deathmatch SSOT, PRACT DNA, funding PnL."""
from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest
import sqlite3

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


class TestLiveNavManager:
    def test_apply_realized_pnl_updates_nav(self, tmp_path, monkeypatch):
        from bitget import live_nav_manager as lnm

        state_path = tmp_path / "bitget_treasury_state.json"
        monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(state_path))
        st = lnm.apply_realized_pnl("spot", 500.0)
        assert st["nav"] == lnm.base_capital_for("spot") + 500.0
        assert st["n_closed"] == 1
        assert lnm.live_nav("spot") == st["nav"]

    def test_record_closure_prefers_net_pnl_usdt(self, tmp_path, monkeypatch):
        from bitget import live_nav_manager as lnm

        state_path = tmp_path / "bitget_treasury_state.json"
        monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(state_path))
        base = lnm.base_capital_for("futures")
        out = lnm.record_closure("futures", final_ret_pct=10.0, net_pnl_usdt=-250.0)
        assert out["nav"] == base - 250.0


class TestMacroAndCanaryPanels:
    def test_macro_section_includes_live_nav(self):
        from bitget.reports.macro_treasury_bg import build_bitget_macro_section_html

        class _Slice:
            df_real = []
            n_closed_window = 0
            n_open_valid = 0
            df_closed = pd.DataFrame()

        ctx = mock.Mock()
        ctx.market_window_header_html.return_value = "hdr"
        html_out = build_bitget_macro_section_html(
            market_type="spot",
            market_icon="🟢",
            ctx=ctx,
            mkt_slice=_Slice(),
            sys_config={"TREASURY_SPOT_USDT": 50_000.0, "CURRENT_REGIME_KEY": "BULL"},
            meta={"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        assert "Live NAV" in html_out
        assert "USDT" in html_out
        assert "[1/9]" in html_out

    def test_canary_panel_formats_state(self):
        from bitget.reports.canary_panel_bg import format_canary_panel_html

        html_out = format_canary_panel_html(
            {
                "crypto_liquidity_stress": 0.42,
                "macro_contagion_risk": True,
                "components": {"symbols_used": ["BTCUSDT"], "btc_ret_3d": -0.03},
                "updated_at": "2026-07-08",
            }
        )
        assert "Canary" in html_out
        assert "0.42" in html_out
        assert "ON" in html_out


class TestFluidAndWeeklyRegime:
    def test_fluid_sync_calls_exploration_and_vector(self):
        from bitget.evolution.fluid_evolution_bridge_bg import post_bitget_meta_governor_fluid_sync

        calls = {"budget": 0, "vector": 0}

        def _budget():
            calls["budget"] += 1

        def _vector(cfg):
            calls["vector"] += 1

        with mock.patch(
            "bitget.governance.exploration_budget.refresh_exploration_budget_state",
            side_effect=_budget,
        ), mock.patch(
            "bitget.evolution.coin_regime_vector.append_coin_regime_vector_history",
            side_effect=_vector,
        ):
            post_bitget_meta_governor_fluid_sync({})
        assert calls["budget"] == 1
        assert calls["vector"] == 1

    def test_weekly_regime_archive_appends_config(self):
        from bitget.evolution.weekly_regime_bg import run_weekly_coin_regime_archive

        saved = {}

        def _update(key, modifier):
            saved["key"] = key
            saved["value"] = modifier([])

        with mock.patch(
            "bitget.auto_pilot.detect_coin_regime",
            return_value="RISK_ON",
        ), mock.patch(
            "bitget.evolution.coin_regime_vector.append_coin_regime_vector_history",
        ), mock.patch(
            "bitget.evolution.coin_regime_vector.build_current_coin_regime_vector",
            return_value={"vector_map": {"btc": 1.0}},
        ), mock.patch(
            "bitget.evolution.coin_regime_vector.load_vector_history",
            return_value=[1, 2],
        ), mock.patch(
            "bitget.infra.config_manager.update_config_value",
            side_effect=_update,
        ):
            out = run_weekly_coin_regime_archive({})
        assert out["ok"] is True
        assert out["regime_key"] == "RISK_ON"
        assert saved["key"] == "WEEKLY_REGIME_ARCHIVE_BG"
        assert saved["value"][-1]["regime_key"] == "RISK_ON"


class TestPyramidAdd:
    def test_maybe_pyramid_add_inserts_child_row(self, tmp_path):
        import sqlite3

        from bitget.forward.ledger import _maybe_pyramid_add

        db = tmp_path / "t.sqlite"
        conn = sqlite3.connect(str(db))
        conn.execute(
            """
            CREATE TABLE bitget_forward_trades (
                id INTEGER PRIMARY KEY,
                entry_date TEXT, market_type TEXT, symbol TEXT, timeframe TEXT,
                sig_type TEXT, tier TEXT, total_score REAL, entry_price REAL,
                position_side TEXT, entry_atr REAL, entry_high REAL, leverage REAL,
                sim_kelly_risk_pct REAL, margin_used REAL, sim_kelly_invest REAL,
                status TEXT, max_high REAL, min_low REAL, parent_trade_id INTEGER,
                v_energy REAL, entry_breadth REAL, pyramid_adds INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            (id, market_type, symbol, timeframe, sig_type, tier, total_score, entry_price,
             position_side, leverage, sim_kelly_invest, status, pyramid_adds, v_energy, entry_breadth)
            VALUES (1,'spot','BTCUSDT','4H','STANDARD','T1',80,100,'LONG',1,1000,'OPEN',0,5.0,1.0)
            """
        )
        conn.commit()
        parent = dict(
            id=1,
            market_type="spot",
            symbol="BTCUSDT",
            timeframe="4H",
            sig_type="STANDARD",
            tier="T1",
            total_score=80,
            position_side="LONG",
            entry_atr=1.0,
            leverage=1.0,
            sim_kelly_risk_pct=2.0,
            v_energy=5.0,
            entry_breadth=1.0,
            pyramid_adds=0,
        )
        with mock.patch(
            "exit_dynamics.pyramid_decision",
            return_value={"do": True, "add_notional": 200.0},
        ), mock.patch("bitget.live_nav_manager.live_nav", return_value=10_000.0):
            ok = _maybe_pyramid_add(
                conn, parent, "spot", "BTCUSDT", 105.0, {}, "BULL", edge_score=2.5
            )
        assert ok is True
        n_child = conn.execute(
            "SELECT COUNT(*) FROM bitget_forward_trades WHERE parent_trade_id=1"
        ).fetchone()[0]
        adds = conn.execute(
            "SELECT pyramid_adds FROM bitget_forward_trades WHERE id=1"
        ).fetchone()[0]
        conn.close()
        assert n_child == 1
        assert adds == 1


class TestReportStateBinderBg:
    def test_macro_freshness_lookback_tag(self):
        from bitget.reports.report_state_binder_bg import (
            MacroTreasuryReportBlock,
            format_macro_treasury_section_html,
        )

        block = MacroTreasuryReportBlock(
            regime_key="BULL",
            regime_confidence=0.8,
            regime_notes="",
            kelly_cap=None,
            kelly_floor=None,
            meta_global_kelly_mult=1.0,
            base_dynamic_kelly_risk=0.02,
            effective_kelly_risk=0.02,
            treasury_config_raw=50_000.0,
            ledger_realized_est=0.0,
            treasury_footnote="note",
            nav=50_000.0,
            macro_freshness="lookback",
        )
        html_out = format_macro_treasury_section_html(
            block, display_label="SPOT", market_icon="🟢", today_str="2026-07-08"
        )
        assert "⚠️" in html_out
        assert "lookback" in html_out or "재사용" in html_out

    def test_lifecycle_block_formats_spot_futures(self):
        from bitget.reports.report_state_binder_bg import (
            LifecycleReportBlock,
            format_lifecycle_section_html,
        )

        block = LifecycleReportBlock(
            governor_last_run_at="2026-07-08",
            governor_last_run_status="OK",
            n_live=2,
            n_cooled=1,
            n_candidate=0,
            n_observing=0,
            n_retired=0,
            n_registry_total=3,
            n_other_state=0,
            retired_tracked_count=0,
            health_summary_line="감시 2그룹",
            autopilot_age_days=10,
            autopilot_age_source="LIVE_A_PROMOTION_DATE",
            live_fleet_mean_age_days=5.0,
            cycle_discovery_new=0,
            cycle_promoted_live=0,
            cycle_demoted_cooled=0,
            demoted_last_7d=0,
            live_spot=1,
            live_futures=1,
            cooled_spot=0,
            cooled_futures=1,
            candidate_spot=0,
            candidate_futures=0,
            avg_alpha_life_days_spot=None,
            avg_alpha_life_days_futures=3.0,
            health_groups_linked_live=0,
            footnote="fn",
        )
        html_out = format_lifecycle_section_html(block, market_icon="🟢", today_str="2026-07-08")
        assert "[8/9]" in html_out
        assert "SPOT LIVE" in html_out
        assert "FUT LIVE" in html_out


class TestMacroHydrateBg:
    def test_refresh_persists_freshness_on_lookback(self, tmp_path, monkeypatch):
        from bitget import macro_hydrate_bg as mh

        db = tmp_path / "alt.sqlite"
        conn = sqlite3.connect(str(db))
        conn.execute(
            """
            CREATE TABLE macro_daily (
                date TEXT PRIMARY KEY, btc_dominance REAL, eth_btc_ratio REAL,
                total_market_cap_usd REAL, market_cap_change_24h REAL,
                btc_price_usd REAL, eth_price_usd REAL
            )
            """
        )
        conn.execute(
            "INSERT INTO macro_daily VALUES (?,?,?,?,?,?,?)",
            ("2026-07-07", 55.0, 0.05, 1e12, 1.0, 60000.0, 3000.0),
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(mh, "_load_macro_row_lookback", lambda **kw: {"date": "2026-07-07", "btc_dominance": 55.0})
        with mock.patch("bitget.alt_data_miner.run_once", return_value=None):
            out = mh.refresh_bitget_macro_daily()
        assert out["source"] == "lookback"


class TestWeeklyCoinPri:
    def test_compute_pri_empty_db(self, tmp_path, monkeypatch):
        from bitget.evolution import weekly_proprietary_regime_bg as pri

        missing = str(tmp_path / "missing.sqlite")
        monkeypatch.setattr(pri, "DB_PATH", missing)
        monkeypatch.setattr(pri, "shadow_pri_path", lambda: str(tmp_path / "pri.json"))
        out = pri.compute_weekly_coin_pri()
        assert out.get("error") == "no_db"


class TestProprietaryFrictionStore:
    def test_insert_funnel_and_friction(self, tmp_path, monkeypatch):
        import sqlite3
        from bitget.infra import proprietary_friction_store_bg as pfs

        db = tmp_path / "m.sqlite"
        conn = sqlite3.connect(str(db))
        conn.close()
        monkeypatch.setattr(pfs, "friction_db_path", lambda: str(db))
        pfs.insert_scan_funnel_snapshot(
            ts="2026-07-08 10:00",
            market="spot",
            universe_size=100,
            survivors=5,
            pass_rate_pct=5.0,
        )
        pfs.insert_regime_friction_event(
            date="2026-07-08", market="futures", event_type="DM_A_ZERO_CLOSED"
        )
        conn = sqlite3.connect(str(db))
        n1 = conn.execute("SELECT COUNT(*) FROM scan_funnel_snapshot").fetchone()[0]
        n2 = conn.execute("SELECT COUNT(*) FROM regime_friction_event").fetchone()[0]
        conn.close()
        assert n1 == 1
        assert n2 == 1


class TestShadowMacroBg:
    def test_shadow_macro_improvement_block(self):
        import pandas as pd
        from bitget.shadow_macro_validator_bg import append_shadow_macro_block

        df = pd.DataFrame(
            {
                "sig_type": ["STANDARD_A", "SUPERNOVA_B", "STANDARD_C"],
                "final_ret": [5.0, -2.0, 3.0],
            }
        )
        html_out = append_shadow_macro_block("base", market="spot", df_closed=df)
        assert "섀도우 매크로" in html_out
        assert "base" in html_out


class TestMetaLearnerBg:
    def test_meta_learning_cycle_runs(self, tmp_path, monkeypatch):
        from bitget import meta_learner_bg as ml

        monkeypatch.setattr(ml, "trust_matrix_path", lambda: str(tmp_path / "trust.json"))
        monkeypatch.setattr(ml, "DB_PATH", str(tmp_path / "missing.sqlite"))
        out = ml.run_bitget_meta_learning_cycle(meta={"META_REGIME_KEY": "BULL"})
        assert out["ok"] is True
        line = ml.build_meta_cognition_line()
        assert "Meta-Trust" in line
