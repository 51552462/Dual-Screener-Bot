"""Phase 6 — deep_dive SQL binding, config_hub, artifact_guard regression tests."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest import mock


class TestReportsMarketTypeBinding(unittest.TestCase):
    def test_norm_market_type_lowercases(self):
        from bitget.forward.reports import _norm_market_type

        self.assertEqual(_norm_market_type("SPOT"), "spot")
        self.assertEqual(_norm_market_type(" Futures "), "futures")

    def test_deep_dive_filters_by_normalized_market_type(self):
        import pandas as pd

        from bitget.forward import reports

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "fwd.sqlite")
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE bitget_forward_trades (
                    id INTEGER PRIMARY KEY,
                    market_type TEXT,
                    status TEXT,
                    symbol TEXT,
                    timeframe TEXT,
                    sig_type TEXT,
                    tier TEXT,
                    total_score REAL,
                    final_ret REAL,
                    dyn_rs REAL,
                    dyn_cpv REAL,
                    dyn_tb REAL,
                    v_energy REAL,
                    margin_used REAL,
                    sim_kelly_invest REAL,
                    flow_tags TEXT
                )
                """
            )
            for i in range(12):
                conn.execute(
                    """
                    INSERT INTO bitget_forward_trades
                    (market_type, status, symbol, timeframe, sig_type, tier, total_score,
                     final_ret, dyn_rs, dyn_cpv, dyn_tb, v_energy, margin_used, sim_kelly_invest, flow_tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "spot",
                        "CLOSED_WIN",
                        f"BTC{i}",
                        "1D",
                        "STANDARD_S1",
                        "80점대",
                        85.0,
                        2.5,
                        5.0,
                        4.0,
                        6.0,
                        7.0,
                        100.0,
                        120.0,
                        "TAG_A",
                    ),
                )
            for i in range(5):
                conn.execute(
                    """
                    INSERT INTO bitget_forward_trades
                    (market_type, status, symbol, timeframe, sig_type, tier, total_score, final_ret)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("futures", "CLOSED_LOSS", f"ETH{i}", "4H", "SUPERNOVA", "70점대", 72.0, -1.0),
                )
            conn.commit()
            conn.close()

            sent: list[str] = []

            def _capture(msg: str) -> None:
                sent.append(msg)

            cfg = {"ANTI_PATTERNS": []}
            with mock.patch.object(reports, "DB_PATH", db_path), mock.patch.object(
                reports, "init_forward_db"
            ), mock.patch.object(reports, "load_system_config", return_value=cfg), mock.patch.object(
                reports, "save_system_config"
            ), mock.patch.object(
                reports, "send_telegram_msg", side_effect=_capture
            ), mock.patch.object(
                reports, "prepare_forward_trades_df", side_effect=lambda df, context=None: df
            ), mock.patch.object(
                reports, "_auto_tune_brain_from_closed_df", return_value=(cfg, [])
            ), mock.patch("builtins.print"):
                reports.run_deep_dive_analysis("SPOT")

            self.assertTrue(sent)
            self.assertIn("SPOT", sent[-1].upper())
            self.assertNotIn("ETH", sent[-1])


class TestConfigHubRoundtrip(unittest.TestCase):
    def test_save_and_load_via_config_hub(self):
        from bitget.infra import config_manager
        from bitget.config_hub import load_config, save_config_atomic

        with tempfile.TemporaryDirectory() as td:
            cfg_path = os.path.join(td, "bitget_system_config.sqlite")
            with mock.patch.object(config_manager, "CONFIG_DB_PATH", cfg_path):
                save_config_atomic({"PHASE6_TEST_KEY": "ok", "WEIGHT_S1": 1.1})
                loaded = load_config()
            self.assertEqual(loaded.get("PHASE6_TEST_KEY"), "ok")
            self.assertAlmostEqual(float(loaded.get("WEIGHT_S1", 0)), 1.1)


class TestArtifactGuardSchema(unittest.TestCase):
    def test_verify_ok_after_init_forward_db(self):
        from bitget.forward.shared import init_forward_db
        from bitget.infra import artifact_guard
        from bitget.infra import data_paths

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "bitget_market_data.sqlite")
            with mock.patch.object(data_paths, "market_data_db_path", return_value=db_path):
                init_forward_db()
                out = artifact_guard.verify_bitget_market_db_schema(heal=False)
            self.assertTrue(out["ok"])
            self.assertEqual(out["missing"], [])


if __name__ == "__main__":
    unittest.main()
