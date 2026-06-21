"""Offline R&D Sandbox — 내부 전용 stress + toxic-inverted mining smoke."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from offline_rnd_sandbox import (
    OfflineRnDSandbox,
    _bbox_centroid,
    _clone_live_dna_templates,
    run_proprietary_stress_test,
    run_toxic_inverted_mining,
)


class TestOfflineRnDSandbox(unittest.TestCase):
    def _seed_db(self, path: str) -> None:
        conn = sqlite3.connect(path)
        try:
            dates = pd.date_range("2025-01-01", periods=120, freq="B")
            closes = 100 + np.cumsum(np.random.randn(len(dates)) * 0.5)
            spy = pd.DataFrame({"Date": dates.strftime("%Y-%m-%d"), "Close": closes})
            spy.to_sql("US_SPY", conn, index=False, if_exists="replace")
            conn.execute(
                """
                CREATE TABLE forward_trades (
                    market TEXT, entry_date TEXT, ticker TEXT,
                    dyn_cpv REAL, dyn_tb REAL, v_energy REAL,
                    ret_pct REAL, sector TEXT, sig_type TEXT
                )
                """
            )
            for i in range(30):
                conn.execute(
                    "INSERT INTO forward_trades VALUES ('US', ?, ?, 0.7, 10, 20, ?, 'Tech', 'DNA_TEST')",
                    (str(dates[i + 40])[:10], f"SYM{i}", float(i % 5 - 2)),
                )
            conn.commit()
        finally:
            conn.close()

    def test_bbox_centroid(self):
        c = _bbox_centroid({"dyn_cpv": {"min": 0.5, "max": 0.7}, "dyn_tb": 10, "v_energy": 20})
        self.assertIsNotNone(c)
        self.assertEqual(len(c), 3)

    def test_clone_live_dna(self):
        cfg = {
            "DNA_SUPERNOVA_US_MULTI": {
                "LIVE_ALPHA_A": {"cpv": 0.7, "tb": 10, "bbe": 20},
            },
            "LIVE_CLUSTER_TEMPLATES": {},
        }
        meta = {
            "META_LIVE_STRATEGY_IDS": ["LIVE_ALPHA_A"],
            "META_STRATEGY_REGISTRY": [{"strategy_id": "LIVE_ALPHA_A", "state": "LIVE"}],
            "META_STRATEGY_HEALTH": {"sig|ALPHA": {"mult": 0.9, "n": 10}},
        }
        tpl = _clone_live_dna_templates("US", cfg, meta)
        self.assertIn("LIVE_ALPHA_A", tpl)

    def test_stress_test_writes_artifact(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
        self._seed_db(path)
        cfg = {
            "DNA_SUPERNOVA_US_MULTI": {"T1": {"cpv": 0.7, "tb": 10, "bbe": 20}},
        }
        meta = {"META_LIVE_STRATEGY_IDS": ["T1"], "META_STRATEGY_HEALTH": {}}
        with patch("offline_rnd_sandbox._artifact_dir") as ad:
            tmp = tempfile.mkdtemp()
            ad.return_value = tmp
            res = run_proprietary_stress_test("US", cfg=cfg, meta=meta, db_path=path)
        self.assertTrue(res.get("ok"))
        self.assertTrue(res.get("defcon_weights"))
        self.assertTrue(os.path.isfile(res.get("artifact", "")))

    def test_toxic_inverted_mining(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
        self._seed_db(path)
        cfg = {
            "TOXIC_ML_ANTIPATTERNS": {
                "rules": {
                    "BAD1": {
                        "dyn_cpv": {"min": 0.9, "max": 1.0},
                        "dyn_tb": {"min": 15, "max": 20},
                        "v_energy": {"min": 25, "max": 30},
                    }
                }
            },
            "ANTI_PATTERNS": [],
        }
        with patch("offline_rnd_sandbox._artifact_dir") as ad:
            tmp = tempfile.mkdtemp()
            ad.return_value = tmp
            res = run_toxic_inverted_mining("US", cfg=cfg, db_path=path)
        self.assertTrue(res.get("ok"))
        theme = res.get("theme") or {}
        self.assertIn("tickers", theme)
        self.assertIn("HIDDEN_SPILLOVER_THEME_US", cfg)

    def test_sandbox_run(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
        self._seed_db(path)
        cfg = {
            "DNA_SUPERNOVA_US_MULTI": {"T1": {"cpv": 0.7, "tb": 10, "bbe": 20}},
            "TOXIC_ML_ANTIPATTERNS": {"rules": {"X": {"dyn_cpv": 0.5, "dyn_tb": 8, "v_energy": 15}}},
        }
        meta = {"META_LIVE_STRATEGY_IDS": ["T1"], "META_STRATEGY_HEALTH": {}}
        with patch("offline_rnd_sandbox.MARKET_DATA_DB_PATH", path):
            with patch("offline_rnd_sandbox._artifact_dir") as ad:
                ad.return_value = tempfile.mkdtemp()
                with patch("meta_governor.load_meta_governor_state", return_value=meta):
                    with patch("config_manager.save_system_config", return_value=True):
                        out = OfflineRnDSandbox().run("US", cfg=cfg)
        self.assertTrue(out.get("ok"))


if __name__ == "__main__":
    unittest.main()
