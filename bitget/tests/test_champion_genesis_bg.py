"""bitget.evolution.champion_genesis_bg — 코인 챔피언 전조 축적·예측·백필(Bitget 자체 DB만 사용)."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest import mock

from bitget.evolution import champion_genesis_bg as cg


def _make_trades_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT,
            exit_date TEXT,
            market_type TEXT,
            symbol TEXT,
            sig_type TEXT,
            status TEXT,
            final_ret REAL,
            dyn_cpv REAL DEFAULT 0.0,
            dyn_tb REAL DEFAULT 0.0,
            v_energy REAL DEFAULT 0.0,
            entry_breadth REAL DEFAULT 1.0,
            flow_tags TEXT DEFAULT ''
        )
        """
    )
    base = datetime(2026, 1, 1)
    rows = []
    # 챔피언(ENGINE1): 대부분 양의 수익, BTC 자산군에 몰림.
    for i in range(12):
        d = (base + timedelta(days=i)).strftime("%Y-%m-%d")
        rows.append((d, d, "spot", "BTC_USDT", "ENGINE1", "CLOSED_TP", 2.0 if i % 3 else -0.5, 0.6, 10.0, 5.0, 1.0, ""))
    # 독성(ENGINE2): 대부분 음의 수익.
    for i in range(12):
        d = (base + timedelta(days=i)).strftime("%Y-%m-%d")
        rows.append((d, d, "spot", "ETH_USDT", "ENGINE2", "CLOSED_SL", -2.0 if i % 3 else 0.3, 0.4, 8.0, -3.0, 1.0, ""))
    conn.executemany(
        """
        INSERT INTO bitget_forward_trades
            (entry_date, exit_date, market_type, symbol, sig_type, status, final_ret,
             dyn_cpv, dyn_tb, v_energy, entry_breadth, flow_tags)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def _synthetic_cfg(as_of: datetime) -> dict:
    """[T0-30, T0] 구간을 채우는 REGIME_VECTOR_HISTORY_BG(>=MIN_WINDOW_POINTS)."""
    history = []
    d = as_of - timedelta(days=25)
    while d <= as_of:
        history.append({"ts": d.strftime("%Y-%m-%d %H:%M:%S"), "vector": [0.1, 0.1, 0.0, 0.0]})
        d += timedelta(days=2)
    return {
        "GENESIS_PRECURSOR_ENABLED_BG": "1",
        "CURRENT_REGIME_KEY": "BULL",
        "REGIME_VECTOR_HISTORY_BG": history,
    }


class TestCaptureChampionPrecursors(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "bitget_market_data.sqlite")
        _make_trades_db(self.db_path)
        self._patcher = mock.patch.object(cg, "_db_path", return_value=self.db_path)
        self._patcher.start()

    def tearDown(self):
        self._patcher.stop()

    def _fake_br(self):
        champion = SimpleNamespace(group_key="ENGINE1", composite_score=1.5)
        arms = [
            SimpleNamespace(rank=1, group_key="ENGINE1", composite_score=1.5, mean_ret=1.0, below_floor=False),
            SimpleNamespace(rank=2, group_key="ENGINE2", composite_score=-0.8, mean_ret=-1.2, below_floor=True),
        ]
        return SimpleNamespace(champion=champion, arms=arms)

    def test_capture_inserts_champion_and_toxic_rows(self):
        cfg = _synthetic_cfg(datetime(2026, 1, 12))
        out = cg.capture_champion_precursors(self._fake_br(), cfg, market="spot")

        self.assertFalse(out.get("skipped"))
        self.assertNotIn("error", out)
        self.assertGreaterEqual(out["captured"], 1)
        self.assertGreaterEqual(out["toxic"], 1)

        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            f"SELECT market, champion_label, kind, status FROM {cg.GENESIS_TABLE}"
        ).fetchall()
        conn.close()
        self.assertGreaterEqual(len(rows), 2)
        kinds = {r[2] for r in rows}
        self.assertIn("champion", kinds)
        self.assertIn("toxic", kinds)
        for r in rows:
            self.assertEqual(r[0], "spot")

    def test_capture_disabled_flag_skips(self):
        cfg = _synthetic_cfg(datetime(2026, 1, 12))
        cfg["GENESIS_PRECURSOR_ENABLED_BG"] = "0"
        out = cg.capture_champion_precursors(self._fake_br(), cfg, market="spot")
        self.assertTrue(out["skipped"])
        self.assertEqual(out["captured"], 0)

    def test_capture_is_idempotent_on_conflict(self):
        cfg = _synthetic_cfg(datetime(2026, 1, 12))
        cg.capture_champion_precursors(self._fake_br(), cfg, market="spot")
        out2 = cg.capture_champion_precursors(self._fake_br(), cfg, market="spot")
        # UNIQUE(market, champion_label, ignition_date, kind) -> 재실행해도 에러 없음.
        self.assertNotIn("error", out2)


class TestPredictionAndBackfillSafeFallbacks(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "bitget_market_data.sqlite")
        _make_trades_db(self.db_path)
        self._patcher = mock.patch.object(cg, "_db_path", return_value=self.db_path)
        self._patcher.start()

    def tearDown(self):
        self._patcher.stop()

    def test_run_precursor_prediction_no_confirmed_rows(self):
        out = cg.run_precursor_prediction({}, market="spot")
        self.assertFalse(out["active"])
        self.assertEqual(out["matches"], 0)

    def test_backfill_no_pending_rows_is_noop(self):
        out = cg.backfill_and_learn({}, market="spot")
        self.assertEqual(out["resolved"], 0)
        self.assertEqual(out["predictions_resolved"], 0)

    def test_genesis_radar_report_block_disabled_returns_empty(self):
        block = cg.genesis_radar_report_block({"GENESIS_PRECURSOR_ENABLED_BG": "0"}, market="spot")
        self.assertEqual(block, "")

    def test_genesis_radar_report_block_enabled_returns_text(self):
        block = cg.genesis_radar_report_block({}, market="spot")
        self.assertIn("전조", block)


if __name__ == "__main__":
    unittest.main()
