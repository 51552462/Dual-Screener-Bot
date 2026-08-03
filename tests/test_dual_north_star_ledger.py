"""Tests for dual north star progress ledger (NS-1 + Claude 조건부 OK)."""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import dual_north_star_ledger as ledger


class TestDualNorthStarLedger(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._ledger = os.path.join(self._tmpdir, "dual_north_star_ledger.json")

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(self._tmpdir, ignore_errors=True)

    @patch.object(ledger, "ledger_path")
    def test_pace_and_composite(self, mock_path) -> None:
        mock_path.return_value = self._ledger
        self.assertEqual(ledger._pace_score(20, 40), 50.0)
        self.assertEqual(ledger._mdd_safety_score(2.5, 5.0), 50.0)
        self.assertEqual(ledger._composite_score(50, 50, measure_only=False), 50.0)

    @patch.object(ledger, "ledger_path")
    def test_leader_b0_side_by_side(self, mock_path) -> None:
        mock_path.return_value = self._ledger
        a = {"phase": "A", "aggregate": {"return_pace_score": 60, "composite_score": 80}}
        b = {"phase": "B0", "aggregate": {"return_pace_score": 0, "composite_score": 40}}
        lead = ledger._compute_leader(a, b)
        self.assertIsNone(lead["leader_track"])
        self.assertEqual(lead["leader_mode"], "side_by_side")

    @patch.object(ledger, "ledger_path")
    def test_leader_b1_goal_achievement(self, mock_path) -> None:
        mock_path.return_value = self._ledger
        a = {"phase": "A", "aggregate": {"return_pace_score": 50, "composite_score": 70}}
        b = {"phase": "B1", "aggregate": {"return_pace_score": 80, "composite_score": 40}}
        lead = ledger._compute_leader(a, b)
        self.assertEqual(lead["leader_track"], "B")
        self.assertEqual(lead["leader_mode"], "goal_achievement")

    @patch.object(ledger, "_read_bitget_track")
    @patch.object(ledger, "_read_stock_track")
    @patch.object(ledger, "ledger_path")
    def test_snapshot_persist_roundtrip(self, mock_path, mock_stock, mock_bitget) -> None:
        mock_path.return_value = self._ledger
        mock_stock.return_value = {
            **ledger.TRACK_A,
            "available": True,
            "forward_trades_count": 0,
            "markets": {},
            "aggregate": {
                "max_mdd_pct": 3.0,
                "avg_return_pct": 15.0,
                "return_pace_score": 37.5,
                "mdd_safety_score": 70.0,
                "composite_score": 50.5,
            },
        }
        mock_bitget.return_value = {
            **ledger.TRACK_B_DEFAULTS,
            "available": True,
            "c2_funding_complete": False,
            "forward_trades_count": 0,
            "portfolio": {},
            "aggregate": {
                "max_mdd_pct": 1.0,
                "avg_return_pct": 2.0,
                "return_pace_score": 0.0,
                "mdd_safety_score": 80.0,
                "composite_score": 40.0,
                "measure_only": True,
            },
        }
        snap = ledger.run_north_star_digest(cadence="daily", persist=True)
        self.assertIn("tracks", snap)
        self.assertTrue(snap["meta"]["show_r1_caveat"])
        self.assertTrue(snap["meta"]["show_r3_bitget_banner"])
        self.assertTrue(os.path.isfile(self._ledger))

    @patch.object(ledger, "ledger_path")
    def test_gate_g1_progression(self, mock_path) -> None:
        mock_path.return_value = self._ledger
        daily = []
        for score in [45] * 28:
            daily.append(
                {
                    "tracks": {
                        "A": {"aggregate": {"composite_score": score, "max_mdd_pct": 4}},
                    }
                }
            )
        track = {"mdd_cap_pct": 10, "aggregate": {"max_mdd_pct": 4, "composite_score": 45}}
        gate = ledger._gate_for_track("A", daily, track, forward_trade_count=50)
        self.assertEqual(gate["gate"], "G1")

    @patch.object(ledger, "ledger_path")
    def test_gate_g2_requires_trades(self, mock_path) -> None:
        mock_path.return_value = self._ledger
        daily = [{"tracks": {"A": {"aggregate": {"composite_score": 65, "max_mdd_pct": 2}}}}] * 56
        track = {"mdd_cap_pct": 10, "aggregate": {"max_mdd_pct": 2, "composite_score": 65}}
        gate_low = ledger._gate_for_track("A", daily, track, forward_trade_count=10)
        self.assertEqual(gate_low["gate"], "G1")
        self.assertTrue(any("forward_trades" in str(r) for r in gate_low.get("block_reasons", [])))
        gate_ok = ledger._gate_for_track("A", daily, track, forward_trade_count=50)
        self.assertEqual(gate_ok["gate"], "G2")

    @patch.object(ledger, "_a06_first_pass", return_value=False)
    @patch.object(ledger, "_c2_funding_pnl_complete", return_value=False)
    @patch.object(ledger, "ledger_path")
    def test_gate_g3_blocked_reasons(self, mock_path, _c2, _a06) -> None:
        mock_path.return_value = self._ledger
        daily = []
        for _ in range(84):
            daily.append({"tracks": {"B": {"aggregate": {"composite_score": 80, "max_mdd_pct": 1}}}})
        track = {"mdd_cap_pct": 5, "aggregate": {"max_mdd_pct": 1, "composite_score": 80}}
        gate = ledger._gate_for_track("B", daily, track, forward_trade_count=100)
        self.assertTrue(gate.get("g3_blocked"))
        self.assertIn("not_candidate_reason", gate)


if __name__ == "__main__":
    unittest.main()
