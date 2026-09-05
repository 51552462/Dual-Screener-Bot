"""TRACK-A-BUGFIX-BATCH-01 — HYBRID_TECH 라벨 · entry_regime 배선."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from forward.ledger import hybrid_tech_exit_reason
from forward.shared import resolve_entry_regime


class TestHybridTechExitReason(unittest.TestCase):
    def test_win_keeps_ikjeol(self):
        self.assertEqual(hybrid_tech_exit_reason(1.63), "하이브리드 추세 이탈 익절")

    def test_loss_uses_sonjeol(self):
        self.assertEqual(hybrid_tech_exit_reason(-3.5), "하이브리드 추세 이탈 손절")

    def test_flat_neutral(self):
        self.assertEqual(hybrid_tech_exit_reason(0.0), "하이브리드 추세 이탈 청산")


class TestResolveEntryRegime(unittest.TestCase):
    def test_meta_ssot_not_unknown(self):
        with patch(
            "bear_defense_booster_guard.resolve_meta_regime_key",
            return_value="BEAR",
        ):
            self.assertEqual(resolve_entry_regime({"CURRENT_REGIME_KEY": "BULL"}), "BEAR")

    def test_config_fallback(self):
        with patch(
            "bear_defense_booster_guard.resolve_meta_regime_key",
            return_value="UNKNOWN",
        ):
            self.assertEqual(
                resolve_entry_regime({"CURRENT_REGIME_KEY": "SIDEWAYS"}),
                "SIDEWAYS",
            )


class TestNavHookUntouched(unittest.TestCase):
    def test_row_str_still_on_ledger_hook(self):
        from reports.forward_report_scalar import row_str
        import inspect
        from forward import ledger

        src = inspect.getsource(ledger)
        self.assertIn("row_str", src)
        self.assertIn("record_closure", src)
        self.assertEqual(row_str.__name__, "row_str")


if __name__ == "__main__":
    unittest.main()
