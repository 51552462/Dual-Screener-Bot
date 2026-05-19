"""doomsday_bridge — DEFCON 매핑·격상 알림 게이트."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from doomsday_bridge import (
    build_defcon_block,
    defcon_level_from_payload,
    maybe_send_defcon_escalation_alert,
)


class TestDefconMapping(unittest.TestCase):
    def test_doomsday_regime_level_1_or_2(self):
        p = {"regime": "DOOMSDAY", "scores": {"Global_Contagion_Score": 90, "KR_Doom_Score": 50}}
        self.assertEqual(defcon_level_from_payload(p), 1)
        p2 = {"regime": "DOOMSDAY", "scores": {"Global_Contagion_Score": 72, "KR_Doom_Score": 50}}
        self.assertEqual(defcon_level_from_payload(p2), 2)

    def test_bull_is_5(self):
        p = {"regime": "BULL", "scores": {"Global_Contagion_Score": 30, "KR_Doom_Score": 20}}
        self.assertEqual(defcon_level_from_payload(p), 5)

    def test_build_block_has_level(self):
        p = {"regime": "ELEVATED", "scores": {"Global_Contagion_Score": 55, "KR_Doom_Score": 45}}
        blk = build_defcon_block(p, 4)
        self.assertEqual(blk["level"], 4)
        self.assertEqual(blk["regime"], "ELEVATED")


class TestEscalationAlert(unittest.TestCase):
    @patch("doomsday_bridge._send_telegram_async")
    @patch("doomsday_bridge.update_system_config")
    def test_critical_escalation_sends(self, _upd, send):
        blk = build_defcon_block(
            {"regime": "DOOMSDAY", "scores": {"Global_Contagion_Score": 80}},
            2,
        )
        ok = maybe_send_defcon_escalation_alert(5, 2, blk, cfg={"DOOMSDAY_ALERT_STATE": {}})
        self.assertTrue(ok)
        send.assert_called_once()

    @patch("doomsday_bridge._send_telegram_async")
    def test_no_alert_when_improving(self, send):
        blk = build_defcon_block({"regime": "BULL", "scores": {}}, 5)
        ok = maybe_send_defcon_escalation_alert(2, 5, blk, cfg={"DOOMSDAY_ALERT_STATE": {}})
        self.assertFalse(ok)
        send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
