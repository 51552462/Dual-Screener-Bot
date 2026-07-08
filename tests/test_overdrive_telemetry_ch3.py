"""Ch.3 — 오버드라이브 텔레메트리·감사 정밀 규칙."""
from __future__ import annotations

import unittest

import pandas as pd

from overdrive_telemetry import (
    annotate_entry_overdrive_candidate,
    append_overdrive_exit_reason,
    build_overdrive_exit_tags,
    detect_overdrive_audit_anomalies,
    evaluate_overdrive_eligibility,
    summarize_overdrive_closed_day,
)


class TestOverdriveEligibility(unittest.TestCase):
    def test_eligible_at_hurdle(self):
        ev = evaluate_overdrive_eligibility(22.0, 20.0)
        self.assertTrue(ev["eligible"])
        self.assertAlmostEqual(ev["tp_boost_mult"], 1.10)

    def test_not_eligible_below(self):
        ev = evaluate_overdrive_eligibility(15.0, 20.0)
        self.assertFalse(ev["eligible"])


class TestEntryAnnotation(unittest.TestCase):
    def test_adds_candidate_tag(self):
        out = annotate_entry_overdrive_candidate(
            "SUPERNOVA [RANK_A]", 25.0, {"DYNAMIC_OD_HURDLE": 20.0}
        )
        self.assertIn("#오버드라이브후보", out)

    def test_skips_low_energy(self):
        out = annotate_entry_overdrive_candidate(
            "ORIGINAL", 8.0, {"DYNAMIC_OD_HURDLE": 20.0}
        )
        self.assertNotIn("#오버드라이브후보", out)


class TestExitTelemetry(unittest.TestCase):
    def test_loss_target_tags(self):
        tags = build_overdrive_exit_tags(
            is_overdrive_on=True,
            v_energy=25.0,
            od_hurdle=20.0,
            final_ret=-3.5,
            exit_type="STAT_MAE",
            exit_reason="수학적 MAE 장중 이탈 칼손절",
        )
        self.assertIn("#오버드라이브_가속", tags)
        self.assertIn("#오버드라이브_대상_손절", tags)

    def test_exit_reason_od_tp(self):
        rsn = append_overdrive_exit_reason(
            "수학적 MAE 장중 이탈",
            is_overdrive_on=True,
            od_hurdle=20.0,
            dyn_mfe_tp_base=10.0,
            dyn_mfe_tp_boosted=11.0,
        )
        self.assertIn("오버드라이브", rsn)
        self.assertIn("OD_TP", rsn)


class TestSummarizeDay(unittest.TestCase):
    def _df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "sig_type": "SUPERNOVA",
                    "v_energy": 12.0,
                    "final_ret": -3.0,
                    "exit_type": "STAT_MAE",
                    "exit_reason": "MAE 손절",
                    "flow_tags": "",
                },
                {
                    "sig_type": "SUPERNOVA",
                    "v_energy": 8.0,
                    "final_ret": -2.0,
                    "exit_type": "STAT_MAE",
                    "exit_reason": "MAE 손절",
                    "flow_tags": "",
                },
            ]
        )

    def test_all_loss_sl_zero_eligible(self):
        s = summarize_overdrive_closed_day(
            self._df(), sys_config={"DYNAMIC_OD_HURDLE": 20.0}
        )
        self.assertEqual(s["eligible_count"], 0)
        self.assertTrue(s["all_loss_sl_day"])

    def test_logged_counts_flow_tags(self):
        df = pd.DataFrame(
            [
                {
                    "sig_type": "SUPERNOVA",
                    "v_energy": 25.0,
                    "final_ret": -3.0,
                    "exit_type": "STAT_MAE",
                    "exit_reason": "MAE",
                    "flow_tags": "#오버드라이브_가속 #오버드라이브_대상_손절",
                }
            ]
        )
        s = summarize_overdrive_closed_day(df, od_hurdle=20.0)
        self.assertEqual(s["eligible_count"], 1)
        self.assertEqual(s["logged_count"], 1)
        self.assertEqual(s["telemetry_gap_count"], 0)


class TestAuditAnomalies(unittest.TestCase):
    def test_no_silent_on_catastrophic_all_loss(self):
        summary = {
            "n_closed": 13,
            "eligible_count": 0,
            "logged_count": 0,
            "telemetry_gap_count": 0,
            "od_hurdle": 20.0,
            "v_energy_max": 15.0,
            "supernova_closed_count": 10,
            "all_loss_sl_day": True,
        }
        codes = [
            a["code"]
            for a in detect_overdrive_audit_anomalies(
                summary, win_rate_today_pct=0.0
            )
        ]
        self.assertNotIn("OVERDRIVE_SILENT", codes)

    def test_telemetry_gap_when_eligible_unlogged(self):
        summary = {
            "n_closed": 5,
            "eligible_count": 3,
            "logged_count": 1,
            "telemetry_gap_count": 2,
            "od_hurdle": 20.0,
            "v_energy_max": 30.0,
            "supernova_closed_count": 3,
            "all_loss_sl_day": False,
        }
        codes = [
            a["code"]
            for a in detect_overdrive_audit_anomalies(summary, win_rate_today_pct=40.0)
        ]
        self.assertIn("OVERDRIVE_TELEMETRY_GAP", codes)

    def test_hurdle_stale_when_sn_high_hurdle(self):
        summary = {
            "n_closed": 8,
            "eligible_count": 0,
            "logged_count": 0,
            "telemetry_gap_count": 0,
            "od_hurdle": 30.0,
            "v_energy_max": 14.0,
            "supernova_closed_count": 6,
            "all_loss_sl_day": True,
        }
        codes = [
            a["code"]
            for a in detect_overdrive_audit_anomalies(summary, win_rate_today_pct=0.0)
        ]
        self.assertIn("OVERDRIVE_HURDLE_STALE", codes)


if __name__ == "__main__":
    unittest.main()
