"""PIL — 프로필 윈도우 · Vitality · 페널티 브리지."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta

import pandas as pd
import pytz

from practitioner_intelligence import compute_vitality
from practitioner_market_profiles import (
    extract_rank_tier,
    resolve_practitioner_profile,
)
from practitioner_penalty_bridge import apply_pil_vitality_penalties
from practitioner_intelligence import PractitionerBrief, PractitionerMarketProfile


class TestProfiles(unittest.TestCase):
    def test_kr_rank_c_short_window(self):
        p = resolve_practitioner_profile("KR", "RANK_C", {})
        self.assertEqual(p.post_mortem_window_days, 14)
        self.assertEqual(p.post_mortem_min_days, 5)

    def test_us_rank_a_long_window(self):
        p = resolve_practitioner_profile("US", "RANK_A", {})
        self.assertEqual(p.post_mortem_window_days, 60)
        self.assertGreaterEqual(p.post_mortem_min_days, 30)

    def test_extract_rank(self):
        self.assertEqual(extract_rank_tier("[SUPERNOVA] RANK_C_단기"), "RANK_C")


class TestVitality(unittest.TestCase):
    def test_zombie_low_activity(self):
        prof = resolve_practitioner_profile("KR", "RANK_C", {})
        tz = pytz.timezone("Asia/Seoul")
        today = datetime.now(tz).date().strftime("%Y-%m-%d")
        rows = []
        for i in range(8):
            rows.append(
                {
                    "status": "OPEN",
                    "code": "005930",
                    "qty": 10,
                    "entry_date": today,
                    "exit_date": None,
                    "final_ret": 0,
                    "bars_held": 50,
                }
            )
        g_all = pd.DataFrame(rows)
        g_closed = pd.DataFrame(columns=g_all.columns)
        valid = pd.Series([True] * len(g_all))
        score, is_zombie, status, _, _, turnover, _ = compute_vitality(
            g_all,
            g_closed,
            profile=prof,
            tz_name="Asia/Seoul",
            valid_open_mask=valid,
        )
        self.assertTrue(is_zombie or score < 0.5)
        self.assertGreaterEqual(turnover, 0.0)


class TestPenaltyBridge(unittest.TestCase):
    def test_penalty_disabled(self):
        brief = PractitionerBrief(
            market="KR",
            group_key="RANK_C_TEST",
            rank_tier="RANK_C",
            profile=resolve_practitioner_profile("KR", "RANK_C", {}),
            is_zombie=True,
            vitality_score=0.2,
        )
        out = apply_pil_vitality_penalties([brief], {"PRACTITIONER_APPLY_PENALTIES": 0})
        self.assertFalse(out.get("applied"))


if __name__ == "__main__":
    unittest.main()
