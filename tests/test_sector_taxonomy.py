"""sector_taxonomy SSOT — 매핑·원시 보존·rollup."""
from __future__ import annotations

import unittest

import pandas as pd

from sector_taxonomy import (
    LEGACY_CATCHALL_KR,
    map_sector_detailed,
    map_standard_sector,
    rollup_sector_entries,
    standard_sectors_for_market,
)
from rotation_sector_filter import is_rotation_eligible_sector


class TestSectorTaxonomy(unittest.TestCase):
    def test_kr_semiconductor_bucket(self):
        self.assertEqual(map_standard_sector("반도체 장비", market="KR"), "반도체/IT")

    def test_kr_battery_split_from_energy(self):
        self.assertEqual(map_standard_sector("2차전지", market="KR"), "2차전지/배터리")

    def test_fine_grained_preserve(self):
        m = map_sector_detailed("조선", market="KR")
        self.assertTrue(m.preserved_fine or m.standard == "조선/방산")

    def test_legacy_catchall_not_rotation_eligible(self):
        self.assertFalse(is_rotation_eligible_sector(LEGACY_CATCHALL_KR))

    def test_short_unknown_label_preserved(self):
        m = map_sector_detailed("철강", market="KR")
        self.assertIn(m.standard, ("철강", "철강/소재"))

    def test_rollup_splits_sectors(self):
        df = pd.DataFrame(
            [
                {"entry_date": "2026-06-01", "sector": "반도체"},
                {"entry_date": "2026-06-01", "sector": "2차전지"},
                {"entry_date": "2026-06-02", "sector": "반도체"},
                {"entry_date": "2026-06-03", "sector": "화학"},
            ]
        )
        panel, unmapped, _ = rollup_sector_entries(df, market="KR")
        sectors = {p.sector for p in panel if p.n_entries > 0}
        self.assertIn("반도체/IT", sectors)
        self.assertTrue(len(sectors) >= 2)
        self.assertNotIn(LEGACY_CATCHALL_KR, sectors)

    def test_standard_sector_count_kr(self):
        self.assertGreaterEqual(len(standard_sectors_for_market("KR")), 14)


if __name__ == "__main__":
    unittest.main()
