"""sector_normalize — DB 적재 전 오염 차단."""
from __future__ import annotations

import unittest

from sector_normalize import normalize_sector_for_db


class TestSectorNormalize(unittest.TestCase):
    def test_verbose_korean_business_text(self) -> None:
        raw = "[금융투자업을/영위하며]"
        out = normalize_sector_for_db(raw, market="KR")
        self.assertNotIn("영위", out)
        self.assertLessEqual(len(out), 24)

    def test_us_fallback(self) -> None:
        out = normalize_sector_for_db("x" * 40, market="US")
        self.assertEqual(out, "US/EQUITY")


if __name__ == "__main__":
    unittest.main()
