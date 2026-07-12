"""Scanner engine pool SSOT — full-scan practitioner resolve (P0-6)."""
from __future__ import annotations

import unittest


class TestScannerEnginePool(unittest.TestCase):
    def test_full_scan_pool_includes_practitioners_without_nameerror(self):
        from bitget.master_scanner import _build_engine_pool

        pool = _build_engine_pool(None)
        names = [n for n, _ in pool]
        self.assertIn("EMA5", names)
        self.assertIn("MASTER", names)
        pract = [n for n in names if n.startswith("PRACT_")]
        self.assertEqual(len(pract), 30, f"expected PRACT_01..30, got {len(pract)}")
        self.assertTrue(all(callable(fn) for _, fn in pool))

    def test_allowlist_excludes_practitioners(self):
        from bitget.master_scanner import _build_engine_pool

        pool = _build_engine_pool("ema5")
        names = [n for n, _ in pool]
        self.assertEqual(names, ["EMA5"])
        self.assertFalse(any(n.startswith("PRACT_") for n in names))

    def test_architecture_scanner_ssot(self):
        from bitget.validation.architecture_checks import check_scanner_engine_pool_ssot

        r = check_scanner_engine_pool_ssot()
        self.assertTrue(r.get("ok"), r)


if __name__ == "__main__":
    unittest.main()
