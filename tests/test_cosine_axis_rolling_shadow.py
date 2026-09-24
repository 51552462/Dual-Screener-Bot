"""US-COSINE-AXIS-01 shadow helpers — no hunter, no network."""
from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "diag_cosine_axis_rolling_shadow",
    ROOT / "scripts" / "diag_cosine_axis_rolling_shadow.py",
)
mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(mod)


class CosineAxisRollingShadowTests(unittest.TestCase):
    def test_prior_window_excludes_eval_day(self):
        by_date = {
            "2026-09-20": [np.array([1.0, 10.0, 20.0])],
            "2026-09-21": [np.array([2.0, 20.0, 40.0])],
            "2026-09-22": [np.array([99.0, 99.0, 99.0])],
        }
        dates = sorted(by_date)
        hist = mod.prior_session_rows(by_date, dates, "2026-09-22", 2)
        self.assertEqual(len(hist), 2)
        self.assertNotIn(99.0, hist[:, 0].tolist())

    def test_same_mu_sd_on_template_and_name(self):
        rows = np.array(
            [
                [0.5, 8.0, 16.0],
                [0.7, 12.0, 24.0],
            ],
            dtype=float,
        )
        mu, sd = mod.fit_mu_sd(rows)
        tpl = np.array([0.6, 10.0, 20.0])
        z_n = mod.apply_z(rows[0], mu, sd)
        z_t = mod.apply_z(tpl, mu, sd)
        self.assertEqual(z_n.shape, (3,))
        self.assertEqual(z_t.shape, (3,))
        self.assertTrue(np.allclose(mu, [0.6, 10.0, 20.0]))

    def test_stats_key_is_design_only(self):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        self.assertIn("COSINE_AXIS_STATS", src)
        self.assertNotIn("set_config_value(", src)
        self.assertNotIn("import supernova_hunter", src)

    def test_us_prop_liq_no_share_min(self):
        # $3 name, 12k shares: live $300k needs ~100k shares; $30k needs 10k.
        self.assertTrue(mod._liq_fail("US", 3.0, 12_000.0, mode="live"))
        self.assertFalse(mod._liq_fail("US", 3.0, 12_000.0, mode="prop"))

    def test_kr_prop_liq_is_usd_fx_not_krw_px(self):
        # 5만주 라이브 fail, $30k×1350 / 5만원 = 810주 → prop pass.
        self.assertTrue(mod._liq_fail("KR", 50_000.0, 40_000.0, mode="live"))
        self.assertFalse(mod._liq_fail("KR", 50_000.0, 40_000.0, mode="prop"))


if __name__ == "__main__":
    unittest.main()
