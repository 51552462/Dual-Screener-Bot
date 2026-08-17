"""Unit tests for SIDE-ALPHA-01 SIDEWAYS MAE_SL exit lever."""
from __future__ import annotations

import os
import unittest

import pandas as pd

from side_alpha_01_exit import (
    BASE_MAE_SL,
    SIDEWAYS_MAE_SL,
    is_sideways_window,
    resolve_mae_sl_for_window,
    simulate_exit_on_bars,
)


class TestSideAlpha01Exit(unittest.TestCase):
    def test_sideways_window_detect(self):
        self.assertTrue(is_sideways_window("2015-06-01", "2016-06-30"))
        self.assertFalse(is_sideways_window("2020-10-01", "2021-11-30"))

    def test_mae_sl_gated(self):
        os.environ.pop("SIDE_ALPHA_01_EXIT", None)
        self.assertEqual(resolve_mae_sl_for_window("2015-06-01", "2016-06-30"), BASE_MAE_SL)
        os.environ["SIDE_ALPHA_01_EXIT"] = "1"
        try:
            self.assertEqual(
                resolve_mae_sl_for_window("2015-06-01", "2016-06-30"), SIDEWAYS_MAE_SL
            )
            self.assertEqual(
                resolve_mae_sl_for_window("2020-10-01", "2021-11-30"), BASE_MAE_SL
            )
        finally:
            os.environ.pop("SIDE_ALPHA_01_EXIT", None)

    def test_simulate_exit_sl_hit(self):
        # 2 bars: day1 dips -4.0 (hits -3.5 but not -4.5), day2 closes flat
        idx = pd.to_datetime(["2020-01-02", "2020-01-03"])
        future = pd.DataFrame(
            {
                "High": [100.0, 100.0],
                "Low": [96.0, 99.0],
                "Close": [99.0, 100.0],
            },
            index=idx,
        )
        ep = 100.0
        self.assertEqual(simulate_exit_on_bars(future, ep, mae_sl=-3.5), -3.5)
        # With -4.5, -4% does not stop → TIME close on last bar = 0%
        self.assertEqual(simulate_exit_on_bars(future, ep, mae_sl=-4.5), 0.0)


if __name__ == "__main__":
    unittest.main()
