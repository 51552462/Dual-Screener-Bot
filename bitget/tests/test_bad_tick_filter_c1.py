"""C-1 — bad tick / flash crash pre-try_add filter."""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from bitget.signal_engines import BadTickResult, evaluate_bad_tick


def _cfg(**overrides) -> dict:
    base = {
        "BAD_TICK_FILTER_ENABLED": True,
        "BAD_TICK_LOOKBACK_BARS": 20,
        "BAD_TICK_ATR_MULT": 6.0,
        "BAD_TICK_GAP_PCT": 0.15,
        "BAD_TICK_ACTION": "skip",
    }
    base.update(overrides)
    return base


def _steady_df(rows: int = 30, price: float = 100.0) -> pd.DataFrame:
    close = price + np.linspace(-0.5, 0.5, rows)
    return pd.DataFrame(
        {
            "Open": close,
            "High": close + 0.3,
            "Low": close - 0.3,
            "Close": close,
            "Volume": [1_000.0] * rows,
        }
    )


class TestBadTickFilterC1(unittest.TestCase):
    def test_spike_flags_bad_tick(self):
        df = _steady_df(30)
        last = df.index[-1]
        prev = df.index[-2]
        df.loc[last, "Open"] = 100.0
        df.loc[last, "Close"] = 130.0
        df.loc[last, "High"] = 135.0
        df.loc[last, "Low"] = 99.0
        df.loc[prev, "Close"] = 100.0

        result = evaluate_bad_tick("BTC_USDT", "futures", df, _cfg())
        self.assertIsInstance(result, BadTickResult)
        self.assertTrue(result.is_bad)
        self.assertGreater(result.gap_pct, 0.15)
        self.assertGreater(result.deviation_ratio, 6.0)

    def test_normal_volatility_no_false_block(self):
        df = _steady_df(40)
        result = evaluate_bad_tick("ETH_USDT", "spot", df, _cfg())
        self.assertFalse(result.is_bad)
        self.assertEqual(result.reason, "ok")

    def test_disabled_filter_soft_pass(self):
        df = _steady_df(30)
        last = df.index[-1]
        df.loc[last, "Close"] = 200.0
        df.loc[last, "High"] = 210.0
        result = evaluate_bad_tick("BTC_USDT", "futures", df, _cfg(BAD_TICK_FILTER_ENABLED=False))
        self.assertFalse(result.is_bad)
        self.assertEqual(result.reason, "disabled")

    def test_short_history_soft_pass(self):
        df = _steady_df(5)
        result = evaluate_bad_tick("BTC_USDT", "futures", df, _cfg())
        self.assertFalse(result.is_bad)
        self.assertEqual(result.reason, "soft_pass_short_history")

    def test_regression_sample_false_block_rate_low(self):
        """Normal replay: no spike bars should stay below 0.5% block rate."""
        blocked = 0
        total = 200
        for i in range(total):
            noise = np.random.default_rng(i).normal(0, 0.15, 35)
            close = 100.0 + np.cumsum(noise)
            df = pd.DataFrame(
                {
                    "Open": close,
                    "High": close + 0.25,
                    "Low": close - 0.25,
                    "Close": close,
                    "Volume": [1_000.0] * len(close),
                }
            )
            if evaluate_bad_tick("SYM", "futures", df, _cfg()).is_bad:
                blocked += 1
        rate = blocked / total
        self.assertLess(rate, 0.005, f"false block rate {rate:.2%} >= 0.5%")


if __name__ == "__main__":
    unittest.main()
