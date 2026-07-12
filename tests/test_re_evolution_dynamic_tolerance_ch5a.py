"""Re-Evolution Dynamic Tolerance (1번) — ATR × 국면 동적 오차율 테스트."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from re_evolution_dynamic_tolerance import (
    REGIME_EV_TOLERANCE_WEIGHT,
    compute_dynamic_ev_tolerance_pct,
    enrich_ev_ramp_config_with_dynamic_tolerance,
    resolve_regime_ev_tolerance_weight,
)


class TestRegimeWeights(unittest.TestCase):
    def test_high_vol_weight(self):
        self.assertEqual(resolve_regime_ev_tolerance_weight("HIGH_VOL"), 1.5)

    def test_sideways_weight(self):
        self.assertEqual(resolve_regime_ev_tolerance_weight("SIDEWAYS"), 0.8)

    def test_bear_panic_weight(self):
        self.assertEqual(resolve_regime_ev_tolerance_weight("BEAR_PANIC"), 1.5)


class TestDynamicToleranceFormula(unittest.TestCase):
    @patch("re_evolution_dynamic_tolerance.fetch_market_atr14_volatility_pct")
    def test_atr_x_regime_high_vol(self, mock_atr):
        mock_atr.return_value = {
            "market": "KR",
            "atr_pct": 2.0,
            "benchmark": "^KS11",
            "source": "yfinance_benchmark",
        }
        meta = {"META_REGIME_KEY": "HIGH_VOL"}
        out = compute_dynamic_ev_tolerance_pct("KR", meta=meta)
        # 2.0% × 1.5 = 3.0%
        self.assertEqual(out["tolerance_pct"], 3.0)
        self.assertEqual(out["regime_weight"], 1.5)
        self.assertEqual(out["source"], "atr_x_regime")

    @patch("re_evolution_dynamic_tolerance.fetch_market_atr14_volatility_pct")
    def test_atr_x_regime_sideways(self, mock_atr):
        mock_atr.return_value = {
            "market": "US",
            "atr_pct": 1.5,
            "benchmark": "SPY",
            "source": "yfinance_benchmark",
        }
        meta = {"META_REGIME_KEY": "SIDEWAYS"}
        out = compute_dynamic_ev_tolerance_pct("US", meta=meta)
        # 1.5% × 0.8 = 1.2%
        self.assertEqual(out["tolerance_pct"], 1.2)
        self.assertEqual(out["regime_weight"], 0.8)

    @patch("re_evolution_dynamic_tolerance.fetch_market_atr14_volatility_pct")
    def test_clamp_max(self, mock_atr):
        mock_atr.return_value = {"market": "KR", "atr_pct": 10.0, "source": "test"}
        meta = {"META_REGIME_KEY": "HIGH_VOL"}
        out = compute_dynamic_ev_tolerance_pct("KR", meta=meta)
        # 10 × 1.5 = 15 → clamp 8
        self.assertEqual(out["tolerance_pct"], 8.0)

    @patch("re_evolution_dynamic_tolerance.fetch_market_atr14_volatility_pct")
    def test_fallback_when_atr_missing(self, mock_atr):
        mock_atr.return_value = {"market": "KR", "atr_pct": None, "source": "unavailable"}
        out = compute_dynamic_ev_tolerance_pct("KR", meta={"META_REGIME_KEY": "BULL"})
        self.assertEqual(out["source"], "fallback_fixed")
        self.assertEqual(out["tolerance_pct"], 2.5)


class TestEnrichRampConfig(unittest.TestCase):
    @patch("re_evolution_dynamic_tolerance.compute_dynamic_ev_tolerance_pct")
    def test_injects_slippage(self, mock_dyn):
        mock_dyn.return_value = {
            "tolerance_pct": 3.2,
            "regime_key": "HIGH_VOL",
            "atr_pct": 2.1,
        }
        out = enrich_ev_ramp_config_with_dynamic_tolerance(
            {"enabled": True, "slippage_tolerance_pct": 2.5},
            "KR",
            meta={"META_REGIME_KEY": "HIGH_VOL"},
        )
        self.assertEqual(out["slippage_tolerance_pct"], 3.2)
        self.assertIn("dynamic_tolerance", out)


class TestPromotionEngineBridge(unittest.TestCase):
    @patch("re_evolution_dynamic_tolerance.fetch_market_atr14_volatility_pct")
    def test_resolve_via_promotion_engine(self, mock_atr):
        from strategy_promotion_engine import resolve_live_ev_verification_tolerance

        mock_atr.return_value = {
            "market": "KR",
            "atr_pct": 2.4,
            "benchmark": "^KS11",
            "source": "yfinance_benchmark",
        }
        meta = {
            "META_REGIME_KEY": "BEAR_PANIC",
            "META_SATELLITE_INTEL": {"bear_phase": "BEAR_PANIC"},
        }
        out = resolve_live_ev_verification_tolerance("KR", meta=meta)
        # 2.4 × 1.5 = 3.6
        self.assertEqual(out["tolerance_pct"], 3.6)
        self.assertEqual(out["regime_key"], "BEAR_PANIC")


if __name__ == "__main__":
    unittest.main()
