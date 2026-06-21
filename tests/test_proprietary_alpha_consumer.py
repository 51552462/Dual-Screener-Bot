"""Alpha Consumer Layer — fluid theme premium + score boost."""
from __future__ import annotations

import unittest

from proprietary_alpha_consumer import (
    FluidThemePremium,
    HiddenThemeContext,
    apply_hidden_theme_score_boost,
    calculate_fluid_theme_premium,
    load_hidden_theme_context,
)


class TestFluidThemePremium(unittest.TestCase):
    def test_bull_regime_cap_115(self):
        p = calculate_fluid_theme_premium(
            theme_confidence=1.0,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        self.assertEqual(p.regime_key, "BULL")
        self.assertAlmostEqual(p.regime_cap, 1.15)
        self.assertAlmostEqual(p.boost_mult, 1.15, places=2)

    def test_chop_regime_cap_105(self):
        p = calculate_fluid_theme_premium(
            theme_confidence=1.0,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "CHOP", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        self.assertAlmostEqual(p.regime_cap, 1.05)
        self.assertAlmostEqual(p.boost_mult, 1.05, places=2)

    def test_bear_regime_cap_102(self):
        p = calculate_fluid_theme_premium(
            theme_confidence=1.0,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "BEAR", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        self.assertAlmostEqual(p.regime_cap, 1.02)
        self.assertAlmostEqual(p.boost_mult, 1.02, places=2)

    def test_high_vol_regime_cap_102(self):
        p = calculate_fluid_theme_premium(
            theme_confidence=0.9,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "HIGH_VOL", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        self.assertAlmostEqual(p.regime_cap, 1.02)
        self.assertLessEqual(p.boost_mult, 1.02)

    def test_kelly_decay_to_identity(self):
        p = calculate_fluid_theme_premium(
            theme_confidence=0.95,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 0.5},
        )
        self.assertAlmostEqual(p.kelly_decay, 0.0)
        self.assertAlmostEqual(p.boost_mult, 1.0)

    def test_kelly_partial_decay(self):
        strong = calculate_fluid_theme_premium(
            theme_confidence=0.8,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        weak = calculate_fluid_theme_premium(
            theme_confidence=0.8,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 0.75},
        )
        self.assertGreater(strong.boost_mult, weak.boost_mult)
        self.assertGreater(weak.boost_mult, 1.0)

    def test_log_line_format(self):
        p = calculate_fluid_theme_premium(
            theme_confidence=0.8,
            match_kind="ticker",
            meta={"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        self.assertIn("🌊 [Fluid Premium]", p.log_line)
        self.assertIn("Regime: BULL", p.log_line)
        self.assertIn("Boost:", p.log_line)


class TestProprietaryAlphaConsumer(unittest.TestCase):
    def test_load_from_config(self):
        cfg = {
            "HIDDEN_SPILLOVER_THEME_US": {
                "tickers": ["AAPL", "MSFT"],
                "sector_hint": "Technology",
                "confidence": 0.8,
                "method": "toxic_inverted_dbscan",
            }
        }
        ctx = load_hidden_theme_context(cfg, "US")
        self.assertTrue(ctx.active)
        self.assertIn("AAPL", ctx.tickers)

    def test_ticker_boost_regime_capped(self):
        ctx = HiddenThemeContext(
            market="US",
            active=True,
            tickers=frozenset({"AAPL"}),
            sector_hint="Technology",
            confidence=0.95,
        )
        meta = {"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 1.0}
        boosted, mult, tag, flog = apply_hidden_theme_score_boost(
            60.0,
            ctx=ctx,
            ticker_code="AAPL",
            sector="Technology",
            market="US",
            meta=meta,
        )
        self.assertLessEqual(mult, 1.15)
        self.assertLessEqual(boosted, 60.0 * 1.15)
        self.assertGreater(boosted, 60.0)
        self.assertIn("HIDDEN_THEME", tag)
        self.assertIn("Fluid Premium", flog)

    def test_no_match_no_boost(self):
        ctx = HiddenThemeContext(
            market="US",
            active=True,
            tickers=frozenset({"AAPL"}),
            confidence=0.9,
        )
        boosted, mult, tag, _ = apply_hidden_theme_score_boost(
            70.0,
            ctx=ctx,
            ticker_code="ZZZZ",
            sector="Energy",
            market="US",
            meta={"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 1.0},
        )
        self.assertEqual(boosted, 70.0)
        self.assertEqual(mult, 1.0)
        self.assertEqual(tag, "")

    def test_chop_limits_boost(self):
        ctx = HiddenThemeContext(
            market="US",
            active=True,
            tickers=frozenset({"NVDA"}),
            confidence=0.99,
        )
        meta = {"META_REGIME_KEY": "CHOP", "META_GLOBAL_KELLY_MULT": 1.0}
        _, mult, _, _ = apply_hidden_theme_score_boost(
            80.0,
            ctx=ctx,
            ticker_code="NVDA",
            sector="",
            market="US",
            meta=meta,
        )
        self.assertLessEqual(mult, 1.05)

    def test_sector_match_weaker_than_ticker(self):
        meta = {"META_REGIME_KEY": "BULL", "META_GLOBAL_KELLY_MULT": 1.0}
        _, mult_ticker, _, _ = apply_hidden_theme_score_boost(
            50.0,
            ctx=HiddenThemeContext(
                market="US",
                active=True,
                tickers=frozenset({"NVDA"}),
                confidence=0.95,
            ),
            ticker_code="NVDA",
            sector="Technology",
            market="US",
            meta=meta,
        )
        _, mult_sector, _, _ = apply_hidden_theme_score_boost(
            50.0,
            ctx=HiddenThemeContext(
                market="US",
                active=True,
                tickers=frozenset(),
                sector_hint="Technology",
                confidence=0.95,
            ),
            ticker_code="OTHER",
            sector="Technology",
            market="US",
            meta=meta,
        )
        self.assertGreater(mult_ticker, mult_sector)


if __name__ == "__main__":
    unittest.main()
