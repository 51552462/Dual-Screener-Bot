"""CAT-C BEAR-UNDERDOG-01 — BEAR×KR_UNDERDOG shadow sig_type suffix only."""
from __future__ import annotations

import unittest

from forward.shared import (
    _BEAR_UNDERDOG_SHADOW_SUFFIX,
    apply_bear_underdog_shadow_sig_type_tag,
    bear_underdog_shadow_tag_enabled,
    is_bear_underdog_shadow_row,
)
from lifecycle_observe_only import apply_lifecycle_observe_only_entry_zero_notional
from re_evolution_strike_guard import apply_shadow_entry_zero_notional


class TestBearUnderdogShadowPredicate(unittest.TestCase):
    def test_a_kr_bear_incubator_underdog_true(self):
        self.assertTrue(
            is_bear_underdog_shadow_row(
                {
                    "market": "KR",
                    "entry_regime": "BEAR",
                    "sig_type": "[INCUBATOR_KR_UNDERDOG_50점]",
                }
            )
        )

    def test_a_us_bear_incubator_underdog_false(self):
        self.assertFalse(
            is_bear_underdog_shadow_row(
                {
                    "market": "US",
                    "entry_regime": "BEAR",
                    "sig_type": "[INCUBATOR_US_UNDERDOG_50점]",
                }
            )
        )

    def test_a_kr_bull_incubator_underdog_false(self):
        self.assertFalse(
            is_bear_underdog_shadow_row(
                {
                    "market": "KR",
                    "entry_regime": "BULL",
                    "sig_type": "[INCUBATOR_KR_UNDERDOG_50점]",
                }
            )
        )

    def test_a_kr_bear_non_incubator_false(self):
        self.assertFalse(
            is_bear_underdog_shadow_row(
                {
                    "market": "KR",
                    "entry_regime": "BEAR",
                    "sig_type": "[SUPERNOVA_COSINE] RANK_A",
                }
            )
        )

    def test_a_kr_bear_incubator_without_underdog_false(self):
        self.assertFalse(
            is_bear_underdog_shadow_row(
                {
                    "market": "KR",
                    "entry_regime": "BEAR",
                    "sig_type": "[INCUBATOR_KR_COSINE_90점]",
                }
            )
        )


class TestBearUnderdogShadowTagging(unittest.TestCase):
    def test_b_preserves_incubator_prefix(self):
        base = "[INCUBATOR_KR_UNDERDOG_50점]"
        tagged = apply_bear_underdog_shadow_sig_type_tag(
            base,
            market="KR",
            entry_regime="BEAR",
            sys_config={},
        )
        self.assertTrue(tagged.startswith("[INCUBATOR_KR_UNDERDOG_50점]"))
        self.assertTrue(tagged.endswith(_BEAR_UNDERDOG_SHADOW_SUFFIX))
        self.assertIn("INCUBATOR", tagged)
        self.assertIn("UNDERDOG", tagged)

    def test_c_killswitch_off_skips_tag(self):
        base = "[INCUBATOR_KR_UNDERDOG_50점]"
        cfg = {"ENABLE_BEAR_UNDERDOG_SHADOW_TAG": False}
        self.assertFalse(bear_underdog_shadow_tag_enabled(cfg))
        tagged = apply_bear_underdog_shadow_sig_type_tag(
            base,
            market="KR",
            entry_regime="BEAR",
            sys_config=cfg,
        )
        self.assertEqual(tagged, base)

    def test_c_predicate_false_leaves_sig_unchanged(self):
        base = "[INCUBATOR_KR_UNDERDOG_50점]"
        tagged = apply_bear_underdog_shadow_sig_type_tag(
            base,
            market="US",
            entry_regime="BEAR",
            sys_config={},
        )
        self.assertEqual(tagged, base)

    def test_idempotent_suffix(self):
        once = apply_bear_underdog_shadow_sig_type_tag(
            "[INCUBATOR_KR_UNDERDOG_50점]",
            market="KR",
            entry_regime="BEAR",
            sys_config={},
        )
        twice = apply_bear_underdog_shadow_sig_type_tag(
            once,
            market="KR",
            entry_regime="BEAR",
            sys_config={},
        )
        self.assertEqual(once, twice)
        self.assertEqual(once.count(_BEAR_UNDERDOG_SHADOW_SUFFIX), 1)


class TestBearUnderdogShadowNamespaceIsolation(unittest.TestCase):
    def test_d_re_evol_shadow_untouched(self):
        shadow_sig, shares, invest, kelly = apply_shadow_entry_zero_notional(
            "[INCUBATOR_KR_UNDERDOG_50점]",
            strategy_id="sid_kr_ud",
        )
        self.assertIn("RE_EVOL_SHADOW", shadow_sig)
        self.assertNotIn("BEAR_UNDERDOG_SHADOW", shadow_sig)
        self.assertEqual((shares, invest, kelly), (0, 0.0, 0.0))

        bear_tagged = apply_bear_underdog_shadow_sig_type_tag(
            "[INCUBATOR_KR_UNDERDOG_50점]",
            market="KR",
            entry_regime="BEAR",
            sys_config={},
        )
        self.assertIn(_BEAR_UNDERDOG_SHADOW_SUFFIX, bear_tagged)
        self.assertNotIn("RE_EVOL_SHADOW", bear_tagged)
        self.assertNotIn("LIFECYCLE_OBSERVE_ONLY", bear_tagged)

    def test_d_lifecycle_observe_only_untouched(self):
        life_sig, shares, invest, kelly = apply_lifecycle_observe_only_entry_zero_notional(
            "RANK_A breakout",
            strategy_id="sid_a",
        )
        self.assertIn("LIFECYCLE_OBSERVE_ONLY", life_sig)
        self.assertNotIn("BEAR_UNDERDOG_SHADOW", life_sig)
        self.assertEqual((shares, invest, kelly), (0, 0.0, 0.0))

        bear_tagged = apply_bear_underdog_shadow_sig_type_tag(
            life_sig,
            market="KR",
            entry_regime="BEAR",
            sys_config={},
        )
        self.assertEqual(bear_tagged, life_sig)


if __name__ == "__main__":
    unittest.main()
