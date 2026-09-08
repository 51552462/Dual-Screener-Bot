"""FAMILY-SLEEVE-DEMOTE-01 — 12키 0.25 SSOT · Kelly + Treasury 동일 테이블."""
from __future__ import annotations

import unittest

from family_sleeve_demote import (
    FAMILY_SLEEVE_DEMOTE_KEYS,
    FAMILY_SLEEVE_DEMOTE_MULT,
    apply_family_sleeve_group_mult,
    is_family_sleeve_demote_key,
)
from meta_governor_consumer import apply_meta_kelly_merge
from meta_treasury_entry_guard import (
    evaluate_meta_group_entry_gate,
    resolve_group_treasury_mult,
)

_CANON_12 = (
    "[STANDARD] B (일반)",
    "[SUPERNOVA_COSINE] RANK_A_장기매집",
    "[SUPERNOVA_COSINE] US_RANK_A_장기매집",
    "🔥 S1 (5선 관통 / 448 완전정배열)",
    "🔥 S1 (대세 추세 돌파 - 소형/테마주)",
    "🔥 S1 (대세 추세 돌파 - 우량/중견주)",
    "🔥 US S1 (5선 관통 / 448 완전정배열)",
    "🔥 S4 (역배열 바닥 탈출 - 우량/중견주)",
    "🔥 S4 (역배열 바닥 탈출 - 초소형 텐배거)",
    "🔥",
    "👑",
    "💎",
)

_WAS_ZERO = (
    "🔥 US S1 (5선 관통 / 448 완전정배열)",
    "[STANDARD] B (일반)",
    "💎",
    "[SUPERNOVA_COSINE] US_RANK_A_장기매집",
)


class TestFamilySleeveTable(unittest.TestCase):
    def test_canon_12_in_ssot(self):
        for k in _CANON_12:
            self.assertTrue(is_family_sleeve_demote_key(k), k)

    def test_seed_s6_excluded(self):
        self.assertFalse(is_family_sleeve_demote_key("🌱"))
        self.assertEqual(apply_family_sleeve_group_mult("🌱", 1.0), 1.0)

    def test_rank_b_unchanged(self):
        self.assertFalse(is_family_sleeve_demote_key("[SUPERNOVA_COSINE] RANK_B_중기스윙"))
        self.assertEqual(
            apply_family_sleeve_group_mult("[SUPERNOVA_COSINE] US_RANK_B_중기스윙", 1.35),
            1.35,
        )


class TestTreasuryAndKelly(unittest.TestCase):
    def test_same_module_ssot(self):
        import family_sleeve_demote as a
        import meta_governor_consumer as b
        import meta_treasury_entry_guard as c

        self.assertIs(a.FAMILY_SLEEVE_DEMOTE_KEYS, FAMILY_SLEEVE_DEMOTE_KEYS)
        self.assertIs(b.apply_family_sleeve_group_mult, apply_family_sleeve_group_mult)
        self.assertIs(c.apply_family_sleeve_group_mult, apply_family_sleeve_group_mult)

    def test_twelve_keys_treasury_0_25_and_gate_open(self):
        meta = {
            "META_TREASURY_MODE": "NORMAL",
            "META_OPERATOR_FLAGS": {},
            "META_REGIME_ACTION": {},
            "META_GROUP_KELLY_MULT": {k: 0.0 for k in _CANON_12},
            "META_STRATEGY_HEALTH": {},
            "META_GLOBAL_KELLY_MULT": 1.0,
            "META_REGIME_KEY": "BULL",
        }
        for k in _CANON_12:
            mult, src = resolve_group_treasury_mult(meta, k, market="US")
            self.assertEqual(mult, FAMILY_SLEEVE_DEMOTE_MULT, k)
            self.assertEqual(src, "FAMILY_SLEEVE_DEMOTE", k)
            ev = evaluate_meta_group_entry_gate(meta, k, market="US", sys_config={})
            self.assertFalse(ev["block_entry"], k)
            self.assertEqual(ev["group_mult"], 0.25, k)

    def test_was_zero_four_keys_enterable(self):
        meta = {
            "META_TREASURY_MODE": "NORMAL",
            "META_OPERATOR_FLAGS": {},
            "META_REGIME_ACTION": {},
            "META_GROUP_KELLY_MULT": {k: 0.0 for k in _WAS_ZERO},
            "META_STRATEGY_HEALTH": {},
        }
        for k in _WAS_ZERO:
            ev = evaluate_meta_group_entry_gate(meta, k, market="US", sys_config={})
            self.assertFalse(ev["block_entry"], k)
            self.assertGreater(ev["group_mult"], 0.0, k)

    def test_kelly_override_not_multiply(self):
        meta = {
            "META_GLOBAL_KELLY_MULT": 1.0,
            "META_REGIME_KEY": "BULL",
            "META_GROUP_KELLY_MULT": {"🔥 US S1 (5선 관통 / 448 완전정배열)": 0.0},
            "META_REGIME_ACTION": {"kelly_cap": 1.0, "kelly_floor": 0.0},
        }
        out = apply_meta_kelly_merge(
            0.02,
            meta,
            ns_prefix="US_",
            core_group_name="🔥 US S1 (5선 관통 / 448 완전정배열)",
        )
        self.assertAlmostEqual(out, 0.02 * 0.25)

    def test_rank_b_kelly_passthrough(self):
        meta = {
            "META_GLOBAL_KELLY_MULT": 1.0,
            "META_REGIME_KEY": "BULL",
            "META_GROUP_KELLY_MULT": {"[SUPERNOVA_COSINE] US_RANK_B_중기스윙": 1.35},
            "META_REGIME_ACTION": {"kelly_cap": 1.0, "kelly_floor": 0.0},
        }
        out = apply_meta_kelly_merge(
            0.02,
            meta,
            ns_prefix="US_",
            core_group_name="[SUPERNOVA_COSINE] US_RANK_B_중기스윙",
        )
        self.assertAlmostEqual(out, 0.02 * 1.35)


if __name__ == "__main__":
    unittest.main()
