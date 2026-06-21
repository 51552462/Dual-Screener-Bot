"""Fluid evolution — elastic / MAB / DNA mutator smoke tests."""
from __future__ import annotations

import unittest

from dna_mutator import mutate_dna_template, mutate_gene_value
from elastic_threshold import ElasticThreshold, ElasticThresholdState, evaluate_scout_candidate
from mab_capital_allocator import MABCapitalAllocator, blend_deathmatch_and_mab


class TestElasticThreshold(unittest.TestCase):
    def test_apply_pair_relieves_when_starvation_high(self):
        et = ElasticThreshold.from_system_config({}, market="KR")
        st_hi = et.apply_pair(0.55, 0.55, starvation=0.9, vol_proxy=1.0)
        st_lo = et.apply_pair(0.55, 0.55, starvation=0.1, vol_proxy=1.0)
        self.assertLess(st_hi.cos_cutoff, st_lo.cos_cutoff)

    def test_scout_near_miss_cosine(self):
        state = ElasticThresholdState(
            cos_cutoff=0.50,
            ml_cutoff=0.50,
            stretch_factor=1.0,
            scout_gap=0.10,
            starvation_index=0.6,
            vol_proxy=1.0,
        )
        v = evaluate_scout_candidate(
            is_pass_cosine=False,
            is_pass_ml_box=False,
            best_cos_sim=0.44,
            eff_cos_cutoff=0.50,
            ml_score=0.2,
            eff_ml_cutoff=0.50,
            state=state,
            sys_config={"ELASTIC_SCOUT_ENABLED": "1"},
        )
        self.assertTrue(v.eligible)
        self.assertEqual(v.path, "COSINE_SCOUT")


class TestMAB(unittest.TestCase):
    def test_blend_weights(self):
        out = blend_deathmatch_and_mab(
            {"A": 1.4, "B": 1.0},
            {"A": 1.0, "B": 1.2},
            exploit_weight=0.7,
        )
        self.assertAlmostEqual(out["A"], 1.4 * 0.7 + 1.0 * 0.3)
        self.assertAlmostEqual(out["B"], 1.0 * 0.7 + 1.2 * 0.3)

    def test_allocator_empty_db(self):
        alloc = MABCapitalAllocator({})
        res = alloc.compute("KR")
        self.assertEqual(res.exploit_ratio, 0.70)


class TestDNAMutator(unittest.TestCase):
    def test_mutate_within_bounds(self):
        parent = {"cpv": 1.0, "tb": 10.0, "bbe": 1.5, "rs": 180.0, "cos_cutoff": 0.75}
        child = mutate_dna_template(parent, rate=0.04)
        for k in ("cpv", "tb", "bbe", "rs", "cos_cutoff"):
            self.assertIn(k, child)
            self.assertNotEqual(child[k], parent[k])


class TestDNAMutator(unittest.TestCase):
    def test_hard_boundaries_clip_leverage(self):
        from dna_mutator import MUTATION_HARD_BOUNDARIES, apply_mutation_hard_boundaries, mutate_dna_template

        tpl = {"cpv": 1.0, "tb": 10.0, "bbe": 1.5, "rs": 180.0, "cos_cutoff": 0.75, "leverage": 5.0}
        child = mutate_dna_template(tpl, rate=0.04)
        lo, hi = MUTATION_HARD_BOUNDARIES["leverage"]
        self.assertLessEqual(child["leverage"], hi)


class TestFluidBridge(unittest.TestCase):
    def test_scout_sig_detection(self):
        from evolution.fluid_evolution_bridge import is_fluid_scout_sig

        self.assertTrue(is_fluid_scout_sig("[🔭SCOUT] COSINE_SCOUT"))
        self.assertFalse(is_fluid_scout_sig("SUPERNOVA RANK_A"))

    def test_blended_overlay_weights(self):
        from evolution.fluid_evolution_bridge import blended_overlay

        out = blended_overlay({"A": 1.4}, {"A": 1.0}, dm_weight=0.7)
        self.assertAlmostEqual(out["A"], 1.4 * 0.7 + 1.0 * 0.3)


class TestScoutCap(unittest.TestCase):
    def test_scout_cap_never_exceeds_03pct(self):
        from elastic_threshold import scout_invest_cap

        cap = scout_invest_cap({}, 100_000_000)
        self.assertLessEqual(cap, 100_000_000 * 0.003 + 1)


if __name__ == "__main__":
    unittest.main()
