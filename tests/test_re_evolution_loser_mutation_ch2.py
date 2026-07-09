"""Re-Evolution Phase 2 — Loser Mutation 테스트."""
from __future__ import annotations

import copy
import unittest
from unittest.mock import patch

from dna_mutator import (
    build_loser_child_mutant,
    crossover_dna_templates,
    diagnose_loser_from_closures,
    mutate_dna_for_failure_diagnosis,
)
from re_evolution_loser_mutation import (
    _pending_demotion_candidates,
    resolve_parent_dna_template,
    run_re_evolution_loser_mutation_cycle,
)


class TestDiagnoseLoser(unittest.TestCase):
    def test_stop_too_tight(self):
        rows = [
            {
                "final_ret": -6.0,
                "bars_held": 3,
                "exit_reason": "손절",
                "entry_price": 100,
                "max_high": 101,
                "dyn_cpv": 0.5,
            },
            {
                "final_ret": -5.5,
                "bars_held": 4,
                "exit_reason": "SL_HIT",
                "entry_price": 200,
                "max_high": 201,
                "dyn_cpv": 0.4,
            },
        ]
        d = diagnose_loser_from_closures(rows)
        self.assertEqual(d["failure_mode"], "stop_too_tight")

    def test_entry_too_aggressive(self):
        rows = [
            {
                "final_ret": -4.0,
                "bars_held": 8,
                "exit_reason": "손절",
                "dyn_cpv": 0.85,
                "entry_price": 100,
                "max_high": 103,
            },
        ]
        d = diagnose_loser_from_closures(rows)
        self.assertEqual(d["failure_mode"], "entry_too_aggressive")


class TestMutateForFailure(unittest.TestCase):
    def test_widens_stop_on_tight_sl(self):
        tpl = {
            "cpv": 1.0,
            "tb": 10.0,
            "bbe": 1.5,
            "rs": 180.0,
            "cos_cutoff": 0.78,
            "stop_loss_pct": 0.04,
        }
        out = mutate_dna_for_failure_diagnosis(
            tpl,
            {"failure_mode": "stop_too_tight"},
            rate=0.05,
        )
        self.assertGreater(float(out["stop_loss_pct"]), 0.04)
        self.assertLess(float(out["cos_cutoff"]), 0.78)


class TestCrossover(unittest.TestCase):
    def test_blend(self):
        loser = {
            "cpv": 1.0,
            "tb": 10.0,
            "bbe": 1.5,
            "rs": 180.0,
            "cos_cutoff": 0.70,
        }
        champ = {
            "cpv": 1.5,
            "tb": 15.0,
            "bbe": 2.0,
            "rs": 200.0,
            "cos_cutoff": 0.82,
        }
        out = crossover_dna_templates(loser, champ, loser_weight=0.5)
        self.assertGreater(out["cpv"], loser["cpv"])
        self.assertLess(out["cpv"], champ["cpv"])


class TestBuildChildMutant(unittest.TestCase):
    def test_re_evolution_flag(self):
        child = build_loser_child_mutant(
            {"cpv": 1.0, "tb": 10.0, "bbe": 1.5, "rs": 180.0, "cos_cutoff": 0.75},
            {"failure_mode": "bleed_streak"},
            {"cpv": 1.2, "tb": 12.0, "bbe": 1.8, "rs": 190.0, "cos_cutoff": 0.80},
        )
        self.assertTrue(child.get("re_evolution"))
        self.assertIn("mutation_kind", child)


class TestResolveParentDna(unittest.TestCase):
    def test_from_multi_pool(self):
        cfg = {
            "DNA_SUPERNOVA_KR_MULTI": {
                "RANK_A": {
                    "cpv": 1.1,
                    "tb": 11.0,
                    "bbe": 2.0,
                    "rs": 175.0,
                    "cos_cutoff": 0.77,
                }
            }
        }
        tpl, src = resolve_parent_dna_template(cfg, "KR", "RANK_A")
        self.assertEqual(tpl["cpv"], 1.1)
        self.assertIn("DNA_SUPERNOVA", src)


class TestPendingCandidates(unittest.TestCase):
    def test_filters_done(self):
        meta = {
            "META_RE_EVOLUTION_DEMOTED": [
                {"group_key": "A", "mutation_done": True},
                {"group_key": "B", "mutation_pending": True},
            ]
        }
        pending = _pending_demotion_candidates(meta)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["group_key"], "B")


class TestMutationCycle(unittest.TestCase):
    @patch("re_evolution_loser_mutation.fetch_loser_closed_rows")
    @patch("meta_governor.save_meta_governor_state_atomic")
    @patch("meta_governor_consumer.invalidate_meta_state_cache")
    @patch("meta_governor_consumer.load_meta_state_resolved")
    def test_creates_child_template(
        self,
        mock_load,
        _inv,
        _save,
        mock_fetch,
    ):
        mock_load.return_value = {
            "META_RE_EVOLUTION_DEMOTED": [
                {
                    "strategy_id": "strat:abc",
                    "market": "KR",
                    "group_key": "RANK_A",
                    "mutation_pending": True,
                }
            ],
        }
        mock_fetch.return_value = [
            {
                "final_ret": -6.0,
                "bars_held": 2,
                "exit_reason": "손절",
                "entry_price": 100,
                "max_high": 100.5,
                "dyn_cpv": 0.5,
            }
        ]
        cfg = {
            "DNA_SUPERNOVA_KR_MULTI": {
                "RANK_A": {
                    "cpv": 1.0,
                    "tb": 10.0,
                    "bbe": 1.5,
                    "rs": 180.0,
                    "cos_cutoff": 0.75,
                    "stop_loss_pct": 0.04,
                },
                "CHAMP_X": {
                    "cpv": 1.3,
                    "tb": 12.0,
                    "bbe": 1.8,
                    "rs": 190.0,
                    "cos_cutoff": 0.80,
                },
            },
            "INCUBATOR_TEMPLATES": {},
            "MUTANT_GENE_POOL": {},
        }
        updated, logs = run_re_evolution_loser_mutation_cycle(cfg)
        inc = updated.get("INCUBATOR_TEMPLATES") or {}
        re_evol = [k for k in inc if str(k).startswith("RE_EVOL_")]
        self.assertEqual(len(re_evol), 1)
        child = inc[re_evol[0]]
        self.assertTrue(child.get("re_evolution"))
        self.assertEqual(child.get("parent_group"), "RANK_A")
        self.assertTrue(any("Re-Evolution" in ln for ln in logs))


if __name__ == "__main__":
    unittest.main()
