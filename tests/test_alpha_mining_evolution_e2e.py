"""E2E smoke — alpha mining + self-evolution pipeline wiring."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from unittest import mock

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _minimal_config() -> dict:
    return {
        "INCUBATOR_TEMPLATES": {},
        "MUTANT_GENE_POOL": {},
        "DNA_BASE_TEMPLATES": {"KR": {}, "US": {}},
        "LINUCB_BANDIT_STATE": {},
    }


def _sample_bandit_summary() -> dict:
    return {
        "updated": 3,
        "apoptosis_removed": 1,
        "markets": {
            "KR": {
                "arms": 2,
                "updated": 2,
                "apoptosis": {
                    "removed": ["WEAK_KR_01"],
                    "vaccines": [{"name": "WEAK_KR_01", "registered": True}],
                    "freed_min_weight": 0.05,
                    "freed_slots": 1,
                },
            },
            "US": {
                "arms": 1,
                "updated": 1,
                "apoptosis": {
                    "removed": [],
                    "vaccines": [],
                    "freed_min_weight": 0.0,
                    "freed_slots": 0,
                },
            },
        },
    }


class TestSelfEvolutionPipelineSmoke(unittest.TestCase):
    def test_run_self_evolution_pipeline_wiring(self) -> None:
        from alpha_mining_orchestrator import run_self_evolution_pipeline

        cfg = _minimal_config()
        updated_cfg = dict(cfg)
        updated_cfg["MUTANT_GENE_POOL"] = {
            "MUTANT_KR_BULL_PARENT_abc123": {
                "market": "KR",
                "parent": "PARENT_A",
                "type": "MUTANT",
            },
            "INCUBATOR_SPIN_elite001": {
                "market": "KR",
                "parent": "LINEAGE_X",
                "type": "ELITE_SPINOFF",
            },
        }
        mut_logs = [
            "🔮 KR 선행 국면 예견: [BULL]",
            "▪️ KR 돌연변이 MUTANT_KR_BULL_PARENT_abc123 ← PARENT_A",
            "🌟 KR 엘리트 스핀오프 독립 승격! [INCUBATOR_SPIN_elite001]",
        ]

        sent: list[str] = []

        with mock.patch(
            "template_bandit.feed_rewards_to_bandit",
            return_value=_sample_bandit_summary(),
        ) as mock_feed, mock.patch(
            "dna_mutator.run_weekend_dna_mutation_cycle",
            return_value=(updated_cfg, mut_logs),
        ) as mock_mutate, mock.patch(
            "alpha_mining_orchestrator._persist_dna_mutation_config",
            return_value=True,
        ):
            result = run_self_evolution_pipeline(
                sys_config=cfg,
                persist=False,
                send_telegram=True,
                send_fn=sent.append,
            )

        mock_feed.assert_called_once()
        mock_mutate.assert_called_once()
        self.assertTrue(result["ok"])
        self.assertIn("feed_rewards_to_bandit", result["steps"])
        self.assertIn("check_apoptosis", result["steps"])
        self.assertIn("register_failed_template", result["steps"])
        self.assertIn("run_weekend_dna_mutation_cycle", result["steps"])

        apo = result["steps"]["check_apoptosis"]
        self.assertEqual(apo["removed_total"], 1)
        self.assertIn("WEAK_KR_01", apo["removed_names"])
        self.assertEqual(apo["vaccines_registered"], 1)

        dna_step = result["steps"]["run_weekend_dna_mutation_cycle"]
        self.assertIn("MUTANT_KR_BULL_PARENT_abc123", dna_step["mutants_created"])
        self.assertIn("INCUBATOR_SPIN_elite001", dna_step["elite_spinoffs"])
        self.assertTrue(sent)
        self.assertIn("주말 자가진화", sent[0])

    def test_evolution_digest_message_shape(self) -> None:
        from evolution_digest import build_weekend_self_evolution_digest_messages

        pipeline_result = {
            "ok": True,
            "started_at": "2026-07-26T04:00:00",
            "steps": {
                "feed_rewards_to_bandit": _sample_bandit_summary(),
                "check_apoptosis": {
                    "removed_total": 1,
                    "removed_names": ["WEAK_KR_01"],
                    "vaccines_registered": 1,
                    "vaccines_failed": 0,
                    "by_market": {
                        "KR": {"removed": ["WEAK_KR_01"], "vaccines_registered": 1},
                    },
                },
                "register_failed_template": {"registered": 1, "failed": 0},
                "run_weekend_dna_mutation_cycle": {
                    "mutants_created": ["MUTANT_KR_BULL_x"],
                    "elite_spinoffs": ["INCUBATOR_ELITE_y"],
                    "logs": ["▪️ KR 돌연변이 MUTANT_KR_BULL_x"],
                },
            },
        }
        msgs = build_weekend_self_evolution_digest_messages(pipeline_result)
        self.assertTrue(msgs)
        joined = "\n".join(msgs)
        self.assertIn("WEAK_KR_01", joined)
        self.assertIn("MUTANT_KR_BULL_x", joined)
        self.assertIn("INCUBATOR_ELITE_y", joined)

    def test_alpha_mining_pipeline_includes_self_evolution(self) -> None:
        import alpha_mining_orchestrator as amo

        evo_stub = {
            "ok": True,
            "steps": {"feed_rewards_to_bandit": {"updated": 0}},
            "errors": [],
        }

        with mock.patch.object(amo, "acquire_orchestrator_lock", return_value=True), mock.patch.object(
            amo, "release_orchestrator_lock"
        ), mock.patch.object(
            amo, "run_self_evolution_pipeline", return_value=evo_stub
        ) as mock_evo, mock.patch.dict(
            "sys.modules",
            {
                "supernova_hunter": mock.MagicMock(
                    hunt_supernovas=mock.MagicMock(),
                    evolve_alpha_factors=mock.MagicMock(),
                )
            },
        ):
            out = amo.run_alpha_mining_pipeline()

        mock_evo.assert_called_once()
        self.assertIn("self_evolution", out["steps"])
        self.assertEqual(out["steps"]["self_evolution"], evo_stub)


class TestConfigDbImportSmoke(unittest.TestCase):
    """Syntax / import / KeyError·TypeError 없이 모듈·함수 호출 가능한지."""

    def test_module_imports(self) -> None:
        import alpha_mining_orchestrator  # noqa: F401
        import evolution_digest  # noqa: F401

    def test_system_auto_pilot_send_fn_optional(self) -> None:
        """send_weekend_self_evolution_digest — send_fn 없을 때 import 실패해도 크래시 없음."""
        from evolution_digest import send_weekend_self_evolution_digest

        ok = send_weekend_self_evolution_digest(
            {
                "ok": True,
                "started_at": "2026-07-26",
                "steps": {
                    "feed_rewards_to_bandit": _sample_bandit_summary(),
                    "check_apoptosis": {"removed_total": 0, "removed_names": []},
                    "register_failed_template": {"registered": 0, "failed": 0},
                    "run_weekend_dna_mutation_cycle": {
                        "mutants_created": ["MUTANT_x"],
                        "elite_spinoffs": [],
                        "logs": [],
                    },
                },
            },
            send_fn=lambda _m: True,
        )
        self.assertTrue(ok)

    def test_summarize_bandit_apoptosis_no_keyerror(self) -> None:
        from alpha_mining_orchestrator import _summarize_bandit_apoptosis

        summary = _summarize_bandit_apoptosis({})
        self.assertEqual(summary["removed_total"], 0)
        summary2 = _summarize_bandit_apoptosis(_sample_bandit_summary())
        self.assertEqual(summary2["removed_total"], 1)

    def test_digest_empty_pipeline_no_crash(self) -> None:
        from evolution_digest import build_weekend_self_evolution_digest_messages

        msgs = build_weekend_self_evolution_digest_messages({"steps": {}})
        self.assertEqual(msgs, [])

    def test_mining_report_json_serializable(self) -> None:
        from alpha_mining_orchestrator import run_self_evolution_pipeline

        with mock.patch(
            "template_bandit.feed_rewards_to_bandit",
            return_value={"updated": 0, "markets": {}},
        ), mock.patch(
            "dna_mutator.run_weekend_dna_mutation_cycle",
            return_value=(_minimal_config(), []),
        ):
            result = run_self_evolution_pipeline(
                sys_config=_minimal_config(),
                persist=False,
                send_telegram=False,
            )
        blob = json.dumps(result, ensure_ascii=False)
        self.assertIn("feed_rewards_to_bandit", blob)


if __name__ == "__main__":
    unittest.main()
