"""F-GATE-01 — registry state(COOLED/RETIRED) 진입 차단."""
from __future__ import annotations

import unittest

from meta_treasury_entry_guard import (
    evaluate_meta_group_entry_gate,
    resolve_group_treasury_mult,
    resolve_registry_state_block,
)


def _meta_with_registry(rows: list[dict]) -> dict:
    return {
        "META_STRATEGY_REGISTRY": rows,
        "META_GROUP_KELLY_MULT": {},
        "META_STRATEGY_HEALTH": {"__meta__": {}},
    }


class TestResolveRegistryStateBlock(unittest.TestCase):
    def test_cooled_blocks(self):
        rows = [
            {
                "market": "KR",
                "group_key": "RANK_A",
                "state": "COOLED",
            }
        ]
        blocked, reason = resolve_registry_state_block(
            "KR", "RANK_A", registry_rows=rows
        )
        self.assertTrue(blocked)
        self.assertEqual(reason, "registry_state_block")

    def test_retired_blocks(self):
        rows = [
            {
                "market": "US",
                "group_key": "MEGA_TREND",
                "state": "RETIRED",
            }
        ]
        blocked, reason = resolve_registry_state_block(
            "US", "MEGA_TREND", registry_rows=rows
        )
        self.assertTrue(blocked)
        self.assertEqual(reason, "registry_state_block")

    def test_candidate_unblocks(self):
        rows = [
            {
                "market": "KR",
                "group_key": "RANK_A",
                "state": "CANDIDATE",
            }
        ]
        blocked, _ = resolve_registry_state_block(
            "KR", "RANK_A", registry_rows=rows
        )
        self.assertFalse(blocked)

    def test_missing_row_not_blocked(self):
        blocked, reason = resolve_registry_state_block(
            "KR",
            "UNKNOWN_GROUP",
            registry_rows=[
                {"market": "KR", "group_key": "OTHER", "state": "RETIRED"}
            ],
        )
        self.assertFalse(blocked)
        self.assertEqual(reason, "")


class TestEvaluateMetaGroupEntryGateFGate01(unittest.TestCase):
    def test_live_candidate_regression_mult_only(self):
        meta = _meta_with_registry(
            [{"market": "KR", "group_key": "RANK_A", "state": "LIVE"}]
        )
        meta["META_GROUP_KELLY_MULT"] = {"RANK_A": 0.5}
        ev = evaluate_meta_group_entry_gate(
            meta, "RANK_A", market="KR", sys_config={}
        )
        self.assertFalse(ev["block_entry"])
        self.assertEqual(ev["group_mult"], 0.5)

    def test_cooled_blocks_even_default_mult(self):
        meta = _meta_with_registry(
            [{"market": "KR", "group_key": "RANK_A", "state": "COOLED"}]
        )
        mult, source = resolve_group_treasury_mult(meta, "RANK_A", market="KR")
        self.assertEqual(mult, 1.0)
        self.assertEqual(source, "default")

        ev = evaluate_meta_group_entry_gate(
            meta, "RANK_A", market="KR", sys_config={}
        )
        self.assertTrue(ev["block_entry"])
        self.assertEqual(ev["source"], "registry_state_block")
        self.assertIn("registry_state_block", ev["reason"])

    def test_redemption_candidate_unblocks(self):
        meta = _meta_with_registry(
            [{"market": "KR", "group_key": "RANK_A", "state": "CANDIDATE"}]
        )
        ev = evaluate_meta_group_entry_gate(
            meta, "RANK_A", market="KR", sys_config={}
        )
        self.assertFalse(ev["block_entry"])

    def test_kill_switch_restores_mult_only_behavior(self):
        meta = _meta_with_registry(
            [{"market": "KR", "group_key": "RANK_A", "state": "RETIRED"}]
        )
        ev = evaluate_meta_group_entry_gate(
            meta,
            "RANK_A",
            market="KR",
            sys_config={"ENABLE_REGISTRY_STATE_ENTRY_GATE": False},
        )
        self.assertFalse(ev["block_entry"])

    def test_no_registry_row_uses_empty_group_mult_path(self):
        meta = {
            "META_STRATEGY_REGISTRY": [],
            "META_GROUP_KELLY_MULT": {},
            "META_STRATEGY_HEALTH": {"__meta__": {}},
        }
        ev = evaluate_meta_group_entry_gate(
            meta, "RANK_A", market="KR", sys_config={}
        )
        self.assertFalse(ev["block_entry"])
        self.assertEqual(ev["source"], "default")


if __name__ == "__main__":
    unittest.main()
