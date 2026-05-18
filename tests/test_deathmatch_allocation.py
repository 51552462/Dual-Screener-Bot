"""P2 — 데스매치 → META_GROUP_KELLY_MULT overlay."""
from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass
from typing import Optional

from deathmatch_allocation import (
    compute_group_allocation_overlay,
    health_to_group_mult,
    merge_group_kelly_from_overlay,
    proposal_to_config_audit,
)


@dataclass
class _Arm:
    arm_id: str = "a"
    label: str = "L"
    group_key: str = "G1"
    n_valid: int = 10
    rank: int = 1
    composite_score: float = 2.0
    hurdle_passed: bool = True
    champion_eligible: bool = True
    below_floor: bool = False
    relative_exempt: bool = False


class TestGroupAllocation(unittest.TestCase):
    def test_hurdle_fail_zero_overlay(self):
        dmcfg = {"bottom_pct": 0.2, "allocation_hurdle_fail_mult": 0.0}
        good = _Arm(group_key="GOOD", rank=1)
        bad = _Arm(group_key="BAD", rank=2, hurdle_passed=False, champion_eligible=False)
        prop = compute_group_allocation_overlay(
            [good, bad], dmcfg=dmcfg, champion_group_key="GOOD"
        )
        self.assertEqual(prop["group_mult"]["BAD"], 0.0)
        self.assertGreater(prop["group_mult"]["GOOD"], 1.0)

    def test_merge_health_overlay(self):
        health = {"KR|G1": {"mult": 0.8}, "KR|G2": {"mult": 1.0}}
        hm = health_to_group_mult(health)
        merged = merge_group_kelly_from_overlay(hm, {"G1": 1.25, "G2": 0.0}, max_mult=1.5)
        self.assertAlmostEqual(merged["G1"], 1.0)
        self.assertEqual(merged["G2"], 0.0)

    def test_audit_shape(self):
        prop = {
            "group_mult": {"A": 1.25},
            "standby_groups": [],
            "boost_groups": ["A"],
            "eligible_n": 2,
        }
        audit = proposal_to_config_audit(prop)
        self.assertIn("A", audit["weight_mult"])


if __name__ == "__main__":
    unittest.main()
