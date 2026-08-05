"""A-1-R3: REGIME_TRANSITION_CLUTCH.mult falsy 0.0 read — Kelly meta chain."""
from __future__ import annotations

import pytest

from meta_governor_consumer import apply_meta_kelly_merge
from performance_budget_governor import resolve_config_float


class TestRegimeTransitionClutchMult:
    def test_clutch_mult_zero_not_misread(self):
        clutch = {"active": True, "mult": 0.0}
        assert resolve_config_float(clutch, "mult", default=1.0) == 0.0

    def test_clutch_mult_applies_when_between_zero_and_one(self):
        cfg = {
            "REGIME_TRANSITION_CLUTCH": {"active": True, "mult": 0.25},
            "KELLY_THROTTLE_MULT_KR": 1.0,
        }
        meta = {"META_GLOBAL_KELLY_MULT": 1.0}
        out = apply_meta_kelly_merge(
            0.02,
            meta,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
        )
        assert out == pytest.approx(0.005)

    def test_clutch_mult_zero_does_not_coerce_to_default(self):
        """mult=0.0 must not become 1.0 — clutch branch inactive (0 < cm false)."""
        cfg = {
            "REGIME_TRANSITION_CLUTCH": {"active": True, "mult": 0.0},
            "KELLY_THROTTLE_MULT_KR": 1.0,
        }
        meta = {"META_GLOBAL_KELLY_MULT": 1.0}
        out = apply_meta_kelly_merge(
            0.02,
            meta,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
        )
        assert out == pytest.approx(0.02)

    def test_clutch_inactive_skips_mult(self):
        cfg = {
            "REGIME_TRANSITION_CLUTCH": {"active": False, "mult": 0.25},
            "KELLY_THROTTLE_MULT_KR": 1.0,
        }
        meta = {"META_GLOBAL_KELLY_MULT": 1.0}
        out = apply_meta_kelly_merge(
            0.02,
            meta,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
        )
        assert out == pytest.approx(0.02)
