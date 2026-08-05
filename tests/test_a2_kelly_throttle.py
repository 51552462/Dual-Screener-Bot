"""A-2: Kelly throttle merge chain — NORMAL tier regression + LOCKDOWN gate."""
from __future__ import annotations

import pytest

from meta_governor_consumer import apply_meta_kelly_merge
from performance_budget_governor import is_block_new_entries, resolve_kelly_throttle_mult


class TestKellyThrottleNormalTier:
    def test_normal_tier_passthrough_meta_none(self):
        cfg = {"KELLY_THROTTLE_MULT_KR": 1.0}
        out = apply_meta_kelly_merge(
            0.01,
            None,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
        )
        assert out == pytest.approx(0.01)

    def test_normal_tier_passthrough_with_meta(self):
        cfg = {"KELLY_THROTTLE_MULT_KR": 1.0}
        meta = {
            "META_GLOBAL_KELLY_MULT": 1.0,
            "META_REGIME_KEY": "BULL",
            "META_REGIME_ACTION": {"kelly_cap": 0.05, "kelly_floor": 0.0},
        }
        out = apply_meta_kelly_merge(
            0.02,
            meta,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
            core_group_name="S1",
        )
        assert out == pytest.approx(0.02)

    def test_caution_tier_half_throttle(self):
        cfg = {"KELLY_THROTTLE_MULT_KR": 0.5}
        out = apply_meta_kelly_merge(
            0.02,
            None,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
        )
        assert out == pytest.approx(0.01)

    def test_us_market_specific_throttle(self):
        cfg = {
            "KELLY_THROTTLE_MULT_KR": 1.0,
            "KELLY_THROTTLE_MULT_US": 0.7,
        }
        out = apply_meta_kelly_merge(
            0.01,
            None,
            ns_prefix="US_",
            sys_config=cfg,
            entry_facts={"market": "US"},
        )
        assert out == pytest.approx(0.007)

    def test_composite_throttle_when_no_market_key(self):
        cfg = {"KELLY_THROTTLE_MULT": 0.6}
        assert resolve_kelly_throttle_mult(cfg) == pytest.approx(0.6)


class TestKellyThrottleLockdownGate:
    def test_throttle_zero_yields_zero_kelly(self):
        cfg = {"KELLY_THROTTLE_MULT_KR": 0.0}
        meta = {"META_GLOBAL_KELLY_MULT": 1.5, "META_REGIME_KEY": "BULL"}
        out = apply_meta_kelly_merge(
            0.02,
            meta,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
            core_group_name="S1",
        )
        assert out == 0.0

    def test_block_new_entries_lockdown_gate(self):
        cfg = {
            "ENABLE_PERFORMANCE_BUDGET_GOVERNOR": True,
            "PERFORMANCE_BUDGET_BLOCK_NEW_ENTRIES_KR": True,
        }
        assert is_block_new_entries(cfg, "KR") is True
        assert is_block_new_entries(cfg, "US") is False

    def test_block_new_entries_off_when_governor_disabled(self):
        cfg = {
            "ENABLE_PERFORMANCE_BUDGET_GOVERNOR": False,
            "PERFORMANCE_BUDGET_BLOCK_NEW_ENTRIES_KR": True,
        }
        assert is_block_new_entries(cfg, "KR") is False
