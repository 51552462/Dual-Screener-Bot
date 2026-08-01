"""A-1-R1: LOCKDOWN mult=0 falsy read 회귀 — Kelly throttle + position quota."""
from __future__ import annotations

import pytest

from meta_governor_consumer import apply_meta_kelly_merge, _resolve_performance_budget_mult
from performance_budget_governor import (
    resolve_config_float,
    resolve_kelly_throttle_mult,
    resolve_max_open_positions,
    resolve_position_quota_mult,
)


class TestResolveConfigFloat:
    def test_zero_not_coerced_to_default(self):
        cfg = {"KELLY_THROTTLE_MULT_KR": 0.0}
        assert resolve_config_float(cfg, "KELLY_THROTTLE_MULT_KR", default=1.0) == 0.0

    def test_missing_key_uses_default(self):
        assert resolve_config_float({}, "KELLY_THROTTLE_MULT", default=1.0) == 1.0


class TestKellyThrottleLockdown:
    def test_resolve_kelly_throttle_mult_zero_kr(self):
        cfg = {"KELLY_THROTTLE_MULT_KR": 0.0, "KELLY_THROTTLE_MULT": 1.0}
        assert resolve_kelly_throttle_mult(cfg, "KR") == 0.0

    def test_resolve_kelly_throttle_mult_zero_global(self):
        cfg = {"KELLY_THROTTLE_MULT": 0.0}
        assert resolve_kelly_throttle_mult(cfg) == 0.0

    def test_resolve_performance_budget_mult_zero_not_misread(self):
        cfg = {"KELLY_THROTTLE_MULT_KR": 0.0}
        assert _resolve_performance_budget_mult(cfg, {"market": "KR"}) == 0.0

    def test_lockdown_kelly_mult_zero_step4_return(self):
        """LOCKDOWN: KELLY_THROTTLE=0 → apply_meta_kelly_merge(meta=None) ≈ 0."""
        cfg = {"KELLY_THROTTLE_MULT_KR": 0.0}
        out = apply_meta_kelly_merge(
            0.01,
            None,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
        )
        assert out == 0.0


class TestQuotaMultLockdownRegression:
    def test_position_quota_mult_zero(self):
        cfg = {"POSITION_QUOTA_MULT_KR": 0.0}
        assert resolve_position_quota_mult(cfg, "KR") == 0.0
        assert resolve_max_open_positions(cfg, "KR") == 0
