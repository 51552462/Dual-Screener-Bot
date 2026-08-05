"""A-1-R2: META_GLOBAL_KELLY_MULT falsy 0.0 read — Kelly meta chain."""
from __future__ import annotations

import pytest

from meta_governor_consumer import apply_meta_kelly_merge
from performance_budget_governor import resolve_config_float


class TestMetaGlobalKellyMultLockdown:
    def test_meta_global_kelly_mult_zero_not_misread(self):
        meta = {"META_GLOBAL_KELLY_MULT": 0.0}
        assert (
            resolve_config_float(meta, "META_GLOBAL_KELLY_MULT", default=1.0) == 0.0
        )

    def test_lockdown_meta_global_kelly_mult_step4_return(self):
        """META_GLOBAL_KELLY_MULT=0 → apply_meta_kelly_merge meta chain ≈ 0."""
        meta = {"META_GLOBAL_KELLY_MULT": 0.0}
        cfg = {"KELLY_THROTTLE_MULT_KR": 1.0}
        out = apply_meta_kelly_merge(
            0.01,
            meta,
            ns_prefix="KR_",
            sys_config=cfg,
            entry_facts={"market": "KR"},
        )
        assert out == pytest.approx(0.0)
