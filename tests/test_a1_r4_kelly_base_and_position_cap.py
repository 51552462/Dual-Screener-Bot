"""A-1-R4: DYNAMIC_KELLY_RISK + MAX_POSITION_PCT falsy 0.0 read — Kelly sizing chain."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from meta_governor_consumer import (
    effective_max_position_pct,
    resolve_trading_kelly_base,
)
from performance_budget_governor import resolve_config_float


class TestDynamicKellyRiskFalsy:
    def test_dynamic_kelly_risk_zero_not_misread(self):
        cfg = {"DYNAMIC_KELLY_RISK": 0.0}
        assert resolve_config_float(cfg, "DYNAMIC_KELLY_RISK", default=0.01) == 0.0

    def test_resolve_trading_kelly_base_fallback_zero(self):
        """Graceful path 실패 시 DYNAMIC_KELLY_RISK=0.0 → 0.0 (or 0.01 오독 금지)."""
        cfg = {"DYNAMIC_KELLY_RISK": 0.0}
        with patch(
            "regime_kelly_failsafe.resolve_graceful_base_kelly",
            side_effect=RuntimeError("forced fallback"),
        ):
            out = resolve_trading_kelly_base(cfg, meta=None)
        assert out == pytest.approx(0.0)


class TestMaxPositionPctFalsy:
    def test_max_position_pct_zero_not_misread(self):
        cfg = {"MAX_POSITION_PCT": 0.0}
        assert resolve_config_float(cfg, "MAX_POSITION_PCT", default=0.25) == 0.0

    def test_effective_max_position_pct_zero(self):
        cfg = {"MAX_POSITION_PCT": 0.0}
        assert effective_max_position_pct(cfg, meta=None) == pytest.approx(0.0)

    def test_effective_max_position_pct_meta_min_with_zero_base(self):
        cfg = {"MAX_POSITION_PCT": 0.0}
        meta = {"META_MAX_POSITION_PCT": 0.15}
        assert effective_max_position_pct(cfg, meta) == pytest.approx(0.0)

    def test_effective_max_position_pct_meta_tighter_than_sys(self):
        cfg = {"MAX_POSITION_PCT": 0.25}
        meta = {"META_MAX_POSITION_PCT": 0.10}
        assert effective_max_position_pct(cfg, meta) == pytest.approx(0.10)
