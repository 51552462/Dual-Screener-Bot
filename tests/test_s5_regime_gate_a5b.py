"""A-5b: S5 regime gate — Option A (OR) BEAR/HIGH_VOL vs budget DEFENSE_ARM."""
from __future__ import annotations

import pytest

from meta_governor_consumer import resolve_defense_arm_weight

_SIG = "Dante[INVERSE_ETF]"


class TestS5RegimeGateOptionA:
    @pytest.mark.parametrize(
        "regime",
        ["BEAR", "HIGH_VOL"],
    )
    def test_regime_on_budget_off_allows_s5(self, regime: str):
        cfg = {
            "WEIGHT_S5": 1.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": False,
            "ENABLE_S5_REGIME_GATE": True,
        }
        out = resolve_defense_arm_weight("KR", regime, _SIG, cfg)
        assert out > 0.0

    @pytest.mark.parametrize(
        "regime",
        ["BULL", "SIDEWAYS", "UNKNOWN"],
    )
    def test_regime_off_budget_off_blocks_s5(self, regime: str):
        cfg = {
            "WEIGHT_S5": 1.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": False,
            "ENABLE_S5_REGIME_GATE": True,
        }
        assert resolve_defense_arm_weight("KR", regime, _SIG, cfg) == pytest.approx(0.0)

    def test_budget_on_bull_regime_still_allows_s5(self):
        cfg = {
            "WEIGHT_S5": 1.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_US": True,
            "ENABLE_S5_REGIME_GATE": True,
        }
        out = resolve_defense_arm_weight("US", "BULL", _SIG, cfg)
        assert out == pytest.approx(1.0)

    def test_regime_gate_disabled_budget_only_a5a(self):
        """ENABLE_S5_REGIME_GATE=False → BEAR alone does not open S5."""
        cfg = {
            "WEIGHT_S5": 1.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": False,
            "ENABLE_S5_REGIME_GATE": False,
        }
        assert resolve_defense_arm_weight("KR", "BEAR", _SIG, cfg) == pytest.approx(0.0)

    def test_regime_gate_disabled_budget_on_still_works(self):
        cfg = {
            "WEIGHT_S5": 1.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": True,
            "ENABLE_S5_REGIME_GATE": False,
        }
        assert resolve_defense_arm_weight("KR", "BULL", _SIG, cfg) == pytest.approx(1.0)

    def test_high_vol_clamps_to_spec_bounds(self):
        cfg = {
            "WEIGHT_S5": 2.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": False,
            "ENABLE_S5_REGIME_GATE": True,
        }
        out = resolve_defense_arm_weight("KR", "HIGH_VOL", _SIG, cfg)
        assert out == pytest.approx(1.55)
