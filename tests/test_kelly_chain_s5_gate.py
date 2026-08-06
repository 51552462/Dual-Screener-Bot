"""A-5a: S5 defense arm — Kelly Step1 gate + resolve_defense_arm_weight."""
from __future__ import annotations

import pytest

from meta_governor_consumer import (
    is_s5_sig_type,
    resolve_defense_arm_weight,
)


class TestIsS5SigType:
    def test_inverse_etf(self):
        assert is_s5_sig_type("Dante_INVERSE_ETF_Sniper[V1][INVERSE_ETF]")

    def test_blackhole(self):
        assert is_s5_sig_type("FOO_BLACKHOLE_BAR")

    def test_toxic_fade_alone_not_s5(self):
        """TOXIC_FADE 단독 — CAT-I 역이용 카운터트레이드, S5(방어arm) 스펙 밖."""
        assert not is_s5_sig_type("FOO_TOXIC_FADE_BAR")

    def test_toxic_fade_with_inverse_is_s5_via_inverse_marker(self):
        sig = "Dante_TOXIC_FADE[SEC][TOXIC_FADE][INVERSE_ETF]"
        assert is_s5_sig_type(sig)

    def test_supernova_not_s5(self):
        assert not is_s5_sig_type("SUPERNOVA_COSINE foo")


class TestResolveDefenseArmWeight:
    def test_inactive_defense_arm_returns_zero(self):
        """BULL + budget off — s5_arm_active=False → 0."""
        cfg = {
            "WEIGHT_S5": 1.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": False,
        }
        sig = "Dante[INVERSE_ETF]"
        assert resolve_defense_arm_weight("KR", "BULL", sig, cfg) == pytest.approx(0.0)

    def test_active_defense_arm_clamps_weight(self):
        cfg = {
            "WEIGHT_S5": 2.0,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_US": True,
        }
        sig = "Dante[INVERSE_ETF]"
        out = resolve_defense_arm_weight("US", "BULL", sig, cfg)
        assert out == pytest.approx(1.15)

    def test_merge_disabled_returns_neutral(self):
        cfg = {
            "ENABLE_WEIGHT_S5_MERGE": False,
            "PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": False,
        }
        sig = "Dante[INVERSE_ETF]"
        assert resolve_defense_arm_weight("KR", "BEAR", sig, cfg) == pytest.approx(1.0)

    def test_non_s5_sig_returns_one(self):
        cfg = {"PERFORMANCE_BUDGET_DEFENSE_ARM_ACTIVE_KR": False}
        assert resolve_defense_arm_weight("KR", "BEAR", "SUPERNOVA", cfg) == pytest.approx(1.0)
