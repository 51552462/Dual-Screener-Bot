"""A-4: 비대칭 히스테리시스 Adapter — resolve_transition_hysteresis_days."""
from __future__ import annotations

import pytest

from predictive_regime_ensemble import (
    _apply_hysteresis,
    resolve_transition_hysteresis_days,
)

_RISK_EXPANDING = (
    ("BULL", "SIDEWAYS"),
    ("BULL", "HIGH_VOL"),
    ("BULL", "BEAR"),
    ("SIDEWAYS", "HIGH_VOL"),
    ("SIDEWAYS", "BEAR"),
    ("HIGH_VOL", "BEAR"),
)

_RISK_REDUCING = (
    ("BEAR", "HIGH_VOL"),
    ("BEAR", "SIDEWAYS"),
    ("BEAR", "BULL"),
    ("HIGH_VOL", "SIDEWAYS"),
    ("HIGH_VOL", "BULL"),
    ("SIDEWAYS", "BULL"),
)


class TestAsymmetricHysteresisAdapter:
    @pytest.mark.parametrize("frm,to", _RISK_EXPANDING)
    def test_hysteresis_risk_expanding_override_1day(self, frm: str, to: str):
        assert resolve_transition_hysteresis_days(frm, to, rl_days=4) == 1

    @pytest.mark.parametrize("frm,to", _RISK_REDUCING)
    def test_hysteresis_risk_reducing_uses_rl_value(self, frm: str, to: str):
        assert resolve_transition_hysteresis_days(frm, to, rl_days=4) == 4

    def test_unknown_treated_as_reducing_fallback(self):
        assert resolve_transition_hysteresis_days("BULL", "UNKNOWN", rl_days=3) == 3
        assert resolve_transition_hysteresis_days("UNKNOWN", "BEAR", rl_days=3) == 3

    def test_asymmetric_hysteresis_killswitch_off(self):
        assert (
            resolve_transition_hysteresis_days("BULL", "BEAR", rl_days=4, enabled=False)
            == 4
        )

    def test_crisis_synced_bypasses_adapter(self):
        """force_immediate=True — crisis/crisis_synced 즉시 전환 회귀."""
        hyst = {"KR": {"current": "BULL", "candidate": "BULL", "streak": 0}}
        final, rec = _apply_hysteresis(
            hyst, "KR", "BEAR", 99, force_immediate=True
        )
        assert final == "BEAR"
        assert rec["current"] == "BEAR"

    def test_expanding_commits_in_one_streak_with_adapter(self):
        n = resolve_transition_hysteresis_days("BULL", "BEAR", rl_days=5)
        assert n == 1
        hyst: dict = {}
        final, _ = _apply_hysteresis(hyst, "KR", "BEAR", n)
        assert final == "BEAR"
