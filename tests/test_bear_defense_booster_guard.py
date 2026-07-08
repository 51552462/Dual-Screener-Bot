"""P0 bear attack booster guard tests."""
from bear_defense_booster_guard import (
    clamp_bear_attack_booster_mult,
    is_analog_v_recovery_unlock,
    is_defensive_regime,
)


def test_defensive_regimes():
    assert is_defensive_regime("BEAR")
    assert is_defensive_regime("HIGH_VOL")
    assert not is_defensive_regime("BULL")
    assert not is_defensive_regime("SIDEWAYS")


def test_clamp_bear_no_unlock():
    cfg = {"REGIME_ANALOG_SCORE": {"best_episode": "EXTREME_CRASH", "front_run_favorable": False, "score": 0.9}}
    assert clamp_bear_attack_booster_mult(1.35, "BEAR", cfg) == 1.0
    assert clamp_bear_attack_booster_mult(1.35, "BULL", cfg) == 1.35
    assert clamp_bear_attack_booster_mult(0.9, "BEAR", cfg) == 0.9


def test_clamp_v_recovery_unlock(monkeypatch):
    monkeypatch.setattr(
        "regime_analog_engine.load_regime_analog",
        lambda cfg: {
            "best_episode": "V_RECOVERY",
            "front_run_favorable": True,
            "score": 0.85,
        },
    )
    assert is_analog_v_recovery_unlock({})
    assert clamp_bear_attack_booster_mult(1.35, "BEAR", {}) == 1.35


def test_clamp_v_recovery_low_score(monkeypatch):
    monkeypatch.setattr(
        "regime_analog_engine.load_regime_analog",
        lambda cfg: {
            "best_episode": "V_RECOVERY",
            "front_run_favorable": True,
            "score": 0.5,
        },
    )
    assert not is_analog_v_recovery_unlock({})
    assert clamp_bear_attack_booster_mult(1.35, "HIGH_VOL", {}) == 1.0


def test_template_bandit_ceiling_bear():
    from bear_defense_booster_guard import ceiling_template_bandit_mult

    assert ceiling_template_bandit_mult(2.0, "BEAR", {}) == 1.2
    assert ceiling_template_bandit_mult(1.5, "HIGH_VOL", {}) == 1.2
    assert ceiling_template_bandit_mult(1.1, "BEAR", {}) == 1.1
    assert ceiling_template_bandit_mult(0.5, "BEAR", {}) == 0.5
    assert ceiling_template_bandit_mult(2.0, "BULL", {}) == 2.0


def test_template_bandit_ceiling_no_v_recovery_bypass(monkeypatch):
    """P0-2: V_RECOVERY does NOT bypass 1.2 bandit ceiling (unlike genesis/ace 1.0 cap)."""
    from bear_defense_booster_guard import ceiling_template_bandit_mult

    monkeypatch.setattr(
        "regime_analog_engine.load_regime_analog",
        lambda cfg: {
            "best_episode": "V_RECOVERY",
            "front_run_favorable": True,
            "score": 0.95,
        },
    )
    assert ceiling_template_bandit_mult(2.0, "BEAR", {}) == 1.2


def test_prebuy_advantage_blocked_in_defensive():
    from bear_defense_booster_guard import allow_prebuy_advantage_boost

    assert not allow_prebuy_advantage_boost("BEAR")
    assert not allow_prebuy_advantage_boost("HIGH_VOL")
    assert allow_prebuy_advantage_boost("BULL")
    assert allow_prebuy_advantage_boost("SIDEWAYS")
