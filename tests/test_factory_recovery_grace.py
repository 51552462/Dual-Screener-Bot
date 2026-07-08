"""P0-4 factory recovery grace × BEAR regime tests."""
import pytest


@pytest.fixture(autouse=True)
def _clear_recovery_env(monkeypatch):
    monkeypatch.delenv("FACTORY_RECOVERY_GRACE", raising=False)
    monkeypatch.delenv("REPORT_STALENESS_RECOVERY_GRACE", raising=False)


def test_grace_env_on_but_bear_enforces_penalties(monkeypatch):
    monkeypatch.setenv("FACTORY_RECOVERY_GRACE", "1")
    monkeypatch.setattr(
        "bear_defense_booster_guard.resolve_meta_regime_key",
        lambda cfg: "BEAR",
    )
    from factory_recovery_grace import practitioner_penalties_relaxed

    assert practitioner_penalties_relaxed({}) is False


def test_grace_env_on_bull_allows_relaxed(monkeypatch):
    monkeypatch.setenv("FACTORY_RECOVERY_GRACE", "1")
    monkeypatch.setattr(
        "bear_defense_booster_guard.resolve_meta_regime_key",
        lambda cfg: "BULL",
    )
    from factory_recovery_grace import practitioner_penalties_relaxed

    assert practitioner_penalties_relaxed({}) is True


def test_high_vol_grace_still_allowed_by_p0_4_spec(monkeypatch):
    """P0-4 spec: BEAR only — HIGH_VOL + grace env may still relax (explicit boundary)."""
    monkeypatch.setenv("FACTORY_RECOVERY_GRACE", "1")
    monkeypatch.setattr(
        "bear_defense_booster_guard.resolve_meta_regime_key",
        lambda cfg: "HIGH_VOL",
    )
    from factory_recovery_grace import practitioner_penalties_relaxed

    assert practitioner_penalties_relaxed({}) is True


def test_bear_blocks_config_relaxed_default(monkeypatch):
    """BEAR must override PRACTITIONER_PENALTIES_RELAXED default (1)."""
    monkeypatch.setattr(
        "bear_defense_booster_guard.resolve_meta_regime_key",
        lambda cfg: "BEAR",
    )
    from factory_recovery_grace import practitioner_penalties_relaxed

    assert practitioner_penalties_relaxed({"PRACTITIONER_PENALTIES_RELAXED": 1}) is False


def test_recovery_grace_blocked_helper():
    from bear_defense_booster_guard import recovery_grace_blocked_by_regime

    assert recovery_grace_blocked_by_regime("BEAR")
    assert not recovery_grace_blocked_by_regime("HIGH_VOL")
    assert not recovery_grace_blocked_by_regime("BULL")
