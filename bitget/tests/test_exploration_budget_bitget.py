"""bitget.governance.exploration_budget — MAB Clock SSOT + bounded rolling read."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest import mock

import pytest


def test_exploration_budget_module_uses_clock_ssot():
    import inspect

    from bitget.governance import exploration_budget as eb

    src = inspect.getsource(eb)
    assert "datetime.now(" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_now" in src
    assert "utc_now_iso" in src
    assert "parse_utc_iso" in src
    assert "forward_exploration_budget_closed_sql" in src


def test_regime_shift_defense_window(tmp_path, monkeypatch):
    from bitget.governance import exploration_budget as eb

    saved: list[dict] = []

    def _set(key, val):
        if key == eb.STATE_KEY:
            saved.append(dict(val))

    anchor = datetime(2026, 7, 11, 12, 0, 0, tzinfo=timezone.utc)
    state = {
        "explore_pct": 0.35,
        "champion_pct": 0.65,
        "mode": "EXPANDED",
        "last_regime_key": "BEAR",
        "regime_shift_at": "2026-07-10T00:00:00+00:00",
        "updated_at": "2026-07-10T00:00:00+00:00",
    }

    with mock.patch("bitget.infra.config_manager.get_config_value", side_effect=lambda k, d=None: {
        eb.STATE_KEY: state,
        "CURRENT_REGIME_KEY": "BEAR",
    }.get(k, d)), mock.patch("bitget.infra.config_manager.set_config_value", side_effect=_set), mock.patch(
        "bitget.governance.exploration_budget.utc_now", return_value=anchor
    ), mock.patch(
        "bitget.governance.exploration_budget.compute_rolling_bucket_returns"
    ) as roll:
        out = eb.refresh_exploration_budget_state(force=False)
    roll.assert_not_called()
    assert out["mode"] == "REGIME_SHIFT_DEFENSE"
    assert out["explore_pct"] == eb.EXPLORE_DEFAULT_PCT
    assert out["defense_days_remaining"] == pytest.approx(5.5, abs=0.1)


def test_trigger_regime_shift_reset_resets_to_safe_line(monkeypatch):
    from bitget.governance import exploration_budget as eb

    saved = {}

    def _set(key, val):
        saved[key] = val

    with mock.patch("bitget.infra.config_manager.set_config_value", side_effect=_set), mock.patch(
        "bitget.forward.shared.send_telegram_msg"
    ), mock.patch("bitget.governance.exploration_budget.utc_now_iso", return_value="2026-07-11T12:00:00+00:00"):
        out = eb.trigger_regime_shift_reset(previous_regime="BULL", new_regime="BEAR", notify=False)
    assert out["mode"] == "REGIME_SHIFT_DEFENSE"
    assert out["explore_pct"] == eb.EXPLORE_DEFAULT_PCT
    assert saved[eb.STATE_KEY]["last_regime_key"] == "BEAR"
