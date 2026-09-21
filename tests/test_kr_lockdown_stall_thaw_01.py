"""KR-LOCKDOWN-STALL-THAW-01 — 디렉터 1회 무장 슬롯 (evaluate 한곳)."""
from __future__ import annotations

import inspect

import pytest

from performance_budget_governor import (
    compute_kr_thaw_kelly_mult,
    consume_kr_lockdown_thaw_slot,
    evaluate_performance_budget,
    is_block_new_entries,
    is_kr_lockdown_thaw_armed,
    resolve_kelly_throttle_mult,
    resolve_max_open_positions,
    sync_performance_budget_to_config_kv,
)


def _lock_nav(monkeypatch, *, kr_nav=892_000.0, kr_hwm=1_000_000.0, us_nav=1_000_000.0, us_hwm=1_000_000.0):
    def _gst(market):
        m = str(market or "").upper()
        if "US" in m:
            return {"nav": us_nav, "hwm": us_hwm}
        return {"nav": kr_nav, "hwm": kr_hwm}

    monkeypatch.setattr("performance_budget_governor.get_market_state", _gst)


class TestArmedDefaultOff:
    def test_armed_missing_is_off(self):
        assert is_kr_lockdown_thaw_armed({}) is False
        assert is_kr_lockdown_thaw_armed({"KR_LOCKDOWN_THAW_ARMED": 0}) is False

    def test_armed_zero_lockdown_unchanged(self, monkeypatch):
        _lock_nav(monkeypatch)
        cfg = {
            "ENABLE_PERFORMANCE_BUDGET_GOVERNOR": True,
            "KR_LOCKDOWN_THAW_ARMED": 0,
            "KR_LOCKDOWN_THAW_OPEN_COUNT": 0,
            "KR_REGIME_KEY": "BEAR",
        }
        ev = evaluate_performance_budget("KR", sys_config=cfg)
        assert ev["band"] == "LOCKDOWN"
        assert ev["block_new_entries"] is True
        assert ev["kelly_throttle_mult"] == 0.0
        assert ev["stall_thaw_active"] is False
        assert resolve_max_open_positions(
            {**cfg, "POSITION_QUOTA_MULT_KR": ev["position_quota_mult"]}, "KR"
        ) == 0


class TestArmedThawKrOnly:
    def test_armed_opens_one_kr_slot(self, monkeypatch):
        _lock_nav(monkeypatch)
        cfg = {
            "ENABLE_PERFORMANCE_BUDGET_GOVERNOR": True,
            "KR_LOCKDOWN_THAW_ARMED": 1,
            "KR_LOCKDOWN_THAW_OPEN_COUNT": 0,
            "KR_REGIME_KEY": "BEAR",
        }
        ev = evaluate_performance_budget("KR", sys_config=cfg)
        assert ev["band"] == "LOCKDOWN"
        assert ev["block_new_entries"] is False
        assert ev["stall_thaw_active"] is True
        f = compute_kr_thaw_kelly_mult(892_000.0, 1_000_000.0, cfg)
        assert ev["kelly_throttle_mult"] == pytest.approx(f, rel=1e-6)
        assert ev["kelly_throttle_mult_true"] == 0.0
        assert resolve_max_open_positions(
            {**cfg, "POSITION_QUOTA_MULT_KR": ev["position_quota_mult"]}, "KR"
        ) == 1

    def test_us_and_combined_stay_zero(self, monkeypatch):
        _lock_nav(monkeypatch)
        captured = {}

        def _set(key, value):
            captured[key] = value

        monkeypatch.setattr("performance_budget_governor.set_config_value", _set)
        cfg = {
            "ENABLE_PERFORMANCE_BUDGET_GOVERNOR": True,
            "KR_LOCKDOWN_THAW_ARMED": 1,
            "KR_LOCKDOWN_THAW_OPEN_COUNT": 0,
            "KR_REGIME_KEY": "BEAR",
            "US_REGIME_KEY": "BULL",
        }
        out = sync_performance_budget_to_config_kv(sys_config=cfg)
        assert out["KR"]["stall_thaw_active"] is True
        assert out["KR"]["kelly_throttle_mult"] > 0
        assert out["US"]["block_new_entries"] is False
        assert out["US"]["kelly_throttle_mult"] == 1.0
        assert out["combined_kelly_throttle_mult"] == 0.0
        assert captured.get("KELLY_THROTTLE_MULT") == 0.0
        assert captured.get("PERFORMANCE_BUDGET_BLOCK_NEW_ENTRIES_US") is False
        assert captured.get("KELLY_THROTTLE_MULT_US") == 1.0

    def test_open_ge_1_skips_thaw(self, monkeypatch):
        _lock_nav(monkeypatch)
        cfg = {
            "ENABLE_PERFORMANCE_BUDGET_GOVERNOR": True,
            "KR_LOCKDOWN_THAW_ARMED": 1,
            "KR_LOCKDOWN_THAW_OPEN_COUNT": 1,
            "KR_REGIME_KEY": "BEAR",
        }
        ev = evaluate_performance_budget("KR", sys_config=cfg)
        assert ev["stall_thaw_active"] is False
        assert ev["block_new_entries"] is True
        assert ev["kelly_throttle_mult"] == 0.0


class TestConsumeAndEpsilon:
    def test_consume_disarms(self, monkeypatch):
        store = {"KR_LOCKDOWN_THAW_ARMED": 1}

        def _set(key, value):
            store[key] = value

        monkeypatch.setattr("config_manager.load_system_config", lambda: store)
        monkeypatch.setattr("performance_budget_governor.set_config_value", _set)
        monkeypatch.setattr(
            "ops_logger.insert_ops_event", lambda **kwargs: None
        )
        ok = consume_kr_lockdown_thaw_slot(market="KR")
        assert ok is True
        assert store["KR_LOCKDOWN_THAW_ARMED"] == 0

    def test_consume_us_noop(self):
        assert consume_kr_lockdown_thaw_slot(market="US") is False

    def test_default_eps_0_05(self):
        f = compute_kr_thaw_kelly_mult(892_000.0, 1_000_000.0, {})
        expected = (0.05 / 100.0) * 1_000_000.0 / (892_000.0 * (15.0 / 100.0))
        assert f == pytest.approx(expected, rel=1e-9)
        assert f < 0.01


class TestLayerSignaturesAndNoSharedHole:
    def test_layer_signatures_unchanged(self):
        assert list(inspect.signature(is_block_new_entries).parameters) == [
            "sys_config",
            "market",
        ]
        assert list(inspect.signature(resolve_kelly_throttle_mult).parameters) == [
            "sys_config",
            "market",
        ]
        assert list(inspect.signature(resolve_max_open_positions).parameters)[:2] == [
            "sys_config",
            "market",
        ]

    def test_try_add_has_no_thaw_exception(self):
        import forward.shared as shared
        import inspect as ins

        src = ins.getsource(shared.try_add_virtual_position)
        assert "KR_LOCKDOWN_THAW_ARMED" not in src
        assert "stall_thaw" not in src
