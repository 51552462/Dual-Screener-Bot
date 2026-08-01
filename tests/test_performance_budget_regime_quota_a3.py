"""A-3: 국면연동 포지션 쿼터 — performance_budget_governor."""
from __future__ import annotations

import pytest

from performance_budget_governor import (
    DEFAULT_POSITION_QUOTA_REGIME_MAP,
    resolve_max_open_positions,
    resolve_position_quota_regime_map,
    resolve_regime_base_max_open,
)


def _cfg(
    *,
    market: str = "KR",
    regime: str = "BULL",
    mult: float = 1.0,
    us_regime: str | None = None,
    us_mult: float | None = None,
) -> dict:
    c: dict = {
        f"{market}_REGIME_KEY": regime,
        f"POSITION_QUOTA_MULT_{market}": mult,
    }
    if us_regime is not None:
        c["US_REGIME_KEY"] = us_regime
    if us_mult is not None:
        c["POSITION_QUOTA_MULT_US"] = us_mult
    return c


class TestRegimeMapSSOT:
    def test_default_map_matches_handoff(self):
        m = resolve_position_quota_regime_map({})
        assert m["BULL"] == 20
        assert m["SIDEWAYS"] == 15
        assert m["HIGH_VOL"] == 10
        assert m["BEAR"] == 8
        assert m["DEFAULT"] == 20

    def test_config_override(self):
        custom = {"BULL": 18, "DEFAULT": 20}
        m = resolve_position_quota_regime_map({"POSITION_QUOTA_REGIME_MAP": custom})
        assert m["BULL"] == 18


class TestResolveMaxOpenPositions:
    def test_bear_normal_budget_blocks_ninth(self):
        """BEAR + mult=1.0 → quota=8 → 9번째 거부 (count>=8)."""
        cfg = _cfg(market="KR", regime="BEAR", mult=1.0)
        assert resolve_max_open_positions(cfg, "KR") == 8

    def test_high_vol_normal_budget_blocks_eleventh(self):
        cfg = _cfg(market="US", regime="HIGH_VOL", mult=1.0)
        c = {"US_REGIME_KEY": "HIGH_VOL", "POSITION_QUOTA_MULT_US": 1.0}
        assert resolve_max_open_positions(c, "US") == 10

    def test_bear_defense_mult_floor_two(self):
        """BEAR + mult=0.35 → floor(8×0.35)=2."""
        cfg = _cfg(market="KR", regime="BEAR", mult=0.35)
        assert resolve_max_open_positions(cfg, "KR") == 2

    def test_lockdown_mult_zero_no_min_one(self):
        cfg = _cfg(market="KR", regime="BEAR", mult=0.0)
        assert resolve_max_open_positions(cfg, "KR") == 0

    def test_unknown_regime_fallback_twenty(self):
        cfg = {"KR_REGIME_KEY": "UNKNOWN", "POSITION_QUOTA_MULT_KR": 1.0}
        assert resolve_regime_base_max_open(cfg, "KR") == 20
        assert resolve_max_open_positions(cfg, "KR") == 20

    def test_regime_ensemble_markets_fallback(self):
        cfg = {
            "REGIME_ENSEMBLE": {"markets": {"KR": {"regime": "SIDEWAYS"}}},
            "POSITION_QUOTA_MULT_KR": 1.0,
        }
        assert resolve_max_open_positions(cfg, "KR") == 15

    def test_kr_us_regime_not_mixed(self):
        cfg = {
            "KR_REGIME_KEY": "BEAR",
            "US_REGIME_KEY": "BULL",
            "POSITION_QUOTA_MULT_KR": 1.0,
            "POSITION_QUOTA_MULT_US": 1.0,
        }
        assert resolve_max_open_positions(cfg, "KR") == 8
        assert resolve_max_open_positions(cfg, "US") == 20

    def test_multiply_not_min(self):
        """곱연산: SIDEWAYS(15) × 0.6 = 9, not min(15, ...)."""
        cfg = _cfg(market="KR", regime="SIDEWAYS", mult=0.6)
        assert resolve_max_open_positions(cfg, "KR") == 9

    def test_chop_maps_to_sideways(self):
        cfg = _cfg(market="KR", regime="CHOP", mult=1.0)
        assert resolve_max_open_positions(cfg, "KR") == 15


class TestDefaultConstant:
    def test_module_default(self):
        assert DEFAULT_POSITION_QUOTA_REGIME_MAP["BEAR"] == 8
