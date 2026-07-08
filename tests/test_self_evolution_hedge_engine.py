"""Self-Evolution Hedge Engine — Axis 1 intrinsic panic scaling tests."""
from self_evolution_hedge_engine import (
    EnsembleIntrinsicContext,
    classify_intrinsic_bear_phase,
    load_ensemble_intrinsic_context,
    resolve_intrinsic_base_cap_pct,
)


def _ensemble_cfg(
    *,
    score=-0.28,
    regime="BEAR",
    crisis=False,
    vix_factor=-0.50,
    short=-0.30,
    long=-0.20,
    breadth=-0.10,
):
    return {
        "REGIME_ENSEMBLE": {
            "markets": {
                "US": {
                    "score": score,
                    "regime": regime,
                    "raw_regime": regime,
                    "crisis": crisis,
                    "factor_states": {
                        "short_trend": short,
                        "long_trend": long,
                        "vix": vix_factor,
                        "breadth": breadth,
                        "pri": -0.05,
                    },
                    "probs": {"BEAR": 0.55, "SIDEWAYS": 0.25, "BULL": 0.20},
                }
            }
        },
        "DOOMSDAY_DEFCON": {"level": 4},
    }


def test_load_context_from_config():
    ctx = load_ensemble_intrinsic_context(_ensemble_cfg(), "US")
    assert ctx.source == "config"
    assert ctx.score == -0.28
    assert ctx.factor_states["vix"] == -0.50


def test_intrinsic_grind():
    ctx = EnsembleIntrinsicContext(
        market="US",
        score=-0.25,
        regime="BEAR",
        raw_regime="BEAR",
        factor_states={"vix": -0.4, "short_trend": -0.2, "long_trend": -0.1, "breadth": 0.0},
        probs={"BEAR": 0.4},
        source="config",
    )
    phase, reason = classify_intrinsic_bear_phase(ctx)
    assert phase == "BEAR_GRIND"
    assert reason == "ensemble_grind"


def test_intrinsic_panic_crisis():
    ctx = EnsembleIntrinsicContext(
        market="US",
        score=-0.30,
        regime="HIGH_VOL",
        crisis=True,
        factor_states={"vix": -0.9},
        source="config",
    )
    phase, reason = classify_intrinsic_bear_phase(ctx)
    assert phase == "BEAR_PANIC"
    assert reason == "ensemble_crisis"


def test_intrinsic_accel_score():
    ctx = EnsembleIntrinsicContext(
        market="US",
        score=-0.40,
        regime="BEAR",
        factor_states={"vix": -0.5, "short_trend": -0.2, "long_trend": -0.1},
        source="config",
    )
    phase, _ = classify_intrinsic_bear_phase(ctx)
    assert phase == "BEAR_ACCEL"


def test_intrinsic_accel_breadth():
    ctx = EnsembleIntrinsicContext(
        market="US",
        score=-0.10,
        regime="SIDEWAYS",
        factor_states={"breadth": -0.60, "vix": -0.3},
        source="config",
    )
    phase, reason = classify_intrinsic_bear_phase(ctx)
    assert phase == "BEAR_ACCEL"
    assert reason == "ensemble_breadth_collapse"


def test_resolve_intrinsic_grind_cap():
    cap, meta = resolve_intrinsic_base_cap_pct("US", _ensemble_cfg())
    assert meta["intrinsic"] is True
    assert meta["bear_phase"] == "BEAR_GRIND"
    assert cap == 0.20
    assert meta["ensemble_source"] == "config"
    assert "[내재]" in meta["summary"]


def test_resolve_intrinsic_panic_cap():
    cfg = _ensemble_cfg(score=-0.32, crisis=True, vix_factor=-0.92)
    cap, meta = resolve_intrinsic_base_cap_pct("US", cfg)
    assert meta["bear_phase"] == "BEAR_PANIC"
    assert cap == 0.50


def test_external_fallback_when_no_ensemble(monkeypatch):
    monkeypatch.setattr(
        "self_evolution_hedge_engine._fetch_vix_level",
        lambda **kwargs: 22.0,
    )
    monkeypatch.setattr(
        "self_evolution_hedge_engine._fetch_benchmark_20d_return_pct",
        lambda market: -3.0,
    )
    cap, meta = resolve_intrinsic_base_cap_pct("US", {})
    assert meta["intrinsic"] is False
    assert meta["ensemble_source"] == "external_fallback"
    assert "[폴백]" in meta["summary"]
