"""Self-Evolution Hedge Engine — Axis 2 RL Hedge tests."""
from self_evolution_hedge_engine import (
    InverseSleeveRLContext,
    apply_rl_hedge_cap,
    fetch_inverse_sleeve_rl_context,
    resolve_self_evolution_hedge_cap_pct,
)


def test_rl_full_on_profit():
    ctx = InverseSleeveRLContext(
        market="US", n_closed=2, weighted_ret_pct=3.2, verdict="profitable",
        sources=("forward_trades",),
    )
    final, meta = apply_rl_hedge_cap(0.35, ctx)
    assert final == 0.35
    assert meta["rl_mode"] == "rl_hedge_full"


def test_rl_whipsaw_shrink_panic():
    ctx = InverseSleeveRLContext(
        market="US", n_closed=1, weighted_ret_pct=-1.5, verdict="whipsaw",
        sources=("live_nav_manager",),
    )
    final, meta = apply_rl_hedge_cap(0.50, ctx)
    assert final == 0.15
    assert meta["rl_mode"] == "rl_hedge_whipsaw_shrink"


def test_rl_neutral_insufficient():
    ctx = InverseSleeveRLContext(market="US", n_closed=0, verdict="insufficient")
    final, meta = apply_rl_hedge_cap(0.20, ctx)
    assert final == 0.20
    assert meta["rl_mode"] == "rl_hedge_neutral"


def test_fetch_rl_context_prefers_db(monkeypatch):
    monkeypatch.setattr(
        "self_evolution_hedge_engine.fetch_inverse_sleeve_realized_stats",
        lambda market, **kwargs: {
            "n_closed": 2,
            "weighted_ret_pct": 2.0,
            "total_invest": 10000.0,
            "verdict": "profitable",
        },
    )
    monkeypatch.setattr(
        "live_nav_manager.get_inverse_sleeve_rl_stats",
        lambda market, **kwargs: {"n_closed": 1, "verdict": "whipsaw"},
    )
    ctx = fetch_inverse_sleeve_rl_context("US")
    assert ctx.n_closed == 2
    assert ctx.verdict == "profitable"
    assert "forward_trades" in ctx.sources


def test_resolve_self_evolution_full_stack(monkeypatch):
    monkeypatch.setattr(
        "self_evolution_hedge_engine.fetch_inverse_sleeve_rl_context",
        lambda market, **kwargs: InverseSleeveRLContext(
            market="US", n_closed=0, verdict="insufficient",
        ),
    )
    cfg = {
        "REGIME_ENSEMBLE": {
            "markets": {
                "US": {
                    "score": -0.25,
                    "regime": "BEAR",
                    "raw_regime": "BEAR",
                    "crisis": False,
                    "factor_states": {"vix": -0.4, "short_trend": -0.2, "long_trend": -0.1},
                    "probs": {"BEAR": 0.5},
                }
            }
        },
        "DOOMSDAY_DEFCON": {"level": 4},
    }
    cap, meta = resolve_self_evolution_hedge_cap_pct("US", cfg)
    assert meta["bear_phase"] == "BEAR_GRIND"
    assert cap == 0.20
    assert meta["rl_mode"] == "rl_hedge_neutral"
