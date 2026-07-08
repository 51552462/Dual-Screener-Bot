"""Dynamic Hedge Cap — macro stress scaling + hedge efficacy RL tests."""
import sqlite3
from datetime import datetime, timedelta

from dynamic_hedge_cap import (
    CAP_BY_BEAR_PHASE,
    HEDGE_EFFICACY_WHIPSAW_MAX_CAP,
    apply_hedge_efficacy_rl,
    classify_bear_stress_phase,
    cap_pct_for_bear_phase,
    fetch_inverse_sleeve_realized_stats,
    resolve_dynamic_inverse_cap_pct,
)


def _ensemble_cfg_us(score=-0.28, crisis=False, vix_factor=-0.50):
    return {
        "REGIME_ENSEMBLE": {
            "markets": {
                "US": {
                    "score": score,
                    "regime": "BEAR",
                    "raw_regime": "BEAR",
                    "crisis": crisis,
                    "factor_states": {
                        "short_trend": -0.30,
                        "long_trend": -0.20,
                        "vix": vix_factor,
                        "breadth": -0.10,
                        "pri": -0.05,
                    },
                    "probs": {"BEAR": 0.55, "SIDEWAYS": 0.25, "BULL": 0.20},
                }
            }
        },
        "DOOMSDAY_DEFCON": {"level": 4},
    }


def _mock_macro(monkeypatch):
    monkeypatch.setattr(
        "dynamic_hedge_cap.fetch_inverse_sleeve_realized_stats",
        lambda market, **kwargs: {
            "verdict": "insufficient",
            "n_closed": 0,
            "weighted_ret_pct": 0.0,
            "lookback_days": 5,
        },
    )


def test_classify_bear_panic_vix():
    assert classify_bear_stress_phase(-0.10, 31.0) == "BEAR_PANIC"
    assert cap_pct_for_bear_phase("BEAR_PANIC") == 0.50


def test_classify_bear_panic_defcon():
    assert classify_bear_stress_phase(0.0, 20.0, defcon_level=2) == "BEAR_PANIC"


def test_classify_bear_accel_score():
    assert classify_bear_stress_phase(-0.40, 22.0) == "BEAR_ACCEL"
    assert cap_pct_for_bear_phase("BEAR_ACCEL") == 0.35


def test_classify_bear_accel_dd():
    assert classify_bear_stress_phase(-0.20, 22.0, dd_20d_pct=-9.0) == "BEAR_ACCEL"


def test_classify_bear_grind():
    assert classify_bear_stress_phase(-0.25, 20.0) == "BEAR_GRIND"
    assert cap_pct_for_bear_phase("BEAR_GRIND") == 0.20


def test_classify_neutral():
    assert classify_bear_stress_phase(0.05, 18.0) == "NEUTRAL"
    assert cap_pct_for_bear_phase("NEUTRAL") == 0.25


def test_classify_elevated_vix_grind_becomes_accel():
    """VIX ≥ 25 이면 완만 score 도 가속 구간 캡(35%) 적용."""
    assert classify_bear_stress_phase(-0.25, 28.0) == "BEAR_ACCEL"


def test_resolve_from_config_cache(monkeypatch):
    _mock_macro(monkeypatch)
    cfg = _ensemble_cfg_us()
    cap, meta = resolve_dynamic_inverse_cap_pct("US", cfg)
    assert meta["bear_phase"] == "BEAR_GRIND"
    assert meta["macro_cap_pct"] == CAP_BY_BEAR_PHASE["BEAR_GRIND"]
    assert cap == CAP_BY_BEAR_PHASE["BEAR_GRIND"]
    assert meta["intrinsic"] is True
    assert meta["efficacy_mode"] == "efficacy_neutral" or meta.get("rl_mode") == "rl_hedge_neutral"
    assert meta["ensemble_score"] == -0.28


def test_resolve_panic_from_config(monkeypatch):
    _mock_macro(monkeypatch)
    cfg = _ensemble_cfg_us(score=-0.50, crisis=True, vix_factor=-0.95)
    cfg["REGIME_ENSEMBLE"]["markets"]["KR"] = cfg["REGIME_ENSEMBLE"]["markets"]["US"]
    cap, meta = resolve_dynamic_inverse_cap_pct("KR", cfg)
    assert meta["bear_phase"] == "BEAR_PANIC"
    assert meta["macro_cap_pct"] == 0.50
    assert cap == 0.50


def test_efficacy_profitable_full_macro():
    final, audit = apply_hedge_efficacy_rl(
        0.50,
        {"verdict": "profitable", "n_closed": 2, "weighted_ret_pct": 3.5},
    )
    assert final == 0.50
    assert audit["efficacy_mode"] == "efficacy_full"


def test_efficacy_whipsaw_shrink_panic():
    final, audit = apply_hedge_efficacy_rl(
        0.50,
        {"verdict": "whipsaw", "n_closed": 1, "weighted_ret_pct": -2.0},
    )
    assert final == HEDGE_EFFICACY_WHIPSAW_MAX_CAP
    assert audit["efficacy_mode"] == "efficacy_whipsaw_shrink"


def test_efficacy_whipsaw_shrink_grind():
    final, _ = apply_hedge_efficacy_rl(
        0.20,
        {"verdict": "whipsaw", "n_closed": 1, "weighted_ret_pct": -1.0},
    )
    assert final == 0.10


def test_efficacy_insufficient_neutral():
    final, audit = apply_hedge_efficacy_rl(
        0.35,
        {"verdict": "insufficient", "n_closed": 0},
    )
    assert final == 0.35
    assert audit["efficacy_mode"] == "efficacy_neutral"


def test_resolve_whipsaw_overrides_panic_cap(monkeypatch):
    _mock_macro(monkeypatch)
    monkeypatch.setattr(
        "self_evolution_hedge_engine.fetch_inverse_sleeve_rl_context",
        lambda market, **kwargs: __import__(
            "self_evolution_hedge_engine", fromlist=["InverseSleeveRLContext"]
        ).InverseSleeveRLContext(
            market="US",
            n_closed=2,
            weighted_ret_pct=-4.0,
            total_invest=20000.0,
            total_net_pnl_abs=-800.0,
            verdict="whipsaw",
            sources=("forward_trades",),
        ),
    )
    cfg = _ensemble_cfg_us(score=-0.45, crisis=True, vix_factor=-0.95)
    cap, meta = resolve_dynamic_inverse_cap_pct("US", cfg)
    assert meta["macro_cap_pct"] == 0.50 or meta.get("base_cap_pct") == 0.50
    assert cap == 0.15
    assert "whipsaw" in meta.get("rl_mode", meta.get("efficacy_mode", ""))



def test_fetch_inverse_sleeve_from_sqlite(tmp_path):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE forward_trades (
            market TEXT, status TEXT, sig_type TEXT, final_ret REAL,
            invest_amount REAL, sim_kelly_invest REAL,
            entry_date TEXT, exit_date TEXT
        )
        """
    )
    today = datetime.now().strftime("%Y-%m-%d")
    conn.execute(
        """
        INSERT INTO forward_trades VALUES
        ('US','CLOSED_WIN','Dante[INVERSE_ETF]', 5.0, 10000, 10000, ?, ?),
        ('US','CLOSED_LOSS','Dante[INVERSE_ETF]', -3.0, 5000, 5000, ?, ?)
        """,
        (today, today, today, today),
    )
    conn.commit()
    conn.close()

    stats = fetch_inverse_sleeve_realized_stats("US", db_path=str(db))
    assert stats["n_closed"] == 2
    assert stats["verdict"] == "profitable"
    assert stats["weighted_ret_pct"] > 0
