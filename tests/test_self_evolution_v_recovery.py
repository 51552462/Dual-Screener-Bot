"""Self-Evolution Hedge Engine — Axis 3 V-Recovery Kill Switch tests."""
from __future__ import annotations

import sqlite3

import live_nav_manager as lnm
from inverse_etf_sniper import INVERSE_SIG_MARKER, enforce_v_recovery_kill_switch
from self_evolution_hedge_engine import (
    EnsembleIntrinsicContext,
    detect_v_recovery_transition,
    evaluate_v_recovery_kill_switch,
    persist_hedge_regime_snapshot,
    reset_hedge_rl_after_v_recovery,
)


def _ensemble_cfg(*, score=0.20, short_trend=0.60):
    return {
        "HEDGE_VRECOVERY_LAST_REGIME": "BEAR",
        "REGIME_ENSEMBLE": {
            "markets": {
                "US": {
                    "score": score,
                    "regime": "BULL",
                    "raw_regime": "BULL",
                    "factor_states": {"short_trend": short_trend, "long_trend": 0.1, "vix": 0.2},
                    "probs": {"BULL": 0.6},
                }
            }
        },
    }


def test_detect_bear_to_bull_meta():
    ok, reason = detect_v_recovery_transition(
        previous_regime="BEAR",
        current_regime="BULL",
    )
    assert ok is True
    assert reason == "meta_bear_to_bull"


def test_detect_high_vol_to_bull():
    ok, reason = detect_v_recovery_transition(
        previous_regime="HIGH_VOL",
        current_regime="BULL",
    )
    assert ok is True
    assert reason == "meta_bear_to_bull"


def test_detect_prev_not_defensive():
    ok, reason = detect_v_recovery_transition(
        previous_regime="BULL",
        current_regime="BULL",
    )
    assert ok is False
    assert reason == "prev_not_defensive"


def test_detect_ensemble_bull_score_cross():
    ctx = EnsembleIntrinsicContext(
        market="US",
        score=0.22,
        regime="SIDEWAYS",
        source="config",
    )
    ok, reason = detect_v_recovery_transition(
        previous_regime="BEAR",
        current_regime="SIDEWAYS",
        ensemble_ctx=ctx,
    )
    assert ok is True
    assert reason == "ensemble_bull_score_cross"


def test_detect_short_trend_rebound():
    ctx = EnsembleIntrinsicContext(
        market="US",
        score=0.05,
        regime="SIDEWAYS",
        factor_states={"short_trend": 0.58},
        source="config",
    )
    ok, reason = detect_v_recovery_transition(
        previous_regime="HIGH_VOL",
        current_regime="SIDEWAYS",
        ensemble_ctx=ctx,
    )
    assert ok is True
    assert reason == "ensemble_short_trend_rebound"


def test_detect_analog_v_recovery(monkeypatch):
    monkeypatch.setattr(
        "bear_defense_booster_guard.is_analog_v_recovery_unlock",
        lambda cfg=None: True,
    )
    ok, reason = detect_v_recovery_transition(
        previous_regime="BEAR",
        current_regime="SIDEWAYS",
        sys_config={},
        ensemble_ctx=EnsembleIntrinsicContext(market="US", score=-0.1, regime="BEAR"),
    )
    assert ok is True
    assert reason == "analog_v_recovery"


def test_evaluate_not_triggered(monkeypatch):
    cfg = {"HEDGE_VRECOVERY_LAST_REGIME": "BEAR", "CURRENT_REGIME_KEY": "BEAR"}
    monkeypatch.setattr(
        "self_evolution_hedge_engine.resolve_current_meta_regime_key",
        lambda sys_config=None: "BEAR",
    )
    out = evaluate_v_recovery_kill_switch(cfg)
    assert out["triggered"] is False
    assert out["previous_regime"] == "BEAR"
    assert out["current_regime"] == "BEAR"


def test_evaluate_triggered_bear_to_bull(monkeypatch):
    cfg = _ensemble_cfg()
    monkeypatch.setattr(
        "self_evolution_hedge_engine.resolve_current_meta_regime_key",
        lambda sys_config=None: "BULL",
    )
    out = evaluate_v_recovery_kill_switch(cfg)
    assert out["triggered"] is True
    assert out["reason"] == "meta_bear_to_bull"


def test_reset_hedge_rl_clears_sleeve_log(tmp_path, monkeypatch):
    path = tmp_path / "treasury_state.json"
    monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(path))
    lnm.record_inverse_sleeve_closure(
        "US",
        final_ret_pct=-1.0,
        invest_amount=5000.0,
        exit_date="2026-07-08",
    )
    assert lnm.get_inverse_sleeve_rl_stats("US")["n_closed"] == 1

    persisted: dict = {}

    def _set(k, v):
        persisted[k] = v

    monkeypatch.setattr("config_manager.set_config_value", _set)
    out = reset_hedge_rl_after_v_recovery(reason="meta_bear_to_bull")
    assert lnm.get_inverse_sleeve_rl_stats("US")["n_closed"] == 0
    assert out["config_persisted"] is True
    assert "HEDGE_RL_RESET_AT" in persisted


def test_persist_hedge_regime_snapshot(monkeypatch):
    stored: dict = {}
    monkeypatch.setattr("config_manager.set_config_value", lambda k, v: stored.update({k: v}))
    cur = persist_hedge_regime_snapshot({}, current_regime="BULL")
    assert cur == "BULL"
    assert stored["HEDGE_VRECOVERY_LAST_REGIME"] == "BULL"


def _mk_inverse_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT, market TEXT, code TEXT, name TEXT,
            sig_type TEXT, entry_price REAL, invest_amount REAL,
            max_high REAL, min_low REAL, status TEXT,
            exit_date TEXT, exit_reason TEXT, final_ret REAL
        )
        """
    )
    sig = f"Dante{INVERSE_SIG_MARKER}"
    conn.execute(
        """
        INSERT INTO forward_trades
        (entry_date, market, code, name, sig_type, entry_price, invest_amount,
         max_high, min_low, status)
        VALUES ('2026-07-01', 'US', 'SQQQ', 'SQQQ', ?, 10.0, 1000.0, 10.0, 10.0, 'OPEN')
        """,
        (sig,),
    )
    conn.commit()
    return conn


def test_enforce_v_recovery_closes_and_resets_rl(tmp_path, monkeypatch):
    conn = _mk_inverse_db()
    path = tmp_path / "treasury_state.json"
    monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(path))
    lnm.record_inverse_sleeve_closure(
        "US",
        final_ret_pct=2.0,
        invest_amount=1000.0,
        exit_date="2026-07-07",
    )

    cfg = {
        "HEDGE_VRECOVERY_LAST_REGIME": "BEAR",
        "INVERSE_MODE_ACTIVE": True,
        "CURRENT_REGIME_KEY": "BULL",
    }
    released: list[tuple] = []
    monkeypatch.setattr(
        "inverse_etf_sniper._fetch_last_close",
        lambda mkt, code: 11.0,
    )
    monkeypatch.setattr(
        "inverse_etf_sniper.release_tail_amount",
        lambda mkt, amt: released.append((mkt, amt)),
    )
    monkeypatch.setattr("config_manager.set_config_value", lambda k, v: None)
    monkeypatch.setattr(
        "self_evolution_hedge_engine.resolve_current_meta_regime_key",
        lambda sys_config=None: "BULL",
    )

    out = enforce_v_recovery_kill_switch(conn, cfg)
    assert out["triggered"] is True
    assert out["closed"] == 1
    assert out["reason"] == "meta_bear_to_bull"

    row = conn.execute("SELECT status, exit_reason FROM forward_trades WHERE id=1").fetchone()
    assert row["status"] in ("CLOSED_WIN", "CLOSED_LOSS")
    assert row["exit_reason"] == "V_RECOVERY_KILL_SWITCH"
    assert released and released[0][0] == "US"
    assert lnm.get_inverse_sleeve_rl_stats("US")["n_closed"] == 0


def test_enforce_v_recovery_no_trigger_persists_regime(monkeypatch):
    conn = _mk_inverse_db()
    cfg = {"HEDGE_VRECOVERY_LAST_REGIME": "BEAR", "CURRENT_REGIME_KEY": "BEAR"}
    stored: dict = {}
    monkeypatch.setattr(
        "self_evolution_hedge_engine.resolve_current_meta_regime_key",
        lambda sys_config=None: "BEAR",
    )
    monkeypatch.setattr("config_manager.set_config_value", lambda k, v: stored.update({k: v}))

    out = enforce_v_recovery_kill_switch(conn, cfg)
    assert out["triggered"] is False
    assert out["closed"] == 0
    row = conn.execute("SELECT status FROM forward_trades WHERE id=1").fetchone()
    assert row["status"] == "OPEN"
    assert stored.get("HEDGE_VRECOVERY_LAST_REGIME") == "BEAR"
