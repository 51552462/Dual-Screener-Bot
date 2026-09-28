"""A5-EVENTLOG-01 — ops_events instrumentation (gate results unchanged)."""
from __future__ import annotations

import json
import sqlite3

from bitget.infra import ops_logger
from bitget.infra.config_bounds import validate_config_write_reject
from bitget.trading.execution_safety import (
    ExecutionGateOutcome,
    evaluate_gross_notional_gate,
    evaluate_portfolio_mdd_gate,
    resolve_max_leverage,
)
from bitget.trading.tail_risk_gate import process_tail_fund_drawdown_on_snap


def _ops(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE ops_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            component TEXT NOT NULL,
            severity TEXT NOT NULL,
            event TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.commit()
    conn.close()


def _patch_ops(monkeypatch, tmp_path, ops: str) -> None:
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops)
    monkeypatch.setattr(ops_logger, "OPS_HEALTH_DB_PATH", ops)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    monkeypatch.setenv("A1A5_EVENT_LOG_ENABLED", "true")


def _count(path: str, event: str) -> int:
    conn = sqlite3.connect(path)
    n = conn.execute("SELECT COUNT(*) FROM ops_events WHERE event=?", (event,)).fetchone()[0]
    conn.close()
    return int(n)


def _payloads(path: str, event: str):
    conn = sqlite3.connect(path)
    rows = conn.execute(
        "SELECT payload_json FROM ops_events WHERE event=?", (event,)
    ).fetchall()
    conn.close()
    return [json.loads(r[0]) for r in rows]


def test_a1_transition_once_then_spam_zero(tmp_path, monkeypatch):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _patch_ops(monkeypatch, tmp_path, ops)
    cfg = {
        "PORTFOLIO_MDD_BREAKER_ENABLED": True,
        "PORTFOLIO_NAV_PEAK": 1000.0,
        "PORTFOLIO_MDD_CURRENT_TIER": "NORMAL",
        "PORTFOLIO_MDD_REDUCE_PCT": 0.15,
        "PORTFOLIO_MDD_BLOCK_PCT": 0.20,
        "PORTFOLIO_MDD_HALT_PCT": 0.30,
        "TREASURY_SPOT_USDT": 425.0,
        "TREASURY_FUTURES_USDT": 425.0,
    }
    from unittest.mock import patch

    with patch("bitget.trading.execution_safety._persist_portfolio_mdd_state", return_value=True):
        snap1 = evaluate_portfolio_mdd_gate(cfg)
        assert snap1["tier"] == "REDUCE"
        assert _count(ops, "portfolio_mdd_tier_transition") == 1
        cfg["PORTFOLIO_MDD_CURRENT_TIER"] = "REDUCE"
        snap2 = evaluate_portfolio_mdd_gate(cfg)
        assert snap2["tier"] == "REDUCE"
        assert _count(ops, "portfolio_mdd_tier_transition") == 1


def test_a2_debit_one_event(tmp_path, monkeypatch):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _patch_ops(monkeypatch, tmp_path, ops)
    cfg = {
        "TAIL_FUND_CONSUMPTION_ENABLED": True,
        "TAIL_RISK_FUND_SPOT": 80.0,
        "TAIL_RISK_FUND_FUTURES": 20.0,
        "PORTFOLIO_MDD_BLOCK_PCT": 0.20,
    }
    snap = {"tier": "BLOCK", "dd_pct": 0.25, "nav_peak": 1000.0, "nav_current": 750.0}
    from unittest.mock import patch

    with patch("bitget.trading.tail_risk_gate._persist_tail_fund_balances", return_value=True):
        out = process_tail_fund_drawdown_on_snap(cfg, snap)
    assert abs(out["debited"] - 50.0) < 1e-9
    assert _count(ops, "tail_fund_debit") == 1
    p = _payloads(ops, "tail_fund_debit")[0]
    assert p["trigger_tier"] == "BLOCK"
    assert abs(float(p["debit_amount"]) - 50.0) < 1e-9


def test_a3_clamp_one_and_zero(tmp_path, monkeypatch):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _patch_ops(monkeypatch, tmp_path, ops)
    cfg = {"MAX_LEVERAGE": 5}
    assert resolve_max_leverage(20.0, cfg) == 5.0
    assert _count(ops, "leverage_clamped") == 1
    p = _payloads(ops, "leverage_clamped")[0]
    assert p["market_type"] == "FUT"
    assert p["requested_leverage"] == 20.0
    assert resolve_max_leverage(3.0, cfg) == 3.0
    assert _count(ops, "leverage_clamped") == 1


def test_a4_block_one_event(tmp_path, monkeypatch):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _patch_ops(monkeypatch, tmp_path, ops)
    from unittest.mock import patch

    snap = {
        "blocked": True,
        "bypassed": False,
        "gross_notional_pct": 90.0,
        "max_gross_notional_pct": 80.0,
        "gross_notional": 900.0,
        "nav_current": 1000.0,
    }
    with patch("bitget.trading.execution_safety.portfolio_gross_snapshot", return_value=snap):
        gate = evaluate_gross_notional_gate({})
    assert gate.outcome == ExecutionGateOutcome.GROSS_BLOCKED
    assert _count(ops, "gross_notional_blocked") == 1


def test_a5_reject_one_event(tmp_path, monkeypatch):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _patch_ops(monkeypatch, tmp_path, ops)
    ok, reason = validate_config_write_reject("MAX_LEVERAGE", 99)
    assert ok is False
    assert reason is not None
    assert _count(ops, "config_write_rejected") == 1
    p = _payloads(ops, "config_write_rejected")[0]
    assert p["config_key"] == "MAX_LEVERAGE"
    assert p["bound_max"] == 10.0


def test_kill_switch_false_no_events_results_unchanged(tmp_path, monkeypatch):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops)
    monkeypatch.setattr(ops_logger, "OPS_HEALTH_DB_PATH", ops)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    monkeypatch.setenv("A1A5_EVENT_LOG_ENABLED", "false")
    cfg = {"MAX_LEVERAGE": 5}
    assert resolve_max_leverage(20.0, cfg) == 5.0
    ok, _ = validate_config_write_reject("MAX_LEVERAGE", 99)
    assert ok is False
    from unittest.mock import patch

    with patch(
        "bitget.trading.execution_safety.portfolio_gross_snapshot",
        return_value={
            "blocked": True,
            "bypassed": False,
            "gross_notional_pct": 90.0,
            "max_gross_notional_pct": 80.0,
            "gross_notional": 900.0,
            "nav_current": 1000.0,
        },
    ):
        gate = evaluate_gross_notional_gate({})
    assert gate.outcome == ExecutionGateOutcome.GROSS_BLOCKED
    n = sqlite3.connect(ops).execute("SELECT COUNT(*) FROM ops_events").fetchone()[0]
    assert n == 0
