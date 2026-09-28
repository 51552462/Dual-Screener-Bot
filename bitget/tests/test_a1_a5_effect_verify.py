"""A-EFFECTVERIFY-01 — read-only A-1~A-5 snapshot fixtures."""
from __future__ import annotations

import json
import sqlite3
from datetime import date

from bitget.observability.a1_a5_effect_verify_bg import collect_a1_a5_effect_snapshot


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


def _insert(path: str, ts: str, event: str, payload: dict) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO ops_events (ts_utc, component, severity, event, payload_json) VALUES (?,?,?,?,?)",
        (ts, "test", "info", event, json.dumps(payload)),
    )
    conn.commit()
    conn.close()


def test_zero_transitions_in_window(tmp_path):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _insert(
        ops,
        "2026-07-01T00:00:00Z",
        "portfolio_mdd_tier_change",
        {"from_tier": "NORMAL", "to_tier": "REDUCE", "nav": 1000, "dd_pct": 0.16},
    )
    snap = collect_a1_a5_effect_snapshot(
        date(2026, 8, 1),
        date(2026, 9, 26),
        {"ops_db_path": ops, "A1A5_EFFECT_VERIFY_ENABLED": True},
    )
    assert snap.a1.source_available is True
    assert snap.a1.value["transition_count"] == 0
    assert "0건" in snap.a1.note


def test_one_or_more_transitions_mock(tmp_path):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _insert(
        ops,
        "2026-08-15T12:00:00Z",
        "portfolio_mdd_tier_change",
        {"from_tier": "NORMAL", "to_tier": "BLOCK", "nav": 900, "dd_pct": 0.21},
    )
    _insert(
        ops,
        "2026-08-20T12:00:00Z",
        "max_leverage_clamp",
        {"market_type": "futures", "requested": 10, "cap": 5},
    )
    _insert(
        ops,
        "2026-08-20T12:01:00Z",
        "max_leverage_clamp",
        {"market_type": "spot", "requested": 10, "cap": 5},
    )
    snap = collect_a1_a5_effect_snapshot(
        date(2026, 8, 1),
        date(2026, 9, 26),
        {"ops_db_path": ops, "A1A5_EFFECT_VERIFY_ENABLED": True},
    )
    assert snap.a1.value["transition_count"] == 1
    assert snap.a1.value["max_nav_mdd_pct"] == 21.0
    assert snap.a3.source_available is True
    assert snap.a3.value["clamp_count"] == 1


def test_missing_source_null(tmp_path):
    ops = str(tmp_path / "ops.sqlite")
    _ops(ops)
    _insert(ops, "2026-08-10T00:00:00Z", "heartbeat.tick", {})
    snap = collect_a1_a5_effect_snapshot(
        date(2026, 8, 1),
        date(2026, 9, 26),
        {"ops_db_path": ops, "A1A5_EFFECT_VERIFY_ENABLED": True},
    )
    assert snap.a1.source_available is False
    assert snap.a1.value is None
    assert "로그 소스 없음" in snap.a1.note
    assert snap.a2.source_available is False
    assert snap.a5.source_available is False
