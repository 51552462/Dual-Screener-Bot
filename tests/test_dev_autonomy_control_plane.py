"""Deterministic report intake and routing tests — no external AI calls."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import pytest

from dev_autonomy.control_plane import (
    AutonomyEnvelope,
    ControlAction,
    ControlPlaneStore,
    NormalizedReport,
    decide_report,
    normalize_north_star_ledger,
    read_latest_bitget_digest,
)


def _report(**overrides) -> NormalizedReport:
    values = {
        "report_id": "r-1",
        "source": "test",
        "track": "A",
        "observed_at": "2026-08-30T00:00:00+00:00",
        "source_status": "OBSERVE_HOLD",
        "cursor_action": "OBSERVE_HOLD",
        "metrics": {},
        "payload_hash": "abc",
    }
    values.update(overrides)
    return NormalizedReport(**values)


def _envelope() -> AutonomyEnvelope:
    return AutonomyEnvelope.from_dict(
        {
            "envelope_id": "week-1",
            "valid_from": "2026-08-30T00:00:00Z",
            "valid_until": "2026-09-06T00:00:00Z",
            "allowed_tracks": ["A"],
            "allowed_actions": ["CURSOR_IMPLEMENT", "CLAUDE_REVIEW"],
            "max_tasks_per_day": 1,
            "require_pull_request": True,
            "allow_deploy": False,
            "allow_live": False,
            "allow_merge": False,
        }
    )


def test_uploaded_report_shape_routes_low_mdd_buffer_to_claude():
    ledger = {
        "updated_at": "2026-08-30T10:30:00Z",
        "latest": {
            "cadence": "daily",
            "tracks": {
                "A": {
                    "mdd_cap_pct": 10.0,
                    "forward_trades_count": 373,
                    "forward_book": {"open_total": 6, "closed_total": 0},
                    "aggregate": {"max_mdd_pct": 8.98, "composite_score": 4.1},
                }
            },
            "period_returns": {"A": {"total_pct": -6.25}},
        },
        "history": {"daily": [{}] * 22},
    }

    report = normalize_north_star_ledger(ledger)
    decision = decide_report(report)

    assert report.cursor_action == "RECALL_FORK"
    assert "CLOSED_COUNT_INCONSISTENT" in report.flags
    assert report.metrics["daily_n"] == 22
    assert decision.action == ControlAction.CLAUDE_REVIEW
    assert decision.reason_code == "MDD_BUFFER_LOW"
    assert decision.execution_authorized is False


def test_closed_zero_with_existing_trades_is_data_integrity_review():
    report = _report(
        metrics={
            "mdd_pct": 2.0,
            "mdd_cap_pct": 10.0,
            "closed_total": 0,
            "forward_trades_count": 373,
        },
        flags=("CLOSED_COUNT_INCONSISTENT",),
    )
    decision = decide_report(report)
    assert decision.action == ControlAction.CLAUDE_REVIEW
    assert decision.reason_code == "DATA_INTEGRITY_CLOSED_COUNT"


def test_mdd_cap_breach_halts_without_authorizing_execution():
    decision = decide_report(_report(metrics={"mdd_pct": 10.1, "mdd_cap_pct": 10.0}))
    assert decision.action == ControlAction.SAFETY_HALT
    assert decision.execution_authorized is False


def test_wait_director_is_deferred_to_weekend():
    decision = decide_report(_report(source_status="WAIT_DIRECTOR", cursor_action=""))
    assert decision.action == ControlAction.WAIT_WEEKEND


def test_terminal_ssot_with_deploy_hint_waits_for_weekend():
    decision = decide_report(_report(source_status="SUB_DONE", cursor_action="", flags=("VPS_OR_DEPLOY",)))
    assert decision.action == ControlAction.WAIT_WEEKEND
    assert decision.reason_code == "WEEKEND_OPERATION"


def test_verification_without_matching_handoff_is_quarantined():
    decision = decide_report(
        _report(
            track="IV",
            source_status="WAIT_CLAUDE_OK",
            cursor_action="",
            flags=("HANDOFF_MISSING",),
        )
    )
    assert decision.action == ControlAction.QUARANTINE
    assert decision.reason_code == "HANDOFF_MISSING"


def test_wait_cursor_requires_current_bounded_envelope():
    report = _report(source_status="WAIT_CURSOR_IMPL", cursor_action="")
    without = decide_report(report)
    with_envelope = decide_report(
        report,
        envelope=_envelope(),
        now=datetime(2026, 8, 31, tzinfo=timezone.utc),
    )

    assert without.action == ControlAction.WAIT_WEEKEND
    assert with_envelope.action == ControlAction.CURSOR_IMPLEMENT
    assert with_envelope.execution_authorized is False


@pytest.mark.parametrize("unsafe_key", ["allow_deploy", "allow_live", "allow_merge"])
def test_envelope_rejects_unsafe_authority(unsafe_key):
    data = {
        "envelope_id": "bad",
        "valid_from": "2026-08-30T00:00:00Z",
        "valid_until": "2026-09-01T00:00:00Z",
        "allowed_tracks": ["A"],
        "allowed_actions": ["CURSOR_IMPLEMENT"],
        "max_tasks_per_day": 1,
        unsafe_key: True,
    }
    with pytest.raises(ValueError, match="cannot enable"):
        AutonomyEnvelope.from_dict(data)


def test_store_deduplicates_same_report(tmp_path):
    store = ControlPlaneStore(tmp_path / "queue.sqlite")
    report = _report()
    decision = decide_report(report)

    assert store.record(report, decision) is True
    assert store.record(report, decision) is False
    with sqlite3.connect(store.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 1


def test_reads_latest_structured_bitget_digest_without_telegram(tmp_path):
    db = tmp_path / "ops.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE ops_events ("
            "id INTEGER PRIMARY KEY, ts_utc TEXT, severity TEXT, event TEXT, payload_json TEXT)"
        )
        payload = {
            "checks": {"dna_rank": {"diagnosis": {"state": "SYNC_FAIL", "cursor_action": "REPORT_TO_CLAUDE"}}},
            "dashboard": {"problem": [{"id": "dna"}]},
        }
        conn.execute(
            "INSERT INTO ops_events VALUES (1, ?, 'INFO', ?, ?)",
            (
                "2026-08-30T10:00:00Z",
                "post_deploy_obs_digest_daily",
                json.dumps(payload),
            ),
        )

    report = read_latest_bitget_digest(db)
    assert report is not None
    assert report.cursor_action == "REPORT_TO_CLAUDE"
    assert report.metrics["problem_count"] == 1
    assert decide_report(report).action == ControlAction.CLAUDE_REVIEW
