"""D-1b — weekly LLM proposal summary (read-only)."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from bitget.governance.ai_proposal_schema_bg import ensure_llm_proposals_schema
from bitget.infra import ops_logger
from bitget.observability import llm_proposal_summary_bg as bg


def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _ensure_ops_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_events (
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
    finally:
        conn.close()


def _insert_proposal(
    db_path: str,
    *,
    category: str,
    risk_class: str,
    recorded_at: str,
) -> None:
    ensure_llm_proposals_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO bitget_llm_proposals (
                recorded_at, category, risk_class, rationale, params_json, payload_json
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                recorded_at,
                category,
                risk_class,
                "test rationale",
                "{}",
                json.dumps({"category": category, "risk_class": risk_class}),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_parse_error(ops_path: str, *, ts_utc: str) -> None:
    conn = sqlite3.connect(ops_path)
    try:
        conn.execute(
            """
            INSERT INTO ops_events (ts_utc, component, severity, event, payload_json)
            VALUES (?,?,?,?,?)
            """,
            (
                ts_utc,
                "governance.ai_proposal",
                "WARN",
                "llm_proposal_parse_error",
                json.dumps({"code": "parse_error", "message": "bad json"}),
            ),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    market_db = str(tmp_path / "market.sqlite")
    ops_db = str(tmp_path / "ops.sqlite")
    sqlite3.connect(market_db).close()
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
    monkeypatch.setattr(ops_logger, "OPS_HEALTH_DB_PATH", ops_db)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    return {"market": market_db, "ops": ops_db}


class TestLlmProposalSummaryAggregation:
    def test_synthetic_rows_grouped_correctly(self, dbs) -> None:
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(days=1))
        _insert_proposal(dbs["market"], category="CAT-F", risk_class="critical", recorded_at=ts)
        _insert_proposal(dbs["market"], category="CAT-F", risk_class="critical", recorded_at=ts)
        _insert_proposal(dbs["market"], category="CAT-M", risk_class="low", recorded_at=ts)

        summary = bg.compute_llm_proposal_summary_bg(
            window_days=7,
            market_db_path=dbs["market"],
            ops_db_path=dbs["ops"],
        )

        assert summary["total_count"] == 3
        assert {g["key"]: g["count"] for g in summary["by_category"]} == {
            "CAT-F": 2,
            "CAT-M": 1,
        }
        assert {g["key"]: g["count"] for g in summary["by_risk_class"]} == {
            "critical": 2,
            "low": 1,
        }

    def test_persist_writes_weekly_summary_event(self, dbs) -> None:
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=3))
        _insert_proposal(dbs["market"], category="CAT-K", risk_class="high", recorded_at=ts)

        result = bg.run_llm_proposal_summary_job(
            window_days=7,
            market_db_path=dbs["market"],
            ops_db_path=dbs["ops"],
        )

        assert result is not None
        assert result["inserted"] is True
        assert result["summary"]["total_count"] == 1

        conn = sqlite3.connect(dbs["ops"])
        try:
            row = conn.execute(
                """
                SELECT component, event, payload_json
                FROM ops_events
                WHERE event = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                ("llm_proposal_summary_weekly",),
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == "observability.llm_proposal"
        payload = json.loads(row[2])
        assert payload["total_count"] == 1
        assert payload["by_risk_class"][0]["key"] == "high"


class TestParseErrorRate:
    def test_parse_error_rate_when_denominator_exists(self, dbs) -> None:
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=2))
        _insert_proposal(dbs["market"], category="CAT-F", risk_class="critical", recorded_at=ts)
        _insert_parse_error(dbs["ops"], ts_utc=ts)

        summary = bg.compute_llm_proposal_summary_bg(
            window_days=7,
            market_db_path=dbs["market"],
            ops_db_path=dbs["ops"],
        )

        assert summary["parse_error_count"] == 1
        assert summary["parse_error_rate_pct"] == pytest.approx(50.0)
        assert summary["parse_attempt_denominator"] == 2

    def test_empty_window_parse_error_rate_null(self, dbs) -> None:
        summary = bg.compute_llm_proposal_summary_bg(
            window_days=7,
            market_db_path=dbs["market"],
            ops_db_path=dbs["ops"],
        )

        assert summary["total_count"] == 0
        assert summary["parse_error_count"] == 0
        assert summary["parse_error_rate_pct"] is None
        assert summary["parse_attempt_denominator"] is None


class TestEnableIsolation:
    def test_disabled_job_returns_none(self, dbs, monkeypatch) -> None:
        monkeypatch.setenv("AI_PROPOSAL_SUMMARY_ENABLED", "0")

        result = bg.run_llm_proposal_summary_job(
            market_db_path=dbs["market"],
            ops_db_path=dbs["ops"],
        )

        assert result is None

        conn = sqlite3.connect(dbs["ops"])
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM ops_events WHERE event = ?",
                ("llm_proposal_summary_weekly",),
            ).fetchone()[0]
        finally:
            conn.close()
        assert int(n) == 0

    def test_scan_pipelines_exclude_llm_proposal_summary(self) -> None:
        from bitget.pipelines import bitget_pipelines as bp

        for fn_name in ("_pipeline_scan_spot", "_pipeline_scan_futures", "_pipeline_scan_all"):
            steps = getattr(bp, fn_name)()
            names = [s.name for s in steps]
            assert "llm_proposal_summary" not in names

    def test_weekly_evolution_includes_llm_proposal_summary_after_bad_tick(self) -> None:
        from bitget.pipelines import bitget_pipelines as bp

        steps = bp._pipeline_weekly_evolution()
        names = [s.name for s in steps]
        assert "bad_tick_skip_summary" in names
        assert "llm_proposal_summary" in names
        assert names.index("llm_proposal_summary") > names.index("bad_tick_skip_summary")
