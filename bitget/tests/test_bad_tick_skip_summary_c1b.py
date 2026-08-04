"""C-1b — weekly bad_tick_filtered skip summary (read-only)."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from bitget.infra import ops_logger
from bitget.observability import bad_tick_skip_summary_bg as bg


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


def _insert_bad_tick(
    db_path: str,
    *,
    symbol: str,
    market_type: str,
    reason: str,
    ts_utc: str,
    scanner: str = "supernova",
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO ops_events (ts_utc, component, severity, event, payload_json)
            VALUES (?,?,?,?,?)
            """,
            (
                ts_utc,
                f"scanner.{scanner}",
                "INFO",
                "bad_tick_filtered",
                json.dumps(
                    {
                        "symbol": symbol,
                        "market_type": market_type,
                        "scanner": scanner,
                        "reason": reason,
                        "action": "skip",
                    }
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_denominator_event(
    db_path: str,
    *,
    event: str,
    count: int,
    ts_utc: str,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO ops_events (ts_utc, component, severity, event, payload_json)
            VALUES (?,?,?,?,?)
            """,
            (
                ts_utc,
                "scanner.test",
                "INFO",
                event,
                json.dumps({"scan_count": count}),
            ),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def ops_db(tmp_path, monkeypatch):
    db = tmp_path / "ops.sqlite"
    _ensure_ops_schema(str(db))
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", str(db))
    monkeypatch.setattr(ops_logger, "OPS_HEALTH_DB_PATH", str(db))
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    return str(db)


class TestBadTickSkipSummaryAggregation:
    def test_synthetic_rows_grouped_correctly(self, ops_db):
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(days=1))
        _insert_bad_tick(ops_db, symbol="BTC_USDT", market_type="futures", reason="gap_spike", ts_utc=ts)
        _insert_bad_tick(ops_db, symbol="BTC_USDT", market_type="futures", reason="gap_spike", ts_utc=ts)
        _insert_bad_tick(ops_db, symbol="ETH_USDT", market_type="spot", reason="atr_spike", ts_utc=ts)

        summary = bg.compute_bad_tick_skip_summary_bg(window_days=7, ops_db_path=ops_db)

        assert summary.total_skips == 3
        assert summary.window_days == 7
        assert summary.skip_rate_pct is None
        assert summary.denominator_count is None
        assert summary.denominator_source is None
        assert {g["key"]: g["count"] for g in summary.by_symbol} == {
            "BTC_USDT": 2,
            "ETH_USDT": 1,
        }
        assert {g["key"]: g["count"] for g in summary.by_market_type} == {
            "futures": 2,
            "spot": 1,
        }
        assert {g["key"]: g["count"] for g in summary.by_reason} == {
            "gap_spike": 2,
            "atr_spike": 1,
        }

    def test_persist_writes_weekly_summary_event(self, ops_db):
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=2))
        _insert_bad_tick(ops_db, symbol="SOL_USDT", market_type="spot", reason="gap_spike", ts_utc=ts)

        result = bg.run_bad_tick_skip_summary_job(window_days=7, ops_db_path=ops_db)

        assert result is not None
        assert result["inserted"] is True
        assert result["summary"]["total_skips"] == 1

        conn = sqlite3.connect(ops_db)
        try:
            row = conn.execute(
                """
                SELECT component, event, payload_json
                FROM ops_events
                WHERE event = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                ("bad_tick_skip_summary_weekly",),
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == "observability.bad_tick"
        payload = json.loads(row[2])
        assert payload["total_skips"] == 1
        assert payload["skip_rate_pct"] is None


class TestDenominatorBranch:
    def test_no_denominator_skip_rate_null(self, ops_db):
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=1))
        _insert_bad_tick(ops_db, symbol="BTC_USDT", market_type="futures", reason="gap_spike", ts_utc=ts)

        summary = bg.compute_bad_tick_skip_summary_bg(window_days=7, ops_db_path=ops_db)

        assert summary.skip_rate_pct is None
        assert summary.denominator_source is None

    def test_scan_funnel_summary_denominator_computes_rate(self, ops_db):
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=1))
        _insert_bad_tick(ops_db, symbol="BTC_USDT", market_type="futures", reason="gap_spike", ts_utc=ts)
        _insert_bad_tick(ops_db, symbol="ETH_USDT", market_type="spot", reason="gap_spike", ts_utc=ts)
        _insert_denominator_event(
            ops_db,
            event="scan_funnel_summary",
            count=1000,
            ts_utc=ts,
        )

        summary = bg.compute_bad_tick_skip_summary_bg(window_days=7, ops_db_path=ops_db)

        assert summary.denominator_count == 1000
        assert summary.denominator_source == "scan_funnel_summary"
        assert summary.skip_rate_pct == pytest.approx(0.2)


class TestEnableIsolation:
    def test_disabled_job_returns_none(self, ops_db, monkeypatch):
        monkeypatch.setenv("BAD_TICK_SKIP_SUMMARY_ENABLED", "0")

        result = bg.run_bad_tick_skip_summary_job(ops_db_path=ops_db)

        assert result is None
        conn = sqlite3.connect(ops_db)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM ops_events WHERE event = ?",
                ("bad_tick_skip_summary_weekly",),
            ).fetchone()[0]
        finally:
            conn.close()
        assert int(n) == 0

    def test_scan_pipelines_exclude_bad_tick_skip_summary(self):
        from bitget.pipelines import bitget_pipelines as bp

        for fn_name in ("_pipeline_scan_spot", "_pipeline_scan_futures", "_pipeline_scan_all"):
            steps = getattr(bp, fn_name)()
            names = [s.name for s in steps]
            assert "bad_tick_skip_summary" not in names

    def test_weekly_evolution_includes_bad_tick_skip_summary_after_walk_forward(self):
        from bitget.pipelines import bitget_pipelines as bp

        steps = bp._pipeline_weekly_evolution()
        names = [s.name for s in steps]
        assert "walk_forward_shadow" in names
        assert "bad_tick_skip_summary" in names
        assert names.index("bad_tick_skip_summary") > names.index("walk_forward_shadow")

    def test_walk_forward_shadow_step_unchanged_when_summary_disabled(self, monkeypatch):
        monkeypatch.setenv("BAD_TICK_SKIP_SUMMARY_ENABLED", "0")
        from bitget.pipelines import bitget_pipelines as bp

        steps = bp._pipeline_weekly_evolution()
        wf = next(s for s in steps if s.name == "walk_forward_shadow")
        assert wf.critical is False
        assert wf.fn is bp._step_walk_forward_shadow
