"""D-3a — weekly cost report (read-only)."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from bitget.infra import ops_logger
from bitget.observability import cost_report_bg as bg


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


def _ensure_forward_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bitget_forward_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_date TEXT,
                exit_date TEXT,
                market_type TEXT,
                symbol TEXT,
                status TEXT,
                entry_price REAL,
                quantity REAL,
                leverage REAL,
                sim_kelly_invest REAL,
                final_ret REAL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _insert_forward_trade(
    db_path: str,
    *,
    entry_date: str,
    exit_date: str,
    quantity: float,
    entry_price: float,
    leverage: float = 1.0,
    sim_kelly_invest: float = 0.0,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO bitget_forward_trades (
                entry_date, exit_date, market_type, symbol, status,
                entry_price, quantity, leverage, sim_kelly_invest, final_ret
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                entry_date,
                exit_date,
                "futures",
                "BTC_USDT",
                "CLOSED_WIN",
                entry_price,
                quantity,
                leverage,
                sim_kelly_invest,
                5.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_llm_cache_schema(cache_path: str) -> None:
    conn = sqlite3.connect(cache_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_cache (
                cache_key TEXT PRIMARY KEY,
                response_text TEXT,
                created_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


class TestCostReportAggregation:
    def test_synthetic_aggregation(self, tmp_path, monkeypatch):
        forward_db = str(tmp_path / "market.sqlite")
        ops_db = str(tmp_path / "ops.sqlite")
        cache_db = str(tmp_path / "llm_cache.sqlite")
        _ensure_forward_schema(forward_db)
        _ensure_ops_schema(ops_db)
        _ensure_llm_cache_schema(cache_db)

        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=2))
        _insert_forward_trade(
            forward_db,
            entry_date=ts,
            exit_date=ts,
            quantity=2.0,
            entry_price=100.0,
            leverage=3.0,
        )

        conn = sqlite3.connect(cache_db)
        try:
            conn.execute(
                "INSERT INTO llm_cache (cache_key, response_text, created_at) VALUES (?,?,?)",
                ("k1", "resp", ts[:19]),
            )
            conn.execute(
                "INSERT INTO llm_cache (cache_key, response_text, created_at) VALUES (?,?,?)",
                ("k2", "resp2", ts[:19]),
            )
            conn.commit()
        finally:
            conn.close()

        monkeypatch.setattr(bg, "_llm_cache_db_path", lambda: cache_db)

        summary = bg.compute_weekly_cost_report_bg(
            window_days=7,
            forward_db_path=forward_db,
            ops_db_path=ops_db,
            cache_db_path=cache_db,
        )

        assert summary["gemini_call_count"] == 2
        assert summary["gemini_call_count_source"] == "llm_call_cache_proxy"
        assert summary["paper_notional_traded_usd"] == pytest.approx(600.0)
        assert summary["gemini_cost_estimate_usd"] is None
        assert summary["cost_basis"] == "no_usd_unit_rate"
        assert summary["exchange_fee_estimate_usd"] is None
        assert summary["fee_basis"] == "no_fee_rate_ssot"
        assert summary["llm_token_metering_available"] is False

    def test_null_cost_fee_when_no_basis(self, tmp_path):
        forward_db = str(tmp_path / "market.sqlite")
        ops_db = str(tmp_path / "ops.sqlite")
        _ensure_forward_schema(forward_db)
        _ensure_ops_schema(ops_db)

        summary = bg.compute_weekly_cost_report_bg(
            window_days=7,
            forward_db_path=forward_db,
            ops_db_path=ops_db,
            cache_db_path=str(tmp_path / "missing.sqlite"),
        )

        assert summary["gemini_call_count"] == 0
        assert summary["gemini_call_count_source"] == "none"
        assert summary["gemini_cost_estimate_usd"] is None
        assert summary["cost_basis"] == "no_usd_unit_rate"
        assert summary["exchange_fee_estimate_usd"] is None
        assert summary["fee_basis"] == "no_fee_rate_ssot"


class TestCostReportJob:
    def test_disabled_returns_none(self, tmp_path, monkeypatch):
        ops_db = str(tmp_path / "ops.sqlite")
        _ensure_ops_schema(ops_db)
        monkeypatch.setenv("COST_REPORT_ENABLED", "0")

        result = bg.run_cost_report_job(ops_db_path=ops_db)

        assert result is None
        conn = sqlite3.connect(ops_db)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM ops_events WHERE event = ?",
                ("cost_report_weekly",),
            ).fetchone()[0]
        finally:
            conn.close()
        assert int(n) == 0

    def test_job_persists_ops_event(self, tmp_path, monkeypatch):
        forward_db = str(tmp_path / "market.sqlite")
        ops_db = str(tmp_path / "ops.sqlite")
        _ensure_forward_schema(forward_db)
        _ensure_ops_schema(ops_db)
        monkeypatch.setenv("COST_REPORT_ENABLED", "1")
        monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
        monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))

        result = bg.run_cost_report_job(
            forward_db_path=forward_db,
            ops_db_path=ops_db,
            cache_db_path=str(tmp_path / "missing.sqlite"),
        )

        assert result is not None
        assert result["inserted"] is True
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
                ("cost_report_weekly",),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        assert row[0] == "observability.cost"
        payload = json.loads(row[2])
        assert payload["fee_basis"] == "no_fee_rate_ssot"


class TestWeeklyEvolutionHookOrder:
    def test_cost_report_after_llm_proposal_summary(self):
        from bitget.pipelines import bitget_pipelines as bp

        steps = bp._pipeline_weekly_evolution()
        names = [s.name for s in steps]
        assert "llm_proposal_summary" in names
        assert "cost_report" in names
        assert names.index("cost_report") == names.index("llm_proposal_summary") + 1

    def test_scan_pipelines_exclude_cost_report(self):
        from bitget.pipelines import bitget_pipelines as bp

        for fn_name in ("_pipeline_scan_spot", "_pipeline_scan_futures", "_pipeline_scan_all"):
            steps = getattr(bp, fn_name)()
            names = [s.name for s in steps]
            assert "cost_report" not in names
