"""D-3b — paper vs real parity monitor scaffold (no pipeline wiring)."""
from __future__ import annotations

import inspect
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from bitget.observability import parity_monitor_bg as pm


def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


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
                sim_kelly_invest REAL,
                final_ret REAL
            );
            CREATE TABLE IF NOT EXISTS bitget_real_execution (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                market_type TEXT,
                symbol TEXT,
                virtual_trade_id INTEGER,
                realized_pnl_usdt REAL,
                realized_ret_pct REAL,
                exec_ok INTEGER
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


class TestParityComputeSynthetic:
    def test_matched_diff_calculation(self, tmp_path):
        db_path = str(tmp_path / "market.sqlite")
        _ensure_forward_schema(db_path)
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=1))

        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                INSERT INTO bitget_forward_trades (
                    entry_date, exit_date, market_type, symbol, status,
                    sim_kelly_invest, final_ret
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (ts, ts, "futures", "BTC_USDT", "CLOSED_WIN", 1000.0, 5.0),
            )
            conn.execute(
                """
                INSERT INTO bitget_real_execution (
                    created_at, market_type, symbol, virtual_trade_id,
                    realized_pnl_usdt, realized_ret_pct, exec_ok
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (ts, "futures", "BTC_USDT", 1, 45.0, 4.5, 1),
            )
            conn.commit()
        finally:
            conn.close()

        out = pm.compute_paper_vs_real_parity_bg(window_days=7, forward_db_path=db_path)

        assert out["matched_count"] == 1
        assert out["total_paper_pnl_usd"] == pytest.approx(50.0)
        assert out["total_real_pnl_usd"] == pytest.approx(45.0)
        assert out["total_parity_diff_usd"] == pytest.approx(-5.0)
        assert out["matches"][0]["diff_usd"] == pytest.approx(-5.0)

    def test_unmatched_counts(self, tmp_path):
        db_path = str(tmp_path / "market.sqlite")
        _ensure_forward_schema(db_path)
        now = datetime.now(timezone.utc)
        ts = _utc_iso(now - timedelta(hours=1))

        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                INSERT INTO bitget_forward_trades (
                    entry_date, exit_date, market_type, symbol, status,
                    sim_kelly_invest, final_ret
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (ts, ts, "spot", "ETH_USDT", "CLOSED_LOSS", 500.0, -2.0),
            )
            conn.execute(
                """
                INSERT INTO bitget_real_execution (
                    created_at, market_type, symbol, virtual_trade_id,
                    realized_pnl_usdt, realized_ret_pct, exec_ok
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (ts, "futures", "SOL_USDT", 99, 10.0, 1.0, 1),
            )
            conn.commit()
        finally:
            conn.close()

        out = pm.compute_paper_vs_real_parity_bg(window_days=7, forward_db_path=db_path)

        assert out["matched_count"] == 0
        assert out["unmatched_paper_count"] == 1
        assert out["unmatched_real_count"] == 1


class TestParityScaffoldIsolation:
    def test_parity_monitor_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("PARITY_MONITOR_ENABLED", raising=False)
        assert pm.parity_monitor_enabled() is False

    def test_no_pipeline_hook_for_parity(self):
        from bitget.pipelines import bitget_pipelines as bp

        pipeline_builders = [
            bp._pipeline_weekly_evolution,
            bp._pipeline_scan_spot,
            bp._pipeline_scan_futures,
            bp._pipeline_scan_all,
            bp._pipeline_daily_audit,
            bp._pipeline_reconcile,
        ]
        for builder in pipeline_builders:
            steps = builder()
            names = [s.name for s in steps]
            assert "parity_monitor" not in names
            assert "paper_vs_real_parity" not in names

    def test_bitget_pipelines_source_has_no_parity_import(self):
        from bitget.pipelines import bitget_pipelines as bp

        src = inspect.getsource(bp)
        assert "parity_monitor_bg" not in src
        assert "compute_paper_vs_real_parity_bg" not in src
        assert "run_parity" not in src

    def test_compute_reports_enabled_flag_without_wiring(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PARITY_MONITOR_ENABLED", "0")
        db_path = str(tmp_path / "market.sqlite")
        _ensure_forward_schema(db_path)

        out = pm.compute_paper_vs_real_parity_bg(window_days=7, forward_db_path=db_path)

        assert out["parity_monitor_enabled"] is False
        assert out["matched_count"] == 0
