"""I-GMM-DNA-01b — weekly GMM/DNA alpha report (read-only)."""
from __future__ import annotations

import json
import sqlite3

from bitget.infra import ops_logger
from bitget.observability import gmm_dna_alpha_report_bg as bg


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
                status TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _insert_trade(db_path: str, *, market_type: str, status: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO bitget_forward_trades (
                entry_date, exit_date, market_type, symbol, status
            ) VALUES (?,?,?,?,?)
            """,
            ("2026-08-10T00:00:00+00:00", None, market_type, "BTC_USDT", status),
        )
        conn.commit()
    finally:
        conn.close()


class TestCosEffParse:
    def test_parse_and_ratios(self, tmp_path, monkeypatch):
        forward_db = str(tmp_path / "market.sqlite")
        _ensure_forward_schema(forward_db)
        _insert_trade(forward_db, market_type="futures", status="OPEN")
        _insert_trade(forward_db, market_type="spot", status="CLOSED_WIN")
        _insert_trade(forward_db, market_type="futures", status="CLOSED_LOSS")

        monkeypatch.setattr(
            bg,
            "_dna_rank_and_shape",
            lambda: (
                {"RANK1": True, "RANK2": True, "RANK3": False},
                {"prototype_ohlcv": 1, "neutral_fallback": 1},
            ),
        )

        log_text = (
            "시계열 게이트 Cos_eff=0.000 < elastic\n"
            "Cos_eff=0.612 OK\n"
            "Cos_eff=0.400 OK\n"
        )
        summary = bg.compute_weekly_gmm_dna_alpha_report_bg(
            window_days=7,
            forward_db_path=forward_db,
            log_text=log_text,
        )

        assert summary["cos_eff_sample_count"] == 3
        assert summary["cos_eff_zero_ratio"] == 0.333333
        assert summary["cos_eff_mean_nonzero"] == 0.506
        assert summary["open_count_by_market"] == {"FUT": 1}
        assert summary["closed_count_by_market"] == {"SPOT": 1, "FUT": 1}
        assert summary["dna_rank_keys_present"]["RANK1"] is True
        assert summary["shape_source_distribution"]["neutral_fallback"] == 1
        assert summary["log_source_used"] == "file"

    def test_unavailable_nulls_when_no_logs(self, tmp_path, monkeypatch):
        forward_db = str(tmp_path / "market.sqlite")
        _ensure_forward_schema(forward_db)
        monkeypatch.setattr(bg, "_read_journal_text", lambda *_a, **_k: None)
        monkeypatch.setattr(bg, "_read_file_log_text", lambda *_a, **_k: None)
        monkeypatch.setattr(
            bg, "_dna_rank_and_shape", lambda: ({"RANK1": False, "RANK2": False, "RANK3": False}, {})
        )

        summary = bg.compute_weekly_gmm_dna_alpha_report_bg(
            window_days=7,
            forward_db_path=forward_db,
        )

        assert summary["cos_eff_sample_count"] is None
        assert summary["cos_eff_zero_ratio"] is None
        assert summary["cos_eff_mean_nonzero"] is None
        assert summary["log_source_used"] == "unavailable"


class TestGmmDnaReportJob:
    def test_disabled_returns_none(self, tmp_path, monkeypatch):
        ops_db = str(tmp_path / "ops.sqlite")
        _ensure_ops_schema(ops_db)
        monkeypatch.setenv("GMM_DNA_ALPHA_REPORT_ENABLED", "0")

        result = bg.run_gmm_dna_alpha_report_job(ops_db_path=ops_db, log_text="")

        assert result is None
        conn = sqlite3.connect(ops_db)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM ops_events WHERE event = ?",
                ("gmm_dna_alpha_report_weekly",),
            ).fetchone()[0]
        finally:
            conn.close()
        assert int(n) == 0

    def test_job_persists_ops_event(self, tmp_path, monkeypatch):
        forward_db = str(tmp_path / "market.sqlite")
        ops_db = str(tmp_path / "ops.sqlite")
        _ensure_forward_schema(forward_db)
        _ensure_ops_schema(ops_db)
        monkeypatch.setenv("GMM_DNA_ALPHA_REPORT_ENABLED", "1")
        monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
        monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
        monkeypatch.setattr(
            bg, "_dna_rank_and_shape", lambda: ({"RANK1": True, "RANK2": False, "RANK3": False}, {})
        )

        result = bg.run_gmm_dna_alpha_report_job(
            forward_db_path=forward_db,
            ops_db_path=ops_db,
            log_text="Cos_eff=0.100 OK\n",
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
                ("gmm_dna_alpha_report_weekly",),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        assert row[0] == "observability.dna"
        payload = json.loads(row[2])
        assert payload["cos_eff_sample_count"] == 1
        assert payload["dna_rank_keys_present"]["RANK1"] is True


class TestWeeklyEvolutionHookOrder:
    def test_gmm_dna_report_after_cost_report(self):
        from bitget.pipelines import bitget_pipelines as bp

        steps = bp._pipeline_weekly_evolution()
        names = [s.name for s in steps]
        assert "cost_report" in names
        assert "gmm_dna_alpha_report" in names
        assert names.index("gmm_dna_alpha_report") == names.index("cost_report") + 1

    def test_scan_pipelines_exclude_gmm_dna_report(self):
        from bitget.pipelines import bitget_pipelines as bp

        for fn_name in ("_pipeline_scan_spot", "_pipeline_scan_futures", "_pipeline_scan_all"):
            steps = getattr(bp, fn_name)()
            names = [s.name for s in steps]
            assert "gmm_dna_alpha_report" not in names
