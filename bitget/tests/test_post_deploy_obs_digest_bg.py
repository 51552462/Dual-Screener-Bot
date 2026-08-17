"""POST_DEPLOY_OBS daily telegram digest (read-only)."""
from __future__ import annotations

import json
import sqlite3

from bitget.infra import ops_logger
from bitget.observability import post_deploy_obs_digest_bg as bg


def _ensure_ops(db: str) -> None:
    conn = sqlite3.connect(db)
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


def _ensure_fwd(db: str) -> None:
    conn = sqlite3.connect(db)
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
        conn.execute(
            "INSERT INTO bitget_forward_trades (entry_date, market_type, symbol, status) "
            "VALUES (?,?,?,?)",
            ("2026-08-17T00:00:00+00:00", "futures", "BTC_USDT", "OPEN"),
        )
        conn.commit()
    finally:
        conn.close()


class TestPostDeployObsDigest:
    def test_compute_and_pastes(self, tmp_path, monkeypatch):
        fwd = str(tmp_path / "m.sqlite")
        _ensure_fwd(fwd)
        monkeypatch.setattr(
            "bitget.observability.gmm_dna_alpha_report_bg._dna_rank_and_shape",
            lambda: (
                {"RANK1": True, "RANK2": False, "RANK3": False},
                {"prototype_ohlcv": 1},
            ),
        )
        snap = bg.compute_post_deploy_obs_digest(
            window_days=2,
            forward_db_path=fwd,
            log_text="Cos_eff=0.500 OK\nCos_eff=0.000 x\n",
            include_server_probes=False,
        )
        assert snap["checks"]["forward_book"]["open_total"] == 1
        assert snap["checks"]["cos_eff"]["sample_count"] == 2
        assert snap["checks"]["dna_rank"]["ok"] is True
        assert "---CURSOR---" in bg.format_cursor_paste(snap)
        assert "Claude Pro" in bg.format_claude_paste(snap)
        assert "C-2" in bg.format_cursor_paste(snap)

    def test_disabled(self, monkeypatch):
        monkeypatch.setenv("POST_DEPLOY_OBS_DIGEST_ENABLED", "0")
        assert bg.run_post_deploy_obs_digest_job(dry_run=True) is None

    def test_job_persist_dry_run(self, tmp_path, monkeypatch):
        fwd = str(tmp_path / "m.sqlite")
        ops = str(tmp_path / "ops.sqlite")
        _ensure_fwd(fwd)
        _ensure_ops(ops)
        monkeypatch.setenv("POST_DEPLOY_OBS_DIGEST_ENABLED", "1")
        monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops)
        monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
        monkeypatch.setattr(
            "bitget.observability.gmm_dna_alpha_report_bg._dna_rank_and_shape",
            lambda: ({"RANK1": False, "RANK2": False, "RANK3": False}, {}),
        )
        out = bg.run_post_deploy_obs_digest_job(
            forward_db_path=fwd,
            log_text="Cos_eff=0.000\n",
            dry_run=True,
            persist=True,
            include_server_probes=False,
        )
        assert out is not None
        assert out["persisted"] is True
        conn = sqlite3.connect(ops)
        try:
            row = conn.execute(
                "SELECT event, payload_json FROM ops_events WHERE event=?",
                ("post_deploy_obs_digest_daily",),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        payload = json.loads(row[1])
        assert payload["digest_id"] == "BITGET_POST_DEPLOY_OBS_DAILY"
