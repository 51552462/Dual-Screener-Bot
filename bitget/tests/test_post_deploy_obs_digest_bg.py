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
                status TEXT,
                timeframe TEXT,
                mfe REAL,
                dyn_cpv REAL,
                dyn_tb REAL,
                v_energy REAL,
                dyn_rs REAL,
                v_rs REAL
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


def _diag_stub(**overrides):
    base = {
        "state": "DATA_WAIT_LOW_MFE",
        "cursor_action": "OBSERVE_HOLD",
        "plain": bg._DNA_STATE_PLAIN["DATA_WAIT_LOW_MFE"],
        "checked_at": "2026-08-20 00:00 KST",
        "n_closed_by_tf": {"4H": 7, "1H": 2, "2H": 1},
        "n_mfe8_by_tf": {"4H": 0, "1H": 0, "2H": 0},
        "templates_present": False,
        "gmm_cluster_n": 0,
        "last_error": None,
        "gmm_min_rows": 12,
    }
    base.update(overrides)
    return base


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
        monkeypatch.setattr(
            bg,
            "collect_dna_diagnosis",
            lambda **_k: _diag_stub(
                state="RANK_OK",
                cursor_action="NONE",
                plain=bg._DNA_STATE_PLAIN["RANK_OK"],
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
        assert snap["checks"]["dna_rank"]["diagnosis"]["state"] == "RANK_OK"
        assert "dashboard" in snap
        assert snap["dashboard"]["progress_pct"] >= 0
        assert "---CURSOR---" in bg.format_cursor_paste(snap)
        html = bg.format_digest_html(snap)
        assert "코인 연습 · 오늘 한눈에" in html
        assert "잘 되고 있어요" in html
        assert "나중이에요" in html
        assert "DNA 다 컸어요" in html
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
        monkeypatch.setattr(
            bg,
            "collect_dna_diagnosis",
            lambda **_k: _diag_stub(),
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
        assert "dashboard" in payload
        assert "problem" in payload["dashboard"] or "working" in payload["dashboard"]
        assert payload["checks"]["dna_rank"]["diagnosis"]["state"] == "DATA_WAIT_LOW_MFE"


class TestDnaDiagnosisUx01:
    def test_rank_ok(self):
        cfg = {
            "CRYPTO_DNA_ALPHA_RANK1": {"shape": [0.1] * 20},
            "CRYPTO_DNA_ALPHA_RANK2": {"shape": [0.1] * 20},
            "CRYPTO_DNA_ALPHA_RANK3": {"shape": [0.1] * 20},
            "BITGET_GMM_DNA_TEMPLATES": {},
        }
        out = bg.diagnose_dna_state(cfg, {"4H": 1}, {"4H": 0}, 12)
        assert out["state"] == "RANK_OK"
        assert out["cursor_action"] == "NONE"

    def test_data_wait_2026_08_19(self):
        closed = {"1H": 2, "2H": 1, "4H": 7}
        mfe = {"1H": 0, "2H": 0, "4H": 0}
        out = bg.diagnose_dna_state({}, closed, mfe, 12, rank_all_present=False)
        assert out["state"] == "DATA_WAIT_LOW_MFE"
        assert out["cursor_action"] == "OBSERVE_HOLD"
        assert "덜 모였어요" in out["plain"]

    def test_gmm_empty(self):
        mfe = {"4H": 12, "1H": 0}
        out = bg.diagnose_dna_state(
            {"BITGET_GMM_DNA_TEMPLATES": {}},
            {"4H": 20},
            mfe,
            12,
            rank_all_present=False,
        )
        assert out["state"] == "GMM_EMPTY"
        assert out["cursor_action"] == "DIRECTOR_SSH_CHECK"

    def test_sync_fail(self):
        gmm = {
            "TF_4H": {
                "templates": {
                    "GMM_CLUSTER_1": {"mean_mfe": 9.0, "sample_size": 20},
                }
            }
        }
        out = bg.diagnose_dna_state(
            {"BITGET_GMM_DNA_TEMPLATES": gmm},
            {"4H": 20},
            {"4H": 15},
            12,
            rank_all_present=False,
        )
        assert out["state"] == "SYNC_FAIL"
        assert out["cursor_action"] == "REPORT_TO_CLAUDE"
        assert out["templates_present"] is True
        assert out["gmm_cluster_n"] == 1

    def test_db_path_or_env(self):
        out = bg.diagnose_dna_state({}, {}, {}, 12, db_ok=False, last_error="db_missing")
        assert out["state"] == "DB_PATH_OR_ENV"
        assert out["cursor_action"] == "DIRECTOR_SSH_CHECK"

    def test_dashboard_data_wait_is_missing_not_problem(self, monkeypatch):
        snap = {
            "checks": {
                "forward_book": {"ok": True, "closed_total": 10, "open_total": 0},
                "cos_eff": {"ok": False, "warn": True, "sample_count": 0},
                "dna_rank": {
                    "ok": False,
                    "diagnosis": _diag_stub(),
                },
            },
            "server_ops": {},
        }
        monkeypatch.setattr(bg, "_count_recent_ops_event", lambda *a, **k: 0)
        dash = bg.build_kid_dashboard(snap)
        dna_miss = [x for x in dash["missing"] if x["id"] == "dna"]
        dna_prob = [x for x in dash["problem"] if x["id"] == "dna"]
        assert dna_miss and not dna_prob
        assert "덜 모였어요" in dna_miss[0]["plain"]

    def test_kill_switch_legacy_wording(self, monkeypatch):
        monkeypatch.setenv("POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED", "0")
        snap = {
            "checks": {
                "forward_book": {"ok": True, "closed_total": 1, "open_total": 0},
                "cos_eff": {"ok": True, "sample_count": 2},
                "dna_rank": {"ok": False, "diagnosis": _diag_stub()},
            },
            "server_ops": {},
        }
        monkeypatch.setattr(bg, "_count_recent_ops_event", lambda *a, **k: 1)
        dash = bg.build_kid_dashboard(snap)
        dna_prob = [x for x in dash["problem"] if x["id"] == "dna"]
        assert dna_prob
        assert "RANK1~3이 비어" in dna_prob[0]["plain"]
