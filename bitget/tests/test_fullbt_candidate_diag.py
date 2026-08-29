"""FULL-BT-FUT-DIAG-1 — candidate reject ops_events tag (read-only)."""
from __future__ import annotations

import json
import sqlite3

from bitget.infra import ops_logger
from bitget.observability import fullbt_candidate_diag_bg as diag


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


def test_tag_reject_writes_ops_event(tmp_path, monkeypatch):
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    monkeypatch.setenv("FULLBT_CANDIDATE_DIAG_ENABLED", "true")

    diag.tag_candidate_reject_reason(
        "pilot-test",
        "BTCUSDT",
        "futures",
        (False, "중복 보유 중"),
    )

    conn = sqlite3.connect(ops_db)
    try:
        row = conn.execute(
            "SELECT component, event, payload_json FROM ops_events WHERE event=?",
            ("fullbt_candidate_reject",),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "observability.fullbt_candidate_diag"
    payload = json.loads(row[2])
    assert payload["run_id"] == "pilot-test"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["market_type"] == "futures"
    assert payload["ok"] is False
    assert payload["reject_msg"] == "중복 보유 중"


def test_tag_accept_is_noop(tmp_path, monkeypatch):
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    monkeypatch.setenv("FULLBT_CANDIDATE_DIAG_ENABLED", "true")

    diag.tag_candidate_reject_reason(
        "pilot-test", "ETHUSDT", "futures", (True, "ok")
    )

    conn = sqlite3.connect(ops_db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM ops_events").fetchone()[0]
    finally:
        conn.close()
    assert n == 0


def test_kill_switch_disables_tag(tmp_path, monkeypatch):
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    monkeypatch.setenv("FULLBT_CANDIDATE_DIAG_ENABLED", "false")

    diag.tag_candidate_reject_reason(
        "pilot-test", "SOLUSDT", "futures", (False, "시장 쿼터 초과")
    )

    conn = sqlite3.connect(ops_db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM ops_events").fetchone()[0]
    finally:
        conn.close()
    assert n == 0


def test_market_type_passed_through_not_hardcoded(tmp_path, monkeypatch):
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    monkeypatch.setenv("FULLBT_CANDIDATE_DIAG_ENABLED", "true")

    diag.tag_candidate_reject_reason(
        "r1", "X", "spot", (False, "현물(Spot) 시장 숏(Short) 진입 불가")
    )
    conn = sqlite3.connect(ops_db)
    try:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM ops_events WHERE event=?",
                ("fullbt_candidate_reject",),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    assert payload["market_type"] == "spot"


def test_retag_from_full_bt_diag(tmp_path, monkeypatch):
    ops_db = str(tmp_path / "ops.sqlite")
    full_db = str(tmp_path / "full.sqlite")
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", ops_db)
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))
    monkeypatch.setenv("FULLBT_CANDIDATE_DIAG_ENABLED", "true")

    conn = sqlite3.connect(full_db)
    try:
        conn.execute(
            """
            CREATE TABLE full_bt_diag (
                run_id TEXT, market_type TEXT, symbol TEXT, metric TEXT,
                engine_name TEXT, step INTEGER, count INTEGER, detail TEXT,
                updated_at TEXT, tf TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO full_bt_diag (
                run_id, market_type, symbol, metric, engine_name, step,
                count, detail, updated_at, tf
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "pilot-fut-20260829T062221Z",
                "futures",
                "BTCUSDT",
                "gate_reject",
                "eng",
                5,
                1,
                "중복 보유 중",
                "2026-08-29",
                "1D",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    n = diag.retag_rejects_from_full_bt_diag(
        run_id="pilot-fut-20260829T062221Z",
        db_path=full_db,
    )
    assert n == 1
    conn = sqlite3.connect(ops_db)
    try:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM ops_events WHERE event=?",
                ("fullbt_candidate_reject",),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    assert payload["reject_msg"] == "중복 보유 중"
    assert payload["market_type"] == "futures"
